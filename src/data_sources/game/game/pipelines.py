from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.config import team_name_mapper
from src.database_orm import GamesTable
from src.utils.data_source_utils import BasePipeline


class CoversHistoricScoresAndOddsPipeline(BasePipeline):
    ITEM_CLASS = GamesTable

    def process_item(self, item):
        """
        Process each scraped item - clean, organize, and verify.

        Args:
            item (dict): The scraped item.

        Returns:
            dict: The processed item.
        """
        self.scraped_items += 1
        try:
            # Convert the game_datetime to the desired YYYYMMDD format for creating game_id.
            # Handle both date-only and datetime formats
            game_datetime_str = item["game_datetime"]
            if " " in game_datetime_str:
                # Full datetime format: "2024-01-15 19:30:00"
                parsed_datetime = datetime.strptime(game_datetime_str, "%Y-%m-%d %H:%M:%S")
            else:
                # Date-only format: "2024-01-15"
                parsed_datetime = datetime.strptime(game_datetime_str, "%Y-%m-%d")

            game_date = parsed_datetime.strftime("%Y%m%d")
            item["game_datetime"] = parsed_datetime

            # Run the home_team and away_team through the team_name_mapper function.
            mapped_home_team = team_name_mapper(item["home_team"])
            mapped_away_team = team_name_mapper(item["away_team"])

            # Concatenate game_date, mapped_home_team, and mapped_away_team to create the game_id.
            game_id = f"{game_date}{mapped_home_team}{mapped_away_team}"

            # Assign the generated game_id and the mapped team names to the item.
            item["game_id"] = game_id
            item["home_team"] = mapped_home_team
            item["away_team"] = mapped_away_team

            # Append the item to the list of scraped data.
            self.nba_data.append(item)

        except Exception as e:
            self.errors["processing"].append([e, item])

        return item

    def process_dataset(self):
        seen_game_ids = set()
        unique_data = []

        for entry in self.nba_data:
            if entry["game_id"] not in seen_game_ids:
                seen_game_ids.add(entry["game_id"])
                unique_data.append(entry)

        self.nba_data = unique_data

    def save_to_database(self):
        """
        Save scraped data to database using upsert logic.

        Covers is the PRIMARY data source for game data. Uses SQLite INSERT ...
        ON CONFLICT DO UPDATE to handle concurrent writes safely.

        Fields updated on conflict:
        - game_datetime: Updated only if Covers has a time OR no existing time
          (preserves Odds API times for completed games where Covers only has date)
        - game_completed: Updated (detected from postgamebox class)
        - open_line: Primary data from Covers (historic opening lines)
        - home_score, away_score: Updated if not null

        Fields NOT touched (preserved from Odds API if available):
        - scores_last_update, odds_last_update: Tracking timestamps from Odds API
        """
        table = self.ITEM_CLASS.__table__

        # Remove duplicates based on 'game_id' from self.nba_data
        unique_data = {row["game_id"]: row for row in self.nba_data}
        rows = list(unique_data.values())

        with self.Session() as session:
            try:
                # Pre-fetch existing games to check their times
                game_ids = [row["game_id"] for row in rows]
                existing_games = {}
                if game_ids:
                    stmt = select(GamesTable.game_id, GamesTable.game_datetime).where(
                        GamesTable.game_id.in_(game_ids)
                    )
                    for game_id, game_datetime in session.execute(stmt):
                        existing_games[game_id] = game_datetime

                for row in rows:
                    # Update fields that Covers owns (primary source)
                    update_dict = {
                        "open_line": row["open_line"],
                        "game_completed": row.get("game_completed"),
                    }

                    # Smart datetime handling: preserve existing non-midnight times
                    # Covers only has times for pregame boxes (upcoming games)
                    # Odds API has times for all games, so preserve those for past games
                    new_datetime = row["game_datetime"]
                    existing_datetime = existing_games.get(row["game_id"])

                    if existing_datetime is not None:
                        # Check if existing has a real time (not midnight)
                        existing_has_time = (
                            existing_datetime.hour != 0 or existing_datetime.minute != 0
                        )
                        # Check if new data has a real time
                        new_has_time = new_datetime.hour != 0 or new_datetime.minute != 0

                        # Only update datetime if:
                        # 1. New data has a real time, OR
                        # 2. Existing data doesn't have a real time
                        if new_has_time or not existing_has_time:
                            update_dict["game_datetime"] = new_datetime
                        # Otherwise, preserve the existing time from Odds API
                    else:
                        # No existing game, use whatever we have
                        update_dict["game_datetime"] = new_datetime

                    # Only update scores if they have values (game completed)
                    if row.get("home_score") is not None:
                        update_dict["home_score"] = row["home_score"]
                    if row.get("away_score") is not None:
                        update_dict["away_score"] = row["away_score"]

                    stmt = sqlite_insert(table).values(row)
                    stmt = stmt.on_conflict_do_update(index_elements=["game_id"], set_=update_dict)
                    session.execute(stmt)

                session.commit()  # Commit all changes after processing all rows

                self.saved_items += len(rows)

            except Exception as e:
                print(f"  ERROR: Database save failed: {str(e)}")
                self.errors["saving"]["other"].append(e)
                raise e

        self.nba_data = []  # Empty the list after saving the data
