import os
import sys
from datetime import datetime

from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
sys.path.append(os.path.join(here, "../../.."))
sys.path.append(os.path.join(here, "../../../.."))

from config import team_name_mapper
from database_orm import GamesTable
from utils.data_source_utils import BasePipeline


class CoversHistoricScoresAndOddsPipeline(BasePipeline):
    ITEM_CLASS = GamesTable

    def process_item(self, item, spider):
        """
        This method is called for each item that is scraped. It cleans, organizes, and verifies
        the item before appending it to the list of scraped data.

        Args:
            item (dict): The scraped item.
            spider (scrapy.Spider): The spider that scraped the item.

        Returns:
            dict: The processed item.
        """
        self.scraped_items += 1
        try:
            # Convert the game_datetime to the desired YYYYMMDD format for creating game_id.
            game_date = datetime.strptime(
                item["game_datetime"], "%Y-%m-%d %H:%M:%S"
            ).strftime("%Y%m%d")

            item["game_datetime"] = datetime.strptime(
                item["game_datetime"], "%Y-%m-%d %H:%M:%S"
            )

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
        table = self.ITEM_CLASS.__table__

        # Remove duplicates based on 'game_id' from self.nba_data
        unique_data = {row["game_id"]: row for row in self.nba_data}
        rows = list(unique_data.values())

        with self.Session() as session:
            try:
                for row in rows:
                    update_dict = {
                        "game_datetime": row["game_datetime"],
                        "open_line": row["open_line"],
                        "home_score": row["home_score"],
                        "away_score": row["away_score"],
                    }

                    stmt = pg_insert(table).values(row)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["game_id"], set_=update_dict
                    )
                    session.execute(stmt)

                session.commit()  # Commit all changes after processing all rows

                self.saved_items += len(rows)

            except Exception as e:
                print(
                    f"Error: Unable to insert/update data into the RDS table. Details: {str(e)}"
                )
                self.errors["saving"]["other"].append(e)
                raise e

            finally:
                non_integrity_error_count = (
                    len(self.errors["saving"]["other"])
                    + len(self.errors["processing"])
                    + len(self.errors["find_season_information"])
                )

                print(f"Processed {len(rows)} rows successfully.")
                print(f"Total items processed: {self.scraped_items}")
                print(f"Total other errors: {non_integrity_error_count}")

        self.nba_data = []  # Empty the list after saving the data
