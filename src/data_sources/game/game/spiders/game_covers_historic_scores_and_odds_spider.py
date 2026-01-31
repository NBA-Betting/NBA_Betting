import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

import scrapy

from ..item_loaders import CoversHistoricScoresAndOddsItemLoader
from ..items import CoversHistoricScoresAndOddsItem
from src.utils.data_source_utils import BaseSpider
from src.utils.timezone import get_current_time


class GameCoversHistoricScoresAndOddsSpider(BaseSpider):
    name = "game_covers_historic_scores_and_odds_spider"
    pipeline_name = "CoversHistoricScoresAndOddsPipeline"
    project_section = "game"
    first_season_start_year = 1996

    custom_settings = BaseSpider.create_pipeline_settings(project_section, pipeline_name)

    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super().__init__(dates, save_data=save_data, view_data=view_data, *args, **kwargs)

    def setup_dates(self, dates_input):
        # Validate the input
        if not isinstance(dates_input, str):
            raise TypeError(f"dates_input should be str, but got {type(dates_input)}")

        if dates_input == "daily_update":
            # Get current time in configured timezone
            current_time = get_current_time()
            # Get today's date
            todays_date = current_time.strftime("%Y-%m-%d")
            # Calculate yesterday's date by subtracting a day
            yesterdays_date = (current_time - timedelta(days=1)).strftime("%Y-%m-%d")
            # Calculate tomorrow's date by adding a day
            tomorrows_date = (current_time + timedelta(days=1)).strftime("%Y-%m-%d")

            return [todays_date, yesterdays_date, tomorrows_date]

        # Check if input is a season or multiple seasons
        if "-" not in dates_input:
            seasons_list = [season.strip() for season in dates_input.split(",")]
            # Generate all dates in those seasons
            dates = []
            for season in seasons_list:
                dates.extend(self._generate_dates_for_season(int(season)))
            return dates

        # Assuming the remaining input would be individual dates
        dates_list = dates_input.split(",")
        for date_str in dates_list:
            try:
                # Validate each date
                datetime.strptime(date_str.strip(), "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"Invalid date format: {date_str}. Date format should be 'YYYY-MM-DD'"
                )

        return [date_str.strip() for date_str in dates_list]

    async def start(self):
        """Generate initial requests for each date (Scrapy 2.13+ async API)."""
        base_url = "https://www.covers.com/sports/NBA/matchups"
        params = {}

        for date_str in self.dates:
            params["selectedDate"] = date_str
            url = base_url + "?" + urlencode(params)
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Get the date from the URL for game_datetime
        from urllib.parse import parse_qs, urlparse

        parsed_url = urlparse(response.url)
        query_params = parse_qs(parsed_url.query)
        game_date = query_params.get("selectedDate", [None])[0]

        # Select game boxes using the new structure (article.gamebox)
        gameboxes = response.css("article.gamebox")

        for gamebox in gameboxes:
            loader = CoversHistoricScoresAndOddsItemLoader(
                item=CoversHistoricScoresAndOddsItem(), selector=gamebox
            )

            # Get team shortnames from data attributes
            home_team = gamebox.css("::attr(data-home-team-shortname)").get()
            away_team = gamebox.css("::attr(data-away-team-shortname)").get()

            # Determine game completion status from gamebox class
            # postgamebox = completed, pregamebox = not started, ingamebox = in progress
            gamebox_classes = gamebox.css("::attr(class)").get() or ""
            game_completed = "postgamebox" in gamebox_classes

            # Extract game time for pregame boxes (postgame boxes show scores instead)
            # Time is in gamebox-time area, format like "8:00 PM ET"
            game_time = None
            if not game_completed:
                time_area = gamebox.css("div.gamebox-time").get() or ""
                time_match = re.search(r"(\d{1,2}:\d{2}\s*(?:AM|PM))", time_area, re.IGNORECASE)
                if time_match:
                    game_time = time_match.group(1).strip().upper()

            # Get scores from the score elements
            # Away score has class 'team-score away', home has 'team-score home'
            away_score = gamebox.css("strong.team-score.away::text").get()
            home_score = gamebox.css("strong.team-score.home::text").get()

            # Get spread/line data
            # For completed games (postgamebox): trending-and-cover-by-container has format "DAL -3"
            # For upcoming games (pregamebox): team_odds_and_consensus div has the line
            open_line = None

            if game_completed:
                # Completed games: get from trending-and-cover-by-container
                spread_text = gamebox.css("div.trending-and-cover-by-container span::text").getall()
                for text in spread_text:
                    text = text.strip()
                    parts = text.split()
                    if len(parts) >= 2:
                        team_code = parts[0].upper()
                        try:
                            line_value = float(parts[1])
                            # Check if this is home team's line
                            if home_team and team_code == home_team.upper():
                                open_line = line_value
                                break
                            # If away team, convert to home team perspective
                            elif away_team and team_code == away_team.upper():
                                open_line = -line_value
                                break
                        except ValueError:
                            continue
            else:
                # Upcoming/in-progress games: get from team_odds_and_consensus
                # Structure: first team-consensus is away (+X), second is home (-X)
                # We want the home team's line (the second team-consensus span)
                consensus_spans = gamebox.css("div.team_odds_and_consensus span.team-consensus")
                if len(consensus_spans) >= 2:
                    # Get the home team's spread from the second span
                    home_consensus = consensus_spans[1]
                    # The spread text is directly in the span (not in the strong tag)
                    # Format: "-4  <strong...>58%</strong>" or just "-4"
                    spread_text = home_consensus.css("::text").getall()
                    for text in spread_text:
                        text = text.strip()
                        if text:
                            # Try to parse as a number (e.g., "-4", "+3.5")
                            try:
                                open_line = float(text)
                                break
                            except ValueError:
                                continue

            # Build game_datetime: combine date with time if available
            # Format: "2025-01-22" + "8:00 PM" -> "2025-01-22 20:00:00"
            game_datetime = game_date
            if game_time and game_date:
                try:
                    # Parse time like "8:00 PM" or "10:30 AM"
                    time_obj = datetime.strptime(game_time, "%I:%M %p")
                    game_datetime = f"{game_date} {time_obj.strftime('%H:%M:%S')}"
                except ValueError:
                    pass  # Keep date-only if time parsing fails

            # Add values to loader
            loader.add_value("game_datetime", game_datetime)
            loader.add_value("home_team", home_team.upper() if home_team else None)
            loader.add_value("away_team", away_team.upper() if away_team else None)
            loader.add_value("home_score", home_score)
            loader.add_value("away_score", away_score)
            loader.add_value("open_line", open_line)
            loader.add_value("game_completed", game_completed)

            yield loader.load_item()
