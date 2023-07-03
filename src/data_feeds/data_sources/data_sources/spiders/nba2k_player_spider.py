import scrapy

from ..item_loaders import Nba2kPlayerItemLoader
from ..items import Nba2kPlayerItem
from .base_spider import BaseSpider


class Nba2kPlayerSpider(BaseSpider):
    name = "nba2k_player_spider"
    allowed_domains = ["www.2kratings.com"]
    custom_settings = {
        # "ITEM_PIPELINES": {"data_sources.pipelines.Nba2kPlayerPipeline": 300},
        "ZYTE_API_TRANSPARENT_MODE": True,
    }
    first_season = 0
    dates = None

    def __init__(self, save_data=False, view_data=True, *args, **kwargs):
        super().__init__(
            save_data=save_data,
            view_data=view_data,
            dates=self.dates,
            first_season=self.first_season,
            *args,
            **kwargs,
        )

    def start_requests(self):
        base_url = "https://www.2kratings.com/current-teams"

        def parse_team_list(response):
            a_elements = response.css(
                "table.table.table-sm.table-striped.nowrap.mb-0.table-bordered.table-hover tbody tr td:first-child a"
            )

            # Collect all the <a> tags as strings
            a_tags = [a_element.get() for a_element in a_elements]

            # Print the collected <a> tags
            print("------------------")
            print("a_tags: ", a_tags)
            print("------------------")

            for team_url in team_urls:
                yield scrapy.Request(base_url + team_url, callback=parse_players)

        def parse_players(response):
            player_urls = response.css("div.entries a::attr(href)").getall()
            for player_url in player_urls:
                yield scrapy.Request(player_url, callback=self.parse)

        # Start with the current teams page to get the list of teams
        yield scrapy.Request(base_url, callback=parse_team_list)

    def parse(self, response):
        loader = Nba2kPlayerItemLoader(item=Nba2kPlayerItem(), response=response)
        loader.add_value("access_date", None)  # Add logic to get the access_date
        loader.add_css("player_name", "h1.entry-title::text")
        loader.add_css(
            "bronze_badges",
            "div.badges-container span[data-original-title='Bronze Badges']::text",
        )
        loader.add_css(
            "silver_badges",
            "div.badges-container span[data-original-title='Silver Badges']::text",
        )
        loader.add_css(
            "gold_badges",
            "div.badges-container span[data-original-title='Gold Badges']::text",
        )
        loader.add_css(
            "hall_of_fame_badges",
            "div.badges-container span[data-original-title='Hall of Fame Badges']::text",
        )

        # Main categories
        loader.add_css(
            "outside_scoring",
            "div.card-title:contains('Outside Scoring') span.attribute-box::text",
        )
        loader.add_css(
            "inside_scoring",
            "div.card-title:contains('Inside Scoring') span.attribute-box::text",
        )
        loader.add_css(
            "athleticism",
            "div.card-title:contains('Athleticism') span.attribute-box::text",
        )
        loader.add_css(
            "playmaking",
            "div.card-title:contains('Playmaking') span.attribute-box::text",
        )
        loader.add_css(
            "defending", "div.card-title:contains('Defending') span.attribute-box::text"
        )
        loader.add_css(
            "rebounding",
            "div.card-title:contains('Rebounding') span.attribute-box::text",
        )
        loader.add_css(
            "intangibles",
            "div.card-title:contains('Intangibles') span.attribute-box::text",
        )
        loader.add_css(
            "potential", "div.card-title:contains('Potential') span.attribute-box::text"
        )
        loader.add_css(
            "total_attributes",
            "div.card-title:contains('Total Attributes') span.attribute-box::text",
        )

        # Subcategories
        # Outside Scoring
        loader.add_css(
            "close_shot", "li:contains('Close Shot') span.attribute-box::text"
        )
        loader.add_css(
            "mid_range_shot", "li:contains('Mid-Range Shot') span.attribute-box::text"
        )
        loader.add_css(
            "three_point_shot",
            "li:contains('Three-Point Shot') span.attribute-box::text",
        )
        loader.add_css(
            "free_throw", "li:contains('Free Throw') span.attribute-box::text"
        )
        loader.add_css("shot_iq", "li:contains('Shot IQ') span.attribute-box::text")
        loader.add_css(
            "offensive_consistency",
            "li:contains('Offensive Consistency') span.attribute-box::text",
        )

        # Athleticism
        loader.add_css("speed", "li:contains('Speed') span.attribute-box::text")
        loader.add_css(
            "acceleration", "li:contains('Acceleration') span.attribute-box::text"
        )
        loader.add_css("strength", "li:contains('Strength') span.attribute-box::text")
        loader.add_css("vertical", "li:contains('Vertical') span.attribute-box::text")
        loader.add_css("stamina", "li:contains('Stamina') span.attribute-box::text")
        loader.add_css("hustle", "li:contains('Hustle') span.attribute-box::text")
        loader.add_css(
            "overall_durability",
            "li:contains('Overall Durability') span.attribute-box::text",
        )

        # Inside Scoring
        loader.add_css("layup", "li:contains('Layup') span.attribute-box::text")
        loader.add_css(
            "standing_dunk", "li:contains('Standing Dunk') span.attribute-box::text"
        )
        loader.add_css(
            "driving_dunk", "li:contains('Driving Dunk') span.attribute-box::text"
        )
        loader.add_css("post_hook", "li:contains('Post Hook') span.attribute-box::text")
        loader.add_css("post_fade", "li:contains('Post Fade') span.attribute-box::text")
        loader.add_css(
            "post_control", "li:contains('Post Control') span.attribute-box::text"
        )
        loader.add_css("draw_foul", "li:contains('Draw Foul') span.attribute-box::text")
        loader.add_css("hands", "li:contains('Hands') span.attribute-box::text")

        # Playmaking
        loader.add_css(
            "pass_accuracy", "li:contains('Pass Accuracy') span.attribute-box::text"
        )
        loader.add_css(
            "ball_handle", "li:contains('Ball Handle') span.attribute-box::text"
        )
        loader.add_css(
            "speed_with_ball", "li:contains('Speed with Ball') span.attribute-box::text"
        )
        loader.add_css("pass_iq", "li:contains('Pass IQ') span.attribute-box::text")
        loader.add_css(
            "pass_vision", "li:contains('Pass Vision') span.attribute-box::text"
        )

        # Defending
        loader.add_css(
            "interior_defense",
            "li:contains('Interior Defense') span.attribute-box::text",
        )
        loader.add_css(
            "perimeter_defense",
            "li:contains('Perimeter Defense') span.attribute-box::text",
        )
        loader.add_css("steal", "li:contains('Steal') span.attribute-box::text")
        loader.add_css("block", "li:contains('Block') span.attribute-box::text")
        loader.add_css(
            "lateral_quickness",
            "li:contains('Lateral Quickness') span.attribute-box::text",
        )
        loader.add_css(
            "help_defense_iq", "li:contains('Help Defense IQ') span.attribute-box::text"
        )
        loader.add_css(
            "pass_perception", "li:contains('Pass Perception') span.attribute-box::text"
        )
        loader.add_css(
            "defensive_consistency",
            "li:contains('Defensive Consistency') span.attribute-box::text",
        )

        # Rebounding
        loader.add_css(
            "offensive_rebound",
            "li:contains('Offensive Rebound') span.attribute-box::text",
        )
        loader.add_css(
            "defensive_rebound",
            "li:contains('Defensive Rebound') span.attribute-box::text",
        )

        yield loader.load_item()
