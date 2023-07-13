import os
import sys

from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader

from .items import (
    NbastatsGeneralAdvancedItem,
    NbastatsGeneralFourfactorsItem,
    NbastatsGeneralOpponentItem,
    NbastatsGeneralTraditionalItem,
)

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
sys.path.append(os.path.join(here, "../../../../.."))

from data_source_utils import convert_date_format_1, convert_season_to_long

from config import team_name_mapper


class NbastatsGeneralTraditionalItemLoader(ItemLoader):
    default_item_class = NbastatsGeneralTraditionalItem
    default_output_processor = TakeFirst()

    team_name_in = MapCompose(str.strip, team_name_mapper)
    to_date_in = MapCompose(str.strip, convert_date_format_1)
    season_in = MapCompose(str.strip, convert_season_to_long)
    season_type_in = MapCompose(str.strip)
    games_in = MapCompose(str.strip)
    gp_in = MapCompose(int)
    w_in = MapCompose(int)
    l_in = MapCompose(int)
    w_pct_in = MapCompose(float)
    min_in = MapCompose(float)
    fgm_in = MapCompose(float)
    fga_in = MapCompose(float)
    fg_pct_in = MapCompose(float)
    fg3m_in = MapCompose(float)
    fg3a_in = MapCompose(float)
    fg3_pct_in = MapCompose(float)
    ftm_in = MapCompose(float)
    fta_in = MapCompose(float)
    ft_pct_in = MapCompose(float)
    oreb_in = MapCompose(float)
    dreb_in = MapCompose(float)
    reb_in = MapCompose(float)
    ast_in = MapCompose(float)
    tov_in = MapCompose(float)
    stl_in = MapCompose(float)
    blk_in = MapCompose(float)
    blka_in = MapCompose(float)
    pf_in = MapCompose(float)
    pfd_in = MapCompose(float)
    pts_in = MapCompose(float)
    plus_minus_in = MapCompose(float)


class NbastatsGeneralAdvancedItemLoader(ItemLoader):
    default_item_class = NbastatsGeneralAdvancedItem
    default_output_processor = TakeFirst()

    team_name_in = MapCompose(str.strip, team_name_mapper)
    to_date_in = MapCompose(str.strip, convert_date_format_1)
    season_in = MapCompose(str.strip, convert_season_to_long)
    season_type_in = MapCompose(str.strip)
    games_in = MapCompose(str.strip)
    gp_in = MapCompose(int)
    w_in = MapCompose(int)
    l_in = MapCompose(int)
    w_pct_in = MapCompose(float)
    min_in = MapCompose(float)
    e_off_rating_in = MapCompose(float)
    off_rating_in = MapCompose(float)
    e_def_rating_in = MapCompose(float)
    def_rating_in = MapCompose(float)
    e_net_rating_in = MapCompose(float)
    net_rating_in = MapCompose(float)
    ast_pct_in = MapCompose(float)
    ast_to_in = MapCompose(float)
    ast_ratio_in = MapCompose(float)
    oreb_pct_in = MapCompose(float)
    dreb_pct_in = MapCompose(float)
    reb_pct_in = MapCompose(float)
    tm_tov_pct_in = MapCompose(float)
    efg_pct_in = MapCompose(float)
    ts_pct_in = MapCompose(float)
    e_pace_in = MapCompose(float)
    pace_in = MapCompose(float)
    pace_per40_in = MapCompose(float)
    poss_in = MapCompose(int)
    pie_in = MapCompose(float)


class NbastatsGeneralFourfactorsItemLoader(ItemLoader):
    default_item_class = NbastatsGeneralFourfactorsItem
    default_output_processor = TakeFirst()

    team_name_in = MapCompose(str.strip, team_name_mapper)
    to_date_in = MapCompose(str.strip, convert_date_format_1)
    season_in = MapCompose(str.strip, convert_season_to_long)
    season_type_in = MapCompose(str.strip)
    games_in = MapCompose(str.strip)
    gp_in = MapCompose(int)
    w_in = MapCompose(int)
    l_in = MapCompose(int)
    w_pct_in = MapCompose(float)
    min_in = MapCompose(float)
    efg_pct_in = MapCompose(float)
    fta_rate_in = MapCompose(float)
    tm_tov_pct_in = MapCompose(float)
    oreb_pct_in = MapCompose(float)
    opp_efg_pct_in = MapCompose(float)
    opp_fta_rate_in = MapCompose(float)
    opp_tov_pct_in = MapCompose(float)
    opp_oreb_pct_in = MapCompose(float)


class NbastatsGeneralOpponentItemLoader(ItemLoader):
    default_item_class = NbastatsGeneralOpponentItem
    default_output_processor = TakeFirst()

    team_name_in = MapCompose(str.strip, team_name_mapper)
    to_date_in = MapCompose(str.strip, convert_date_format_1)
    season_in = MapCompose(str.strip, convert_season_to_long)
    season_type_in = MapCompose(str.strip)
    games_in = MapCompose(str.strip)
    gp_in = MapCompose(int)
    w_in = MapCompose(int)
    l_in = MapCompose(int)
    w_pct_in = MapCompose(float)
    min_in = MapCompose(float)
    opp_fgm_in = MapCompose(float)
    opp_fga_in = MapCompose(float)
    opp_fg_pct_in = MapCompose(float)
    opp_fg3m_in = MapCompose(float)
    opp_fg3a_in = MapCompose(float)
    opp_fg3_pct_in = MapCompose(float)
    opp_ftm_in = MapCompose(float)
    opp_fta_in = MapCompose(float)
    opp_ft_pct_in = MapCompose(float)
    opp_oreb_in = MapCompose(float)
    opp_dreb_in = MapCompose(float)
    opp_reb_in = MapCompose(float)
    opp_ast_in = MapCompose(float)
    opp_tov_in = MapCompose(float)
    opp_stl_in = MapCompose(float)
    opp_blk_in = MapCompose(float)
    opp_blka_in = MapCompose(float)
    opp_pf_in = MapCompose(float)
    opp_pfd_in = MapCompose(float)
    opp_pts_in = MapCompose(float)
    plus_minus_in = MapCompose(float)
