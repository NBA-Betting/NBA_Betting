import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst
from scrapy.loader import ItemLoader

# IMPORT ITEMS HERE
from .items import FivethirtyeightPlayerItem, InpredictableWPAItem

# ADD NEW ITEM LOADERS HERE


class FivethirtyeightPlayerItemLoader(ItemLoader):
    default_item_class = FivethirtyeightPlayerItem
    default_output_processor = TakeFirst()

    priority_in = MapCompose(int)
    to_date_in = MapCompose()
    player_name_in = MapCompose(str.strip)
    player_id_in = MapCompose(str.strip)
    season_in = MapCompose(int)
    poss_in = MapCompose(int)
    mp_in = MapCompose(int)
    raptor_box_offense_in = MapCompose(float)
    raptor_box_defense_in = MapCompose(float)
    raptor_box_total_in = MapCompose(float)
    raptor_onoff_offense_in = MapCompose(float)
    raptor_onoff_defense_in = MapCompose(float)
    raptor_onoff_total_in = MapCompose(float)
    raptor_offense_in = MapCompose(float)
    raptor_defense_in = MapCompose(float)
    raptor_total_in = MapCompose(float)
    war_total_in = MapCompose(float)
    war_reg_season_in = MapCompose(float)
    war_playoffs_in = MapCompose(float)
    predator_offense_in = MapCompose(float)
    predator_defense_in = MapCompose(float)
    predator_total_in = MapCompose(float)
    pace_impact_in = MapCompose(float)


class InpredictableWPAItemLoader(ItemLoader):
    default_item_class = InpredictableWPAItem
    default_output_processor = TakeFirst()

    rnk_in = MapCompose(int)
    player_in = MapCompose(str.strip)
    pos_in = MapCompose(str.strip)
    gms_in = MapCompose(int)
    wpa_in = MapCompose(float)
    ewpa_in = MapCompose(float)
    clwpa_in = MapCompose(float)
    gbwpa_in = MapCompose(float)
    sh_in = MapCompose(float)
    to_in = MapCompose(float)
    ft_in = MapCompose(float)
    reb_in = MapCompose(float)
    ast_in = MapCompose(float)
    stl_in = MapCompose(float)
    blk_in = MapCompose(float)
    kwpa_in = MapCompose(float)
    to_date_in = MapCompose(str.strip)
