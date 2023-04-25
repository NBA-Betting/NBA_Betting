import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Join, MapCompose, TakeFirst

# IMPORT ITEMS HERE
from .items import InpredictableWPAItem

# ADD NEW ITEM LOADERS HERE


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
