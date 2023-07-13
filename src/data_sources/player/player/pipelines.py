import os
import sys

import pandas as pd

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
sys.path.append(os.path.join(here, "../../.."))

from data_source_utils import BasePipeline

from database_orm import (
    FivethirtyeightPlayerTable,
    NbaStatsBoxscoresAdvAdvancedTable,
    NbaStatsBoxscoresAdvMiscTable,
    NbaStatsBoxscoresAdvScoringTable,
    NbaStatsBoxscoresAdvTraditionalTable,
    NbaStatsBoxscoresAdvUsageTable,
    NbaStatsBoxscoresTraditionalTable,
    NbaStatsPlayerGeneralAdvancedTable,
    NbaStatsPlayerGeneralDefenseTable,
    NbaStatsPlayerGeneralMiscTable,
    NbaStatsPlayerGeneralOpponentTable,
    NbaStatsPlayerGeneralScoringTable,
    NbaStatsPlayerGeneralTraditionalTable,
    NbaStatsPlayerGeneralUsageTable,
)


class FivethirtyeightPlayerPipeline(BasePipeline):
    ITEM_CLASS = FivethirtyeightPlayerTable

    def process_dataset(self):
        # Remove duplicates and update records
        df = pd.DataFrame(self.nba_data)
        df.sort_values(
            by=["player_id", "season", "priority"], ascending=False, inplace=True
        )
        df.drop_duplicates(subset=["player_id", "season"], keep="first", inplace=True)

        # Remove the "priority" column as it's not needed anymore
        df.drop(columns=["priority"], inplace=True)

        # Convert the DataFrame back to a list of dictionaries
        self.nba_data = df.to_dict("records")


class NbaStatsPlayerGeneralTraditionalPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralTraditionalTable


class NbaStatsPlayerGeneralAdvancedPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralAdvancedTable


class NbaStatsPlayerGeneralMiscPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralMiscTable


class NbaStatsPlayerGeneralUsagePipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralUsageTable


class NbaStatsPlayerGeneralScoringPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralScoringTable


class NbaStatsPlayerGeneralOpponentPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralOpponentTable


class NbaStatsPlayerGeneralDefensePipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralDefenseTable


class NbaStatsBoxscoresTraditionalPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresTraditionalTable


class NbaStatsBoxscoresAdvTraditionalPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvTraditionalTable


class NbaStatsBoxscoresAdvAdvancedPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvAdvancedTable


class NbaStatsBoxscoresAdvMiscPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvMiscTable


class NbaStatsBoxscoresAdvScoringPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvScoringTable


class NbaStatsBoxscoresAdvUsagePipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvUsageTable
