import os
import sys

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
sys.path.append(os.path.join(here, "../../.."))

from database_orm import (
    NbastatsGeneralAdvancedTable,
    NbastatsGeneralFourfactorsTable,
    NbastatsGeneralOpponentTable,
    NbastatsGeneralTraditionalTable,
)
from utils.data_source_utils import BasePipeline


class NbastatsGeneralTraditionalPipeline(BasePipeline):
    ITEM_CLASS = NbastatsGeneralTraditionalTable


class NbastatsGeneralAdvancedPipeline(BasePipeline):
    ITEM_CLASS = NbastatsGeneralAdvancedTable


class NbastatsGeneralFourfactorsPipeline(BasePipeline):
    ITEM_CLASS = NbastatsGeneralFourfactorsTable


class NbastatsGeneralOpponentPipeline(BasePipeline):
    ITEM_CLASS = NbastatsGeneralOpponentTable
