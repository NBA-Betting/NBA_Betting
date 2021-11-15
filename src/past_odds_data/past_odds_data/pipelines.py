from datetime import datetime
from scrapy.exceptions import DropItem


class PastOddsPipeline:

    team_abbr = {
        "BK": "BKN",
        "CHAR": "CHA",
        "GS": "GSW",
        "NETS": "BKN",
        "NJ": "BKN",
        "NO": "NOP",
        "NY": "NYK",
        "PHO": "PHX",
        "SA": "SAS",
    }

    def process_item(self, item, spider):
        if item["home"]:
            if item["opponent"].upper() in self.team_abbr:
                item["opponent"] = self.team_abbr[item["opponent"].upper()]
            league_year = item["league_year"]
            start_year = league_year.split("-")[0]
            end_year = league_year.split("-")[1]
            date = item["date"]
            if date.split()[0] in ["Nov", "Dec"]:
                date = datetime.strptime(f"{date} {start_year}", "%b %d %Y")
                item["date"] = date.strftime("%Y-%m-%d")
            elif (date.split()[0] == "Oct") and (int(date.split()[1]) > 12):
                date = datetime.strptime(f"{date} {start_year}", "%b %d %Y")
                item["date"] = date.strftime("%Y-%m-%d")
            else:
                date = datetime.strptime(f"{date} {end_year}", "%b %d %Y")
                item["date"] = date.strftime("%Y-%m-%d")
            return item
        else:
            raise DropItem()
