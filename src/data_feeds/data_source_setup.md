## Step 1: Determine name for new data source

## Step 2: Use Google Inspect to find the columns to be saved

## Step 3: Add Table to database_orm.py

```python
class DataSourceNameTable(Base):  # CamelCase name of data source + Table
    __tablename__ = "ibd_data_source_name"  # ibd_ + Lowercase name of data source
    __table_args__ = (PrimaryKeyConstraint(
        "column2", "column1"
    ),)  # Can be 1 or more columns
    column1 = Column(Integer)  # Lowercase name of column
    column2 = Column(String)  # Other datatype options: Integer, Float, Date, Boolean
    column3 = Column(String)
```

### Run database_orm.py to create the table in the database
### Check the table in the database to make sure it looks right

<br>

## Step 4: Add Scrapy Item to items.py

```python
class DataSourceNameItem(scrapy.Item):  # CamelCase name of data source + Item
    column1 = scrapy.Field()  # Lowercase name of column
    column2 = scrapy.Field()
    column3 = scrapy.Field()
```

<br>

## Step 5: Add Scrapy ItemLoader to item_loaders.py

```python
from .items import DataSourceNameItem  # CamelCase name of data source + Item

class DataSourceNameItemLoader(ItemLoader):  # CamelCase name of data source + ItemLoader
    default_item_class = DataSourceNameItem  # CamelCase name of data source + Item
    default_output_processor = TakeFirst()

    column1_in = MapCompose(int)  # Lowercase name of column + _in
    column2_in = MapCompose(
        str.strip
    )  # Commonly used MapCompose functions: str.strip, float, int
    column3_in = MapCompose(str.strip)
```

<br>

## Step 6: Add Scrapy Spider to spiders folder

### Filename should be lowercase data_source_name + _spider.py

### Import BaseSpider from base_spider.py, Item from items.py, and ItemLoader from item_loaders.py
```python
from data_sources.spiders.base_spider import BaseSpider
from data_sources.items import DataSourceNameItem
from data_sources.item_loaders import DataSourceNameItemLoader
```

### Update spider name, allowed_domains, custom_settings, and first season
```python
class BaseSpider(Spider):
    name = "<data_source_name>_spider"  # Update: data_source_name
    allowed_domains = []  # Update
    custom_settings = {
        "ITEM_PIPELINES": {
            "data_sources.pipelines.BasePipeline": 300
        }  # Update: DataSourceName + Pipeline
    }
    # ...
    first_season = 0  # Update: First season of data source
```

### Update init method to with dates logic
```python
    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super().__init__(
            dates,
            save_data=save_data,
            view_data=view_data,
            first_season=self.first_season,
            *args,
            **kwargs,
        )

        if dates in ["all", "daily_update"]:
            self.dates = dates
        else:
            raise ValueError(
                f"Invalid date format: {dates}. Date format should be 'all' or 'daily_update'"
            )
```

### Update find_season_information method (if necessary)
```python
def find_season_information(self, date_str):
    # Logic to use NBA_IMPORTANT_DATES to find necessary season information
    pass
```

### Update start_requests method
```python
def start_requests(self):
    base_url = ""  # Update: Base URL for the data source
    params = {}  # Update: Parameters for the data source.
    # Example: {"season": "2020-21", "frdt": "2020-12-22", "todt": "2020-12-22"}

    # Update this section to create all starting urls needed
    for date_str in self.dates:
        url = base_url + "?" + urlencode(params)
        yield scrapy.Request(url, callback=self.parse)
```

### Update parse method
```python
def parse(self, response):
    # Code to get to the table/iterable for the data
    # Example:
    table_rows = response.css(".iptbl table tr")

    # Code to parse table/iterable to add data to items
    # Example:
    for row in table_rows[3:]:  # Skip the header rows
        data = {
            "column1": row.css("td:nth-child(1)::text").get(),
            "column2": row.css("td:nth-child(2) a::text").get(),
            "column3": row.css("td:nth-child(3)::text").get(),
        }
        yield data

    # Code to get to the next page if pagination
    # Example:
    next_page_links = response.css("div.slbl a::attr(href)").getall()
    for link in next_page_links:
        next_page_url = response.urljoin(link)
        yield scrapy.Request(next_page_url, callback=self.parse)
```

<br>

## Step 7: Add Scrapy Pipeline to pipelines.py
```python
from src.database_orm import (
    DataSourceNameTable,  # CamelCase name of data source + Table
)

class DataSourceNamePipeline(BasePipeline):  # CamelCase name of data source + Pipeline
    ITEM_CLASS = DataSourceNameTable  # CamelCase name of data source + Table

    # Define process_item, process_dataset, and save_to_database (if necessary)

    def process_item(self, item, spider):
        """
        This method is called for each item that is scraped. It cleans organizes, and verifies
        the item before appending it to the list of scraped data.

        Args:
            item (dict): The scraped item.
            spider (scrapy.Spider): The spider that scraped the item.

        Returns:
            dict: The processed item.
        """
        try:
            # Item processing logic here
            self.nba_data.append(item)
            return item
        except Exception as e:
            self.processing_errors += 1
            return False

    def process_dataset(self):
        """
        This method can be overridden by subclasses to process the full dataset
        once all items have been processed individually.
        """
        # Dataset processing logic here
        pass

    def save_to_database(self):
        # Remove duplicates and update records
        df = pd.DataFrame(self.nba_data)
        df.sort_values(by=["player_id", "season", "priority"], ascending=False, inplace=True)
        df.drop_duplicates(subset=["player_id", "season"], keep="first", inplace=True)

        # Remove the "priority" column as it's not needed anymore
        df.drop(columns=["priority"], inplace=True)

        # Convert the DataFrame back to a list of dictionaries
        self.nba_data = df.to_dict("records")

        # Call the save_to_database method from the BasePipeline
        super().save_to_database()
```

<br>

## Step 8: Test Scrapy Spider
```bash
scrapy crawl spider_name -a dates='YYYY-MM-DD,YYYY-MM-DD' -a view_data=True -a save_data=False
```

<br>

## Step 9: Add Scrapy Spider to run_spiders.py and test
```python
from data_sources.spiders.data_source_name_spider import DataSourceNameSpider

run_spider(
    DataSourceNameSpider,  # CamelCase name of data source + Spider
    save_data=True,  # Test with False first
    view_data=True,
    dates="2023-04-01",  # Test one, multiple, and all dates
)
```

<br>

## Step 10: Upload to Github

<br>

## Step 11: Update EC2
### Run deploy.sh to pull from Github and restart the server
### Test the spider on the server

<br>

## Step 13: Download historical data if available
### Preferably using local machine
