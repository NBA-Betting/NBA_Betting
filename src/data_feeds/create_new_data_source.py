import json
import os
import subprocess
import sys

ROOT_DIR = os.path.abspath(
    "/home/jeff/Documents/NBA_Betting"
)  # Current working directory

PROJECT_SECTIONS = [
    "Player",
    "Team",
    "Player Network",
    "Game",
    "Betting",
]


def is_in_root_dir():
    return os.path.abspath(os.getcwd()) == ROOT_DIR


def to_snake_case(string):
    """
    Convert a string to snake_case.
    Assumes the input string can have spaces or already be in snake_case.
    """
    return string.replace(" ", "_").lower()


def to_camel_case(string):
    """
    Convert a string to CamelCase.
    Assumes the input string can have spaces or already be in snake_case.
    """
    return "".join(word.title() for word in string.replace(" ", "_").split("_"))


def user_verification(items_to_verify):
    print()
    for item in items_to_verify:
        print(f"Please check {item}.")
        user_confirmation = input("If it has been set up properly, please enter 'yes': ")

        if user_confirmation.lower() not in ["yes", "y"]:
            print(f"{item}: Setup not confirmed. Please check and try again.")
            sys.exit()

    for item in items_to_verify:
        print(f"{item}: Setup confirmed.")


def get_input(prompt):
    value = ""
    while not value:
        value = input(prompt)
    return value


def get_section(prompt):
    section = ""
    while section not in PROJECT_SECTIONS:
        section = input(prompt)
    return section


def get_json_data(prompt):
    while True:
        filename = input(prompt)
        if os.path.exists(filename) and filename.endswith(".json"):
            try:
                with open(filename, "r") as file:
                    data_column_dict = json.load(file)
                return data_column_dict
            except json.JSONDecodeError:
                print("The file is not valid JSON.")
        else:
            print("The file does not exist or is not a .json file.")


def validate_data_structure(data_column_dict):
    DATATYPES = ["S", "F", "I", "D", "B"]
    data_structure = {}
    primary_keys = []

    for column, value in data_column_dict.items():
        if not isinstance(column, str) or not isinstance(value, str):
            return None, None

        # Extract the datatype and primary key status
        datatype = "".join([char for char in value if char in DATATYPES])
        pk = "PK" if "PK" in value else ""

        # Check if datatype and primary key status form the original value
        if value != datatype + pk or len(datatype) != 1:
            return None, None

        data_structure[column] = datatype
        if pk:
            primary_keys.append(column)

    return data_structure, primary_keys


def confirm_data_source_details(data):
    print("\nConfirm New Data Source Details:")
    print("----------------------------------")
    for key, value in data.items():
        print(f"{key}: {value}")

    decision = ""
    options = ["Confirm", "Restart", "Exit"]
    while decision not in options:
        decision = input("\nPlease type 'Confirm', 'Restart', or 'Exit': ")
    if decision == "Confirm":
        pass
    elif decision == "Restart":
        return create_new_data_source()
    elif decision == "Exit":
        sys.exit()  # Exit the program


def confirm_data_structure():
    print("\nData Structure File Requirements:")
    print("1. The file must be a .json file.")
    print("2. The file must be a dictionary format.")
    print("3. The keys must be the column names.")
    print("4. The values must be in the format: <datatype><primary key>")
    print("\t- S: String, F: Float, I: Integer, D: Date, or B: Boolean")
    print("\t- PK: Primary Key (optional)")
    print(
        """Example:
                   {
                    'column1': 'SPK',
                    'column2': 'F',
                    'column3': 'DPK',
                    'column4': 'F'
                   }"""
    )
    raw_data_structure = get_json_data("Enter Data Structure Filename: ")
    data_structure, primary_keys = validate_data_structure(raw_data_structure)

    if data_structure is None:
        print("The data structure does not match the specifications. Please try again.")
        return create_new_data_source()

    data = {"Data Structure": data_structure, "Primary Keys": primary_keys}
    print(f"\nNew Data Structure Details:")
    print("-----------------------------")
    print(f"Data Structure:")
    for key, value in data["Data Structure"].items():
        print(f"\t{key}: {value}")
    print(f"Primary Keys: {', '.join(data['Primary Keys'])}")

    decision = ""
    options = ["Confirm", "Restart", "Exit"]
    while decision not in options:
        decision = input("\nPlease type 'Confirm', 'Restart', or 'Exit': ")
    if decision == "Confirm":
        return data_structure, primary_keys
    elif decision == "Restart":
        return create_new_data_source()
    elif decision == "Exit":
        sys.exit()  # Exit the program


def insert_table_definition_into_orm(
    data_source_name,
    project_section,
    data_structure,
    primary_key_constraint,
    data_source_provider,
    data_source_url,
    data_source_description,
):
    file_path = "./src/database_orm.py"

    # Mapping the given datatype keys to their SQLAlchemy datatype classes
    type_mapping = {
        "S": "String",
        "D": "Date",
        "F": "Float",
        "I": "Integer",
        "B": "Boolean",
    }

    # Format class name, table name and primary key constraint
    class_name = f"{to_camel_case(data_source_name)}Table"
    table_name = f"{to_snake_case(project_section)}_{to_snake_case(data_source_name)}"

    # Prepare the primary key constraint
    primary_key_constraint = ", ".join(f'"{col}"' for col in primary_key_constraint)

    # Prepare the columns
    columns = "\n    ".join(
        f"{col_name} = Column({type_mapping[col_type]})"
        for col_name, col_type in data_structure.items()
    )

    # Full class template
    class_template = f'''
class {class_name}(Base):
    """
    Data source provider: {data_source_provider}
    Data source URL: {data_source_url}
    Data source description: {data_source_description}
    """
    __tablename__ = "{table_name}"
    __table_args__ = (PrimaryKeyConstraint(
        {primary_key_constraint}
    ),)
    {columns}
'''

    # Read the current contents of the file
    with open(file_path, "r") as f:
        file_contents = f.read()

    # Split the contents on the line before the main block
    parts = file_contents.split('if __name__ == "__main__":')

    # Strip any leading/trailing whitespace, then add back exactly two newlines
    parts = [part.rstrip() + "\n\n" for part in parts]

    # Insert the new class into the file contents
    new_contents = f'{parts[0]}{class_template}\n\nif __name__ == "__main__":{parts[1]}'

    # Write the new contents back to the file
    with open(file_path, "w") as f:
        f.write(new_contents)


def run_database_orm(data_source_name, project_section):
    # Convert data_source_name and project_section to appropriate cases
    data_source_name_snake = to_snake_case(data_source_name)
    project_section_snake = to_snake_case(project_section)

    # Calculate table name
    table_name = f"{project_section_snake}_{data_source_name_snake}"

    subprocess.run(["python", "./src/database_orm.py"])
    print(f"\n{table_name} table added to database_orm.py and created in database.")

    user_verification(
        [
            f"{to_camel_case(data_source_name)}Table in database_orm.py",
            f"{table_name} in nba_betting database",
        ]
    )


def append_scrapy_item_class(data_source_name, project_section, data_structure):
    # Convert project_section to snake case
    project_section_snake = to_snake_case(project_section)

    # Define file path
    file_path = (
        f"./src/data_sources/{project_section_snake}/{project_section_snake}/items.py"
    )

    class_name = to_camel_case(data_source_name) + "Item"
    columns = "\n    ".join(
        f"{col_name} = scrapy.Field()" for col_name in data_structure.keys()
    )

    class_template = f"""
class {class_name}(scrapy.Item):
    {columns}
    """
    with open(file_path, "r+") as f:
        content = f.read()
        # Check if there are two new lines at the end of the file
        if not content.endswith("\n\n"):
            # If not, add them
            f.write("\n" * (2 - content.count("\n", len(content) - 2)))

        f.write(class_template + "\n\n")

    print(f"\n{class_name} added to {file_path}")

    user_verification([f"{class_name} in {file_path}"])


def append_scrapy_item_loader_class(data_source_name, project_section, data_structure):
    # Convert project_section to snake case
    project_section_snake = to_snake_case(project_section)

    # Define file path
    file_path = f"./src/data_sources/{project_section_snake}/{project_section_snake}/item_loaders.py"

    class_name = to_camel_case(data_source_name) + "ItemLoader"
    item_class_name = to_camel_case(data_source_name) + "Item"

    # Mapping datatypes to corresponding MapCompose functions
    datatype_mapping = {
        "S": "str.strip",
        "D": "",
        "F": "float",
        "I": "int",
        "B": "",
    }

    columns = "\n    ".join(
        f"{col_name}_in = MapCompose({datatype_mapping[col_type]})"
        for col_name, col_type in data_structure.items()
    )

    class_template = f"""
class {class_name}(ItemLoader):
    default_item_class = {item_class_name}
    default_output_processor = TakeFirst()

    {columns}
    """

    # Load file contents into memory
    with open(file_path, "r") as f:
        content = f.readlines()

    # Search for the import line
    for i, line in enumerate(content):
        if line.strip().startswith("from .items import"):
            import_line_index = i
            import_line = line.strip()
            break

    # Determine how to update the import line based on its current structure
    if import_line == "from .items import":
        # Case 1: Import line is empty, add new item class
        new_import_line = f"from .items import {item_class_name}"
        content[import_line_index] = new_import_line
    elif "(" not in import_line:
        # Case 2: Import line is a single line, append new item class
        new_import_line = f"{import_line}, {item_class_name}"
        content[import_line_index] = new_import_line
    else:
        # Case 3: Import line spans multiple lines
        # Search for line containing closing parenthesis
        for j in range(i, len(content)):
            if ")" in content[j]:
                # Add new item class before closing parenthesis
                content[j] = content[j].replace(")", f"\t{item_class_name},\n)")
                break

    # Add two newlines if they're not already present
    if not "".join(content[-2:]) == "\n\n":
        content.append("\n" * (2 - "".join(content[-2:]).count("\n")))

    # Append new class definition
    content.append(class_template + "\n\n")

    # Write the updated content back to the file
    with open(file_path, "w") as f:
        f.write("".join(content))

    print(f"\n{class_name} added to {file_path}")

    user_verification([f"{class_name} in {file_path}"])


def append_scrapy_pipeline_class(data_source_name, project_section):
    # Convert project_section to snake case
    project_section_snake = to_snake_case(project_section)

    # Define file path
    file_path = f"./src/data_sources/{project_section_snake}/{project_section_snake}/pipelines.py"

    class_name = to_camel_case(data_source_name) + "Pipeline"
    table_name_orm = to_camel_case(data_source_name) + "Table"

    class_template = f"""
class {class_name}(BasePipeline):
    ITEM_CLASS = {table_name_orm}
    """

    # Load file contents into memory
    with open(file_path, "r") as f:
        content = f.readlines()

    # Search for the import line
    for i, line in enumerate(content):
        if line.strip().startswith("from ....database_orm import"):
            import_line_index = i
            import_line = line.strip()
            break

    # Determine how to update the import line based on its current structure
    if import_line == "from ....database_orm import":
        # Case 1: Import line is empty, add new table class
        new_import_line = f"from ....database_orm import {table_name_orm}\n"
        content[import_line_index] = new_import_line
    elif "(" not in import_line:
        # Case 2: Import line is a single line, append new table class
        new_import_line = f"{import_line}, {table_name_orm}\n"
        content[import_line_index] = new_import_line
    else:
        # Case 3: Import line spans multiple lines
        # Search for line containing closing parenthesis
        for j in range(i, len(content)):
            if ")" in content[j]:
                # Add new tabel class before closing parenthesis
                content[j] = content[j].replace(")", f"\t{table_name_orm},\n)\n")
                break

    # Add two newlines if they're not already present
    if not "".join(content[-2:]) == "\n\n":
        content.append("\n" * (2 - "".join(content[-2:]).count("\n")))

    # Append new class definition
    content.append(class_template + "\n\n")

    # Write the updated content back to the file
    with open(file_path, "w") as f:
        f.write("".join(content))

    print(f"\n{class_name} added to {file_path}")

    user_verification([f"{class_name} in {file_path}"])


def create_scrapy_spider(data_source_name, project_section, data_structure):
    # Naming setup
    filename = (
        to_snake_case(project_section)
        + "_"
        + to_snake_case(data_source_name)
        + "_spider"
    )
    file_location = f"./src/data_sources/{to_snake_case(project_section)}/{to_snake_case(project_section)}/spiders/{filename}.py"

    class_name = (
        to_camel_case(project_section) + to_camel_case(data_source_name) + "Spider"
    )
    spider_name = (
        to_snake_case(project_section)
        + "_"
        + to_snake_case(data_source_name)
        + "_spider"
    )

    # File contents
    imports_settings_class = f"""
import json
import os
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pytz
import scrapy
from dotenv import load_dotenv

from ....data_source_utils import BaseSpider
from ..item_loaders import {to_camel_case(data_source_name)}ItemLoader
from ..items import {to_camel_case(data_source_name)}Item

load_dotenv()
ZYTE_API_KEY = os.environ.get("ZYTE_API_KEY")


class {class_name}(BaseSpider):
    name = "{spider_name}"
    pipeline_name = "{to_camel_case(data_source_name)}Pipeline"
    project_section = "{to_snake_case(project_section)}"
    first_season_start_year = {int(input("What is the first year of data available?:"))}

    def __init__(
        self, dates, save_data=False, view_data=True, use_zyte=False, *args, **kwargs
    ):
        super().__init__(
            dates,
            save_data=save_data,
            view_data=view_data,
            use_zyte=use_zyte,
            *args,
            **kwargs
        )

"""

    start_requests_method = """
    def start_requests(self):
        base_url = ""  # Update: Base URL for the data source
        headers = {}  # Update: Headers for the data source, if necessary
        # Headers can be found in the Network tab of Google Dev Tools
        params = {}  # Update: Parameters for the data source.
        # Example: {"season": "2020-21", "frdt": "2020-12-22", "todt": "2020-12-22"}

        # Update this section to create all starting urls needed
        for date_str in self.dates:
            url = base_url + "?" + urlencode(params)
            yield scrapy.Request(url, callback=self.parse)
    """

    columns = "\n\t\t".join(
        f'loader.add_value("{col_name}", {col_name})'
        for col_name in data_structure.keys()
    )

    parse_method = f"""
    def parse(self, response):
        # Code to get to the table/iterable for the data
        # Example:
        # table_rows = response.css(".iptbl table tr")

        loader = {to_camel_case(data_source_name)}ItemLoader(
                item={to_camel_case(data_source_name)}Item()
        )

        # Example:
        # loader.add_value("url", response.url)
        # loader.add_xpath('name', '//div[@class="name"]/text()')  # replace with your actual XPath
        # loader.add_css('description', '.description::text')  # replace with your actual CSS selector

        {columns}

        yield loader.load_item()

        # Code to get to the next page if pagination
        # Example:
        # next_page_links = response.css("div.slbl a::attr(href)").getall()
        # for link in next_page_links:
        #     next_page_url = response.urljoin(link)
        #     yield scrapy.Request(next_page_url, callback=self.parse)

    """

    full_file_contents = imports_settings_class + start_requests_method + parse_method

    # Create the file
    with open(file_location, "w") as f:
        f.write(full_file_contents)

    print(f"\n{class_name} added to {filename}.py")

    user_verification([f"{class_name} in {filename}.py"])


def provide_further_instructions():
    overview_of_steps_completed = f"""\n
    The data source has been started.
    The following steps have been completed:
        1. A new table has been added to database_orm.py
        2. The new table has been created in the database.
        3. A new Scrapy Item class has been added to items.py
        4. A new Scrapy Item Loader class has been added to item_loaders.py
        5. A new Scrapy Pipeline class has been added to pipelines.py
        6. A new Scrapy Spider has been added to spiders.py"""

    next_steps = f"""\n
    The following steps need to be completed:
        1. Finish the start_requests method in the spider.
        2. Finish the parse method in the spider.
        3. Test the spider:
            - Verify the spider is recognized by Scrapy with the following command:
            scrapy list
            - Run the spider in the terminal with the following command:
            scrapy crawl spider_name -a dates='YYYY-MM-DD,YYYY-MM-DD' -a view_data=True -a save_data=False
        4. Collect historical data. Preferably on a local machine and without using Zyte.
        5. Add the spider to the appropriate DAG in airflow_dags.
        6. Test the DAG in Airflow.
            - Test the DAG using the following command:
            airflow tasks test <dag_name> <task_name>
        7. Commit and push the changes to GitHub.
        8. Pull the changes to the EC2 instance.
        9. Test the DAG in Airflow on the EC2 instance.
            - Test the DAG using the Airflow UI at nba-betting.us:8080
    """

    print(overview_of_steps_completed)
    print(next_steps)


def create_new_data_source():
    print("Create New Data Source Helper")
    print("-----------------------------")

    # Get data source details

    data = {
        "Data Source Name": get_input("Enter Data Source Name: "),
        "Data Source Provider": get_input("Enter Data Source Provider: "),
        "Data Source URL": get_input("Enter Data Source URL: "),
        "Data Source Description": get_input("Enter Data Source Description: "),
        "Project Section": get_section(f"Enter Project Section {PROJECT_SECTIONS}: "),
    }

    confirm_data_source_details(data)

    # Get data structure details

    data_structure, primary_keys = confirm_data_structure()
    data["Data Structure"] = data_structure
    data["Primary Keys"] = primary_keys

    # Append new table to database_orm.py
    insert_table_definition_into_orm(
        data_source_name=data["Data Source Name"],
        project_section=data["Project Section"],
        data_structure=data["Data Structure"],
        primary_key_constraint=data["Primary Keys"],
        data_source_provider=data["Data Source Provider"],
        data_source_url=data["Data Source URL"],
        data_source_description=data["Data Source Description"],
    )

    # Run database_orm.py
    run_database_orm(data["Data Source Name"], data["Project Section"])

    # Scrapy Item Class
    append_scrapy_item_class(
        data["Data Source Name"], data["Project Section"], data["Data Structure"]
    )

    # Scrapy Item Loader Class
    append_scrapy_item_loader_class(
        data["Data Source Name"], data["Project Section"], data["Data Structure"]
    )

    # Scrapy Pipeline Class
    append_scrapy_pipeline_class(data["Data Source Name"], data["Project Section"])

    # Scrapy Spider
    create_scrapy_spider(
        data["Data Source Name"], data["Project Section"], data["Data Structure"]
    )

    # Provide further instructions
    provide_further_instructions()


if __name__ == "__main__":
    if not is_in_root_dir():
        print("This script should only be run from the root directory.")
        sys.exit(1)
    create_new_data_source()
