import sys
import re
import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine

from financials import BankAccount

sys.path.append('../../')
from ...passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.options.display.max_rows = 50
pd.options.display.max_columns = 100
pd.set_option("display.width", 200)


def show_game_records(database_connection):

    while True:
        num_records_input = input("Number of records to show: ")
        if num_records_input in ("All", "all", "ALL"):
            records_to_show = "ALL"
            break
        try:
            num_records_input_ = int(num_records_input)
            records_to_show = num_records_input_
        except ValueError:
            print("Valid inputs include integers or ALL.")
            continue
        else:
            break

    while True:
        date_range_user_prompt = """\nWhich date range would you like to use?
Options include: 
    All - All records in database.
    Today - Records for todays games.
    Week - Records from the last week.
Date Range: """
        date_range_input = input(date_range_user_prompt)
        if date_range_input in ("All", "all", "ALL"):
            date_range = "all"
            break
        elif date_range_input in ("Today", "today", "TODAY"):
            date_range = "today"
            break
        elif date_range_input in ("Week", "week", "WEEK"):
            date_range = "week"
            break
        else:
            print("Invalid date range.")
            continue

    todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    todays_date_str = todays_datetime.strftime("%Y-%m-%d")

    last_week_datetime = todays_datetime - datetime.timedelta(days=7)
    last_week_date_str = last_week_datetime.strftime("%Y-%m-%d")

    if date_range == "all":
        query_date_range = "1900-01-01"
    elif date_range == "today":
        query_date_range = todays_date_str
    elif date_range == "week":
        query_date_range = last_week_date_str
    else:
        print("Error in date range selection.")

    query = f"""SELECT * FROM game_records
                FULL OUTER JOIN bets
                ON game_records.game_id = bets.game_id
                WHERE game_records.date >= '{query_date_range}'
                ORDER BY game_records.game_id DESC
                LIMIT {records_to_show}"""
    game_records = pd.read_sql_query(query, database_connection)

    columns_to_show = [
        "game_id",
        "date",
        "home",
        "away",
        "home_line",
        "game_score",
        "rec_bet_direction",
        "rec_bet_amount",
        "game_result",
        "bet_outcome",
        "bet_amount",
        "bet_line",
        "bet_direction",
        "bet_price",
        "bet_location",
        "bet_profit_loss",
    ]

    print("\n")
    print(game_records[columns_to_show])


class Bet:
    def __init__(self):
        self.bank = BankAccount(connection)
        self.game_id = None
        self.game_record = None
        self.bet_record = None
        self.bet_datetime = None
        self.bet_outcome = None
        self.bet_direction = None
        self.bet_amount = None
        self.bet_location = None
        self.bet_line = None
        self.bet_price = None
        self.bet_status = None
        self.bet_profit_loss = None

        self.verify_game_id()
        self.load_game_record()
        self.create_or_load_bet_record()
        self.user_action_choice()

    def verify_game_id(self):
        while True:
            print(
                "\nGame ID consists of date - home team - road team. \n Example: 20220529MIABOS"
            )
            active_game_id_input = input("Enter game id of game to bet: ")
            pattern = re.compile(
                "^(\d{4})(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])([A-Z]{6})$")
            if pattern.match(active_game_id_input):
                self.game_id = active_game_id_input
                break
            else:
                continue

    def load_game_record(self):
        stmt = f"""SELECT COUNT(*) FROM game_records WHERE game_id = '{self.game_id}'
                ;"""

        game_records_available = int(connection.execute(stmt).fetchone()[0])

        if game_records_available == 1:
            game_record_query = (
                f"SELECT * FROM game_records WHERE game_id = '{self.game_id}';"
            )
            self.game_record = pd.read_sql(game_record_query,
                                           connection,
                                           parse_dates=["date"])
            print(self.game_record)
        else:
            print(f"No records available for game_id {self.game_id}")

    def create_or_load_bet_record(self):
        stmt = f"""SELECT COUNT(*) FROM bets WHERE game_id = '{self.game_id}'
                ;"""

        bet_records_available = int(connection.execute(stmt).fetchone()[0])

        if bet_records_available == 0:
            is_create_new_record = input("Create new bet record? Y/N: ")
            if is_create_new_record in ["Y", "y"]:
                self.create_bet_record()
            else:
                return

        elif bet_records_available == 1:
            print("One bet record available for this game id.")
            stmt = f"""SELECT * FROM bets WHERE game_id = '{self.game_id}'
                ;"""

            self.bet_record = pd.read_sql(stmt, connection)
            self.bet_datetime = self.bet_record["bet_datetime"][0].strftime(
                "%Y-%m-%d %H:%M:%S")
            self.bet_outcome = self.bet_record["bet_outcome"]
            self.bet_direction = self.bet_record["bet_direction"]
            self.bet_amount = self.bet_record["bet_amount"]
            self.bet_location = self.bet_record["bet_location"]
            self.bet_line = self.bet_record["bet_line"]
            self.bet_price = self.bet_record["bet_price"]
            self.bet_status = self.bet_record["bet_status"]
            self.bet_profit_loss = self.bet_record["bet_profit_loss"]
            print(self.bet_record)

        else:
            print(f"Error - Multiple Bet Records for Game ID: {self.game_id}")

    def user_action_choice(self):
        while True:
            print(
                "\nWhat to do next?\nUpdate bet record, Delete bet record, or Exit"
            )
            user_action_input = input()
            if user_action_input in ["Update", "update", "UPDATE"]:
                print("\n")
                print(self.bet_record)
                self.update_bet_record()
            elif user_action_input in ["Delete", "delete", "DELETE"]:
                print("\n")
                print(self.bet_record)
                confirm_delete = input(
                    "Are you sure you want to delete the above bet record? Y/N: "
                )
                if confirm_delete in ["Y", "y"]:
                    self.delete_bet_record()
                    self.bank.withdraw(self.bet_profit_loss)
                    print("\nBet record deleted successfully.")
                    return
                else:
                    continue
            elif user_action_input in ["Exit", "exit", "EXIT"]:
                exit()
            else:
                print("Invalid response. Please try again.")
                continue

    def create_bet_record(self):
        self.bet_datetime = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")

        while True:
            print(
                "\nWhere is the bet placed at? Examples: DraftKings, FanDuel, MGM, Test"
            )
            location_input = input("Bet Location: ")
            valid_bet_location_lst = [
                "Draftkings",
                "DraftKings",
                "draftkings",
                "FanDuel",
                "Fanduel",
                "fanduel",
                "MGM",
                "mgm",
                "Test",
                "test",
            ]
            if location_input in valid_bet_location_lst:
                self.bet_location = location_input
                break
            else:
                print("Invalid bet location. Please try again.")
                continue

        while True:
            line_input = input("\nBet Line: ")
            try:
                float(line_input)
            except ValueError:
                print(
                    line_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                self.bet_line = float(line_input)
                break

        while True:
            amount_input = input("\nBet Amount: ")
            try:
                float(amount_input)
            except ValueError:
                print(
                    amount_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                self.bet_amount = float(amount_input)
                break

        while True:
            print("\nIs the bet on the home team or the away team?")
            direction_input = input("Bet Direction: ")
            valid_bet_direction_lst = [
                "Home",
                "home",
                "HOME",
                "Away",
                "away",
                "AWAY",
            ]
            if direction_input in valid_bet_direction_lst:
                self.bet_direction = direction_input
                break
            else:
                print("Invalid bet direction. Options include Home or Away.")
                continue

        while True:
            price_input = input("\nBet Price: ")
            try:
                float(price_input)
            except ValueError:
                print(
                    price_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                self.bet_price = float(price_input)
                break

        while True:
            print("\nWhat is the outcome of the bet?")
            print("Options include: Win, Loss, Unknown, No Bet")
            outcome_input = input("Bet Outcome: ")
            valid_bet_outcome_lst = [
                "Win",
                "win",
                "WIN",
                "Loss",
                "loss",
                "LOSS",
                "Unknown",
                "unknown",
                "UNKNOWN",
                "No Bet",
                "no bet",
                "NO BET",
                "No bet",
            ]
            if outcome_input in valid_bet_outcome_lst:
                self.bet_outcome = outcome_input
                break
            else:
                print("Invalid bet outcome. Please try again.")
                continue

        while True:
            print("\nIs the bet active or inactive?")
            status_input = input("Bet Status: ")
            valid_bet_status_lst = [
                "Active",
                "active",
                "ACTIVE",
                "Inactive",
                "inactive",
                "INACTIVE",
            ]
            if status_input in valid_bet_status_lst:
                self.bet_status = status_input
                break
            else:
                print("Invalid bet status. Please try again.")
                continue

        while True:
            pf_input = input("\nBet Profit/Loss: ")
            try:
                float(pf_input)
            except ValueError:
                print(
                    pf_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                self.bet_profit_loss = float(pf_input)
                break

        new_bet_record = pd.Series({
            "bet_datetime": self.bet_datetime,
            "bet_location": self.bet_location,
            "bet_line": self.bet_line,
            "bet_direction": self.bet_direction,
            "bet_amount": self.bet_amount,
            "bet_price": self.bet_price,
            "bet_profit_loss": self.bet_profit_loss,
            "bet_status": self.bet_status,
            "bet_outcome": self.bet_outcome,
        })
        print("\n")
        print(new_bet_record)
        is_confirm_new_record = input(
            "Create bet record with above details? Y/N: ")
        if is_confirm_new_record in ["Y", "y"]:
            self.bet_record = new_bet_record
            self.create_bet_record_save()
            self.bank.deposit(self.bet_profit_loss)
            print("New bet record saved successfully.")
        else:
            return

    def update_bet_record(self):
        print(f"\nCurrent Bet Datetime: {self.bet_datetime}")
        do_update_datetime = input("Update bet datetime? Y/N: ")
        if do_update_datetime in ["Y", "y"]:
            new_bet_datetime = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S")
        else:
            new_bet_datetime = self.bet_datetime

        while True:
            print(
                "\nWhere is the bet placed at? Examples: DraftKings, FanDuel, MGM, Test"
            )
            location_input = input("Bet Location: ")
            valid_bet_location_lst = [
                "Draftkings",
                "DraftKings",
                "draftkings",
                "FanDuel",
                "Fanduel",
                "fanduel",
                "MGM",
                "mgm",
                "Test",
                "test",
            ]
            if location_input in valid_bet_location_lst:
                new_bet_location = location_input
                break
            else:
                print("Invalid bet location. Please try again.")
                continue

        while True:
            line_input = input("\nBet Line: ")
            try:
                float(line_input)
            except ValueError:
                print(
                    line_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                new_bet_line = float(line_input)
                break

        while True:
            amount_input = input("\nBet Amount: ")
            try:
                float(amount_input)
            except ValueError:
                print(
                    amount_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                new_bet_amount = float(amount_input)
                break

        while True:
            print("\nIs the bet on the home team or the away team?")
            direction_input = input("Bet Direction: ")
            valid_bet_direction_lst = [
                "Home",
                "home",
                "HOME",
                "Away",
                "away",
                "AWAY",
            ]
            if direction_input in valid_bet_direction_lst:
                new_bet_direction = direction_input
                break
            else:
                print("Invalid bet direction. Options include Home or Away.")
                continue

        while True:
            price_input = input("\nBet Price: ")
            try:
                float(price_input)
            except ValueError:
                print(
                    price_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                new_bet_price = float(price_input)
                break

        while True:
            print("\nWhat is the outcome of the bet?")
            print("Options include: Win, Loss, Unknown, No Bet")
            outcome_input = input("Bet Outcome: ")
            valid_bet_outcome_lst = [
                "Win",
                "win",
                "WIN",
                "Loss",
                "loss",
                "LOSS",
                "Unknown",
                "unknown",
                "UNKNOWN",
                "No Bet",
                "no bet",
                "NO BET",
                "No bet",
            ]
            if outcome_input in valid_bet_outcome_lst:
                new_bet_outcome = outcome_input
                break
            else:
                print("Invalid bet outcome. Please try again.")
                continue

        while True:
            print("\nIs the bet active or inactive?")
            status_input = input("Bet Status: ")
            valid_bet_status_lst = [
                "Active",
                "active",
                "ACTIVE",
                "Inactive",
                "inactive",
                "INACTIVE",
            ]
            if status_input in valid_bet_status_lst:
                new_bet_status = status_input
                break
            else:
                print("Invalid bet status. Please try again.")
                continue

        while True:
            pf_input = input("\nBet Profit/Loss: ")
            try:
                float(pf_input)
            except ValueError:
                print(
                    pf_input,
                    "is not an float or integer number.  Please re-enter.",
                )
                continue
            else:
                new_bet_profit_loss = float(pf_input)
                break

        new_bet_record = pd.Series({
            "bet_datetime": new_bet_datetime,
            "bet_location": new_bet_location,
            "bet_line": new_bet_line,
            "bet_direction": new_bet_direction,
            "bet_amount": new_bet_amount,
            "bet_price": new_bet_price,
            "bet_profit_loss": new_bet_profit_loss,
            "bet_status": new_bet_status,
            "bet_outcome": new_bet_outcome,
        })

        diff_bet_profit_loss = new_bet_profit_loss - self.bet_profit_loss

        print("\n")
        print(new_bet_record)
        is_confirm_new_record = input(
            "Update bet record with above details? Y/N: ")
        if is_confirm_new_record in ["Y", "y"]:
            self.bet_record = new_bet_record
            self.bet_datetime = new_bet_datetime
            self.bet_outcome = new_bet_outcome
            self.bet_direction = new_bet_direction
            self.bet_line = new_bet_line
            self.bet_amount = new_bet_amount
            self.bet_location = new_bet_location
            self.bet_price = new_bet_price
            self.bet_status = new_bet_status
            self.bet_profit_loss = new_bet_profit_loss
            self.update_bet_record_save()
            self.bank.deposit(diff_bet_profit_loss[0])
            print("Bet record updated successfully.")
        else:
            return

    def create_bet_record_save(self):
        stmt = f"""
            INSERT INTO bets (game_id,
                                bet_datetime,
                                bet_status,
                                bet_outcome,
                                bet_amount,
                                bet_price,
                                bet_location,
                                bet_line,
                                bet_profit_loss,
                                bet_direction)
            VALUES ('{self.game_id}',
                    '{self.bet_datetime}',
                    '{self.bet_status}',
                    '{self.bet_outcome}',
                    {self.bet_amount},
                    {self.bet_price},
                    '{self.bet_location}',
                    {self.bet_line},
                    {self.bet_profit_loss},
                    '{self.bet_direction}')
            ;
            """

        connection.execute(stmt)

    def update_bet_record_save(self):
        stmt = f"""
                UPDATE bets
                SET bet_datetime = '{self.bet_datetime}',
                    bet_status = '{self.bet_status}',
                    bet_outcome = '{self.bet_outcome}',
                    bet_amount = {self.bet_amount},
                    bet_price = {self.bet_price},
                    bet_location = '{self.bet_location}',
                    bet_line = {self.bet_line},
                    bet_profit_loss = {self.bet_profit_loss},
                    bet_direction = '{self.bet_direction}'
                WHERE game_id = '{self.game_id}'
                ;
                """

        connection.execute(stmt)

    def delete_bet_record(self):
        stmt = f"""DELETE FROM bets WHERE game_id = '{self.game_id}'
                ;"""

        connection.execute(stmt)


if __name__ == "__main__":
    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"
    port = "5432"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}")

    with engine.connect() as connection:
        show_game_records(connection)
        while True:
            active_bet = Bet()

# Script to create original table in psql command line.
"""CREATE TABLE bets (
    game_id varchar,
    bet_datetime timestamp,
    bet_status varchar,
    bet_outcome varchar,
    bet_amount float4,
    bet_price int4,
    bet_location varchar,
    bet_line float4,
    bet_profit_loss float4,
    bet_direction varchar
);"""
