import os
from datetime import datetime

import pytz
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")


class BettingAccount:
    def __init__(self, connection):
        self.connection = connection
        self.balance = self.load_balance()

    def deposit(self, amount):
        self.balance = self.load_balance()
        self.balance += amount
        self.save_balance()

    def withdraw(self, amount):
        self.balance = self.load_balance()
        self.balance -= amount
        self.save_balance()

    def get_balance(self):
        self.balance = self.load_balance()
        return self.balance

    def set_balance(self, new_balance):
        self.balance = self.load_balance()
        self.balance = new_balance
        self.save_balance()

    def load_balance(self):
        stmt = """SELECT balance FROM betting_account
                  WHERE datetime = (SELECT MAX(datetime) FROM betting_account)
               ;"""

        return self.connection.execute(stmt).fetchone()[0]

    def save_balance(self):
        current_datetime = datetime.now(pytz.timezone("America/Denver")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        stmt = "INSERT INTO betting_account (datetime, balance) VALUES (%s, %s);"
        self.connection.execute(stmt, (current_datetime, float(self.balance)))


def update_betting_account_balance():
    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"
    port = "5432"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    )

    with engine.connect() as connection:
        betting_account = BettingAccount(connection)
        betting_account.save_balance()


if __name__ == "__main__":
    pass
    # update_betting_account_balance()
