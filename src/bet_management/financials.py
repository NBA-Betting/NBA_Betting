from datetime import datetime
from sqlalchemy import create_engine


class BankAccount:
    def __init__(self):
        self.balance = self.load_balance(connection=connection)

    def deposit(self, amount):
        self.balance = self.load_balance(connection=connection)
        self.balance += amount
        self.save_balance(connection=connection)
        BankAccountLog("Deposit")

    def withdraw(self, amount):
        self.balance = self.load_balance(connection=connection)
        self.balance -= amount
        self.save_balance(connection=connection)
        BankAccountLog("Withdrawal")

    def get_balance(self):
        self.balance = self.load_balance(connection=connection)
        return self.balance

    def set_balance(self, new_balance):
        self.balance = self.load_balance(connection=connection)
        self.balance = new_balance
        self.save_balance(connection=connection)
        BankAccountLog("Balance Update")

    def load_balance(self, connection):
        stmt = """SELECT balance FROM bank_account
                  WHERE datetime = (SELECT MAX(datetime) FROM bank_account)
               ;"""

        return connection.execute(stmt).fetchone()[0]

    def save_balance(self, connection):
        stmt = f"""INSERT INTO bank_account (datetime, balance)
                  VALUES ('{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}',{self.balance})
                ;"""

        connection.execute(stmt)


class BankAccountLog:
    def __init__(self, description):
        self.transaction_id = description[0].upper() + datetime.now().strftime(
            "%d%m%Y_%H%M%S%f"
        )
        self.transaction_datetime = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        self.previous_balance = self.load_most_recent_balance(
            connection=connection
        )
        self.new_balance = BankAccount().get_balance()
        self.balance_change = self.new_balance - self.previous_balance
        self.transaction_description = description

        self.save_log_entry(connection=connection)

    def load_most_recent_balance(self, connection):
        stmt = """SELECT new_balance FROM bank_account_log
                  WHERE transaction_datetime = (SELECT MAX(transaction_datetime) FROM bank_account_log)
               ;"""

        return connection.execute(stmt).fetchone()[0]

    def save_log_entry(self, connection):
        stmt = f"""INSERT INTO bank_account_log (transaction_id,
                                                transaction_datetime,
                                                previous_balance,
                                                new_balance,
                                                balance_change,
                                                transaction_description)
                  VALUES ('{self.transaction_id}',
                          '{self.transaction_datetime}',
                          {self.previous_balance},
                          {self.new_balance},
                          {self.balance_change},
                          '{self.transaction_description}')
                ;"""

        connection.execute(stmt)


# On Click of Button on Web App
# Small Form Appears with game_id autofilled
# User inputs amount, location, and price. Input validation within web app.
# Below Object is created when user saves form.
class Bet:
    def __init__(
        self, game_id, direction=None, amount=None, location=None, price=None
    ):
        records_available = self.check_for_bet_record(
            game_id=game_id, connection=connection
        )

        if records_available == 1:
            self.load_bet_record(game_id=game_id, connection=connection)
        elif records_available == 0:
            self.game_id = game_id
            self.bet_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            self.bet_outcome = "Unknown"  # Win, Loss, Unknown, No Bet
            self.bet_direction = direction
            self.bet_amount = amount
            self.bet_location = location
            self.bet_price = price
            self.bet_status = "Active"
            self.bet_profit_loss = 0

            self.create_bet_record(connection=connection)
            BankAccount().withdraw(self.bet_amount)
        else:
            print(
                f"Error when checking for number of bet records. {records_available} records available."
            )
        # self.save_to_game_record_db(connection=connection)

    def void_bet(self):
        BankAccount().deposit(self.bet_amount)

        self.delete_bet_record(connection=connection)
        # self.save_to_game_record_db(connection=connection)

    # TODO: Link game outcome
    def enter_bet_outcome(self, outcome):
        """Was the bet a win, loss, or unknown?

        Args:
            outcome (str): 'Win', 'Loss', 'Unknown', or 'No Bet'
        """
        if outcome.upper() in ["WIN", "LOSS", "UNKNOWN", "NO BET"]:
            if self.bet_status == "Closed":
                print("Bet Already Closed")
            else:
                self.bet_outcome = outcome
                if outcome.upper() in ["WIN", "LOSS"]:
                    self.bet_status = "Closed"
                else:
                    self.bet_status = "Unknown"
                if outcome.upper() == "WIN":
                    self.bet_profit_loss = self.bet_amount * -(
                        100 / self.bet_price
                    )
                    BankAccount().deposit(
                        self.bet_amount + self.bet_profit_loss
                    )
                elif outcome.upper() == "LOSS":
                    self.bet_profit_loss = -self.bet_amount

                self.update_bet_record(connection=connection)
                # self.save_to_game_record_db(connection=connection)

        else:
            print(
                "Invalid Response. Options Include: Win, Loss, Unknown, No Bet"
            )

    def check_for_bet_record(self, game_id, connection):
        stmt = f"""SELECT COUNT(*) FROM bets WHERE game_id = '{game_id}'
                ;"""

        return int(connection.execute(stmt).fetchone()[0])

    def load_bet_record(self, game_id, connection):
        stmt = f"""SELECT * FROM bets WHERE game_id = '{game_id}'
                ;"""

        result = connection.execute(stmt).fetchone()
        self.game_id = game_id
        self.bet_datetime = result["bet_datetime"]
        self.bet_outcome = result["bet_outcome"]
        self.bet_direction = result["bet_direction"]
        self.bet_amount = result["bet_amount"]
        self.bet_location = result["bet_location"]
        self.bet_price = result["bet_price"]
        self.bet_status = result["bet_status"]
        self.bet_profit_loss = result["bet_profit_loss"]

    def create_bet_record(self, connection):
        stmt = f"""
                INSERT INTO bets (game_id,
                                  bet_datetime,
                                  bet_status,
                                  bet_outcome,
                                  bet_amount,
                                  bet_price,
                                  bet_location,
                                  bet_profit_loss,
                                  bet_direction)
                VALUES ('{self.game_id}',
                        '{self.bet_datetime}',
                        '{self.bet_status}',
                        '{self.bet_outcome}',
                        {self.bet_amount},
                        {self.bet_price},
                        '{self.bet_location}',
                        {self.bet_profit_loss},
                        '{self.bet_direction}')
                ;
                """

        connection.execute(stmt)

    def delete_bet_record(self, connection):
        stmt = f"""DELETE FROM bets WHERE game_id = '{self.game_id}'
                ;"""

        connection.execute(stmt)

    def update_bet_record(self, connection):
        stmt = f"""
                UPDATE bets
                SET bet_status = '{self.bet_status}',
                    bet_outcome = '{self.bet_outcome}',
                    bet_profit_loss = {self.bet_profit_loss}
                WHERE game_id = '{self.game_id}'
                ;"""

        connection.execute(stmt)

    def save_to_game_record_db(self, connection):
        stmt = f"""
                UPDATE betting_data
                SET bet_amount = {self.bet_amount},
                    bet_direction = {self.bet_direction},
                    bet_price = {self.bet_price},
                    bet_location = {self.bet_location},
                    bet_result = {self.bet_outcome},
                    bet_win_loss = {self.bet_profit_loss}
                WHERE game_id = '{self.game_id}'
                ;"""

        # connection.execute(stmt)


if __name__ == "__main__":
    username = "postgres"
    password = ""
    endpoint = ""
    database = "nba_betting"
    port = "5432"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    )

    with engine.connect() as connection:

        Bet(
            game_id="test_1",
            direction="Home",
            amount=200,
            location="FanDuel",
            price=-110,
        )
        Bet(
            game_id="test_5",
            direction="Away",
            amount=100,
            location="DraftKings",
            price=-110,
        )
        Bet(game_id="test_1")
        Bet(game_id="test_1").void_bet()
        Bet(game_id="test_5").enter_bet_outcome("loss")


# Scripts to create original tables in psql command line.
"""CREATE TABLE bank_account (
    datetime timestamp,
    balance float4
);"""

"""CREATE TABLE bank_account_log (
    transaction_id varchar,
    transaction_datetime timestamp,
    previous_balance float4,
    new_balance float4,
    balance_change float4,
    transaction_description varchar
);"""

"""CREATE TABLE bets (
    game_id varchar,
    bet_datetime timestamp,
    bet_status varchar,
    bet_outcome varchar,
    bet_amount float4,
    bet_price int4,
    bet_location varchar,
    bet_profit_loss float4,
    bet_direction varchar
);"""

"""CREATE TABLE betting_data (
    game_id varchar,
    game_info varchar,
    date date,
    time time,
    home varchar,
    away varchar,
    home_line float4,
    home_line_price int4,
    away_line float4,
    away_line_price int4,
    ml_prediction float4,
    ml_pred_direction varchar,
    ml_pred_line_margin float4,
    ml_win_prob float4,
    ml_ev float4,
    ml_ev_vig float4,
    dl_prediction float4,
    dl_pred_direction varchar,
    dl_pred_line_margin float4,
    dl_win_prob float4,
    dl_ev float4,
    dl_ev_vig float4,
    game_score float4,
    rec_bet_direction varchar,
    rec_bet_amount float4,
    predicted_win_loss float4,
    game_result int4,
    bet_result varchar,
    bet_amount float4,
    bet_direction varchar,
    bet_price int4,
    bet_location varchar,
    bet_win_loss float4
);"""
