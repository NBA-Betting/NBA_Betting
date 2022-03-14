from datetime import datetime

# TODO: Load and Save to RDS on all actions.
# TODO: All functions except get_balance trigger log entry.
class BankAccount:
    def __init__(self, balance):
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount

    def withdraw(self, amount):
        self.balance -= amount

    def get_balance(self):
        return self.balance

    def set_balance(self, new_balance):
        self.balance = new_balance

# TODO: Automatic Daily Log at midnight
# TODO: Save to RDS
class BankAccountLog:
    def __init__(self, description):
        self.transaction_id = description[0].upper() + datetime.now().strftime("%d%m%Y_%H%M%S")
        self.transaction_datetime = datetime.now().strftime("%d%m%Y_%H%M%S")
        self.previous_balance = # Inbound from DB
        self.new_balance = # Inbound from BankAccount
        self.balance_change = self.new_balance - self.previous_balance
        self.transaction_description = description
        

class Bet:
    def __init__(self, game_id, amount, location, price):
        self.bet_id = game_id + datetime.now().strftime("%d%m%Y_%H%M%S")
        self.bet_datetime = datetime.now()
        self.bet_outcome = "Unknown"  # Win, Loss, Unknown
        self.bet_amount = amount
        self.bet_location = location
        self.bet_price = price
        self.bet_status = "Active"
        self.bet_profit_loss = 0
        self.game_id = game_id

    def void_bet(self):
        self.bet_status = 'Void'

    # TODO: Input updates self.bet_profit_loss based on bet_amount and bet_price.
    def enter_bet_outcome(self, outcome):
        """Was the bet a win, loss, or unknown?

        Args:
            outcome (str): 'Win', 'Loss', or 'Unknown'
        """
        if outcome.upper() in ['WIN', 'LOSS', 'UNKNOWN']:
            self.bet_outcome = outcome
            if outcome in ['Win', 'Loss']:
                self.bet_status = 'Closed'
            else:
                self.bet_status = 'Unknown'
        else:
            print('Invalid Response. Options Include: Win, Loss, Unknown')


# Script to create original tables in psql command line.

"""CREATE TABLE bank_account (
    datetime timestamp,
    balance float4
);"""

"""CREATE TABLE bank_account_log (
    transaction_id int,
    transaction_datetime timestamp,
    previous_balance float4,
    new_balance float4,
    balance_change float4,
    transaction_description varchar
);"""

"""CREATE TABLE bets (
    game_id varchar,
    bet_id varchar,
    bet_datetime timestamp,
    bet_status varchar,
    bet_outcome varchar,
    bet_amount float4,
    bet_price int4,
    bet_location varchar,
    bet_profit_loss float4
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