"""
NBA Betting - Financial Management

Handles betting account balance tracking and profit/loss calculations.
"""

from sqlalchemy import text

from src.database import get_engine
from src.utils.timezone import format_datetime, get_current_time


def calculate_profit_loss(bet_amount: float, bet_price: int, won: bool | None) -> float:
    """
    Calculate profit/loss from a bet based on American odds.

    Args:
        bet_amount: Amount wagered (e.g., 100.0)
        bet_price: American odds (e.g., -110 for favorites, +150 for underdogs)
        won: True if won, False if lost, None if push (tie)

    Returns:
        Profit if won (positive), loss amount (negative), or 0.0 for push

    Examples:
        >>> calculate_profit_loss(100, -110, True)   # Win at -110 odds
        90.91
        >>> calculate_profit_loss(100, +150, True)   # Win at +150 odds
        150.0
        >>> calculate_profit_loss(100, -110, False)  # Loss
        -100.0
        >>> calculate_profit_loss(100, -110, None)   # Push
        0.0
    """
    # Push - bet is returned, no profit or loss
    if won is None:
        return 0.0

    if not won:
        return -bet_amount

    # Handle invalid odds (shouldn't happen, but defensive)
    if bet_price == 0:
        return 0.0

    if bet_price > 0:
        # Underdog: bet $100 to win $bet_price
        return bet_amount * (bet_price / 100)
    else:
        # Favorite: bet $|bet_price| to win $100
        return bet_amount * (100 / abs(bet_price))


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
        stmt = text("""SELECT balance FROM betting_account
                  WHERE datetime = (SELECT MAX(datetime) FROM betting_account)
               ;""")

        result = self.connection.execute(stmt).fetchone()
        if result is None:
            # No balance records - return default starting balance
            from src.config import STARTING_BALANCE

            return STARTING_BALANCE
        return result[0]

    def save_balance(self):
        current_datetime = format_datetime(get_current_time())
        stmt = text("INSERT INTO betting_account (datetime, balance) VALUES (:dt, :bal);")
        self.connection.execute(stmt, {"dt": current_datetime, "bal": float(self.balance)})
        self.connection.commit()


def determine_bet_outcome(
    game_result: float, spread: float, bet_direction: str, push_threshold: float = 0.0
) -> bool | None:
    """
    Determine if a spread bet won, lost, or pushed.

    Args:
        game_result: Actual game margin (home_score - away_score)
        spread: The spread line (negative = home favored, e.g., -4.0)
        bet_direction: "home" or "away"
        push_threshold: Margin for push detection (default 0.0 for exact match)

    Returns:
        True if bet won, False if lost, None if push

    Note:
        Spread convention: negative means home team is favored.
        E.g., spread=-4 means home must win by more than 4 to cover.

    Examples:
        >>> determine_bet_outcome(10, -4.0, "home")   # Home won by 10, spread was -4
        True  # Home covered (won by more than 4)
        >>> determine_bet_outcome(3, -4.0, "home")    # Home won by 3, spread was -4
        False  # Home didn't cover (needed to win by more than 4)
        >>> determine_bet_outcome(-4, -4.0, "home")   # Home lost by 4, spread was -4
        None  # Push (exact spread match)
    """
    # Calculate cover margin: game_result - (-spread) = game_result + spread
    # For home bet: positive means home covered, negative means away covered
    cover_margin = game_result + spread

    # Check for push (exact spread match)
    if abs(cover_margin) <= push_threshold:
        return None

    if bet_direction.lower() == "home":
        return cover_margin > 0
    elif bet_direction.lower() == "away":
        return cover_margin < 0
    else:
        raise ValueError(f"Invalid bet_direction: {bet_direction}. Must be 'home' or 'away'.")


def calculate_spread_bet_profit(
    game_result: float,
    spread: float,
    bet_direction: str,
    bet_amount: float = 100.0,
    bet_price: int = -110,
) -> float:
    """
    Calculate profit/loss for a spread bet given game result.

    Convenience function combining determine_bet_outcome and calculate_profit_loss.

    Args:
        game_result: Actual game margin (home_score - away_score)
        spread: The spread line (negative = home favored)
        bet_direction: "home" or "away"
        bet_amount: Amount wagered (default $100)
        bet_price: American odds (default -110)

    Returns:
        Profit/loss amount (positive for win, negative for loss, 0 for push)

    Examples:
        >>> calculate_spread_bet_profit(10, -4.0, "home")  # Home covered
        90.91  # Win at -110 odds
        >>> calculate_spread_bet_profit(3, -4.0, "home")   # Home didn't cover
        -100.0  # Loss
    """
    outcome = determine_bet_outcome(game_result, spread, bet_direction)
    return calculate_profit_loss(bet_amount, bet_price, outcome)


def update_betting_account_balance():
    """Update the betting account balance using the centralized database connection."""
    engine = get_engine()

    with engine.connect() as connection:
        betting_account = BettingAccount(connection)
        betting_account.save_balance()


if __name__ == "__main__":
    pass
    # update_betting_account_balance()
