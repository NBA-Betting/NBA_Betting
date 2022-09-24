# NBA Betting Outline and Action Plan

## Inbound Data

## ETL

## Feature Creation

## Modeling

## Bet Management

## Deployment
* Daily Updating
    * Test bash script for 4/10/2022
    * When successful switch hard coded dates to proper dates for daily updating.
## Readme and Misc

# Adding Data, Features
* Can I get or create historic data? If so incorporate into ML/DL models. Labeled "Historic" Below.
* If I can't get historic data I need to recreate/create metrics with a way to transfer them to a predicted line between two teams on a given date. These will be aggregated with ML/DL model results.

## All in One Team and/or Player Advanced Stats
* RAPTOR - FiveThirtyEight
    * https://fivethirtyeight.com/features/introducing-raptor-our-new-metric-for-the-modern-nba/
* DARKO - The Athletic
    * https://theathletic.com/2613015/2021/05/26/introducing-darko-an-nba-playoffs-game-projection-and-betting-guide/
    * https://apanalytics.shinyapps.io/DARKO/
* LEBRON - BBall Index
    * https://www.bball-index.com/lebron-database/
* EPM - Dunks and Threes
    * https://dunksandthrees.com/epm

## Long-Run Team Quality and Opponent Team Quality
* General Traditional and Advanced Stats - Done
* League Adjusted - Done
    * Plus or Minus League Average
    * Standard Deviations from League Average
* League Rankings - Done
* Elo Ratings
    * Historic --> ML/DL
    * FiveThirtyEight
        * https://fivethirtyeight.com/features/how-we-calculate-nba-elo-ratings/
        * https://projects.fivethirtyeight.com/complete-history-of-the-nba/#warriors
* Power Rankings
    * Decision Rules --> Post ML/DL
    * Combination of various power rankings weighted by age.


## Short-Run Team Performance
* Last 3, 5, 10, 20, X Days or Games - Results and Stats - Mainstream and Advanced
* Win/Loss Streaks

## Injuries, Rest, Fatigue
Who is playing, who is not? Quantify value of absences. Estimated Performance % for Players - Injury % and Fatigue %
* Schedule Effects - Back2Backs, 3in4
    * Decision Rules --> Post ML/DL

## Game Location
* Home vs. Road Adjustment - Baked Into ML/DL
* Altitude Effects for Denver and Utah - Partially baked Into ML/DL
    * Game Altitude(Feet) 

## Seasonal Effects
* Day of Season Feature - Done
* Late Season Tanking
* Early Season Small Sample Size Adjustment
    * Weight rows in modeling

## Other
* Ref Assignments
    * Decision Rules --> Post ML/DL
* Against the Spread Record
    * Decision Rules --> Post ML/DL

# Bankroll Management
* Bet in units instead of a fixed dollar amount
* 1 unit = 1% of current bankroll
* Each bet can have 1 to 5 units
* Aim for 2 to 10 units per day
