# NBA Betting Outline and Action Plan

# RESEARCH LIST
* Cron scheduled jobs monitoring and reporting
* Simulation systems
* NBA analytic sources (preferably with historic data)

# MUST HAVE TODO LIST
## SPECIAL PROJECTS
* Simulation System
    * Ability to test scheduled jobs and other programs
    * Ability to test the outputs of models and overall system performance
## Inbound Data
* Debug/Streamline/Rerun
* Set up monitoring and reporting of scheduled jobs.
## ETL
* Debug/Streamline/Rerun 
## Feature Creation
* Debug/Streamline/Rerun
## Modeling
* Debug/Streamline/Rerun
## Bet Management
* Complete functions and system/process.
* Debug/Streamline/Rerun
## Deployment
* Command Line Tool with Daily Email Report
## Readme and Cleanup
* Quality over Quantity

# NEXT STEPS TODO
## Inbound Data
* Add sources
## ETL

## Feature Creation
* Add Features
## Modeling
* Play around with options and optimize.
## Bet Management

## Deployment
* Web App
* Dashboard

### Feature Ideas
* Comparisons with individual game and team, league, timeframe averages.
* How to introduce the time of the season into ML training?
    * Currently only including the end of season 2-digit year. 
* How to introduce the teams involved as a feature?
    * Currently using team names encoded to int.
* ELO, RAPTOR, DARKO, PIPM, RAPM, EPM
* Feature Creation:
    * Pull and Utilize Features (No adjustments necessary)
    * Engineered Features using typical Data Science feature engineering techniques (interaction, ratio, poly, trig).
    * Manually engineered features. Where can I use my NBA knowledge to gain an advantage?

### Other Considerations
* Need to determine a value for the difference in Home Margin, Predicted Home Margin, and Home Line
    * Dollar value or similar.
    * May not be linear like the differences are.     
    * Injuries, Rest
    * Personal Opinion
    * Schedule Effects
    * Home, Away Advantages. Altitude Effects for Denver and Utah

### Bankroll Management
* Bet in units instead of a fixed dollar amount
* 1 unit = 1% of current bankroll
* Each bet can have 1 to 5 units
* Aim for 2 to 10 units per day