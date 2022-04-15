# NBA Betting Outline and Action Plan

### Feature Ideas
* Comparisons with individual game and team, league, timeframe averages.
* How to introduce the time of the season into ML training?
    * Currently only including the end of season 2-digit year. 
* How to introduce the teams involved as a feature?
    * Currently using team names encoded to int.
* ELO, RAPTOR, DARKO, PIPM, RAPM, EPM

### Other Considerations
* Need to determine a value for the difference in Home Margin, Predicted Home Margin, and Home Line
    * Dollar value or similar.
    * May not be linear like the differences are.     

## Basic Outline
#### Source and Extract Data
* APIs and Web Scraping
* Three parts:
    * Betting Lines and Point Spreads
    * Target - Actual Results of Games, Options:
        * Home Margin (Current)
        * Classification of Home Result vs. Spread
        * Classification of Binned Home Margins
    * Feature Data for Predicting

#### Transform Data and Engineer Features
* Time Adjusted Data - How to get "real time" data for past events.
    * Basketball Reference has some player data via game logs. 
* Feature Creation:
    * Pull and Utilize Features (No adjustments necessary)
    * Engineered Features using typical Data Science feature engineering techniques (interaction, ratio, poly, trig).
    * Manually engineered features. Where can I use my NBA knowledge to gain an advantage?

#### Machine Learning Modeling
* AutoML for Basic Modeling - PyCaret
* Deep Learning for Advanced Modeling - Keras + Tensorflow

#### Integrate Models with other non-ML info sources.
* Injuries, Rest
* Personal Opinion
* Schedule Effects
* Home, Away Advantages. Altitude Effects for Denver and Utah

#### Final Dataset Creation
1. Determine how to define and organize Home and Away Lines, Predicted Results, Actual Results, Cover By, etc.
    * Lining up Amounts and Directions is tricky. Be Careful!
2. Improve Models and Re-test
3. Create Final Algorithms
4. Add data to be displayed to RDS table.
6. Test, Debug, and Schedule Daily Process

#### Frontend, Web App, Dashboard   
Page 1
* Top Left
    * Bankroll Amount, alltime, yearly, monthly, weekly +/- $ and %
    * Yesterdays Results
        * Bets
        * Actual Win/Loss $ and %
    * Todays Action
        * Bets
        * Expected Win $ and %
* Top Right
    * Graph of Bankroll +/- over time
* Bottom
    * Table of Games for today
        * Ability to change dates to see past and ongoing games as well.

Page 2
* Dashboard
    * Ability to integrate financials, model predictions, and features


#### Make Process Production Quality
* OOP, Tests, CI/CD
* Small, High Quality code blocks built on top of each other.

#### Live Test the System

#### Determine Location(s) and Method for Real Money Betting
* Automated or Manual Betting?
* Bankroll Management
* Which Sportbook(s)?
* Private Accounts?
* Will I get banned if I win too consistently?

#### Deploy and Monitor System on Real Live Betting
* Need continuous automated updates prior, during, and after runs.
* Need easily accessible mobile monitoring of system. 
* Need to create stop loss protections.
* Dashboard for past, current, future monitoring.
