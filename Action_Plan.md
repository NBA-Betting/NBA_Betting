# NBA Betting Outline and Action Plan

### Feature Ideas
* Comparisons with individual game and team, league, timeframe averages.

### Other Considerations
* Stats - ELO, RAPTOR, DARKO, PIPM, RAPM, EPM    

## Basic Outline
#### Source and Extract Data
* APIs and Web Scraping
* Three parts:
    * Feature Data for predicting.
    * Betting Lines and Point Spreads
        * Sportsbookreviewsonline.com for historical opening and closing lines.
            * May be interesting to see where and why lines move a lot.
        * Covers.com for historical and recent.
            * Single pull for historical based on databall project guideline.
            * Automated daily pull for newer odds.
        * >> Value difference from target to line.
    * Target - Actual Results of Games

#### Transform Data and Engineer Features
* Time Adjusted Data - How to get "real time" data for past events.
    * Basketball Reference ha team data by day and some player data via game logs. 
* Feature Creation:
    * Pull and Utilize Features (No adjustments necessary)
    * Engineered Features using typical Data Science feature engineering techniques (interaction, poly, trig).
    * Manually engineered features. Where can I use my NBA knowledge to gain an advantage?

#### Machine Learning Modeling
* AutoML for Basic Modeling
* Deep Learning for Advanced Modeling

#### Integrate Models with other non-ML info sources.
* Injuries, Rest
* Personal Opinion
* Schedule Effects
* Home, Away Advantages. Altitude Effects for Denver and Utah

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
