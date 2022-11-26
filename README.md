<img src='images/header-nba.jpg' alt='NBA' width=1400 height=400/>

# NBA Betting

## Table of Contents
* [Introduction](#Introduction)
* [Data Engineering](#Data-Engineering)
* [Exploratory Data Analysis](#Exploratory-Data-Analysis)
* [AutoML](#AutoML)
* [Bet Decision](#Bet-Decision)
* [Deployment](#Deployment)
* [Next Steps](#Next-Steps)
* [Contact](#Contact)
* [Acknowledgments](#Acknowledgments)

<br/><br/>

# Introduction

## Goal

The goal of this project is to create a profitable system for betting on NBA basketball games by leveraging data science and machine learning.

## Motivation

NBA betting sits at an intersection of my interests, knowledge, skills, and goals which makes it a project that is both beneficial and exciting for me. A lucky combination!
* NBA - Huge fan of professional basketball. Especially since 2015. Main focus is on the analytical and financial aspects of the league.
* Data Science - Past Education and Current Career Focus. 
* Sports Stats - Lifelong interest starting with baseball and moving into other sports.
* Investing - Education in Finance - Sportsbetting as an investment option.

## Plan

### How to predict a NBA game result?
|             |             |             |
| ----------- | ----------- | ----------- |
|All in One Team and Player Advanced Stats<ul><li>RAPTOR - FiveThirtyEight</li><li>DARKO - The Athletic</li><li>LEBRON - BBall Index</li><li>EPM - Dunks and Threes</li></ul>|Long-Run Team Quality and Opponent Quality<ul><li>Traditional Box Score Stats</li><li>Advanced Stats</li><li>Elo Ratings</li><li>Power Rankings</li></ul>|Recent Team Performance<ul><li>Last 3, 5, 10, 20 Days</li><li>Game Results</li><li>Traditional Stats</li><li>Advanced Stats</li><ul>|
|Injuries, Rest, Fatigue<ul><li>Estimated Performance % for Players - Injury % and Fatigue %</li><li>Schedule Effects - Back2Backs, 3 Games in 4 Nights</li></ul>|Game Location<ul><li>Home vs. Road Adjustment</li><li>Altitude Effects for Denver and Utah</li></ul>|Seasonal Effects<ul><li>Tanking for Draft Position</li><li>Playoff Seed Positioning</li></ul>|

<br/><br/>

# Data Engineering

## Data Acquisition and Storage

<img src='images/data_engineering_flowchart_1.png' alt='Data Acquisition and Storage' width=1200 height=600/>

## ETL

<img src='images/data_engineering_flowchart_2.png' alt='ETL' width=1200 height=500/>

<br/><br/>

# Exploratory Data Analysis

## Average Vegas Point Spread Error Per Game Over Time

There is a possibility for improvement over the vegas lines for NBA games. The average miss for the vegas line vs the actual game result is over 9 points since 2006!

<img src='images/average_point_spread_error_per_game_over_time.png' alt='Average Point Spread Error Per Game Over Time' width=1200 height=600/>

<br/><br/>

# AutoML

<img src='images/automl_logos.png' alt='AutoML Logos' width=1200/>

<img src='images/classification_model_accuracy.png' alt='Classification Model Accuracy' width=1200/>

Baseline Machine Learning and Deep Learning Model Accuracy is greater than chance but less than what is necessary to be profitable after accounting for the vig (Sportsbook cut for taking bet). This is not troubling for two reasons:
1. Current models and feature set are very simple. Improved models and enhanced feature set coming in the future.
2. Model predictions are only a part of the overall bet decision.

<br/><br/>

# Bet Decision

<img src='images/bet_decisions.png' alt='Bet Decisions' width=1200 height=600/>

<br/><br/>

# Deployment

## Web App

<img src='images/web_app_home_page.png' alt='Home Page' width=560 height=400/>
<img src='images/web_app_dashboard.png' alt='Dashboard' width=560 height=400/>

<br/><br/>

# Next Steps

* Add premier NBA advanced stats like DARKO and EPM to feature set and prediction models.
* Test more Deep Learning model constructs.
* Continue testing project on 2022-2023 NBA Season.
* Expand Web App functionality and make available to public.

<br/><br/>

# Contact

Jeff Johannsen - [LinkedIn](https://www.linkedin.com/in/jeffjohannsen/) - jeffjohannsen7<span>@gmail.</span>com

<br/><br/>

# Acknowledgments

## Data

* [Covers](https://www.covers.com/) - Main source of odds data both live and historic.
* [NBA Stats](https://www.nba.com/stats) - Main source of NBA data.
* [Basketball Reference](https://www.basketball-reference.com/)
* [FiveThirtyEight](https://fivethirtyeight.com/)

## Helpful Projects

* [Databall](https://github.com/klane/databall)

## Tools

Python
* Pandas
* Matplotlib
* Seaborn
* SQLAlchemy
* PyCaret
* AutoKeras
* Keras
* Tensorflow
* Scikit-Learn
* Flask
* Plotly Dash
* Scrapy
* Scrapy Splash  

SQL
* Postgres

AWS
* EC2
* RDS

Other
* Docker
* Cron/Crontab
* HTML/CSS
* Bootstrap
* Chrome Dev Tools
