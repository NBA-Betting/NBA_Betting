<!-- ![Header Image](images/header-nba.jpg) -->

<img src='images/header-nba.jpg' alt='NBA' width=1400 height=400/>

# NBA Betting

>>>Project Demo<<<

## Table of Contents
* [Introduction](#Introduction)
    * [Motivation](#Motivation)
    * [Goals](#Goals)
* [Data Engineering](#Data-Engineering)
* [Exploratory Data Analysis](#Exploratory-Data-Analysis)
* [Feature Engineering](#Feature-Engineering)
* [Data Modeling](#Data-Modeling)
    * [Machine Learning + AutoML](#Machine-Learning-+-AutoML)
    * [Deep Learning](#Deep-Learning)
* [Deployment](#Deployment)
* [Next Steps](#Next-Steps)
* [Contact](#Contact)
* [Acknowledgments](#Acknowledgments)

<br/><br/>

# Introduction

## Motivation

Sports Stats - MLB, Sports Game Simulations, Personal Sports League Record Keeping (Papers from Childhood)
NBA - Steph Curry and 2015-2016 Warriors, Why I think NBA is the best league in sports
Investing - Sports Betting as an investment??

## Goals

The main goal of this project is create an automated system that collects data about NBA games and predicts point spreads at a level that is consistently profitable.

### Secondary Goals

* Learn more about the NBA and uncover interesting stats and trends concerning the NBA and betting data surrounding it.
* Acquire new data skills and improve upon my current knowledge.

<br/><br/>

# Data Engineering

* Needed historic data for models. Data points as of a date in the past. More difficult to locate.
* Combined historic data with current data via daily cron jobs.

## Data Acquisition and Storage

<!-- ![Date Acquisition and Storage](images/data_engineering_flowchart_1.png) -->

<img src='images/data_engineering_flowchart_1.png' alt='Data Acquisition and Storage' width=1200 height=600/>

## ETL

<!-- ![ETL](images/data_engineering_flowchart_2.png) -->

<img src='images/data_engineering_flowchart_2.png' alt='ETL' width=1200 height=500/>

<br/><br/>

# Exploratory Data Analysis

Dataprep.eda for AutoEDA

Interesting Questions:
* Vegas Point Spread vs. Actual Game Spread over time. How has Vegas improved at predicting?
* Biggest Outliers Vegas vs. Actual
* Home vs. Road over time
* Home/Road against the spread over time
* How much is being at home worth over time, Both vegas lines and actual results
* Team Quality vs. Against the Spread results. Are good, average, or bad teams more likely to over-perform the vegas spread?
* Actual Team Record vs. Expected Record, Which is a better predictor of vegas lines and actual results?

## Average Point Spread Error Per Game Over Time

<!-- ![Average Point Spread Error Per Game Over Time](images/average_point_spread_error_per_game_over_time.png) -->

<img src='images/average_point_spread_error_per_game_over_time.png' alt='Average Point Spread Error Per Game Over Time' width=1200 height=700/>

<br/><br/>

# Feature Engineering

<br/><br/>

# Data Modeling

## Machine Learning + AutoML

PyCaret Full Workthrough

### Setup

### Results

## Deep Learning

### Setup

### Results

<br/><br/>

# Deployment

<br/><br/>

# Next Steps

* Improve Models
* Add more data sources.
* Create more useful features.

<br/><br/>

# Contact

Jeff Johannsen - [LinkedIn](https://www.linkedin.com/in/jeffjohannsen/) - jeffjohannsen7<span>@gmail.</span>com

<br/><br/>

# Acknowledgments

## Data

* [Covers](https://www.covers.com/) - Main source of odds data both live and historic.
* [NBA Stats](https://www.nba.com/stats) - Main source of NBA data.
* [Basketball Reference](https://www.basketball-reference.com/)

## Images

## Helpful Projects

* [Databall](https://github.com/klane/databall)

## Tools

Python
* Scrapy
* Splash
* Pandas
* SQLAlchemy
* PyCaret
* Keras
* Tensorflow
* Scikit-Learn
* Flask  

SQL
* Postgres

AWS
* S3
* EC2
* RDS

Docker
Cron/Cronitor
HTML/CSS
Bootstrap
Chrome Dev Tools




