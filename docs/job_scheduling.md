# Scheduled Data Jobs - Crontab on EC2

**IMPORTANT** - AWS uses UTC time which is -7 hours from Mountain Time.
* Example: Job scheduled at 17:00 will run at 10:00 mountain time.

Job Name, Description, Run Time and Interval, Logging

## DATABASE ETL

Daily NBA_Betting Inbound Data Record Creation
* Cron Command - ```0 18 * * * cd ~/etl && python3 daily_db_game_record_creation.py```
* Runtime - Daily at 18:00utc/11:00am mountain

Daily NBA_Betting Inbound Data Update
* Cron Command - ```30 17 * * * cd ~/etl && python3 daily_db_game_record_update.py```
* Runtime - Daily at 17:30utc/10:30am mountain

Daily NBA_Betting Model Ready Data Creation
* Cron Command - ```30 18 * * * cd ~/feature_creation && python3 feature_creation.py```
* Runtime - Daily at 18:30utc/11:30am mountain

## INBOUND DATA STREAMS

Daily Odds from Covers
* Cron Command - ```30 17 * * * cd ~/data_feeds/covers/live_odds && ~/.local/bin/scrapy crawl covers_live_odds_spider```
* Runtime - Daily at 17:30utc/10:00am mountain

Daily Game Results from Covers
* Cron Command - ```0 17 * * * cd ~/data_feeds/covers/game_results && ~/.local/bin/scrapy crawl covers_game_results_spider```
* Runtime - Daily at 17:00utc/10:00am mountain

Daily Standings from Basketball Reference,  
Daily Team Stats from Basketball Reference,  
Daily Opponent Stats from Basketball Reference  
* Cron Command - ```0 17 * * * cd ~/data_feeds/basketball_reference && bash br_basic_cron.sh```
* Runtime - Daily at 17:00utc/10:00am mountain
* Script:
```sudo docker start condescending_newton
cd ~/data_feeds/basketball_reference
~/.local/bin/scrapy crawl BR_standings_spider
~/.local/bin/scrapy crawl BR_team_stats_spider
~/.local/bin/scrapy crawl BR_opponent_stats_spider
sudo docker stop condescending_newton


