# Scheduled Data Jobs - Crontab on EC2

**IMPORTANT** - AWS uses UTC time which is -7 hours from Mountain Time.
* Example: Job scheduled at 17:00 will run at 10:00 mountain time.

Job Name, Description, Run Time and Interval, Logging

## DATABASE ETL

Daily NBA_Betting Database Update

## INBOUND DATA STREAMS

Daily Odds from Covers
* Cron Command - ```0 18 * * * cd ~/data_feeds/covers/live_odds && ~/.local/bin/scrapy crawl covers_live_odds_spider```
* Runtime - Daily at 18:00utc/11am mountain

Daily Game Results from Covers
* Crom Command - ```0 17 * * * cd ~/data_feeds/covers/game_results && ~/.local/bin/scrapy crawl covers_game_results_spider```
* Runtime - Daily at 17:00utc/11am mountain

Daily Standings from Basketball Reference,  
Daily Team Stats from Basketball Reference,  
Daily Opponent Stats from Basketball Reference  
* Cron Command - ```0 17 * * * cd ~/data_feeds/basketball_reference && bash br_basic_cron.sh```
* Runtime - Daily at 17:00utc/11am mountain
* Script:
```sudo docker start condescending_newton
cd ~/data_feeds/basketball_reference
~/.local/bin/scrapy crawl BR_standings_spider
~/.local/bin/scrapy crawl BR_team_stats_spider
~/.local/bin/scrapy crawl BR_opponent_stats_spider
sudo docker stop condescending_newton


