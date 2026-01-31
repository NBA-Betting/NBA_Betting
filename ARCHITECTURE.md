# NBA Betting - System Architecture

> Visual overview of data flow, database schema, and core workflows.

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│    Covers.com       │      NBA.com        │         Odds API                │
│   (Scrapy spider)   │   (nba_api lib)     │        (REST API)               │
│                     │                     │                                 │
│  • Game scores      │  • Team stats       │  • Live betting odds            │
│  • Opening spreads  │  • Box scores       │  • Consensus lines              │
│  • Schedules        │  • Advanced metrics │  • (median across books)        │
└─────────┬───────────┴──────────┬──────────┴───────────────┬─────────────────┘
          │                      │                          │
          ▼                      ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SQLite DATABASE                                    │
│                         (data/nba_betting.db)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────────────────────┐│
│  │   games     │  │    lines    │  │      team_nbastats_general_*        ││
│  │             │  │             │  │  • traditional  • advanced          ││
│  │ • game_id   │  │ • game_id   │  │  • fourfactors  • opponent          ││
│  │ • datetime  │  │ • open_line │  │                                     ││
│  │ • home/away │  │ • curr_line │  │  (4 tables, daily snapshots)        ││
│  │ • scores    │  │ • prices    │  └──────────────────────────────────────┘│
│  └─────────────┘  └─────────────┘                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            ETL PIPELINE                                      │
│                          (src/etl/main_etl.py)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   1. Load games + stats tables                                              │
│   2. Standardize team names (config.TEAM_MAP)                               │
│   3. Pre-merge feature engineering:                                         │
│      • Z-scores, percentiles, rankings                                      │
│      • Rolling stats (all games, last 2 weeks)                              │
│   4. Merge: games ⟕ stats (point-in-time join)                             │
│   5. Post-merge features:                                                   │
│      • Home vs Away differentials                                           │
│      • Rest days, travel, streaks                                           │
│   6. Save to all_features_json (761 features per game)                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         all_features_json                                    │
│                     (Feature Store - JSON blobs)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   game_id (PK)  │  data (JSON)                                              │
│   ──────────────┼─────────────────────────────────────────────────────      │
│   202501150LAL  │  {"home_team": "LAL", "away_team": "MIA",                 │
│                 │   "pts_home_all_traditional": 112.5,                       │
│                 │   "pts_zscore_home_all_traditional": 1.23,                 │
│                 │   ... 761 features ...}                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MODEL TRAINING                                      │
│                       (src/modeling/trainer.py)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   NBAPredictor (AutoGluon)                                                  │
│   ├── Classification: Beat the spread? (yes/no)                             │
│   └── Regression: Predict score differential                                │
│                                                                             │
│   Output: models/nba_cls_YYYY/, models/nba_reg_YYYY/                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PREDICTION & BETTING                                    │
│                    (src/predictions/generator.py)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   1. Load upcoming games + features                                         │
│   2. Run through trained model                                              │
│   3. Apply Kelly Criterion for bet sizing                                   │
│   4. Save to predictions table                                              │
│                                                                             │
│   ┌─────────────────┐     ┌─────────────────┐                              │
│   │   predictions   │     │      bets       │                              │
│   │                 │     │                 │                              │
│   │ • game_id       │     │ • game_id       │                              │
│   │ • model_variant │────▶│ • bet_datetime  │                              │
│   │ • ml/dl preds   │     │ • bet_amount    │                              │
│   │ • game_rating   │     │ • profit/loss   │                              │
│   └─────────────────┘     └─────────────────┘                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WEB APPLICATION                                     │
│                          (src/app/web.py)                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Flask App (port 5000)                                                     │
│   ├── / ────────────────▶ Today's games + predictions                       │
│   └── /dashboard ───────▶ Dash analytics (embedded)                         │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                    DASH DASHBOARD                                    │   │
│   │                   (src/app/dashboard.py)                             │   │
│   │                                                                      │   │
│   │   • Win/Loss charts        • ROI over time                          │   │
│   │   • Accuracy by month      • Bet distribution                       │   │
│   │   • Model performance      • Bankroll tracking                      │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Database Schema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CORE TABLES                                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│        games          │    │        lines        │    │    predictions      │
├───────────────────────┤    ├─────────────────────┤    ├─────────────────────┤
│ game_id (PK)          │◄──▶│ game_id (PK/FK)     │    │ game_id (PK)        │
│ game_datetime         │    │ open_line           │    │ model_variant (PK)  │
│ home_team             │    │ open_line_price     │    │ predicted_at        │
│ away_team             │    │ current_line        │    │ open_spread         │
│ open_line             │    │ current_line_price  │    │ prediction_spread   │
│ home_score            │    │ line_last_update    │    │ home_cover_prob     │
│ away_score            │    │                     │    │ home_cover_pred     │
│ game_completed        │    │ (consensus across   │    │ predicted_margin    │
│ scores_last_update    │    │  all sportsbooks)   │    │ pick                │
│ odds_last_update      │    └─────────────────────┘    │ confidence          │
└───────────────────────┘                               └─────────────────────┘

  model_variant: "standard" (no vegas feature) or "vegas" (with vegas line as feature)

┌─────────────────────────────────────────────────────────────────────────────┐
│                            FEATURE STORE                                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         all_features_json                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ game_id (PK)     │  TEXT     │  Unique game identifier                      │
│ data             │  JSON     │  761 engineered features as JSON blob        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          STATS TABLES (4)                                    │
└─────────────────────────────────────────────────────────────────────────────┘

  team_nbastats_general_traditional    team_nbastats_general_advanced
  ┌────────────────────────────────┐   ┌────────────────────────────────┐
  │ team_name, to_date, games (PK) │   │ team_name, to_date, games (PK) │
  │ gp, w, l, w_pct, pts, +/-      │   │ off_rtg, def_rtg, net_rtg      │
  └────────────────────────────────┘   │ pace, ts_pct, efg_pct          │
                                       └────────────────────────────────┘

  team_nbastats_general_fourfactors    team_nbastats_general_opponent
  ┌────────────────────────────────┐   ┌────────────────────────────────┐
  │ team_name, to_date, games (PK) │   │ team_name, to_date, games (PK) │
  │ efg_pct, fta_rate, tov_pct     │   │ opp_pts, opp_efg, opp_tov      │
  │ oreb_pct, ft_pct               │   │ (opponent stats allowed)       │
  └────────────────────────────────┘   └────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         BETTING TABLES                                       │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────┐    ┌─────────────────────┐
│          bets           │    │   betting_account   │
├─────────────────────────┤    ├─────────────────────┤
│ game_id (PK)            │    │ datetime (PK)       │
│ bet_datetime (PK)       │    │ balance             │
│ bet_status              │    └─────────────────────┘
│ bet_amount              │
│ bet_price               │
│ bet_location            │
│ bet_direction           │
│ bet_line                │
│ bet_profit_loss         │
└─────────────────────────┘
```

---

## Daily Update Workflow

```
┌──────────────────────────────────────────────────────────────────────────┐
│  $ python update_data.py                                                  │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
            ┌───────────────────────────────────────────────┐
            │              1. COLLECT DATA                   │
            ├───────────────────────────────────────────────┤
            │  • Covers.com: Yesterday's scores + today's   │
            │  • NBA.com: Team stats through yesterday      │
            │  • Odds API: Current lines (if key set)       │
            └───────────────────────────────────────────────┘
                                    │
                                    ▼
            ┌───────────────────────────────────────────────┐
            │              2. RUN ETL                        │
            ├───────────────────────────────────────────────┤
            │  • Load new games + stats                      │
            │  • Engineer 761 features per game              │
            │  • Save to all_features_json                   │
            └───────────────────────────────────────────────┘
                                    │
                                    ▼
            ┌───────────────────────────────────────────────┐
            │           3. GENERATE PREDICTIONS              │
            ├───────────────────────────────────────────────┤
            │  • Load upcoming games                         │
            │  • Run through model                           │
            │  • Save predictions                            │
            └───────────────────────────────────────────────┘
                                    │
                                    ▼
            ┌───────────────────────────────────────────────┐
            │             4. VIEW RESULTS                    │
            ├───────────────────────────────────────────────┤
            │  $ python start_app.py                         │
            │  → http://localhost:5000                       │
            └───────────────────────────────────────────────┘
```

---

## Feature Engineering Pipeline

```
Raw Stats (per team, per day)
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PRE-MERGE TRANSFORMATIONS                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   For each stat (e.g., points per game):                                   │
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                    │
│   │    Raw      │    │  Z-Score    │    │ Percentile  │                    │
│   │   Value     │───▶│  (std dev   │───▶│  (rank vs   │                    │
│   │             │    │   from mean)│    │   league)   │                    │
│   └─────────────┘    └─────────────┘    └─────────────┘                    │
│                                                                             │
│   × 2 time windows: "all" (season) + "l2w" (last 2 weeks)                  │
│   × 4 stat tables = ~200 features per team                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         POST-MERGE FEATURES                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Home Team Stats ─────┐                                                    │
│                        ├──▶ Differentials (home - away)                     │
│   Away Team Stats ─────┘                                                    │
│                                                                             │
│   Additional Features:                                                      │
│   • Days of rest (home/away)                                               │
│   • Back-to-back flags                                                      │
│   • Win/loss streaks                                                        │
│   • Season progress (% complete)                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
   761 Features Per Game
```

---

## Key Files Quick Reference

| Purpose | File |
|---------|------|
| **Entry points** | `update_data.py`, `start_app.py` |
| **Config** | `src/config.py` |
| **Database** | `src/database.py`, `src/database_orm.py` |
| **Pipeline** | `src/pipeline.py` |
| **Data collection** | `src/data_sources/team/nbastats_fetcher.py`, `src/data_sources/game/odds_api.py` |
| **ETL** | `src/etl/main_etl.py`, `src/etl/feature_creation.py` |
| **Modeling** | `src/modeling/trainer.py` |
| **Predictions** | `src/predictions/generator.py` |
| **Betting** | `src/betting/account.py` |
| **Web app** | `src/app/web.py` |
| **Dashboard** | `src/app/dashboard.py` |
