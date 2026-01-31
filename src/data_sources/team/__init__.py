"""
Team data sources for NBA statistics.

Primary module: nbastats_fetcher.py - Fetches team stats from NBA.com using nba_api

Archived: archive/scrapy_spiders/ - Original Scrapy-based spiders (blocked by TLS fingerprinting)
"""

from src.data_sources.team.nbastats_fetcher import NBAStatsFetcher

__all__ = ["NBAStatsFetcher"]
