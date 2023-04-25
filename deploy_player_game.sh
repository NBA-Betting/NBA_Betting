#!/bin/bash

# Change to the project directory for player_game branch
cd ~/nba_betting_player_game

# Checkout the player_game branch
git checkout player_game

# Pull the latest changes from the repository
git pull

# Install any new dependencies
pip install -r requirements.txt

# Restart any necessary services
# sudo systemctl restart your_service
