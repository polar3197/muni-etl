#!/bin/bash
set -e

# Step 1: Commit and push changes locally
echo "Committing and pushing local changes..."
git add .
git commit -m "autopilot" || echo "Nothing to commit"
git push

# Step 2: SSH into the Pi and pull
echo "Pulling latest changes on remote Pi..."
ssh caddy "cd ~/muni-etl && git pull"
ssh quentin "cd ~/muni-etl && git pull"
