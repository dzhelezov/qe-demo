Copy#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Kill any existing processes on the required ports
echo "Stopping any existing processes..."
lsof -ti:4999,5000,5001,8000 | xargs kill -9 2>/dev/null || true

# Start node servers
echo "Starting node servers..."
python node_server.py --port 4999 --node_id 1 &
python node_server.py --port 5001 --node_id 2 &

# Wait for nodes to start up
echo "Waiting for nodes to start up..."
sleep 5  # Reduced wait time as data loading should be faster now

# Start main application
echo "Starting main application..."
python app.py