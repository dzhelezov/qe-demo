#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Start node servers
python3 node_server.py --port 5000 &
python3 node_server.py --port 5001 &

# Start main application
python3 app.py