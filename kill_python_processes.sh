#!/bin/bash

# kill_python_processes.sh

# Find all Python processes
python_processes=$(ps aux | grep python | grep -v grep | awk '{print $2}')

if [ -z "$python_processes" ]; then
    echo "No Python processes found."
else
    echo "Killing the following Python processes:"
    ps aux | grep python | grep -v grep
    
    # Kill each process
    for pid in $python_processes; do
        kill -9 $pid
        echo "Killed process $pid"
    done
    
    echo "All specified Python processes have been terminated."
fi

# Additionally, kill processes on specific ports
ports=(5000 5001 8000)
for port in "${ports[@]}"; do
    pid=$(lsof -ti:$port)
    if [ ! -z "$pid" ]; then
        echo "Killing process on port $port (PID: $pid)"
        kill -9 $pid
    fi
done

echo "Cleanup complete."