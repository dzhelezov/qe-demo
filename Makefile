# Makefile for Distributed SQL Query Engine Demo

# Define the shell to use
SHELL := /bin/bash

# Define commands
.PHONY: run stop kill

# Run the demo
run:
	@echo "Starting the demo..."
	@chmod +x run_demo.sh
	@./run_demo.sh

# Stop the demo
stop:
	@echo "Stopping the demo..."
	@lsof -ti:5000,5001,8000 | xargs kill -9 || true
	@echo "Demo stopped."

# Kill all related Python processes
kill:
	@echo "Killing all related Python processes..."
	@chmod +x kill_python_processes.sh
	@./kill_python_processes.sh

# Help command
help:
	@echo "Available commands:"
	@echo "  make run   - Run the demo"
	@echo "  make stop  - Stop the demo (only kills processes on ports 5000, 5001, 8000)"
	@echo "  make kill  - Kill all related Python processes"
	@echo "  make help  - Show this help message"