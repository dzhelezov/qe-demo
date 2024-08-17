# Makefile for Distributed SQL Query Engine Demo

# Define the shell to use
SHELL := /bin/bash

# Define commands
.PHONY: run stop

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

# Help command
help:
	@echo "Available commands:"
	@echo "  make run  - Run the demo"
	@echo "  make stop - Stop the demo"
	@echo "  make help - Show this help message"