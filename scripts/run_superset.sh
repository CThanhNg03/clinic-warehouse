#!/bin/sh

# Load environment variables from .env
export $(grep -v '^#' .env | xargs)

# Activate virtualenv
. "$SUPERSET_HOME/bin/activate"

# Set SUPERSET_CONFIG_PATH to parent of current directory + /config/superset_config.py
export SUPERSET_CONFIG_PATH="$(dirname "$(pwd)")/../config/superset_config.py"

# Optional: echo to confirm
echo "Using config: $SUPERSET_CONFIG_PATH"

# Run Superset
superset run --port "$SUPERSET_PORT"
