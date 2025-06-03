#!/bin/sh
# filepath: src/entrypoint.sh

# Run AuthT.py to update .env and generate SESSION_ID
python AuthT.py

# Export the new SESSION_ID to the environment
export SESSION_ID=$(grep SESSION_ID .env | cut -d '=' -f2-)

export FLASK_APP=flasks.py

# Start the Flask app
exec flask run --host=0.0.0.0 --port=5000