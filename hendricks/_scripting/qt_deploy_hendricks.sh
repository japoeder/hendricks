#!/bin/bash

# Ensure the environment variables are set
if [[ -z "$HENDRICKS_PATH" || -z "$SCRIPTING_PATH" ]]; then
  echo "Error: HENDRICKS_PATH and SCRIPTING_PATH must be set."
  exit 1
fi

# Define the service name
SERVICE_NAME="hendricks.service"

# Pull the latest changes from the Git repository
cd "$HENDRICKS_PATH"
GIT_OUTPUT=$(git pull)

# Check if the repository is already up to date
if [[ "$GIT_OUTPUT" == *"Already up to date."* ]]; then
  echo "Repository is already up to date."
else
  echo "Repository updated."
fi

# Copy scripts to the target directory
cp "$HENDRICKS_PATH/hendricks/_sh/"* "$SCRIPTING_PATH/"
cp "$HENDRICKS_PATH/hendricks/_scripting/"* "$SCRIPTING_PATH/"

# Restart the service using the environment variable for the password
echo "$DS_PWD" | sudo -S systemctl restart "$SERVICE_NAME"

# end of script