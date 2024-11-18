#!/bin/bash

# Define variables
SERVER_USER="japoeder"
SERVER_IP="192.168.1.10"
REMOTE_DIR="home/pydev/quantum_trade/hendricks"
SERVICE_NAME="hendricks.service"

# SSH into the server and execute commands
ssh ${SERVER_USER}@${SERVER_IP} << EOF
  cd ${REMOTE_DIR} || exit
  git pull
  sudo systemctl restart ${SERVICE_NAME}
EOF
