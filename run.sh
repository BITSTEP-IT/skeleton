#!/bin/bash

# Run clean.sh
./docker/clean.sh
if [ $? -ne 0 ]; then
  echo "❌ Failed to execute clean.sh"
  exit 1
else
  echo "✅ Finished running clean.sh"
fi

# Run init.sh
./docker/init.sh
if [ $? -ne 0 ]; then
  echo "❌ Failed to execute init.sh"
  exit 1
else
  echo "✅ Finished running init.sh"
fi

# Run start.sh
./docker/start.sh
if [ $? -ne 0 ]; then
  echo "❌ Failed to execute start.sh"
  exit 1
else
  echo "✅ Finished running start.sh"
fi
