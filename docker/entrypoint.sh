#!/bin/sh

if [ "$DEPLOYMENT_ENV" = "development" ]; then
    fastapi dev main.py --host 0.0.0.0 --port 8000
else
    fastapi run main.py --host 0.0.0.0 --port 8000
fi
