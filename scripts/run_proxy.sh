#!/bin/bash

if [ -z "$TG_TOPIC_ID" ]; then
    echo "ERROR: TG_TOPIC_ID not set" >&2
    exit 1
fi
if [ -z "$TG_CHAT_ID" ]; then
    echo "ERROR: TG_CHAT_ID not set" >&2
    exit 1
fi

while true; do
    echo "[$(date '+%H:%M:%S')] proxy topic=$TG_TOPIC_ID starting..." >&2
    tg-mcp-proxy
    EXIT_CODE=$?
    echo "[$(date '+%H:%M:%S')] proxy exited code=$EXIT_CODE, restart in 1s..." >&2
    sleep 1
done
