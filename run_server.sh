#!/bin/bash
export TELEGRAM_API_ID=39740403
export TELEGRAM_API_HASH=1a6441d48f5fa81b527dca42cf40b661
export TG_TARGET_CHAT=-1003998609906
export TG_TARGET_THREAD=205

while true; do
    tg-mcp-proxy proxy
    sleep 1
done
