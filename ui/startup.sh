#!/bin/bash
# start-all.sh - Inicia Vue y Proxy en background

npm run dev & 
node proxy-server.js

echo "✅ Vue (8080) y Proxy (3000) iniciados"