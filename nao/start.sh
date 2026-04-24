#!/bin/bash
set -a
source "$(dirname "$0")/.env"
set +a

# Generate nao_config.yaml from template with env vars substituted
envsubst < "$(dirname "$0")/nao_config.yaml.template" > "$(dirname "$0")/nao_config.yaml"

cd "$(dirname "$0")" && nao chat
