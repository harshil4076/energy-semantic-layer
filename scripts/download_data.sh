#!/usr/bin/env bash
# download_data.sh
# Downloads the UCI Household Electric Power Consumption dataset from Kaggle.
#
# Prerequisites:
#   pip install kaggle
#   Place your kaggle.json API token in ~/.kaggle/kaggle.json
#   (Get it from https://www.kaggle.com/settings → "Create New Token")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../data"

echo "── Downloading dataset from Kaggle ──"

if ! command -v kaggle &> /dev/null; then
    echo "Error: 'kaggle' CLI not found."
    echo "Install it:  pip install kaggle"
    echo "Then place your API token at ~/.kaggle/kaggle.json"
    exit 1
fi

kaggle datasets download -d uciml/electric-power-consumption-data-set \
    -p "$DATA_DIR" --unzip

# The download produces household_power_consumption.txt in data/
if [ -f "$DATA_DIR/household_power_consumption.txt" ]; then
    echo ""
    echo "✓ Data ready at: $DATA_DIR/household_power_consumption.txt"
    echo "  Next step:  python src/setup_semantic_layer.py"
else
    echo ""
    echo "✗ Expected file not found. Check the Kaggle download output above."
    exit 1
fi
