#!/bin/bash

#TODO: test stream loader shell script

# Default values
URL="http://192.168.1.10:8001/load_ticker"
FROM_DATE="2024-10-03T09:30:00-04:00"
TO_DATE="2024-10-04T00:59:32-04:00"
BATCH_SIZE=50000
COLLECTION_NAME="rawPriceColl"
TICKER="AAPL"

# Help function
function show_help {
    echo "Usage: $0 -t ticker_symbol [-f file] [-d from_date] [-e to_date] [-c collection_name] [-b batch_size] [-h]"
    echo
    echo "Options:"
    echo "  -t    Ticker symbol (required)"
    echo "  -f    File (optional)"
    echo "  -s    From date (default: $FROM_DATE)"
    echo "  -e    To date (default: $TO_DATE)"
    echo "  -c    Collection name (default: $COLLECTION_NAME)"
    echo "  -b    Batch size (default: $BATCH_SIZE)"
    echo "  -h    Show this help message"
}

# Parse options
while getopts ":t:f:s:e:c:b:h" option; do
    case $option in
        t) TICKER_SYMBOL=$OPTARG;;
        f) FILE=$OPTARG;;
        s) FROM_DATE=$OPTARG;;
        e) TO_DATE=$OPTARG;;
        c) COLLECTION_NAME=$OPTARG;;
        b) BATCH_SIZE=$OPTARG;;
        h) show_help
           exit;;
        \?) echo "Error: Invalid option"
            show_help
            exit;;
    esac
done

# Check if ticker symbol is provided
if [ -z "$TICKER_SYMBOL" ]; then
    echo "Error: Ticker symbol is required"
    show_help
    exit 1
fi

# Execute the curl command
curl -X POST $URL \
     -H "Content-Type: application/json" \
     -d "{\"ticker_symbol\": \"$TICKER_SYMBOL\", \"file\": \"$FILE\", \"from_date\":\"$FROM_DATE\", \"to_date\":\"$TO_DATE\", \"collection_name\":\"$COLLECTION_NAME\", \"batch_size\":$BATCH_SIZE}"
