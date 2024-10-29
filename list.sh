#!/bin/bash

# Directory to scan, default to current if not provided
dir="${1:-.}"

# Function to process each directory
process_directory() {
    local current_dir="$1"
    find "$current_dir" -type f \( \
        -name '*.py' -o -name '*.html' -o -name '*.js' -o -name '*.css' -o \
        -name '*.yaml' -o -name '*.txt' -o -name '*.sql' \) \
        -exec echo "" \; \
        -exec echo "" \; \
        -exec echo "### File: {}" \; \
        -exec echo '```' \; \
        -exec cat "{}" \; \
        -exec echo "" \; \
        -exec echo '```' \; \
        -exec echo "" \; \
        -exec echo "" \;
}

# Export results to clipboard
process_directory "$dir" | pbcopy

echo "File listings have been copied to the clipboard."
