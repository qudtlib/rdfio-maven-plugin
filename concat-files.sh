#!/bin/bash

rm -f rdfio-maven-plugin-part*.txt

# Run mvn clean
mvn clean || exit 1

# Constants
HEADER="rdfio-maven-plugin concatenated sources"
SEPARATOR="---------------------------------------------"
CHAR_LIMIT=100000
PART_PREFIX="rdfio-maven-plugin-part"
PART_EXT=".txt"

# Initialize variables
part_num=1
char_count=0
output_file="${PART_PREFIX}${part_num}${PART_EXT}"

# Write header to first file
echo "$HEADER" > "$output_file"
char_count=$(( ${#HEADER} + 1 )) # +1 for newline

# Find all files, excluding hidden directories, and process them
find src -type f -name "*.*" -not -path "./.*/*" -print0 | while IFS= read -r -d '' file; do
    # Print filename to console
    echo "$file"

    # Calculate character count for this file's contribution
    separator_count=${#SEPARATOR}
    file_marker="file $file:"
    file_marker_count=${#file_marker}
    content_count=$(wc -c < "$file" | tr -d ' ')
    total_addition=$(( separator_count + 1 + file_marker_count + 1 + content_count + 1 )) # +1 for each newline

    # Check if adding this file exceeds the limit
    if [ $(( char_count + total_addition )) -gt $CHAR_LIMIT ]; then
        # Start a new file
        part_num=$(( part_num + 1 ))
        output_file="${PART_PREFIX}${part_num}${PART_EXT}"
        echo "$HEADER" > "$output_file"
        char_count=$(( ${#HEADER} + 1 ))
    fi

    # Append to current file
    echo "$SEPARATOR" >> "$output_file"
    echo "$file_marker" >> "$output_file"
    cat "$file" >> "$output_file"
    echo "" >> "$output_file" # Extra newline for consistency

    # Update character count
    char_count=$(( char_count + total_addition ))
done