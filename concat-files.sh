#!/bin/bash

# Usage message function
usage() {
    echo "Usage: $0 [--mvnclean] [root_directory...]" >&2
    echo "Concatenates files from the specified root directories (or current directory if none provided)." >&2
    echo "Options:" >&2
    echo "  --mvnclean    Search for pom.xml from each directory and run mvn clean if found" >&2
    exit 1
}

# Initialize variables
MVNCLEAN=false
ROOT_DIRS=()
ORIGINAL_ARGS=$#

# Parse command-line arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --mvnclean)
            MVNCLEAN=true
            shift
            ;;
        *)
            ROOT_DIRS+=("$1")
            shift
            ;;
    esac
done

# Set root directories to current directory if none provided
if [ ${#ROOT_DIRS[@]} -eq 0 ]; then
    ROOT_DIRS=(".")
fi

# If no arguments were provided at all and no --mvnclean, inform user about default and show usage
if [ "$ORIGINAL_ARGS" -eq 0 ] && [ "$MVNCLEAN" = false ]; then
    echo "No root directories provided, defaulting to current directory (.)."
    usage
fi

# Validate directories
for dir in "${ROOT_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "Error: '$dir' is not a directory" >&2
        usage
    fi
    if [ ! -r "$dir" ]; then
        echo "Error: '$dir' is not readable" >&2
        usage
    fi
done

# Handle --mvnclean option
if [ "$MVNCLEAN" = true ]; then
    for dir in "${ROOT_DIRS[@]}"; do
        echo "Searching for pom.xml starting from $dir..."
        current_dir=$(realpath "$dir")
        while [ "$current_dir" != "/" ]; do
            if [ -f "$current_dir/pom.xml" ]; then
                echo "Found pom.xml in $current_dir, running mvn clean."
                cd "$current_dir" || exit 1
                mvn clean || exit 1
                cd - >/dev/null || exit 1
                break
            fi
            current_dir=$(dirname "$current_dir")
        done
        if [ "$current_dir" = "/" ] && [ ! -f "/pom.xml" ]; then
            echo "No pom.xml found in $dir or its parent directories."
        fi
    done
fi

# Output what the script will do
if [ ${#ROOT_DIRS[@]} -eq 1 ]; then
    echo "Concatenating files from ${ROOT_DIRS[0]} into part files."
else
    echo "Concatenating files from multiple directories into part files: ${ROOT_DIRS[*]}"
fi

# Constants
HEADER="concatenated sources"
SEPARATOR="---------------------------------------------"
CHAR_LIMIT=100000
PART_PREFIX="$(basename "$(pwd)")-part"
PART_EXT=".txt"

# Remove existing part files
rm -f "${PART_PREFIX}"*"${PART_EXT}"

# Initialize variables
part_num=1
char_count=0
output_file="${PART_PREFIX}${part_num}${PART_EXT}"

# Write header to first file
echo "$HEADER" > "$output_file"
char_count=$(( ${#HEADER} + 1 )) # +1 for newline

# Process each directory
for dir in "${ROOT_DIRS[@]}"; do
    # Find all files in specified directory, excluding hidden directories, and process them
    find "$dir" -type f -name "*.*" -not -path "./.*/*" -print0 | while IFS= read -r -d '' file; do
        # Print filename to console
        echo "$file"

        # Skip if file is not readable
        if [ ! -r "$file" ]; then
            echo "Warning: Skipping unreadable file: $file" >&2
            continue
        fi

        # Calculate character count for this file's contribution
        separator_count=${#SEPARATOR}
        file_marker="file $file:"
        file_marker_count=${#file_marker}
        content_count=$(cat "$file" 2>/dev/null | wc -c | tr -d ' ') || content_count=0
        if [ -z "$content_count" ] || ! [[ "$content_count" =~ ^[0-9]+$ ]]; then
            echo "Warning: Could not determine size of $file, assuming 0 characters" >&2
            content_count=0
        fi
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
        cat "$file" >> "$output_file" 2>/dev/null || echo "Warning: Failed to append $file" >&2
        echo "" >> "$output_file" # Extra newline for consistency

        # Update character count
        char_count=$(( char_count + total_addition ))
    done
done