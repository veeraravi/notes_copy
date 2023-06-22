#!/bin/bash

output_file="row_counts.txt"
source_dir="/path/to/files"

# Remove the existing output file if it exists
rm -f "$output_file"

# Loop through all files in the source directory
for file in "$source_dir"/*; do
    if [[ -f "$file" ]]; then
        # Get the filename without the path
        filename=$(basename "$file")

        # Count the number of rows in the file (excluding the header)
        row_count=$(( $(wc -l < "$file") - 1 ))

        # Append the filename and row count to the output file
        echo "$filename ====> $row_count" >> "$output_file"
    fi
done
