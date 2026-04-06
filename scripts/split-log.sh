#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <file.log>" >&2
  exit 2
fi

file=$1
if [ ! -f "$file" ]; then
  echo "File not found: $file" >&2
  exit 2
fi

# Strictly less than 200 MB (decimal, 1000-based)
MAX_BYTES=$((200 * 1000 * 1000 - 1))
filesize=$(stat -c%s -- "$file")

if [ "$filesize" -le "$MAX_BYTES" ]; then
  echo "File is already smaller than 200 MB (decimal); no split required." >&2
  exit 0
fi

# Calculate number of parts and suffix width
parts=$(( (filesize + MAX_BYTES - 1) / MAX_BYTES ))
width=${#parts}
if [ "$width" -lt 1 ]; then
  width=1
fi

# Prepare prefix: basename + dot (e.g., a. -> a.0.log)
name=$(basename -- "$file")
dir=$(dirname -- "$file")
base=${name%.*}
prefix="$dir/$base."

# Use split with byte-count; pass numeric suffixes starting at 0
# --additional-suffix=.log produces final names like a.0.log
split -b "$MAX_BYTES" --numeric-suffixes=0 --suffix-length="$width" -d --additional-suffix=.log -- "$file" "$prefix"

echo "Split $file into $parts parts with max $MAX_BYTES bytes each (decimal MB limit)." >&2
