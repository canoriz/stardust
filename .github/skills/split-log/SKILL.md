---
name: split-log
description: |
  Split large log files into parts strictly smaller than 200 MiB.
  Produces parts named like `a.0.log`, `a.1.log`, ... for an input `a.log`.
---

# Split Log Skill

## Intent

Split a large log file into several smaller files where each part is strictly less than 200 MB (decimal, 200 * 1000 * 1000 bytes). The resulting filenames follow the pattern `<basename>.<index>.log` (e.g. `a.0.log`, `a.1.log`).

## Scope

- Workspace-scoped skill. Place this file at `.github/skills/split-log/SKILL.md` so agents can discover it.
- Provides a reproducible bash snippet, an example `scripts/split-log.sh`, and a short alternative `split -n` variant.

## When to use

- Log rotation or archival when a single `.log` file exceeds 200 MiB.
- Pre-processing large logs for upload, analysis, or version control.

## Step-by-step procedure

1. Check the input file exists and measure its size.
2. Compute the maximum bytes per part: `MAX_BYTES = 200 * 1000 * 1000 - 1` (one byte less than 200 MB, decimal).
3. Compute the number of parts required: `parts = ceil(filesize / MAX_BYTES)`.
4. Compute the numeric suffix width: `width = number_of_digits(parts - 1)` (at least 1).
5. Run `split` to produce parts named `<base>.<index>.log`.

## Example Bash implementation

Save this snippet as `scripts/split-log.sh` or run it directly in a shell. This script is also provided in `scripts/`.

```bash
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

echo "Split $file into $parts parts with max $MAX_BYTES bytes each." >&2
```

## Alternative split command

If you prefer using the `split -n l/{parts}` form to ask `split` to produce a specific number of files, compute `parts` first and run with numeric suffixes and an additional suffix. Example:

```bash
# compute number of parts (strictly less than 200 MB each, decimal)
MAX_BYTES=$((200 * 1000 * 1000 - 1))
filesize=$(stat -c%s -- "$file")
parts=$(( (filesize + MAX_BYTES - 1) / MAX_BYTES ))

# split into `parts` files named like a.0.log, a.1.log, ...
prefix="$dir/$base."
split -n l/$parts -d --additional-suffix=.log -- "$file" "$prefix"
```

Notes:
- `-n l/$parts` asks `split` to produce `$parts` output files of (nearly) equal size; ensure `$parts` is computed as above so each file will be strictly below 200 MB (decimal).
- Behavior depends on GNU `split` version; prefer the byte-wise `-b` approach when you need exact per-file byte limits.

## Quality checks

- Verify every produced part is smaller than 200 MiB:

  ```bash
  for f in a.*.log; do stat -c "%n %s" "$f"; done
  ```

- Verify concatenation restores original file:

  ```bash
  cat a.*.log > a.rejoined.log
  cmp --silent a.log a.rejoined.log && echo "OK" || echo "DIFFER"
  ```

## Example prompts to try

- "Split a.log into parts smaller than 200MiB and name them a.0.log, a.1.log..."
- "Provide a one-line command to split large.log into parts under 200 MiB."

## Related customizations

- Add a wrapper that compresses each part with `gzip` after splitting.
- Create a CI job that automatically archives and uploads parts to object storage.

