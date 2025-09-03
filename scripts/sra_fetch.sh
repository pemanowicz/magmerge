#!/usr/bin/env bash
# Download and organize SRA -> FASTQ(.gz)
# Requirements: SRA Toolkit (prefetch, fasterq-dump), gzip
# Usage:
#   ./sra_fetch.sh [IDS_FILE] [OUTDIR] [--limit N]
# Defaults:
#   IDS_FILE=sra_ids.txt   OUTDIR=output_directory

set -euo pipefail

IDS_FILE="${1:-sra_ids.txt}"
OUTDIR="${2:-output_directory}"
LIMIT=0   # 0 = no limit

# optional --limit argument
if [[ "${3:-}" == "--limit" ]]; then
  LIMIT="${4:-0}"
fi

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "ERROR: '$1' not found in PATH." >&2; exit 1; }
}
need prefetch
need fasterq-dump
need gzip

[[ -s "$IDS_FILE" ]] || { echo "ERROR: IDs file '$IDS_FILE' is missing or empty." >&2; exit 1; }

# Our controlled dirs
mkdir -p "$OUTDIR"
SRA_CACHE="$OUTDIR/.sra_cache"
mkdir -p "$SRA_CACHE"

OK_IDS=()
FAIL_IDS=()

count=0
while IFS= read -r acc || [[ -n "$acc" ]]; do
  acc="$(echo "$acc" | tr -d '[:space:]')"
  [[ -z "$acc" || "${acc:0:1}" == "#" ]] && continue

  # apply --limit
  if [[ "$LIMIT" -gt 0 && "$count" -ge "$LIMIT" ]]; then
    break
  fi
  count=$((count+1))

  echo "== Sample: $acc =="
  tmpdir="$(mktemp -d)"
  sample_dir="$OUTDIR/$acc"
  mkdir -p "$sample_dir"

  # 1) Fetch .sra into our local cache (NOT current dir)
  if ! prefetch --output-directory "$SRA_CACHE" "$acc"; then
    echo "  !! prefetch failed for $acc"
    FAIL_IDS+=("$acc"); rm -rf "$tmpdir"; continue
  fi

  # 2) Convert to FASTQ; write outputs to tmpdir AND force temp files into tmpdir
  if ! fasterq-dump --split-files --threads 8 --outdir "$tmpdir" --temp "$tmpdir" "$acc"; then
    echo "  !! fasterq-dump failed for $acc"
    FAIL_IDS+=("$acc"); rm -rf "$tmpdir"; continue
  fi

  # 3) Detect layout, compress, move into place
  fq1="$tmpdir/${acc}_1.fastq"
  fq2="$tmpdir/${acc}_2.fastq"
  fqS="$tmpdir/${acc}.fastq"

  moved=0
  if [[ -s "$fq1" && -s "$fq2" ]]; then
    gzip -f "$fq1" "$fq2"
    mv -f "${fq1}.gz" "$sample_dir/${acc}_1.fastq.gz"
    mv -f "${fq2}.gz" "$sample_dir/${acc}_2.fastq.gz"
    moved=1
  elif [[ -s "$fqS" ]]; then
    gzip -f "$fqS"
    mv -f "${fqS}.gz" "$sample_dir/${acc}.fastq.gz"
    moved=1
  fi

  if [[ $moved -eq 1 ]]; then
    echo "  OK: files saved under $sample_dir"
    OK_IDS+=("$acc")
  else
    echo "  !! Could not determine layout / missing FASTQ for $acc"
    FAIL_IDS+=("$acc")
  fi

  # 4) Clean temp workdir
  rm -rf "$tmpdir"
done < "$IDS_FILE"

echo "========== SUMMARY =========="
echo "SUCCESS (${#OK_IDS[@]}): ${OK_IDS[*]:-—}"
echo "FAILED  (${#FAIL_IDS[@]}): ${FAIL_IDS[*]:-—}"
