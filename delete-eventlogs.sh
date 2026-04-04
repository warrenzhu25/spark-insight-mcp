#!/bin/bash
#
# Delete Spark event logs from GCS for short-running or name-matched applications.
#
# Uses the Spark History Server REST API to identify applications, then
# deletes matching event log files from GCS via gcloud storage.
#
# Usage:
#   ./delete-eventlogs.sh --gcs-dir gs://bucket/spark-events \
#       --shs-url http://localhost:18080 \
#       --max-duration 5m \
#       --name "my_etl_*"
#
# Options:
#   --gcs-dir        GCS directory containing event logs (required)
#   --shs-url        Spark History Server URL (default: http://localhost:18080)
#   --max-duration   Delete logs for apps that ran LESS than this duration
#                    Supports: 30s, 5m, 2h, 1d (seconds/minutes/hours/days)
#   --name           Delete logs matching this app name pattern (glob-style)
#   --dry-run        Show what would be deleted without actually deleting
#   --limit          Max number of apps to fetch from SHS (default: 1000)

set -euo pipefail

# Defaults
SHS_URL="http://localhost:18080"
GCS_DIR=""
MAX_DURATION=""
NAME_PATTERN=""
DRY_RUN=false
LIMIT=1000

usage() {
    sed -n '3,18p' "$0" | sed 's/^# \?//'
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --gcs-dir)      GCS_DIR="$2"; shift 2 ;;
        --shs-url)      SHS_URL="$2"; shift 2 ;;
        --max-duration) MAX_DURATION="$2"; shift 2 ;;
        --name)         NAME_PATTERN="$2"; shift 2 ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --limit)        LIMIT="$2"; shift 2 ;;
        -h|--help)      usage ;;
        *)              echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "$GCS_DIR" ]]; then
    echo "Error: --gcs-dir is required"
    usage
fi

if [[ -z "$MAX_DURATION" && -z "$NAME_PATTERN" ]]; then
    echo "Error: at least one of --max-duration or --name is required"
    usage
fi

# Remove trailing slash from GCS dir
GCS_DIR="${GCS_DIR%/}"

# Parse duration string to seconds
parse_duration() {
    local input="$1"
    local num="${input%[smhd]}"
    local unit="${input: -1}"

    if ! [[ "$num" =~ ^[0-9]+$ ]]; then
        echo "Error: invalid duration number: $input" >&2
        exit 1
    fi

    case "$unit" in
        s) echo "$num" ;;
        m) echo $((num * 60)) ;;
        h) echo $((num * 3600)) ;;
        d) echo $((num * 86400)) ;;
        *) echo "Error: invalid duration unit '$unit' (use s/m/h/d)" >&2; exit 1 ;;
    esac
}

# Format seconds to human-readable
format_duration() {
    local secs="$1"
    if (( secs < 60 )); then
        echo "${secs}s"
    elif (( secs < 3600 )); then
        echo "$((secs / 60))m $((secs % 60))s"
    elif (( secs < 86400 )); then
        echo "$((secs / 3600))h $((secs % 3600 / 60))m"
    else
        echo "$((secs / 86400))d $((secs % 86400 / 3600))h"
    fi
}

MAX_DURATION_SECS=""
if [[ -n "$MAX_DURATION" ]]; then
    MAX_DURATION_SECS=$(parse_duration "$MAX_DURATION")
fi

echo "=== Spark Event Log Cleanup ==="
echo "SHS URL:       $SHS_URL"
echo "GCS directory: $GCS_DIR"
[[ -n "$MAX_DURATION" ]] && echo "Max duration:  $MAX_DURATION (${MAX_DURATION_SECS}s) — delete apps running less than this"
[[ -n "$NAME_PATTERN" ]] && echo "Name pattern:  $NAME_PATTERN"
$DRY_RUN && echo "Mode:          DRY RUN"
echo ""

# Fetch applications from SHS
echo "Fetching applications from Spark History Server..."
API_URL="${SHS_URL}/api/v1/applications?limit=${LIMIT}"
APPS_JSON=$(curl -sf "$API_URL") || {
    echo "Error: failed to fetch apps from $API_URL"
    exit 1
}

# Process applications with jq
# SHS returns: [{id, name, attempts: [{startTime, endTime, duration, ...}]}]
MATCHED_APPS=$(echo "$APPS_JSON" | jq -r --arg max_dur "${MAX_DURATION_SECS:-}" --arg name_pat "${NAME_PATTERN}" '
    .[] |
    # Get the latest attempt
    . as $app |
    (.attempts // [{}]) | last as $attempt |

    # Calculate duration in seconds from the duration field (ms)
    ($attempt.duration // 0) / 1000 | floor | . as $dur_secs |

    # Apply duration filter
    (if $max_dur != "" then
        $dur_secs < ($max_dur | tonumber)
    else
        true
    end) as $dur_match |

    # Apply name filter (simple glob: * matches anything)
    (if $name_pat != "" then
        ($name_pat | gsub("\\*"; ".*") | gsub("\\?"; ".")) as $regex |
        ($app.name // "") | test($regex)
    else
        true
    end) as $name_match |

    select($dur_match and $name_match) |

    [$app.id, ($app.name // ""), ($dur_secs | tostring)] | @tsv
')

if [[ -z "$MATCHED_APPS" ]]; then
    echo "No matching applications found."
    exit 0
fi

# Count and display matches
MATCH_COUNT=$(echo "$MATCHED_APPS" | wc -l)
echo "Found $MATCH_COUNT matching application(s):"
echo ""
printf "%-45s %-40s %s\n" "APP ID" "NAME" "DURATION"
printf "%-45s %-40s %s\n" "------" "----" "--------"

while IFS=$'\t' read -r app_id app_name dur_secs; do
    printf "%-45s %-40s %s\n" "$app_id" "$app_name" "$(format_duration "$dur_secs")"
done <<< "$MATCHED_APPS"
echo ""

# Confirm deletion unless dry-run
if ! $DRY_RUN; then
    read -p "Delete event logs for these $MATCH_COUNT application(s)? [y/N] " confirm
    if [[ "$confirm" != [yY] ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# Build list of GCS files in the directory
echo "Listing GCS files in $GCS_DIR ..."
GCS_FILES=$(gcloud storage ls "$GCS_DIR/" 2>/dev/null) || {
    echo "Error: failed to list $GCS_DIR"
    exit 1
}

# Match GCS files to app IDs (handles attempt suffixes like _1, _2 and .zstd)
FILES_TO_DELETE=()
DELETED=0
SKIPPED=0

while IFS=$'\t' read -r app_id app_name dur_secs; do
    matched=false
    while IFS= read -r gcs_path; do
        if [[ "$gcs_path" == *"$app_id"* ]]; then
            FILES_TO_DELETE+=("$gcs_path")
            matched=true
        fi
    done <<< "$GCS_FILES"

    if $DRY_RUN; then
        if $matched; then
            echo "[dry-run] Would delete files matching: $app_id"
        else
            echo "[dry-run] No GCS files found for: $app_id"
        fi
    elif ! $matched; then
        echo "Warning: no GCS files found for $app_id"
        ((SKIPPED++))
    fi
done <<< "$MATCHED_APPS"

FILE_COUNT=${#FILES_TO_DELETE[@]}

if (( FILE_COUNT == 0 )); then
    echo "No GCS files found to delete."
    exit 0
fi

if $DRY_RUN; then
    echo ""
    echo "Dry run complete. $FILE_COUNT file(s) across $MATCH_COUNT app(s) would be deleted."
else
    echo ""
    echo "Deleting $FILE_COUNT file(s)..."
    if gcloud storage rm "${FILES_TO_DELETE[@]}" 2>&1; then
        DELETED=$FILE_COUNT
    else
        echo "Error: some deletions may have failed"
    fi
    echo ""
    echo "Done. Deleted: $DELETED file(s), Skipped: $SKIPPED app(s) (no GCS files found)"
fi
