# Optimize `compare_app_performance` Output Size

## Problem
The `compare_app_performance` tool returns too much data, exceeding LLM context windows. The main bloat sources are:

1. **`environment_comparison`** — Lists every differing/unique spark and system property (can be 100+ entries)
2. **`executor_summary_comparison`** — Includes full raw `exec1`/`exec2` summary dicts for both apps
3. **Duplicate data** — `aggregated_overview` duplicates info already in `app_summary_diff`; `recommendations` (full list) overlaps with `key_recommendations`
4. **`stage_deep_dive`** — Each stage has verbose nested metric groups, many with zero values

---

## Change 1: Limit environment comparison output

**File:** `src/spark_history_mcp/tools/comparison_modules/utils.py`
**Function:** `_compare_environments`

- Define a priority list of performance-relevant spark properties (executor memory/cores, shuffle partitions, dynamic allocation, AQE, etc.)
- Sort `spark_properties.different` so priority properties come first, then cap at 10 entries
- Cap `system_properties.different` at 5 entries
- Replace `app1_only` and `app2_only` lists with just counts
- Add a `note` field to `summary`: `"Use get_environment tool for full property details"`

## Change 2: Remove raw executor summaries

**File:** `src/spark_history_mcp/tools/comparison_modules/executors.py`
**Function:** `compare_app_executors`

- Remove full `app1_summary` and `app2_summary` dicts from the return value
- Replace with a `key_metrics` subset containing only: `total_executors`, `active_executors`, `completed_tasks`, `failed_tasks`, `executor_utilization_percent`
- Keep `efficiency_ratios` as-is

## Change 3: Remove redundant sections from top-level output

**File:** `src/spark_history_mcp/tools/comparison_modules/core.py`
**Function:** `compare_app_performance`

- Remove always-empty `application_summary` and `job_performance` keys from `aggregated_overview`
- Remove full `recommendations` list from result (keep only `key_recommendations`)

## Change 4: Filter zero-value metrics in stage deep dive

**File:** `src/spark_history_mcp/tools/comparison_modules/stages.py`

- In `_build_stage_metrics_comparison`: filter out metric groups where all numeric values are 0
- In `_build_executor_analysis`: same pattern — filter out entries where all values are 0

## Files to Modify

| File | Change |
|------|--------|
| `src/spark_history_mcp/tools/comparison_modules/utils.py` | Cap env diffs (10 spark, 5 system), remove app-only lists |
| `src/spark_history_mcp/tools/comparison_modules/executors.py` | Replace raw summaries with key_metrics subset |
| `src/spark_history_mcp/tools/comparison_modules/core.py` | Remove empty keys, remove full recommendations list |
| `src/spark_history_mcp/tools/comparison_modules/stages.py` | Filter zero-value metric groups |
