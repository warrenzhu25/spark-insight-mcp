Plan: Smart Filter for Environment Comparison

Goal
- Reduce noisy diffs in `compare env` by filtering or normalizing auto-generated and volatile values while preserving meaningful config changes.

Scope
- Implement in `src/spark_history_mcp/tools/comparison_modules/utils.py` inside `_compare_environments`.
- Keep existing `filter_auto_generated` behavior; make it “smarter” by default without breaking CLI/API.

Approach
1) Define a small, explicit set of “volatile key” patterns (existing ones) plus a new “volatile value” classifier.
   - Key-based: keep current `auto_gen_patterns`, consider adding a few high-noise keys (e.g., driver/executor pod IDs if present).
   - Value-based: detect and normalize values that look like:
     - UUIDs, Kubernetes pod IDs, Spark app IDs
     - IPs, hostnames, ephemeral ports
     - Timestamps/epoch millis
     - Temp paths or container-specific paths
2) Normalize instead of drop when possible.
   - Example: `spark.driver.host=pod-abc` → `<auto:hostname>`
   - Example: `spark.app.id=spark-...` → `<auto:app_id>`
   - This preserves that the property exists while reducing noisy diffs.
3) Add unit tests for environment comparison:
   - keys filtered by pattern still suppressed
   - normalized values do not show as “different”
   - meaningful differences still appear
4) Update CLI docs briefly to mention smart filtering and how to disable (via `filter_auto_generated=False` if exposed later).

Out of Scope (for now)
- Full value diff tuning per platform (YARN vs K8s vs standalone).
- New CLI flags or config for custom patterns.

Acceptance Criteria
- `compare env` output no longer shows noisy diffs for auto-generated IDs/hosts/timestamps.
- Existing tests pass; new tests cover the smart-filter behavior.
