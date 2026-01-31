MCP Tools Reference
===================

This document lists all MCP tools exposed via `@mcp.tool`, with response shape
summaries and minimal sample outputs. All tools return JSON-like dictionaries.
On failure, most tools include an `error` field.

Conventions
-----------
- `app_id`, `stage_id`, `job_id`, `executor_id` are Spark identifiers.
- Timestamps are ISO strings unless noted.
- Numeric units are noted per field (ms, bytes, GB, etc).

Applications
------------

### `list_applications`
Response format:
- list of applications (id, name, state, start/end times)
Sample:
```
[
  {"id": "app-20250131-0001", "name": "Daily ETL", "state": "FINISHED"},
  {"id": "app-20250131-0002", "name": "Backfill", "state": "FAILED"}
]
```

### `get_application`
Response format:
- application details (id, name, attempts, cores, memory, etc)
Sample:
```
{"id": "app-20250131-0001", "name": "Daily ETL", "attempts": [...]}
```

### `get_environment`
Response format:
- spark_properties, system_properties, classpath_entries, runtime info
Sample:
```
{
  "spark_properties": [["spark.sql.shuffle.partitions", "200"]],
  "system_properties": [["java.version", "17"]],
  "runtime": {"java_version": "17", "scala_version": "2.12"}
}
```

### `get_application_insights`
Response format:
- insights, bottlenecks, recommendations
Sample:
```
{"application_id": "app-20250131-0001", "insights": [...], "recommendations": [...]}
```

### `get_app_summary`
Response format:
- compact app summary metrics
Sample:
```
{
  "application_id": "app-20250131-0001",
  "application_duration_minutes": 8.4,
  "input_data_size_gb": 3.6,
  "total_stages": 10
}
```

Jobs and Stages
--------------

### `list_jobs`
Response format:
- list of job summaries
Sample:
```
[
  {"job_id": 1, "name": "Job 1", "status": "SUCCEEDED"},
  {"job_id": 2, "name": "Job 2", "status": "FAILED"}
]
```

### `list_slowest_jobs`
Response format:
- list of slowest jobs with durations
Sample:
```
[
  {"job_id": 5, "name": "Sort", "duration_seconds": 120.5}
]
```

### `list_stages`
Response format:
- list of stage summaries
Sample:
```
[
  {"stage_id": 3, "name": "Shuffle", "status": "COMPLETE", "num_tasks": 200}
]
```

### `list_slowest_stages`
Response format:
- list of slowest stages with durations
Sample:
```
[
  {"stage_id": 7, "name": "Join", "duration_seconds": 287.0}
]
```

### `get_stage`
Response format:
- stage details (metrics, tasks, status)
Sample:
```
{"stage_id": 3, "name": "Shuffle", "status": "COMPLETE", "num_tasks": 200}
```

### `get_stage_task_summary`
Response format:
- task-level summary and metric distributions
Sample:
```
{"stage_id": 3, "task_time": {"min": 10, "median": 20, "max": 200}}
```

### `list_slowest_sql_queries`
Response format:
- list of slowest SQL executions
Sample:
```
[
  {"id": 12, "description": "SELECT ...", "duration_ms": 120000}
]
```

Executors
---------

### `list_executors`
Response format:
- list of executors with metrics
Sample:
```
[
  {"id": "1", "host_port": "10.0.0.1:12345", "completed_tasks": 100}
]
```

### `get_executor`
Response format:
- executor details and metrics
Sample:
```
{"id": "1", "is_active": true, "failed_tasks": 2, "max_memory": 1073741824}
```

### `get_executor_summary`
Response format:
- aggregated executor metrics for the app
Sample:
```
{"total_executors": 4, "active_executors": 3, "total_cores": 16}
```

### `get_resource_usage_timeline`
Response format:
- time series of resource usage
Sample:
```
{"interval_minutes": 1, "data_points": [{"timestamp": "...", "cpu": 0.7}]}
```

Analysis
--------

### `get_job_bottlenecks`
Response format:
- bottleneck analysis for a job
Sample:
```
{"job_id": 5, "bottlenecks": [...], "recommendations": [...]}
```

### `analyze_auto_scaling`
Response format:
- auto-scaling analysis and recommendations
Sample:
```
{"analysis_type": "Auto-scaling Configuration", "recommendations": {...}}
```

### `analyze_shuffle_skew`
Response format:
- skewed stages and recommendations
Sample:
```
{"skewed_stages": [...], "summary": {...}, "recommendations": [...]}
```

### `analyze_failed_tasks`
Response format:
- failed stages, problematic executors, summary
Sample:
```
{
  "failed_stages": [{"stage_id": 3, "failed_tasks": 5}],
  "problematic_executors": [{"executor_id": "2", "failed_tasks": 4}],
  "summary": {"total_failed_tasks": 9}
}
```

Comparisons
-----------

### `compare_app_performance`
Response format:
- aggregated overview, stage deep dive, app summary diff, recommendations
Sample:
```
{
  "applications": {"app1": {"id": "a1"}, "app2": {"id": "a2"}},
  "aggregated_overview": {...},
  "stage_deep_dive": {...},
  "app_summary_diff": {...},
  "recommendations": [...]
}
```

### `compare_app_summaries`
Response format:
- `app1_summary`, `app2_summary`, `diff`, optional `aggregated_stage_comparison`
Sample:
```
{
  "app1_summary": {"application_id": "a1", "application_duration_minutes": 8.4},
  "app2_summary": {"application_id": "a2", "application_duration_minutes": 5.1},
  "diff": {"application_duration_minutes_change": "-40.0%"},
  "aggregated_stage_comparison": {...}
}
```

### `compare_app_executors`
Response format:
- executor metrics and efficiency ratios
Sample:
```
{"applications": {...}, "executor_comparison": {...}, "efficiency_metrics": {...}}
```

### `compare_app_jobs`
Response format:
- job statistics and ratios
Sample:
```
{"job_statistics": {"app1": {...}, "app2": {...}}, "job_performance_comparison": {...}}
```

### `compare_app_stages_aggregated`
Response format:
- aggregated stage metrics and ratios
Sample:
```
{"aggregated_stage_metrics": {"app1": {...}, "app2": {...}}, "stage_performance_comparison": {...}}
```

### `compare_app_resources`
Response format:
- resource ratios and recommendations
Sample:
```
{"resource_comparison": {"cores_granted_ratio": 1.5}, "recommendations": [...]}
```

### `compare_app_environments`
Response format:
- spark/system property diffs, JVM info, summary
Sample:
```
{"spark_properties": {...}, "system_properties": {...}, "jvm_info": {...}, "summary": {...}}
```

### `compare_app_executor_timeline`
Response format:
- executor timeline comparison and efficiency
Sample:
```
{"timeline_comparison": [...], "resource_efficiency": {...}}
```

### `compare_stages`
Response format:
- stage comparison, significant differences, summary
Sample:
```
{"stage_comparison": {...}, "significant_differences": {...}, "summary": {...}}
```

### `compare_stage_executor_timeline`
Response format:
- stage-level executor timeline comparison
Sample:
```
{"timeline_comparison": [...], "stage_ids": {"app1": 1, "app2": 1}}
```

### `find_top_stage_differences`
Response format:
- top stage differences and analysis parameters
Sample:
```
{
  "top_stage_differences": [{"stage_name": "Join", "time_difference": {...}}],
  "analysis_parameters": {"top_n": 3}
}
```
