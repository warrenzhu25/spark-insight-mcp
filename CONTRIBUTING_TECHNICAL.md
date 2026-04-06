# 🛠️ Technical Contribution Guide (Deep Dive)

This guide provides an exhaustive technical overview of the Spark History Server MCP project, including low-level architecture, implementation patterns, and code examples.

---

## 🏗️ Architecture Deep Dive

The project follows a layered architecture to separate concerns and ensure maintainability.

### 🧩 Layered Structure

1.  **`api/` (Data Acquisition)**: Low-level REST clients. No business logic, only HTTP communication and Pydantic model instantiation.
2.  **`fetchers.py` (Caching & Orchestration)**: A middle layer that handles data fetching, two-tier caching (in-process and disk), and client resolution.
3.  **`tools/` (Business Logic)**: Implements MCP tools. Uses fetchers to get data, processes it, and returns results.
4.  **`comparison_modules/` (Complex Analysis)**: Specialized sub-modules for comparing applications, environments, and stages.
5.  **`models/` (Type Safety)**: Pydantic models mapping Spark's complex JSON responses to Python objects.

---

## 📡 API Client Implementation (`api/`)

The `SparkRestClient` maps Python methods to Spark REST API endpoints.

### Adding a New Endpoint
When adding a new endpoint, follow this pattern:

```python
# src/spark_history_mcp/api/spark_client.py

def get_executor_threads(self, app_id: str, executor_id: str) -> List[ThreadStackTrace]:
    """Fetch thread stack traces for a specific executor."""
    endpoint = f"applications/{app_id}/executors/{executor_id}/threads"
    # _get() automatically prepends /api/v1 and handles auth
    data = self._get(endpoint)
    # Validate and return as Pydantic models
    return [ThreadStackTrace.model_validate(item) for item in data]
```

### URL Normalization
Spark UI often needs attempt IDs. `SparkRestClient` uses a regex pattern to inject `1/` if an attempt ID is missing from YARN-based URLs.

---

## 💾 Caching & Fetching (`fetchers.py`)

Tools should **never** call the API client directly. Use `fetchers.py` instead.

### Two-Tier Caching
1.  **In-Process Cache (`_CACHE`)**: A simple dictionary for the duration of the process.
2.  **Disk Cache (`cache.py`)**: Persistent storage (default: `~/.cache/spark-mcp/`) to avoid re-fetching large datasets across sessions.

### Using Fetchers
```python
# src/spark_history_mcp/tools/your_tool.py
from .fetchers import fetch_stages, fetch_app

def your_logic(app_id: str):
    # This will check in-process cache, then disk, then call the API
    app = fetch_app(app_id)
    stages = fetch_stages(app_id)
    ...
```

---

## 🛠️ Tool Development

All MCP tools are registered using the `@mcp.tool()` decorator.

### Implementation Pattern
```python
@mcp.tool()
def analyze_data(app_id: str, server: Optional[str] = None) -> Dict[str, Any]:
    """Description for AI agents."""
    # 1. Resolve client/context
    client = get_client_or_default(server_name=server)

    # 2. Fetch data via fetchers
    data = fetch_some_data(app_id, server=server)

    # 3. Process logic
    results = complex_calculation(data)

    # 4. Return as Dict (for MCP compatibility)
    return {"status": "success", "results": results}
```

---

## 📊 Comparison Logic & Return Types

Comparison tools (`compare_app_performance`, etc.) are located in `tools/comparison_modules/`.

### How Comparison Works
1.  **Baseline vs. Target**: `app_id1` is treated as the baseline, `app_id2` as the comparison target.
2.  **Significance Filtering**: Metrics are filtered using a `significance_threshold` (default 0.1 or 10%).
3.  **Normalization**: Values like paths, IDs, and IPs are normalized to `<auto>` tokens in `utils._compare_environments` to reduce noise.

### Return Structure & Validation
Most comparison tools return a `Dict[str, Any]`. For complex outputs, we use `schema.py` to define the expected structure.

```python
# src/spark_history_mcp/tools/schema.py
class CompareAppPerformanceOutput(BaseModel):
    applications: Dict[str, Any]
    performance_comparison: Dict[str, Any]
    environment_comparison: Dict[str, Any]
    key_recommendations: List[Dict[str, Any]]
```

Validation can be enabled in `config.yaml` via `debug_validate_schema: true`.

---

## 🧬 Data Modeling (`spark_types.py`)

We use Pydantic extensively for data validation and transformation.

### Advanced Pydantic Patterns
```python
class StageData(BaseModel):
    stage_id: int = Field(alias="stageId")
    status: StageStatus

    # Computed fields for calculated metrics
    @computed_field
    @property
    def duration_ms(self) -> int:
        if self.completion_time and self.submission_time:
            return int((self.completion_time - self.submission_time).total_seconds() * 1000)
        return 0

    # Custom compaction logic for CLI/MCP output
    def to_compact_dict(self) -> Dict[str, Any]:
        return {
            "id": self.stage_id,
            "duration": self.duration_ms,
            ...
        }
```

---

## 💡 Prompt Engineering

Prompts guide AI agents on how to use tools effectively.

```python
@mcp.prompt()
def investigate_skew(app_id: str) -> str:
    return f"""Analyze shuffle skew for {app_id}.
    1. Run analyze_shuffle_skew("{app_id}")
    2. Check the stages with extreme ratios (> 2.0)
    3. Investigate the partitioning strategy...
    """
```

---

## 🧪 Testing Strategy

We use `unittest` with `patch` to mock Spark History Server responses.

### Mocking Example
```python
@patch("requests.Session.request")
def test_your_tool(self, mock_request):
    # 1. Setup mock response
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": "app-123", "name": "test"}
    mock_request.return_value = mock_response

    # 2. Call tool
    result = get_application("app-123")

    # 3. Assert
    self.assertEqual(result["id"], "app-123")
    mock_request.assert_called_once()
```

Refer to `tests/unit/test_tools.py` for comprehensive examples.
