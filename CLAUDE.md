# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Setup and Dependencies
```bash
# Complete development environment setup
task dev-setup

# Install dependencies only
task install

# Start all development services (Spark + MCP + Inspector)
task dev-all

# Stop all background services
task stop-all
```

### Testing and Validation
```bash
# Run tests
task test

# Run tests with verbose output
task test-verbose

# Run end-to-end tests (starts services automatically)
task test-e2e

# Run linting
task lint

# Run linting with auto-fix
task lint-fix

# Run code formatting
task format

# Run type checking
task type-check

# Run all validation checks
task validate

# Run full CI pipeline locally
task ci
```

### Development Servers
```bash
# Start Spark History Server in background
task start-spark-bg

# Start MCP server in background
task start-mcp-bg

# Start MCP Inspector for interactive testing
task start-inspector-bg
# Then open http://localhost:6274

# Start Spark with specific version
task start-spark-bg spark_version=3.5.2
```

### Direct Python Commands
```bash
# Run MCP server directly
uv run -m spark_history_mcp.core.main

# Run specific tests
uv run pytest tests/unit/test_tools.py -v

# Run with coverage
uv run pytest --cov=. --cov-report=term-missing
```

## Architecture

### Core Structure
- **MCP Server**: FastMCP-based server providing 50+ Spark analysis tools
- **Spark Client Layer**: HTTP REST clients for Spark History Server API interaction
- **Tool Layer**: Extensive tool suite organized by analysis patterns
- **Configuration**: YAML-based multi-server configuration with environment variable support
- **Prompts System**: 16 intelligent prompts for structured Spark analysis

### Key Components
- `src/spark_history_mcp/core/app.py` - Main FastMCP server setup and client management
- `src/spark_history_mcp/tools/tools.py` - Complete tool implementation (50+ tools)
- `src/spark_history_mcp/api/spark_client.py` - Spark History Server REST API client
- `src/spark_history_mcp/api/emr_persistent_ui_client.py` - AWS EMR integration client
- `src/spark_history_mcp/prompts/` - Intelligent prompt templates by category
- `src/spark_history_mcp/config/config.py` - Configuration management with Pydantic
- `config.yaml` - Main configuration file for servers and MCP settings

### Tool Categories
The server provides tools organized by analysis patterns:
- **Application Info** - Basic metadata and overview
- **Job Analysis** - Job-level performance analysis
- **Stage Analysis** - Stage-level deep dive and task metrics
- **Executor & Resource Analysis** - Resource utilization tracking
- **SQL & Query Analysis** - SQL performance and execution plans
- **Comparative Analysis** - Cross-application comparisons (8+ tools)
- **SparkInsight Intelligence** - AI-powered analysis tools (5 tools)
- **Intelligent Prompts** - Structured analysis templates (16 prompts)

### Multi-Server Support
The server supports multiple Spark History Servers configured in `config.yaml`:
- Each server has a name (e.g., "local", "production", "staging")
- One server marked as `default: true`
- Tools accept optional `server` parameter to target specific servers
- Supports authentication, SSL verification, and AWS EMR integration

### AWS Integration
- **EMR Support**: Direct integration with EMR Persistent UI via cluster ARN
- **Glue Support**: Compatible with Glue Spark History Server
- Automatic presigned URL generation and session management for EMR

## Testing Strategy

### Test Data
The repository includes real Spark event logs in the root directory:
- `spark-bcec39f6201b42b9925124595baad260` - Successful ETL job
- `spark-110be3a8424d4a2789cb88134418217b` - Data processing job
- `spark-cc4d115f011443d787f03a71a476a745` - Multi-stage analytics job

### Development Workflow
1. Start services: `task dev-all`
2. Test changes: `task test`
3. Validate code: `task validate`
4. Run E2E tests: `task test-e2e`
5. Clean up: `task stop-all`

### Integration Testing
- E2E tests require running Spark History Server with test data
- MCP Inspector provides interactive testing at http://localhost:6274
- Tests cover tool functionality, configuration, and AWS integrations

## Configuration

### Main Config (`config.yaml`)
- Server definitions with URLs, auth, and SSL settings
- MCP transport configuration (stdio or streamable-http)
- Debug and port settings
- Environment variable substitution support

### Environment Variables
All configuration can be overridden via environment variables with pattern:
- `SHS_MCP_*` - MCP server settings
- `SHS_SERVERS_*_*` - Server-specific settings (URL, auth, SSL, etc.)

## Key Development Notes

### Code Style
- Python 3.12+ required
- Uses Ruff for linting and formatting (88-char line length)
- Type checking with MyPy (gradually enabling strict typing)
- Pre-commit hooks for automated validation

### Tool Development
- All tools implemented in `tools/tools.py` as FastMCP tools
- Tools follow consistent parameter patterns (app_id, server, etc.)
- Significance filtering for comparative analysis tools
- Enhanced error handling and logging throughout

### Performance Considerations
- Timeline comparisons use intelligent interval merging
- Significance thresholds filter noise in comparative analysis
- Streamlined output focusing on actionable insights
- Efficient HTTP session reuse across API calls

## Testing Quick Reference

```bash
# Full development setup
task dev-setup && task dev-all

# Quick validation
task validate

# Interactive testing
task start-inspector-bg && open http://localhost:6274

# Clean restart
task stop-all && task dev-all
```