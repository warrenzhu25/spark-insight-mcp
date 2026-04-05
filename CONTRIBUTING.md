# 🤝 Contributing to Spark History Server MCP

Thank you for your interest in contributing! This guide will help you get started with contributing to the Spark History Server MCP project.

## 🚀 Quick Start for Contributors

### 📋 Prerequisites
- 🐍 Python 3.12+
- ⚡ [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager
- 🔥 Docker (for local testing with Spark History Server)
- 📦 Node.js (for MCP Inspector testing)

### 🛠️ Development Setup

1. **🍴 Fork and clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server
```

2. **📦 Install dependencies**
```bash
uv sync --group dev --frozen
```

3. **🔧 Install pre-commit hooks**
```bash
uv run pre-commit install
```

4. **🧪 Run tests to verify setup**
```bash
uv run pytest
```

## 🧪 Testing Your Changes

### 🔬 Local Testing with MCP Inspector
```bash
# Terminal 1: Start Spark History Server with sample data
./start_local_spark_history.sh

# Terminal 2: Test your changes
npx @modelcontextprotocol/inspector uv run -m spark_history_mcp.core.main
# Opens browser at http://localhost:6274 for interactive testing
```

### ✅ Run Full Test Suite
```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=. --cov-report=html

# Run specific test file
uv run pytest test_tools.py -v
```

### 🔍 Code Quality Checks
```bash
# Lint and format (runs automatically on commit)
uv run ruff check --fix
uv run ruff format

# Type checking
uv run mypy .

# Security scanning
uv run bandit -r . -f json -o bandit-report.json
```

## 📝 Contribution Guidelines

### 🎯 Areas for Contribution

#### 🔧 High Priority
- **New MCP Tools**: Additional Spark analysis tools
- **Performance Improvements**: Optimize API calls and data processing
- **Error Handling**: Better error messages and recovery
- **Documentation**: Examples, tutorials, and guides

#### 📊 Medium Priority
- **Testing**: More comprehensive test coverage
- **Monitoring**: Metrics and observability features
- **Configuration**: More flexible configuration options
- **CI/CD**: GitHub Actions improvements

#### 💡 Ideas Welcome
- **AI Agent Examples**: New integration patterns
- **Deployment**: Additional deployment methods
- **Analytics**: Advanced Spark job analysis tools

### 🔀 Pull Request Process

1. **🌿 Create a feature branch**
```bash
git checkout -b feature/your-new-feature
git checkout -b fix/bug-description
git checkout -b docs/improve-readme
```

2. **💻 Make your changes**
- Follow existing code style and patterns
- Add tests for new functionality
- Update documentation as needed
- Ensure all pre-commit hooks pass

3. **✅ Test thoroughly**
```bash
# Run full test suite
uv run pytest

# Test with MCP Inspector
npx @modelcontextprotocol/inspector uv run -m spark_history_mcp.core.main

# Test with real Spark data if possible
```

4. **📤 Submit pull request**
- Use descriptive commit messages
- Reference any related issues
- Include screenshots for UI changes
- Update CHANGELOG.md if applicable

### 💻 Code Style

We use **Ruff** for linting and formatting (automatically enforced by pre-commit):

- **Line length**: 88 characters
- **Target**: Python 3.12+
- **Import sorting**: Automatic with Ruff
- **Type hints**: Encouraged but not required for all functions

### 🧪 Adding New MCP Tools

When adding new tools, follow this pattern:

```python
@mcp.tool()
def your_new_tool(
    app_id: str,
    server: Optional[str] = None,
    # other parameters
) -> YourReturnType:
    """
    Brief description of what this tool does.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use

    Returns:
        Description of return value
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Your implementation here
    return client.your_method(app_id)
```

**Don't forget to add tests:**

```python
@patch("tools.get_client_or_default")
def test_your_new_tool(self, mock_get_client):
    """Test your new tool functionality"""
    # Setup mocks
    mock_client = MagicMock()
    mock_client.your_method.return_value = expected_result
    mock_get_client.return_value = mock_client

    # Call the tool
    result = your_new_tool("spark-app-123")

    # Verify results
    self.assertEqual(result, expected_result)
    mock_client.your_method.assert_called_once_with("spark-app-123")
```

## 🐛 Reporting Issues

### 🔍 Bug Reports
Include:
- **Environment**: Python version, OS, uv version
- **Steps to reproduce**: Clear step-by-step instructions
- **Expected vs actual behavior**: What should happen vs what happens
- **Logs**: Relevant error messages or logs
- **Sample data**: Spark application IDs that reproduce the issue (if possible)

### 💡 Feature Requests
Include:
- **Use case**: Why is this feature needed?
- **Proposed solution**: How should it work?
- **Alternatives**: Other approaches considered
- **Examples**: Sample usage or screenshots

## 📖 Documentation

### 📝 Types of Documentation
- **README.md**: Main project overview and quick start
- **CONTRIBUTING_TECHNICAL.md**: Deep dive into architecture and internal logic
- **TESTING.md**: Comprehensive testing guide
- **examples/integrations/**: AI agent integration examples
- **Code comments**: Inline documentation for complex logic

### 🎨 Documentation Style
- Use emojis consistently for visual appeal
- Include code examples for all features
- Provide screenshots for UI elements
- Keep language clear and beginner-friendly

## 🌟 Recognition

Contributors are recognized in:
- **GitHub Contributors** section
- **Release notes** for significant contributions
- **Project documentation** for major features

## 📞 Getting Help

- **💬 Discussions**: [GitHub Discussions](https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server/discussions)
- **🐛 Issues**: [GitHub Issues](https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server/issues)
- **📚 Documentation**: Check existing docs first

## 📜 Code of Conduct

- **🤝 Be respectful**: Treat everyone with kindness and professionalism
- **🎯 Stay on topic**: Keep discussions relevant to the project
- **🧠 Be constructive**: Provide helpful feedback and suggestions
- **🌍 Be inclusive**: Welcome contributors of all backgrounds and skill levels

---

**🎉 Thank you for contributing to Spark History Server MCP!**

Your contributions help make Apache Spark monitoring more intelligent and accessible to the community.
