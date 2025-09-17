# 🏗️ Build Pipeline Documentation

This document describes the comprehensive build pipeline setup for SparkInsight AI.

## 📋 Pipeline Overview

The build pipeline consists of several automated workflows that ensure code quality, security, and reliability:

### 🔄 CI Workflow (`.github/workflows/ci.yml`)

**Triggers:** Push to `main`/`develop`, Pull Requests to `main`

**Jobs:**

1. **Code Quality & Security** (`code-quality`)
   - ✅ Pre-commit hooks validation
   - 🔒 Security scanning with Bandit
   - 📤 Security report artifacts

2. **Tests** (`test`)
   - 🐍 Multi-Python version testing (3.12, 3.13)
   - 🖥️ Multi-OS testing (Ubuntu, Windows, macOS)
   - 🧪 Unit tests with coverage
   - 🔍 Type checking with MyPy
   - 📋 Linting with Ruff
   - 📊 Coverage reporting to Codecov

3. **Integration Tests** (`integration`)
   - 🔄 End-to-end testing with Spark History Server
   - 🧪 MCP server integration tests

4. **Dependency Security** (`dependency-check`)
   - 🔒 Vulnerability scanning with Safety
   - 📦 Dependency analysis

5. **Build Validation** (`build-test`)
   - 📦 Package building test
   - ✅ Installation verification
   - 🎯 CLI functionality test

### 🏗️ Build Workflow (`.github/workflows/build.yaml`)

**Triggers:** Push to `main`, Pull Requests to `main`

**Jobs:**
- 🔍 Pre-commit validation
- ⚡ Fast quality checks

### 🚀 Release Workflow (`.github/workflows/release.yaml`)

**Triggers:** Version tags (`v*.*.*`)

**Jobs:**
- 🐳 Docker image build and push to GitHub Container Registry
- 📦 Python package build and publish to PyPI
- 🔒 Trusted publishing with OIDC

## 🛠️ Development Tools

### Task Runner (`Taskfile.yml`)

**Core Commands:**
```bash
task install        # Install dependencies
task lint           # Run linting
task format         # Format code
task type-check     # Type checking
task test           # Run tests
task test-e2e       # End-to-end tests
task security       # Security scanning
task pre-commit     # Pre-commit hooks
task ci             # Full CI pipeline locally
```

**Development Commands:**
```bash
task dev-setup      # Complete dev environment setup
task start-spark-bg # Start Spark History Server
task start-mcp-bg   # Start MCP server
task start-inspector-bg # Start MCP Inspector
task dev-all        # Start all development services
task stop-all       # Stop all services
```

### Quality Tools

- **🧹 Code Formatting:** Ruff (configured in `pyproject.toml`)
- **🔍 Linting:** Ruff with strict rules
- **🏷️ Type Checking:** MyPy with gradual typing
- **🧪 Testing:** Pytest with coverage reporting
- **🔒 Security:** Bandit for static security analysis
- **🛡️ Dependencies:** Safety for vulnerability scanning
- **🪝 Pre-commit:** Automated quality checks on commit

## 🔐 Security Features

### Static Analysis
- **Bandit:** Python security linter
- **Safety:** Dependency vulnerability scanner
- **Ruff:** Security-focused linting rules

### Dependency Management
- **Dependabot:** Automated dependency updates
- **uv:** Fast, secure dependency resolution
- **Pinned versions:** Reproducible builds

### Access Control
- **CODEOWNERS:** Required reviews for critical files
- **Branch protection:** Enforced quality gates
- **Trusted publishing:** Secure PyPI deployment

## 📊 Quality Gates

All changes must pass these automated checks:

✅ **Code Quality**
- Linting (Ruff)
- Formatting (Ruff)
- Type checking (MyPy)
- Pre-commit hooks

✅ **Testing**
- Unit tests (>80% coverage)
- Integration tests
- Multi-platform testing

✅ **Security**
- Static security analysis
- Dependency vulnerability scanning
- No secrets in code

✅ **Build**
- Package builds successfully
- CLI functionality verified
- Docker image builds

## 🚀 Deployment Pipeline

### Automated Releases
1. **Tag Creation:** Create version tag (`v1.2.3`)
2. **Build & Test:** Full CI pipeline validation
3. **Docker:** Build and push to `ghcr.io`
4. **PyPI:** Publish package with trusted publishing
5. **Artifacts:** Release artifacts available

### Manual Deployment
```bash
# Local build testing
task ci

# Create release
git tag v1.2.3
git push origin v1.2.3
```

## 🔧 Configuration Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Project metadata, dependencies, tool configuration |
| `Taskfile.yml` | Development task automation |
| `.pre-commit-config.yaml` | Pre-commit hook configuration |
| `.github/workflows/` | CI/CD pipeline definitions |
| `.github/dependabot.yml` | Automated dependency updates |
| `.github/CODEOWNERS` | Code review requirements |

## 📈 Monitoring & Reporting

- **📊 Coverage:** Codecov integration
- **🔒 Security:** Bandit report artifacts
- **📦 Dependencies:** Dependabot alerts
- **🔍 Quality:** Ruff and MyPy reports

## 🧪 Testing Strategy

### Unit Tests
- **Framework:** Pytest
- **Coverage:** >80% target
- **Async support:** pytest-asyncio

### Integration Tests
- **Spark Integration:** Real Spark History Server
- **MCP Testing:** Full protocol validation
- **CLI Testing:** Command-line interface validation

### End-to-End Tests
- **Full Stack:** Spark + MCP + CLI
- **Real Data:** Sample Spark event logs
- **Performance:** Basic performance validation

## 📚 Best Practices

### Development Workflow
1. **Branch:** Create feature branch
2. **Develop:** Write code with tests
3. **Validate:** Run `task ci` locally
4. **Commit:** Pre-commit hooks run automatically
5. **PR:** Create pull request
6. **Review:** Automated + manual review
7. **Merge:** All quality gates must pass

### Code Quality
- **Type Hints:** Gradual adoption of type annotations
- **Documentation:** Docstrings for public APIs
- **Testing:** Test-driven development encouraged
- **Security:** Security-first mindset

## 🔍 Troubleshooting

### Common Issues

**Build Failures:**
- Check Python version compatibility (3.12+)
- Verify uv installation
- Review dependency conflicts

**Test Failures:**
- Ensure Spark History Server is running for e2e tests
- Check environment variables
- Review test data availability

**Security Scan Issues:**
- Review Bandit configuration in `pyproject.toml`
- Update `skips` list for false positives
- Address legitimate security concerns

### Getting Help

1. **Documentation:** Check README.md and TESTING.md
2. **Issues:** Create GitHub issue with full context
3. **Discussions:** Use GitHub Discussions for questions

---

**🎯 Pipeline Status:** All automated quality gates ensure reliable, secure, and maintainable code.