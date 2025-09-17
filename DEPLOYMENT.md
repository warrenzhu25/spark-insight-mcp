# 🚀 Deployment Guide

This guide covers various deployment options for SparkInsight AI.

## 🔐 Environment Variables

```bash
SHS_MCP_PORT - Port for MCP server (default: 18888)
SHS_MCP_DEBUG - Enable debug mode (default: false)
SHS_MCP_ADDRESS - Address for MCP server (default: localhost)
SHS_MCP_TRANSPORT - MCP transport mode (default: streamable-http)
SHS_SERVERS_*_URL - URL for a specific server
SHS_SERVERS_*_AUTH_USERNAME - Username for a specific server
SHS_SERVERS_*_AUTH_PASSWORD - Password for a specific server
SHS_SERVERS_*_AUTH_TOKEN - Token for a specific server
SHS_SERVERS_*_VERIFY_SSL - Whether to verify SSL for a specific server (true/false)
SHS_SERVERS_*_TIMEOUT - HTTP request timeout in seconds for a specific server (default: 30)
SHS_SERVERS_*_EMR_CLUSTER_ARN - EMR cluster ARN for a specific server
```

## 🌐 Multi-Spark History Server Setup

Setup multiple Spark history servers in the config.yaml and choose which server you want the LLM to interact with for each query.

```yaml
servers:
  production:
    default: true
    url: "http://prod-spark-history:18080"
    auth:
      username: "user"
      password: "pass"
  staging:
    url: "http://staging-spark-history:18080"
```

### Usage Examples

💁 User Query: "Can you get application <app_id> using production server?"

🤖 AI Tool Request:
```json
{
  "app_id": "<app_id>",
  "server": "production"
}
```

🤖 AI Tool Response:
```json
{
  "id": "<app_id>",
  "name": "app_name",
  "coresGranted": null,
  "maxCores": null,
  "coresPerExecutor": null,
  "memoryPerExecutorMB": null,
  "attempts": [
    {
      "attemptId": null,
      "startTime": "2023-09-06T04:44:37.006000Z",
      "endTime": "2023-09-06T04:45:40.431000Z",
      "lastUpdated": "2023-09-06T04:45:42Z",
      "duration": 63425,
      "sparkUser": "spark",
      "appSparkVersion": "3.3.0",
      "completed": true
    }
  ]
}
```

## 📔 AWS Integration Guides

If you are an existing AWS user looking to analyze your Spark Applications, we provide detailed setup guides for:

- **[AWS Glue Users](examples/aws/glue/README.md)** - Connect to Glue Spark History Server
- **[Amazon EMR Users](examples/aws/emr/README.md)** - Use EMR Persistent UI for Spark analysis

These guides provide step-by-step instructions for setting up the Spark History Server MCP with your AWS services.

## 🚀 Kubernetes Deployment

Deploy using Kubernetes with Helm:

> ⚠️ **Work in Progress**: We are still testing and will soon publish the container image and Helm registry to GitHub for easy deployment.

```bash
# 📦 Deploy with Helm
helm install spark-history-mcp ./deploy/kubernetes/helm/spark-history-mcp/

# 🎯 Production configuration
helm install spark-history-mcp ./deploy/kubernetes/helm/spark-history-mcp/ \
  --set replicaCount=3 \
  --set autoscaling.enabled=true \
  --set monitoring.enabled=true
```

📚 See [`deploy/kubernetes/helm/`](deploy/kubernetes/helm/) for complete deployment manifests and configuration options.

## 🤖 AI Agent Integration

### Quick Start Options

| Integration | Transport | Best For |
|-------------|-----------|----------|
| **[Local Testing](TESTING.md)** | HTTP | Development, testing tools |
| **[Claude Desktop](examples/integrations/claude-desktop/)** | STDIO | Interactive analysis |
| **[Amazon Q CLI](examples/integrations/amazon-q-cli/)** | STDIO | Command-line automation |
| **[Kiro](examples/integrations/kiro/)** | HTTP | IDE integration, code-centric analysis |
| **[LangGraph](examples/integrations/langgraph/)** | HTTP | Multi-agent workflows |
| **[Strands Agents](examples/integrations/strands-agents/)** | HTTP | Multi-agent workflows |