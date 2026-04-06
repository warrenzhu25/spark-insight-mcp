import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

from mcp.server.fastmcp import FastMCP

from spark_history_mcp.api.factory import create_spark_client
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config


@dataclass
class AppContext:
    clients: dict[str, SparkRestClient]
    default_client: Optional[SparkRestClient] = None


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    config = Config.from_file("config.yaml")

    clients: dict[str, SparkRestClient] = {}
    default_client = None

    for name, server_config in config.servers.items():
        clients[name] = create_spark_client(server_config)

        if server_config.default:
            default_client = clients[name]

    yield AppContext(clients=clients, default_client=default_client)


def run(config: Config):
    mcp.settings.host = config.mcp.address
    mcp.settings.port = int(config.mcp.port)
    mcp.settings.debug = bool(config.mcp.debug)
    mcp.run(transport=os.getenv("SHS_MCP_TRANSPORT", config.mcp.transports[0]))


mcp = FastMCP("Spark Events", lifespan=app_lifespan)

# Import tools to register them with MCP
# Import prompts to register them with MCP
from spark_history_mcp import (  # noqa: E402,F401
    prompts,
    tools,
)
