from fastmcp import FastMCP

from ledger.mcp.resources import register_resources
from ledger.mcp.tools import register_tools

mcp = FastMCP("TheLedger")


def create_mcp_server(event_store, pool, projection_daemon):
    register_tools(mcp, event_store)
    register_resources(mcp, pool, projection_daemon)
    return mcp
