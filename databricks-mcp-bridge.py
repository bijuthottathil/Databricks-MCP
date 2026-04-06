"""
Databricks MCP Bridge — authenticate to the Databricks MCP Vector Search server
using the NADBXTrainingSPN Azure service principal.

Service Principal: NADBXTrainingSPN (preferred authentication method)

Configuration for biju_gold vector search:
  MCP Server URL: https://adb-1952652121322753.13.azuredatabricks.net/api/2.0/mcp/vector-search/na-dbxtraining/biju_gold
  MCP Tool Name: na-dbxtraining__biju_gold__customer_kwh_embeddingsindex
  Databricks Host: https://adb-1952652121322753.13.azuredatabricks.net

Required environment variables for NADBXTrainingSPN service principal auth:
  - DATABRICKS_CLIENT_ID    (or ARM_CLIENT_ID)     — NADBXTrainingSPN client ID
  - DATABRICKS_CLIENT_SECRET (or ARM_CLIENT_SECRET) — NADBXTrainingSPN client secret
  - DATABRICKS_TENANT_ID    (or ARM_TENANT_ID)     — Azure AD tenant ID

Optional environment variables:
  - DATABRICKS_HOST         — Workspace URL (default: https://adb-1952652121322753.13.azuredatabricks.net)
  - MCP_SERVER_URL          — Full MCP endpoint (default: https://adb-1952652121322753.13.azuredatabricks.net/api/2.0/mcp/vector-search/na-dbxtraining/biju_gold)
  - MCP_TOOL_NAME           — Tool name (default: na-dbxtraining__biju_gold__customer_kwh_embeddingsindex)
  - DATABRICKS_TOKEN        — PAT token (fallback if service principal not configured)

Credentials are loaded from .env file in the same directory for security.
"""

import json
import os
import urllib.request
import urllib.error
import urllib.parse

# Load environment variables from .env file
def _load_env_file():
    """Load environment variables from .env file if it exists."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                # Remove leading 'export' if present
                if line.startswith('export '):
                    line = line[7:].strip()
                # Parse key=value
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    # Only set if not already in environment
                    if key and not os.environ.get(key):
                        os.environ[key] = value

# Load .env file on module import
_load_env_file()

# Azure AD scope for Azure Databricks (fixed resource ID)
DATABRICKS_AZURE_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
AZURE_TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


def _env(key: str, alt: str | None = None) -> str | None:
    return os.environ.get(key) or (os.environ.get(alt) if alt else None)


def get_token() -> str:
    """
    Obtain a Bearer token for Databricks MCP using service principal or PAT.

    Precedence:
      1. Azure service principal (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET + DATABRICKS_TENANT_ID)
         Primary method: NADBXTrainingSPN service principal
      2. DATABRICKS_TOKEN (PAT) as fallback if service principal not configured

    Returns:
        Bearer token string (without "Bearer " prefix).
    """
    # Try service principal first (NADBXTrainingSPN)
    client_id = _env("DATABRICKS_CLIENT_ID", "ARM_CLIENT_ID")
    client_secret = _env("DATABRICKS_CLIENT_SECRET", "ARM_CLIENT_SECRET")
    tenant_id = _env("DATABRICKS_TENANT_ID", "ARM_TENANT_ID")

    if all((client_id, client_secret, tenant_id)):
        # Use service principal authentication (preferred method)
        return _get_token_from_service_principal(client_id, client_secret, tenant_id)

    # Fallback to PAT token if service principal not configured
    pat = _env("DATABRICKS_TOKEN")
    if pat:
        return pat.strip().removeprefix("Bearer ").strip()

    raise ValueError(
        "No valid auth found. Please set service principal env vars (recommended):\n"
        "  DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, DATABRICKS_TENANT_ID\n"
        "(ARM_CLIENT_ID, ARM_CLIENT_SECRET, ARM_TENANT_ID are also accepted)\n"
        "Or set DATABRICKS_TOKEN as fallback."
    )


def _get_token_from_service_principal(client_id: str, client_secret: str, tenant_id: str) -> str:
    """
    Obtain Azure AD token using service principal credentials (e.g., NADBXTrainingSPN).

    Args:
        client_id: Azure AD Application (client) ID
        client_secret: Azure AD client secret
        tenant_id: Azure AD tenant ID

    Returns:
        Access token string (without "Bearer " prefix).
    """
    url = AZURE_TOKEN_URL_TEMPLATE.format(tenant_id=tenant_id)
    body = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": DATABRICKS_AZURE_SCOPE,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        try:
            err_json = json.loads(err_body)
            msg = err_json.get("error_description", err_json.get("error", err_body))
        except Exception:
            msg = err_body
        raise RuntimeError(f"Azure token request failed ({e.code}): {msg}") from e

    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError("Azure token response did not contain access_token")

    return access_token


def call_mcp_tool(
    query: str,
    *,
    mcp_server_url: str | None = None,
    tool_name: str | None = None,
    token: str | None = None,
) -> list | dict:
    """
    Call the Databricks MCP Vector Search tool with the given query.

    Uses service principal to obtain a token unless `token` or DATABRICKS_TOKEN is set.

    Args:
        query: Search query text.
        mcp_server_url: MCP endpoint URL (default: MCP_SERVER_URL env or biju_gold endpoint).
        tool_name: MCP tool name (default: MCP_TOOL_NAME env or customer_kwh_embeddingsindex).
        token: Optional Bearer token; if not set, get_token() is used.

    Returns:
        Parsed result (typically a list of search results).
    """
    # Default to biju_gold configuration if not specified
    default_url = "https://adb-1952652121322753.13.azuredatabricks.net/api/2.0/mcp/vector-search/na-dbxtraining/biju_gold"
    default_tool = "na-dbxtraining__biju_gold__customer_kwh_embeddingsindex"

    url = mcp_server_url or _env("MCP_SERVER_URL") or default_url
    name = tool_name or _env("MCP_TOOL_NAME") or default_tool

    if not url or not name:
        raise ValueError("MCP_SERVER_URL and MCP_TOOL_NAME must be set (env or arguments)")

    bearer = (token or get_token()).strip()
    if not bearer.lower().startswith("bearer "):
        bearer = f"Bearer {bearer}"

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": {"query": query},
        },
    }

    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Authorization", bearer)
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        try:
            err_json = json.loads(err_body)
            msg = err_json.get("message", err_json.get("error_description", err_body))
        except Exception:
            msg = err_body
        raise RuntimeError(f"MCP request failed (HTTP {e.code}): {msg}") from e

    if "error" in result:
        raise RuntimeError(f"MCP error: {json.dumps(result['error'], indent=2)}")

    content = (result.get("result") or {}).get("content")
    if not content or not isinstance(content, list):
        return result.get("result", result)

    first = content[0]
    text = first.get("text") if isinstance(first, dict) else None
    if not text:
        return result.get("result", result)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"text": text}


def run_as_mcp_server():
    """Run as an MCP server for Claude Desktop."""
    import logging

    # Set up logging
    logging.basicConfig(
        filename='/tmp/databricks-mcp-bridge.log',
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("MCP server mode starting...")

    def handle_request(request):
        """Handle MCP JSON-RPC request."""
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")

        # Handle notifications
        if request_id is None:
            if method == "notifications/initialized":
                logging.debug("Received initialized notification")
                return None
            return None

        if method == "initialize":
            logging.debug("Initialize request")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "biju_gold", "version": "1.0.0"}
                }
            }

        elif method == "tools/list":
            logging.debug("Tools list request")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [{
                        "name": "na-dbxtraining__biju_gold__customer_kwh_embeddingsindex",
                        "description": "Search customer energy consumption data using vector similarity",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Search query for customer energy consumption patterns"
                                }
                            },
                            "required": ["query"]
                        }
                    }]
                }
            }

        elif method == "tools/call":
            logging.debug(f"Tools call request: {params}")
            query = params.get("arguments", {}).get("query", "")
            try:
                results = call_mcp_tool(query)
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps(results, indent=2)
                        }]
                    }
                }
            except Exception as e:
                logging.error(f"Error calling tool: {e}", exc_info=True)
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32603, "message": str(e)}
                }

        else:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"}
            }

    # Main loop
    try:
        import sys
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            logging.debug(f"Received: {line}")
            try:
                request = json.loads(line)
                response = handle_request(request)

                if response is not None:
                    response_str = json.dumps(response)
                    logging.debug(f"Sending: {response_str[:200]}...")
                    print(response_str, flush=True)
            except Exception as e:
                logging.error(f"Error: {e}", exc_info=True)
                error_response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32603, "message": str(e)}
                }
                print(json.dumps(error_response), flush=True)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
    finally:
        logging.info("MCP server shutting down")


if __name__ == "__main__":
    import sys

    # Check if running as MCP server (no arguments and stdin is a pipe)
    if len(sys.argv) == 1 and not sys.stdin.isatty():
        run_as_mcp_server()
    else:
        # CLI mode
        query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "test query"
        try:
            token = get_token()
            # Determine which auth method was used
            client_id = _env("DATABRICKS_CLIENT_ID", "ARM_CLIENT_ID")
            if client_id:
                print("Token obtained successfully using NADBXTrainingSPN service principal")
            else:
                print("Token obtained successfully using PAT")

            results = call_mcp_tool(query, token=token)
            print(json.dumps(results, indent=2))
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
