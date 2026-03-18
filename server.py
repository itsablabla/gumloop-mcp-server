"""
Gumloop MCP Server — GARZA OS Integration Layer
Maximum tool coverage: 22 tools across all Gumloop API endpoints.

Transport: StreamableHTTP (FastMCP)
Auth: Bearer token via MCP_AUTH_TOKEN env var
"""

import base64
import json
import os
import time
from typing import Any, Optional

import requests
from fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

# ─── Config ─────────────────────────────────────────────────────────────────────

GUMLOOP_API_KEY = os.environ.get("GUMLOOP_API_KEY", "")
GUMLOOP_USER_ID = os.environ.get("GUMLOOP_USER_ID", "")
MCP_AUTH_TOKEN  = os.environ.get("MCP_AUTH_TOKEN", "")
GUMLOOP_BASE    = "https://api.gumloop.com/api/v1"

# ─── HTTP Helpers ────────────────────────────────────────────────────────────────

def _headers():
    return {
        "Authorization": f"Bearer {GUMLOOP_API_KEY}",
        "Content-Type": "application/json",
    }

def _get(endpoint: str, params: dict = None) -> dict:
    p = dict(params or {})
    p.setdefault("user_id", GUMLOOP_USER_ID)
    r = requests.get(f"{GUMLOOP_BASE}{endpoint}", headers=_headers(), params=p, timeout=30)
    r.raise_for_status()
    return r.json()

def _post(endpoint: str, payload: dict = None) -> Any:
    d = dict(payload or {})
    d.setdefault("user_id", GUMLOOP_USER_ID)
    r = requests.post(f"{GUMLOOP_BASE}{endpoint}", headers=_headers(), json=d, timeout=30)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return r.text

def _safe(fn, *args, **kwargs):
    """Wrap a call and return structured error on failure."""
    try:
        return fn(*args, **kwargs)
    except requests.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text[:300]
        return {"error": True, "status_code": e.response.status_code, "detail": detail}
    except Exception as e:
        return {"error": True, "detail": str(e)}

# ─── FastMCP Server ──────────────────────────────────────────────────────────────

mcp = FastMCP(
    name="Gumloop MCP Server",
    instructions=(
        "This MCP server provides full programmatic control over Gumloop — "
        "Jaden Garza's automation platform. Use it to list, run, monitor, and kill "
        "Gumloop pipelines and AI agents, upload/download files, and inspect run history. "
        "Credentials are pre-configured. All 9 workbooks and all flows are accessible."
    ),
)

# ════════════════════════════════════════════════════════════════════════════════
# SECTION 1 — FLOW DISCOVERY
# ════════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def gumloop_list_flows() -> dict:
    """
    List all saved flows (pipelines) in the Gumloop workspace.
    Returns a list of flows with their saved_item_id, name, and metadata.
    Use saved_item_id to run or inspect a specific flow.
    """
    return _safe(_get, "/list_saved_items")


@mcp.tool()
def gumloop_list_workbooks() -> dict:
    """
    List all workbooks and their embedded flows.
    Returns workbook_id, name, description, and the list of saved flows inside each.
    Use workbook_id for run history queries.

    Known workbooks:
    - 8KmJ5ut1uAvGQQzp9nMiEA  Customer Refunds
    - rahHB1mJFuGpZzFX1MUvoD  Verizon Line
    - 6uGbTPRb3wdyEK1xzvF51j  Activate After Paying
    - oQQaPq4tPxJvR2EPyjoRnY  Cancel Service
    - eN3j57x6fLFnu5bD52g3qZ  Disputes
    - iu58Jga35QjXFViR1DRQFJ  Jaden Email
    - amRCmFe1SzQbx6de5bKTFx  Verizon Line (Copy)
    - wbXC7YFQHeZptXf3CrHrmD  Beeper
    - rq6GMhVYPuQvtSToFvDiNn  Refund Escalation
    """
    return _safe(_get, "/list_workbooks")


@mcp.tool()
def gumloop_get_input_schema(saved_item_id: str) -> dict:
    """
    Get the required input schema for a specific flow before running it.
    Returns a list of input definitions: name, data_type, description, required.
    Always call this before gumloop_run_flow to know what pipeline_inputs to pass.

    Args:
        saved_item_id: The flow ID (from gumloop_list_flows or gumloop_list_workbooks).
    """
    return _safe(_get, "/get_inputs", {"saved_item_id": saved_item_id})


@mcp.tool()
def gumloop_get_run_history(workbook_id: str) -> dict:
    """
    Get the run history for all flows in a workbook (up to 10 most recent runs per flow).
    Returns a dict mapping saved_item_id -> list of run records.
    Each run record includes: run_id, state, created_ts, finished_ts, credit_cost, outputs.

    Args:
        workbook_id: The workbook ID (from gumloop_list_workbooks).
    """
    return _safe(_get, "/get_plrun_saved_item_map", {"workbook_id": workbook_id})


@mcp.tool()
def gumloop_find_flow_by_name(name: str) -> dict:
    """
    Find a flow's saved_item_id by partial name match (case-insensitive).
    Useful when you know the flow name but not its ID.

    Args:
        name: Partial or full name of the flow to search for.
    """
    result = _safe(_get, "/list_saved_items")
    if "error" in result:
        return result
    flows = result.get("saved_items", [])
    name_lower = name.lower()
    matches = []
    for f in flows:
        fname = (f.get("name") or f.get("saved_item_name") or f.get("title") or "").lower()
        if name_lower in fname:
            matches.append(f)
    return {"matches": matches, "count": len(matches)}


@mcp.tool()
def gumloop_find_workbook_by_name(name: str) -> dict:
    """
    Find a workbook's workbook_id by partial name match (case-insensitive).
    Useful when you know the workbook name but not its ID.

    Args:
        name: Partial or full name of the workbook to search for.
    """
    result = _safe(_get, "/list_workbooks")
    if "error" in result:
        return result
    workbooks = result.get("workbooks", [])
    name_lower = name.lower()
    matches = []
    for w in workbooks:
        wname = (w.get("name") or "").lower()
        if name_lower in wname:
            matches.append({
                "workbook_id": w.get("workbook_id"),
                "name": w.get("name"),
                "description": w.get("description"),
                "flow_count": len(w.get("saved_items", [])),
            })
    return {"matches": matches, "count": len(matches)}


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2 — FLOW EXECUTION
# ════════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def gumloop_start_flow(
    saved_item_id: str,
    pipeline_inputs: Optional[str] = None,
) -> dict:
    """
    Start a Gumloop pipeline run asynchronously. Returns immediately with run_id and URL.
    Use gumloop_get_run_status to poll for completion.

    Args:
        saved_item_id: The flow ID to run.
        pipeline_inputs: JSON string of inputs list, e.g.:
            '[{"input_name": "url", "value": "https://example.com"}]'
            Call gumloop_get_input_schema first to know what inputs are required.
    """
    inputs = []
    if pipeline_inputs:
        try:
            inputs = json.loads(pipeline_inputs)
        except Exception:
            return {"error": True, "detail": "pipeline_inputs must be a valid JSON array string"}
    return _safe(_post, "/start_pipeline", {
        "saved_item_id": saved_item_id,
        "pipeline_inputs": inputs,
    })


@mcp.tool()
def gumloop_run_flow_blocking(
    saved_item_id: str,
    pipeline_inputs: Optional[str] = None,
    max_wait_seconds: int = 300,
    poll_interval_seconds: int = 4,
) -> dict:
    """
    Run a Gumloop pipeline and BLOCK until it completes. Returns the final state with outputs.
    Best for short-running flows (<5 min). For long flows, use gumloop_start_flow instead.

    Args:
        saved_item_id: The flow ID to run.
        pipeline_inputs: JSON string of inputs list, e.g.:
            '[{"input_name": "url", "value": "https://example.com"}]'
        max_wait_seconds: Maximum time to wait before returning (default 300s).
        poll_interval_seconds: How often to poll for status (default 4s).
    """
    inputs = []
    if pipeline_inputs:
        try:
            inputs = json.loads(pipeline_inputs)
        except Exception:
            return {"error": True, "detail": "pipeline_inputs must be a valid JSON array string"}

    start_result = _safe(_post, "/start_pipeline", {
        "saved_item_id": saved_item_id,
        "pipeline_inputs": inputs,
    })
    if start_result.get("error"):
        return start_result

    run_id = start_result.get("run_id")
    if not run_id:
        return {"error": True, "detail": "No run_id returned", "raw": start_result}

    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        status = _safe(_get, "/get_pl_run", {"run_id": run_id})
        if status.get("error"):
            return status
        state = status.get("state", "")
        if state in ("DONE", "FAILED", "TERMINATED"):
            return status
        time.sleep(poll_interval_seconds)

    return {"error": True, "detail": f"Timed out after {max_wait_seconds}s", "run_id": run_id, "last_state": state}


@mcp.tool()
def gumloop_get_run_status(run_id: str) -> dict:
    """
    Get the current status and full details of a pipeline run.
    Returns: run_id, state, inputs, outputs, log, credit_cost, created_ts, finished_ts.

    States: RUNNING → DONE | FAILED | TERMINATED

    Args:
        run_id: The run ID returned by gumloop_start_flow or gumloop_run_flow_blocking.
    """
    return _safe(_get, "/get_pl_run", {"run_id": run_id})


@mcp.tool()
def gumloop_kill_flow(run_id: str) -> dict:
    """
    Terminate a currently running pipeline. Returns {success: bool, run_id: str}.
    Use this to stop a runaway or long-running flow.

    Args:
        run_id: The run ID of the pipeline to terminate.
    """
    return _safe(_post, "/kill_pipeline", {"run_id": run_id})


@mcp.tool()
def gumloop_run_flow_by_name(
    flow_name: str,
    pipeline_inputs: Optional[str] = None,
    max_wait_seconds: int = 300,
) -> dict:
    """
    Find a flow by name and run it, blocking until completion.
    Combines gumloop_find_flow_by_name + gumloop_run_flow_blocking in one call.

    Args:
        flow_name: Partial or full name of the flow (case-insensitive).
        pipeline_inputs: JSON string of inputs list.
        max_wait_seconds: Maximum time to wait (default 300s).
    """
    find_result = _safe(_get, "/list_saved_items")
    if find_result.get("error"):
        return find_result
    flows = find_result.get("saved_items", [])
    name_lower = flow_name.lower()
    matched = None
    for f in flows:
        fname = (f.get("name") or f.get("saved_item_name") or f.get("title") or "").lower()
        if name_lower in fname:
            matched = f
            break
    if not matched:
        return {"error": True, "detail": f"No flow found matching '{flow_name}'"}

    saved_item_id = matched.get("saved_item_id") or matched.get("id")
    inputs = []
    if pipeline_inputs:
        try:
            inputs = json.loads(pipeline_inputs)
        except Exception:
            return {"error": True, "detail": "pipeline_inputs must be a valid JSON array string"}

    start_result = _safe(_post, "/start_pipeline", {
        "saved_item_id": saved_item_id,
        "pipeline_inputs": inputs,
    })
    if start_result.get("error"):
        return start_result

    run_id = start_result.get("run_id")
    deadline = time.time() + max_wait_seconds
    state = "RUNNING"
    while time.time() < deadline:
        status = _safe(_get, "/get_pl_run", {"run_id": run_id})
        if status.get("error"):
            return status
        state = status.get("state", "")
        if state in ("DONE", "FAILED", "TERMINATED"):
            return {**status, "flow_name": matched.get("name"), "saved_item_id": saved_item_id}
        time.sleep(4)

    return {"error": True, "detail": f"Timed out after {max_wait_seconds}s", "run_id": run_id}


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 3 — AGENT (GUMMIE) ENDPOINTS
# ════════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def gumloop_start_agent(gummie_id: str, message: str) -> dict:
    """
    Send a message to a Gumloop AI agent asynchronously.
    Returns {interaction_id, status}. Use gumloop_get_agent_status to poll for response.

    Known agents:
    - 5n8Ps6QFYA9RNwzg9poQiq  Competitor Price Comparison

    Args:
        gummie_id: The agent ID (from the URL at gumloop.com/agents/{gummie_id}).
        message: The message to send to the agent.
    """
    return _safe(_post, "/start_agent", {"gummie_id": gummie_id, "message": message})


@mcp.tool()
def gumloop_get_agent_status(interaction_id: str) -> dict:
    """
    Poll the status of an agent interaction.
    States: ASYNC_PROCESSING → COMPLETED | FAILED
    On COMPLETED, returns 'response' (text) and 'messages' (full conversation history).

    Args:
        interaction_id: The interaction ID returned by gumloop_start_agent.
    """
    try:
        r = requests.get(
            f"{GUMLOOP_BASE}/agent_status/{interaction_id}",
            headers=_headers(),
            params={"user_id": GUMLOOP_USER_ID},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text[:300]
        return {"error": True, "status_code": e.response.status_code, "detail": detail}
    except Exception as e:
        return {"error": True, "detail": str(e)}


@mcp.tool()
def gumloop_chat_agent(
    gummie_id: str,
    message: str,
    max_wait_seconds: int = 120,
    poll_interval_seconds: int = 3,
) -> dict:
    """
    Send a message to a Gumloop AI agent and BLOCK until it responds.
    Returns the agent's full response text and conversation history.

    Known agents:
    - 5n8Ps6QFYA9RNwzg9poQiq  Competitor Price Comparison

    Args:
        gummie_id: The agent ID.
        message: The message to send.
        max_wait_seconds: Maximum time to wait for response (default 120s).
        poll_interval_seconds: Polling interval (default 3s).
    """
    start = _safe(_post, "/start_agent", {"gummie_id": gummie_id, "message": message})
    if start.get("error"):
        return start

    interaction_id = start.get("interaction_id")
    if not interaction_id:
        return {"error": True, "detail": "No interaction_id returned", "raw": start}

    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{GUMLOOP_BASE}/agent_status/{interaction_id}",
                headers=_headers(),
                params={"user_id": GUMLOOP_USER_ID},
                timeout=30,
            )
            r.raise_for_status()
            status = r.json()
        except Exception as e:
            return {"error": True, "detail": str(e)}

        state = status.get("state", "")
        if state == "COMPLETED":
            return {
                "response": status.get("response", ""),
                "messages": status.get("messages", []),
                "interaction_id": interaction_id,
                "gummie_id": gummie_id,
                "state": state,
            }
        if state == "FAILED":
            return {"error": True, "detail": "Agent failed", "interaction_id": interaction_id, "raw": status}
        time.sleep(poll_interval_seconds)

    return {"error": True, "detail": f"Agent timed out after {max_wait_seconds}s", "interaction_id": interaction_id}


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 4 — FILE OPERATIONS
# ════════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def gumloop_upload_file(file_name: str, file_content_text: str) -> dict:
    """
    Upload a text file to Gumloop. Content is provided as plain text and auto-encoded.
    Returns {success: bool, file_name: str}.

    Args:
        file_name: The name to give the file (e.g., "report.txt").
        file_content_text: The plain text content of the file.
    """
    b64 = base64.b64encode(file_content_text.encode("utf-8")).decode()
    return _safe(_post, "/upload_file", {
        "file_name": file_name,
        "file_content": b64,
    })


@mcp.tool()
def gumloop_upload_file_base64(file_name: str, file_content_base64: str) -> dict:
    """
    Upload a file to Gumloop using pre-encoded base64 content.
    Use this for binary files (images, PDFs, etc.).
    Returns {success: bool, file_name: str}.

    Args:
        file_name: The name to give the file.
        file_content_base64: The base64-encoded content of the file.
    """
    return _safe(_post, "/upload_file", {
        "file_name": file_name,
        "file_content": file_content_base64,
    })


@mcp.tool()
def gumloop_upload_multiple_files(files_json: str) -> dict:
    """
    Upload multiple files to Gumloop in a single request.
    Returns {success: bool, uploaded_files: list}.

    Args:
        files_json: JSON string of files array, e.g.:
            '[{"file_name": "a.txt", "content": "hello"}, {"file_name": "b.txt", "content": "world"}]'
            Each item needs file_name and content (plain text, auto-encoded).
    """
    try:
        files_list = json.loads(files_json)
    except Exception:
        return {"error": True, "detail": "files_json must be a valid JSON array"}

    encoded = [
        {
            "file_name": f["file_name"],
            "file_content": base64.b64encode(f.get("content", "").encode("utf-8")).decode(),
        }
        for f in files_list
    ]
    return _safe(_post, "/upload_files", {"files": encoded})


@mcp.tool()
def gumloop_download_file(file_name: str) -> dict:
    """
    Download a previously uploaded file from Gumloop by name.
    Returns the file content (may be base64 encoded for binary files).

    Args:
        file_name: The name of the file to download.
    """
    result = _safe(_post, "/download_file", {"file_name": file_name})
    if isinstance(result, str):
        return {"file_name": file_name, "content": result}
    return result


@mcp.tool()
def gumloop_download_multiple_files(file_names_json: str) -> dict:
    """
    Download multiple files from Gumloop in a single request.

    Args:
        file_names_json: JSON string of file names array, e.g.:
            '["report.txt", "data.csv"]'
    """
    try:
        file_names = json.loads(file_names_json)
    except Exception:
        return {"error": True, "detail": "file_names_json must be a valid JSON array of strings"}
    result = _safe(_post, "/download_files", {"file_names": file_names})
    if isinstance(result, str):
        return {"content": result}
    return result


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 5 — ADMIN / ENTERPRISE (stubbed, returns clear errors on personal tier)
# ════════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def gumloop_get_audit_logs(
    organization_id: str,
    start_time: str,
    end_time: str,
) -> dict:
    """
    Retrieve audit logs for a Gumloop organization. Requires enterprise tier.
    Returns a list of audit events with timestamps, user, and action details.

    Args:
        organization_id: The organization ID (enterprise accounts only).
        start_time: ISO 8601 start time, e.g. "2025-01-01T00:00:00Z".
        end_time: ISO 8601 end time, e.g. "2026-03-17T23:59:59Z".
    """
    return _safe(_get, "/get_audit_logs", {
        "organization_id": organization_id,
        "start_time": start_time,
        "end_time": end_time,
    })


@mcp.tool()
def gumloop_export_data(
    start_date: str,
    end_date: str,
    export_level: str = "workspace",
    export_fields_json: str = '["run_id", "saved_item_id", "state", "created_at", "credit_cost"]',
) -> dict:
    """
    Export run data from Gumloop. Requires enterprise tier.
    Returns an export_id to poll with gumloop_get_export_status.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        export_level: Either "workspace" or "organization".
        export_fields_json: JSON array of fields to export.
    """
    try:
        fields = json.loads(export_fields_json)
    except Exception:
        return {"error": True, "detail": "export_fields_json must be a valid JSON array"}
    return _safe(_post, "/export_data", {
        "start_date": start_date,
        "end_date": end_date,
        "export_level": export_level,
        "export_fields": fields,
    })


@mcp.tool()
def gumloop_get_export_status(export_id: str) -> dict:
    """
    Check the status of a data export job initiated by gumloop_export_data.
    Returns export status and download URL when complete.

    Args:
        export_id: The export ID returned by gumloop_export_data.
    """
    return _safe(_get, "/get_data_export_status", {"export_id": export_id})


@mcp.tool()
def gumloop_manage_workspace_users(
    action: str,
    project_id: str,
    user_email: Optional[str] = None,
    role: Optional[str] = None,
) -> dict:
    """
    Manage workspace users (add, remove, update roles). Requires workspace admin role.

    Args:
        action: Action to perform: "add", "remove", or "update".
        project_id: The workbook/project ID to manage users for.
        user_email: Email of the user to add/remove/update.
        role: Role to assign (e.g., "viewer", "editor", "admin").
    """
    payload: dict = {"action": action, "project_id": project_id}
    if user_email:
        payload["user_email"] = user_email
    if role:
        payload["role"] = role
    return _safe(_post, "/manage_workspace_users", payload)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 6 — UTILITY / META TOOLS
# ════════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def gumloop_get_all_run_history() -> dict:
    """
    Convenience tool: Get run history for ALL 9 workbooks in one call.
    Returns a dict mapping workbook_name -> run_history.
    Useful for getting a full picture of recent automation activity.
    """
    wb_result = _safe(_get, "/list_workbooks")
    if wb_result.get("error"):
        return wb_result

    all_history = {}
    for w in wb_result.get("workbooks", []):
        wid   = w.get("workbook_id") or w.get("id")
        wname = w.get("name", wid)
        hist  = _safe(_get, "/get_plrun_saved_item_map", {"workbook_id": wid})
        if not hist.get("error"):
            total = sum(len(v) for v in hist.values() if isinstance(v, list))
            all_history[wname] = {"workbook_id": wid, "total_runs": total, "runs": hist}
        else:
            all_history[wname] = {"workbook_id": wid, "error": hist}

    return {"workbooks": all_history, "total_workbooks": len(all_history)}


@mcp.tool()
def gumloop_server_status() -> dict:
    """
    Check the health and configuration of this MCP server and the Gumloop API connection.
    Returns server config, credential status, and a live API connectivity test.
    """
    api_ok = False
    flow_count = 0
    try:
        r = _get("/list_saved_items")
        flows = r.get("saved_items", [])
        flow_count = len(flows)
        api_ok = True
    except Exception as e:
        api_error = str(e)

    return {
        "server": "Gumloop MCP Server — GARZA OS",
        "version": "2.0.0",
        "tools": 22,
        "api_connected": api_ok,
        "flow_count": flow_count,
        "user_id": GUMLOOP_USER_ID,
        "api_key_configured": bool(GUMLOOP_API_KEY),
        "auth_token_configured": bool(MCP_AUTH_TOKEN),
        "known_workbooks": [
            "Customer Refunds", "Verizon Line", "Activate After Paying",
            "Cancel Service", "Disputes", "Jaden Email",
            "Verizon Line (Copy)", "Beeper", "Refund Escalation",
        ],
        "known_agents": ["Competitor Price Comparison (5n8Ps6QFYA9RNwzg9poQiq)"],
    }


# ─── Auth Middleware ─────────────────────────────────────────────────────────────

class BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Allow health check without auth
        if request.url.path in ("/health", "/"):
            return await call_next(request)
        # Skip auth if no token configured (dev mode)
        if not MCP_AUTH_TOKEN:
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {MCP_AUTH_TOKEN}":
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        return await call_next(request)


# ─── Health & Root Routes ────────────────────────────────────────────────────────

async def health(request: Request):
    return JSONResponse({"status": "ok", "service": "gumloop-mcp-server", "tools": 22})


async def root(request: Request):
    return JSONResponse({
        "service": "Gumloop MCP Server — GARZA OS",
        "version": "2.0.0",
        "tools": 22,
        "mcp_endpoint": "/mcp",
        "health_endpoint": "/health",
    })


# ─── App Assembly ────────────────────────────────────────────────────────────────

app = mcp.http_app(path="/mcp", stateless_http=True)
app.add_middleware(BearerAuthMiddleware)
app.add_route("/health", health, methods=["GET"])
app.add_route("/", root, methods=["GET"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
