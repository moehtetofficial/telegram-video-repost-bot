#!/usr/bin/env python3
"""
Dashboard backend — lightweight aiohttp server that:
  • Shows live status of repost-bot and recap-pipeline containers
  • Streams recent Docker logs for each service
  • Allows manual recap trigger via webhook
  • Displays / edits runtime configuration
  • Serves the static frontend on port 8080
"""

import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("dashboard")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DASH_PORT = int(os.environ.get("DASH_PORT", 8080))
DOCKER_SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
REPOST_HEALTH = os.environ.get("REPOST_HEALTH", "http://repost-bot:7860/health")
RECAP_HEALTH = os.environ.get("RECAP_HEALTH", "http://recap-pipeline:7861/health")
RECAP_WEBHOOK = os.environ.get("RECAP_WEBHOOK", "http://recap-pipeline:7861/process")
ENV_FILE = os.environ.get("ENV_FILE", "/config/.env")

CONTAINER_NAMES = {
    "repost": os.environ.get("REPOST_CONTAINER", "telegram-repost-bot"),
    "recap": os.environ.get("RECAP_CONTAINER", "telegram-recap-pipeline"),
}

STATIC_DIR = Path(__file__).parent / "static"

# In-memory activity log (last 100 events)
_activity: list[dict] = []
MAX_ACTIVITY = 100


def _add_activity(service: str, message: str):
    _activity.insert(0, {
        "time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "service": service,
        "message": message,
    })
    if len(_activity) > MAX_ACTIVITY:
        _activity.pop()


# ═══════════════════════════════════════════════════════════════════════════
#  Docker socket helpers
# ═══════════════════════════════════════════════════════════════════════════

async def _docker_get(path: str) -> dict | list | None:
    """GET request to Docker Engine API via the unix socket."""
    try:
        conn = aiohttp.UnixConnector(path=DOCKER_SOCKET)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.get(f"http://localhost{path}") as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        log.debug("Docker API error (%s): %s", path, e)
    return None


async def _container_info(name: str) -> dict:
    """Return status info for a Docker container by name."""
    data = await _docker_get(f"/containers/{name}/json")
    if data is None:
        return {"name": name, "status": "not_found", "state": "unknown"}

    state = data.get("State", {})
    return {
        "name": name,
        "status": state.get("Status", "unknown"),
        "running": state.get("Running", False),
        "started_at": state.get("StartedAt", ""),
        "restart_count": data.get("RestartCount", 0),
    }


async def _container_logs(name: str, lines: int = 80) -> str:
    """Fetch last N lines of logs from a container."""
    try:
        conn = aiohttp.UnixConnector(path=DOCKER_SOCKET)
        async with aiohttp.ClientSession(connector=conn) as session:
            url = f"http://localhost/containers/{name}/logs"
            params = {"stdout": "true", "stderr": "true", "tail": str(lines)}
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    raw = await resp.read()
                    # Docker log stream has 8-byte header per line; strip it
                    lines_out = []
                    i = 0
                    while i < len(raw):
                        if i + 8 > len(raw):
                            break
                        length = int.from_bytes(raw[i + 4 : i + 8], "big")
                        line = raw[i + 8 : i + 8 + length]
                        lines_out.append(line.decode("utf-8", errors="replace").rstrip())
                        i += 8 + length
                    return "\n".join(lines_out)
    except Exception as e:
        log.debug("Log fetch error (%s): %s", name, e)
    return "(logs unavailable)"


# ═══════════════════════════════════════════════════════════════════════════
#  Health check helpers
# ═══════════════════════════════════════════════════════════════════════════

async def _check_health(url: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                return resp.status == 200
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  API endpoints
# ═══════════════════════════════════════════════════════════════════════════

async def api_status(_request):
    """GET /api/status — live status of both services."""
    repost_info, recap_info, repost_health, recap_health = await asyncio.gather(
        _container_info(CONTAINER_NAMES["repost"]),
        _container_info(CONTAINER_NAMES["recap"]),
        _check_health(REPOST_HEALTH),
        _check_health(RECAP_HEALTH),
    )
    repost_info["healthy"] = repost_health
    recap_info["healthy"] = recap_health
    return web.json_response({"repost": repost_info, "recap": recap_info})


async def api_logs(request):
    """GET /api/logs/{service} — recent container logs."""
    service = request.match_info["service"]
    name = CONTAINER_NAMES.get(service)
    if not name:
        return web.json_response({"error": "unknown service"}, status=404)
    lines = int(request.query.get("lines", 80))
    text = await _container_logs(name, lines)
    return web.json_response({"service": service, "logs": text})


async def api_activity(_request):
    """GET /api/activity — recent activity feed."""
    return web.json_response(_activity)


async def api_recap_trigger(request):
    """POST /api/recap/trigger — forward to recap webhook."""
    try:
        data = await request.json()
    except Exception:
        data = {}

    video_path = data.get("video_path", "")
    if not video_path:
        return web.json_response({"error": "video_path required"}, status=400)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(RECAP_WEBHOOK, json=data) as resp:
                result = await resp.json()
                _add_activity("recap", f"Manual trigger: {video_path}")
                return web.json_response(result, status=resp.status)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)


async def api_config_get(_request):
    """GET /api/config — read current .env values (secrets masked)."""
    config = {}
    mask_keys = {"TELETHON_SESSION_STRING", "TELEGRAM_BOT_TOKEN", "TELEGRAM_API_HASH"}
    if os.path.exists(ENV_FILE):
        for line in Path(ENV_FILE).read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                config[line] = ""  # preserve comments
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip()
                if key in mask_keys and val:
                    config[key] = val[:4] + "****" + val[-4:] if len(val) > 8 else "****"
                else:
                    config[key] = val
    return web.json_response(config)


async def api_config_update(request):
    """POST /api/config — update non-secret .env values."""
    try:
        updates = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    # Never allow overwriting secrets via the dashboard
    forbidden = {"TELETHON_SESSION_STRING", "TELEGRAM_BOT_TOKEN",
                 "TELEGRAM_API_HASH", "TELEGRAM_API_ID"}
    blocked = [k for k in updates if k in forbidden]
    if blocked:
        return web.json_response(
            {"error": f"Cannot update secrets via dashboard: {blocked}"}, status=403
        )

    if not os.path.exists(ENV_FILE):
        return web.json_response({"error": ".env file not found"}, status=404)

    lines = Path(ENV_FILE).read_text().splitlines()
    new_lines = []
    updated_keys = set()

    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}")
                updated_keys.add(key)
                continue
        new_lines.append(line)

    # Append new keys not already in file
    for key, val in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={val}")

    Path(ENV_FILE).write_text("\n".join(new_lines) + "\n")
    _add_activity("dashboard", f"Config updated: {list(updates.keys())}")
    return web.json_response({"status": "updated", "note": "Restart containers to apply."})


async def api_restart(request):
    """POST /api/restart/{service} — restart a container."""
    service = request.match_info["service"]
    name = CONTAINER_NAMES.get(service)
    if not name:
        return web.json_response({"error": "unknown service"}, status=404)

    try:
        conn = aiohttp.UnixConnector(path=DOCKER_SOCKET)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.post(f"http://localhost/containers/{name}/restart") as resp:
                if resp.status == 204:
                    _add_activity("dashboard", f"Restarted {service}")
                    return web.json_response({"status": "restarted"})
                else:
                    text = await resp.text()
                    return web.json_response({"error": text}, status=resp.status)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)


# ═══════════════════════════════════════════════════════════════════════════
#  App setup
# ═══════════════════════════════════════════════════════════════════════════

async def index(_request):
    return web.FileResponse(STATIC_DIR / "index.html")


def create_app() -> web.Application:
    app = web.Application()

    # Frontend
    app.router.add_get("/", index)
    app.router.add_static("/static", STATIC_DIR, show_index=False)

    # API
    app.router.add_get("/api/status", api_status)
    app.router.add_get("/api/logs/{service}", api_logs)
    app.router.add_get("/api/activity", api_activity)
    app.router.add_get("/api/config", api_config_get)
    app.router.add_post("/api/config", api_config_update)
    app.router.add_post("/api/recap/trigger", api_recap_trigger)
    app.router.add_post("/api/restart/{service}", api_restart)

    return app


if __name__ == "__main__":
    log.info("Dashboard starting on port %d", DASH_PORT)
    web.run_app(create_app(), host="0.0.0.0", port=DASH_PORT)
