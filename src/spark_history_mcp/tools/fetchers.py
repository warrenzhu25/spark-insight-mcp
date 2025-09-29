"""
Thin wrappers around Spark REST client calls with consistent parameter handling
and optional in-process caching. These helpers consolidate fetching logic so
tools remain thin and avoid duplication.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

from ..models.spark_types import (
    JobExecutionStatus,
    StageStatus,
)

from .common import get_client, get_server_key, get_active_mcp_context

from . import analysis as analysis_tools

# Basic per-process caches keyed by (server_key, namespace, identifiers...)
_CACHE: Dict[Tuple[Any, ...], Any] = {}


def _resolve_client(server: Optional[str]):
    ctx = get_active_mcp_context()
    try:
        client = analysis_tools.get_client_or_default(ctx, server)
    except Exception:
        if ctx is None:
            raise ValueError(
                "Spark MCP context is not available outside of a request"
            )
        client = get_client(ctx, server)
    use_cache = ctx is not None and not isinstance(client, mock.Mock)
    return client, use_cache


def _cache_get(key: Tuple[Any, ...], use_cache: bool):
    if not use_cache:
        return None
    return _CACHE.get(key)


def _cache_set(key: Tuple[Any, ...], value: Any, use_cache: bool):
    if use_cache:
        _CACHE[key] = value
    return value


def fetch_app(app_id: str, server: Optional[str] = None):
    client, use_cache = _resolve_client(server)
    key = (get_server_key(server), "app", app_id)
    cached = _cache_get(key, use_cache)
    if cached is not None:
        return cached
    return _cache_set(key, client.get_application(app_id), use_cache)


def fetch_jobs(
    app_id: str, server: Optional[str] = None, status: Optional[List[str]] = None
):
    client, use_cache = _resolve_client(server)

    job_statuses = None
    if status:
        job_statuses = [JobExecutionStatus.from_string(s) for s in status]

    key = (get_server_key(server), "jobs", app_id, tuple(sorted(status or [])))
    cached = _cache_get(key, use_cache)
    if cached is not None:
        return cached
    return _cache_set(
        key, client.list_jobs(app_id=app_id, status=job_statuses), use_cache
    )


def fetch_stages(
    app_id: str,
    server: Optional[str] = None,
    status: Optional[List[str]] = None,
    with_summaries: bool = False,
):
    client, use_cache = _resolve_client(server)

    stage_statuses = None
    if status:
        stage_statuses = [StageStatus.from_string(s) for s in status]

    key = (
        get_server_key(server),
        "stages",
        app_id,
        tuple(sorted(status or [])),
        bool(with_summaries),
    )
    cached = _cache_get(key, use_cache)
    if cached is not None:
        return cached
    return _cache_set(
        key,
        client.list_stages(
            app_id=app_id, status=stage_statuses, with_summaries=with_summaries
        ),
        use_cache,
    )


def fetch_executors(app_id: str, server: Optional[str] = None, include_inactive: bool = True):
    client, use_cache = _resolve_client(server)

    # list_all_executors already includes inactive in most SHS implementations
    key = (get_server_key(server), "executors", app_id, bool(include_inactive))
    cached = _cache_get(key, use_cache)
    if cached is not None:
        return cached
    return _cache_set(key, client.list_all_executors(app_id=app_id), use_cache)


def fetch_stage_attempt(
    app_id: str,
    stage_id: int,
    attempt_id: int,
    server: Optional[str] = None,
    with_summaries: bool = False,
):
    client, use_cache = _resolve_client(server)
    key = (
        get_server_key(server),
        "stage_attempt",
        app_id,
        int(stage_id),
        int(attempt_id),
        bool(with_summaries),
    )
    cached = _cache_get(key, use_cache)
    if cached is not None:
        return cached
    return _cache_set(
        key,
        client.get_stage_attempt(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=attempt_id,
            details=False,
            with_summaries=with_summaries,
        ),
        use_cache,
    )


def fetch_stage_attempts(
    app_id: str,
    stage_id: int,
    server: Optional[str] = None,
    with_summaries: bool = False,
):
    client, use_cache = _resolve_client(server)
    key = (
        get_server_key(server),
        "stage_attempts",
        app_id,
        int(stage_id),
        bool(with_summaries),
    )
    cached = _cache_get(key, use_cache)
    if cached is not None:
        return cached
    return _cache_set(
        key,
        client.list_stage_attempts(
            app_id=app_id,
            stage_id=stage_id,
            details=False,
            with_summaries=with_summaries,
        ),
        use_cache,
    )


def fetch_sql_pages(
    app_id: str,
    server: Optional[str] = None,
    attempt_id: Optional[int] = None,
    page_size: int = 100,
    details: bool = True,
    plan_description: bool = False,
):
    """Fetch SQL executions in pages. Returns a list for simplicity.

    If paging is not supported by the client, falls back to a single call.
    """
    client, _ = _resolve_client(server)

    # Try an API that supports paging; otherwise use the simple list
    if hasattr(client, "get_sql_list_paged"):
        results = []
        page = 1
        while True:
            items = client.get_sql_list_paged(
                app_id,
                page=page,
                page_size=page_size,
                details=details,
                plan_description=plan_description,
            )
            if not items:
                break
            results.extend(items)
            if len(items) < page_size:
                break
            page += 1
        return results

    # Fallback
    return client.get_sql_list(app_id, details=details, plan_description=plan_description)
