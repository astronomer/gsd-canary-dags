"""Shared canary helpers.

Pure functions used by the canary Dags. Kept import-light so they parse fast and
can be unit-tested without a running Airflow. Each ``check_*`` function returns a
structured result and never raises, so a single failure does not hide the rest of
the report. ``summarize`` raises once at the end if anything failed.

Uses the Airflow 3 Task SDK (``airflow.sdk``) for Variable and connection access.
"""

from __future__ import annotations

import logging
import os

from airflow.sdk import BaseHook, Variable

log = logging.getLogger("airflow.task")

# Connectivity testing reuses Airflow's own connection-testing switch. It is off
# unless this environment variable is set to "Enabled". Turn it on temporarily to
# have the connection canary call each destination system.
CONNECTIVITY_ENV = "AIRFLOW__CORE__TEST_CONNECTION"


def connectivity_enabled() -> bool:
    """Return True when connection testing is enabled through the environment."""
    return os.environ.get(CONNECTIVITY_ENV, "Disabled").strip().lower() == "enabled"


def check_variable(name: str) -> dict:
    """Resolve one Airflow Variable through the secrets chain. Never raises."""
    result: dict = {"name": name, "kind": "variable", "ok": False, "error": None}
    try:
        Variable.get(name)
        result["ok"] = True
        log.info("OK   variable '%s' resolved.", name)
    except Exception as exc:  # noqa: BLE001 - a canary must report every failure
        result["error"] = f"{type(exc).__name__}: {exc}"
        log.error("FAIL variable '%s' not found: %s", name, exc)
    return result


def check_connection(conn_id: str) -> dict:
    """Check one Airflow Connection. Never raises.

    Always confirms that the Connection resolves. When connection testing is
    enabled through ``AIRFLOW__CORE__TEST_CONNECTION=Enabled``, also calls the
    destination system through ``Connection.test_connection()`` and reports
    whether it is reachable.
    """
    result: dict = {"name": conn_id, "kind": "connection", "ok": False, "error": None}

    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as exc:  # noqa: BLE001 - a canary must report every failure
        result["error"] = f"not found: {type(exc).__name__}: {exc}"
        log.error("FAIL connection '%s' not found: %s", conn_id, exc)
        return result

    if not connectivity_enabled():
        result["ok"] = True
        log.info(
            "OK   connection '%s' resolved (conn_type=%s, host=%s). Connectivity "
            "test skipped; set %s=Enabled to call the destination.",
            conn_id,
            conn.conn_type,
            conn.host,
            CONNECTIVITY_ENV,
        )
        return result

    try:
        status, message = conn.test_connection()
    except Exception as exc:  # noqa: BLE001 - a canary must report every failure
        result["error"] = f"connectivity test errored: {type(exc).__name__}: {exc}"
        log.error("FAIL connection '%s' connectivity test errored: %s", conn_id, exc)
        return result

    if status:
        result["ok"] = True
        log.info("OK   connection '%s' connectivity verified: %s", conn_id, message)
    else:
        result["error"] = f"connectivity failed: {message}"
        log.error("FAIL connection '%s' connectivity failed: %s", conn_id, message)
    return result


def summarize(results: list[dict]) -> None:
    """Log a pass/fail table for every check and fail once if any failed."""
    total = len(results)
    failed = [r for r in results if not r["ok"]]
    log.info("==== canary summary: %d check(s) ====", total)
    for r in results:
        log.info(
            "[%-4s] %-11s %-25s %s",
            "OK" if r["ok"] else "FAIL",
            r["kind"],
            r["name"],
            "" if r["ok"] else r["error"],
        )
    log.info("==== %d passed, %d failed ====", total - len(failed), len(failed))
    if failed:
        raise AssertionError(
            f"{len(failed)}/{total} canary check(s) failed: "
            f"{[r['name'] for r in failed]}"
        )
