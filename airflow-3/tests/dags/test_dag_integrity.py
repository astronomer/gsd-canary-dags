"""Dag-integrity tests for the canary project.

Verify that every Dag parses without import errors, is tagged, exposes the
expected Dag ids, and uses no retries. Canaries must reflect real state, so a
retry would mask a transient failure. The ``retries==0`` check is also a
regression guard: the stock
Astro template asserted ``retries>=2``, which is wrong for a canary.
"""

from __future__ import annotations

import os
import sys

from airflow.models import DagBag

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DAGS_DIR = os.path.join(PROJECT_ROOT, "dags")

# Make `include` importable when tests run outside a full Airflow environment.
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

EXPECTED_DAG_IDS = {"variable_testing", "connection_testing"}


def _dag_bag() -> DagBag:
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors():
    dag_bag = _dag_bag()
    assert not dag_bag.import_errors, f"Dag import errors: {dag_bag.import_errors}"


def test_expected_dags_present():
    dag_bag = _dag_bag()
    missing = EXPECTED_DAG_IDS - set(dag_bag.dag_ids)
    assert not missing, f"Missing expected Dags: {missing}"


def test_dags_tagged():
    dag_bag = _dag_bag()
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"


def test_canaries_have_no_retries():
    """A retry would mask a real, transient failure, so canaries must not retry."""
    dag_bag = _dag_bag()
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args.get("retries", 0) == 0, (
            f"{dag_id} should have retries=0 so the canary reflects real state"
        )
