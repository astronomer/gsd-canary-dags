"""Connection canary.

On-demand Dag that checks each configured Airflow Connection. By default it
verifies that each Connection resolves through the standard resolution chain (the
configured secrets backend, then environment variables, then the metadata
database). When connection testing is enabled through
``AIRFLOW__CORE__TEST_CONNECTION=Enabled``, it also calls each destination system
through ``Connection.test_connection()`` to confirm the destination is reachable.

The list of Connections is a Dag param, so you can override it at trigger time
without editing code. Each Connection is checked in its own mapped task instance
through dynamic task mapping, and a final ``summarize`` task reports every result
and fails the run if any check failed.

Uses the Airflow 3 Task SDK (``airflow.sdk``) imports.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task

from include.canary import check_connection, summarize

DEFAULT_CONNECTIONS = [
    "aws_default",
    "postgres_default",
    "mysql_default",
    "snowflake_default",
    "s3_default",
    "http_default",
    "sftp_default",
    "slack_default",
    "email_default",
    "azure_default",
]


@dag(
    dag_id="connection_testing",
    schedule=None,  # run on demand
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0},  # a canary must reflect real state, never retry
    tags=["canary", "connections"],
    doc_md=__doc__,
    params={
        "connections": Param(
            DEFAULT_CONNECTIONS,
            type="array",
            items={"type": "string"},
            title="Connections to check",
            description="Airflow Connections the canary verifies.",
        ),
    },
)
def connection_testing():
    @task
    def get_connections(**context) -> list[str]:
        return context["params"]["connections"]

    results = task(check_connection).expand(conn_id=get_connections())
    task(summarize)(results)


connection_testing()
