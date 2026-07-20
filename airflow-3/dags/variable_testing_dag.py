"""Variable canary.

On-demand Dag that verifies that each configured Airflow Variable resolves through
the standard resolution chain (the configured secrets backend, then environment
variables, then the metadata database). The list of Variables is a Dag param, so
you can override it at trigger time without editing code. Each Variable is checked
in its own mapped task instance through dynamic task mapping, and a final
``summarize`` task reports every result and fails the run if any check failed.

Uses the Airflow 3 Task SDK (``airflow.sdk``) imports.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task

from include.canary import check_variable, summarize

DEFAULT_VARIABLES = [
    "environment",
    "log_level",
    "api_endpoint",
    "batch_size",
    "notification_channel",
    "example_config",
]


@dag(
    dag_id="variable_testing",
    schedule=None,  # run on demand
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0},  # a canary must reflect real state, never retry
    tags=["canary", "variables"],
    doc_md=__doc__,
    params={
        "variables": Param(
            DEFAULT_VARIABLES,
            type="array",
            items={"type": "string"},
            title="Variables to check",
            description="Airflow Variables the canary verifies.",
        ),
    },
)
def variable_testing():
    @task
    def get_variables(**context) -> list[str]:
        return context["params"]["variables"]

    results = task(check_variable).expand(name=get_variables())
    task(summarize)(results)


variable_testing()
