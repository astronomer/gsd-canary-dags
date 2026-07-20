# Canary Dags — Airflow 2

Astro project targeting Astro Runtime 13.8.0 (Apache Airflow 2.11.2).

See the [repository README](../README.md) for the full description. This project
uses Airflow 2 import idioms (`airflow.decorators`, `airflow.models`,
`airflow.hooks.base`). The Airflow 3 equivalent is in [`../airflow-3/`](../airflow-3/).

## Dags

| Dag | Purpose | Param |
| --- | --- | --- |
| `variable_testing` | Verify that each Airflow Variable resolves | `variables` (array) |
| `connection_testing` | Verify that each Airflow Connection resolves | `connections` (array) |

Both run on demand (`schedule=None`), check one item per mapped task, report a
full pass/fail table through `summarize`, and use `retries=0`.

## Usage

Push this project to an Astro Deployment on the matching Astro Runtime:

```bash
astro deploy
```

In the Airflow UI, trigger the `variable_testing` or `connection_testing` Dag
with config, and edit the `variables` or `connections` field to set what it
checks. Open the `summarize` task's logs to read the pass/fail table. The run
fails if any check fails.

To iterate on the project locally:

```bash
astro dev parse     # validate that Dags parse
astro dev start     # run Airflow locally
astro dev pytest    # run the Dag-integrity tests
```

## Test connectivity to destinations

By default, `connection_testing` only checks that each Connection resolves. To
also call each destination and confirm it is reachable, set the
`AIRFLOW__CORE__TEST_CONNECTION` environment variable to `Enabled`, which the
canary reads at run time. Turn it off again when you finish, because connection
testing reaches external systems and Airflow disables it by default for security.

Enable it locally by adding the variable to `.env`, then restart:

```bash
echo 'AIRFLOW__CORE__TEST_CONNECTION=Enabled' >> .env
astro dev restart
```

Enable it on a Deployment, then remove it when done:

```bash
astro deployment variable create AIRFLOW__CORE__TEST_CONNECTION=Enabled \
  --deployment-id <deployment-id>
```

You can also set the variable in the Astro UI under your Deployment's environment
variables. Connectivity testing requires the provider package for each `conn_type`
and network access from the workers to the destination.

## Layout

- `dags/` contains the two canary Dags.
- `include/canary.py` contains the shared, import-light check and report helpers.
- `tests/dags/test_dag_integrity.py` checks parsing, tags, expected ids, and `retries==0`.
