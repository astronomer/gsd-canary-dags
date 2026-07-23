# Canary Dags for Airflow 2

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

Running locally only confirms that the Dags parse and pass their structure tests;
it does not reach your real Variables, Connections, or destinations:

```bash
astro dev parse     # validate that Dags parse
astro dev pytest    # run the Dag-integrity tests
```

## Test connectivity to destinations

By default, `connection_testing` only checks that each Connection resolves. It can
also call each destination to confirm it is reachable. This exercises the network
path from your Astro Deployment to your own infrastructure, so run it on a
Deployment; a local Airflow cannot reach those systems. Connectivity testing is off
by default because it reaches external systems, and Airflow disables it by default
for security. To test connectivity on a Deployment:

1. Install the provider package for each `conn_type` you want to test, because
   connectivity goes through the connection's hook. Add any missing
   [provider packages](https://airflow.apache.org/docs/apache-airflow-providers/packages-ref.html)
   to `requirements.txt` and `astro deploy` (for example,
   `apache-airflow-providers-snowflake` for `snowflake` connections). A `conn_type`
   with no installed provider is reported as an `Unknown hook type` failure.
2. Set `AIRFLOW__CORE__TEST_CONNECTION` to `Enabled` on the Deployment. This is
   mandatory: Airflow disables connection testing by default, so without it the
   canary only checks resolution. Set it in the Astro UI under environment
   variables, or with the Astro CLI:

   ```bash
   astro deployment variable create AIRFLOW__CORE__TEST_CONNECTION=Enabled \
     --deployment-id <deployment-id>
   ```

3. Trigger `connection_testing` and read the `summarize` task's logs. Each
   Connection now reports whether its destination is reachable, not only that it
   resolves.
4. Set `AIRFLOW__CORE__TEST_CONNECTION` back to `Disabled` (or remove it) when you
   finish, so routine canary runs do not reach external systems.

Connectivity also requires network access from the Deployment's workers to each
destination.

## Layout

- `dags/` contains the two canary Dags.
- `include/canary.py` holds the shared, import-light check and report helpers.
- `tests/dags/test_dag_integrity.py` checks parsing, tags, expected ids, and `retries==0`.
