# GSD Canary Dags

Canary Dags (directed acyclic graphs) for Astronomer deployments running Apache
Airflow. They verify that the pieces an environment depends on resolve correctly:
Airflow Variables, Connections, and by extension the configured secrets backend.
Run them on demand after you push code or change configuration to confirm the
environment is wired correctly.

## Repository layout

This repository holds two self-contained Astro projects, one for each Airflow
major version. A single Astro Runtime image is either Airflow 2 or Airflow 3, so
each version gets its own deployable project rather than one project with version
shims.

| Folder | Astro Runtime | Airflow | Dag idioms |
| --- | --- | --- | --- |
| `airflow-2/` | 13.8.0 | 2.11.2 | `airflow.decorators`, `airflow.models`, `airflow.hooks.base` |
| `airflow-3/` | 3.3-2 | 3.3.0 | Task SDK (`airflow.sdk`) |

Both projects contain the same two canaries with identical behavior:

- `variable_testing` checks that each Airflow Variable resolves.
- `connection_testing` checks that each Airflow Connection resolves, and optionally that its destination is reachable.

## What the canaries do

- Run on demand (`schedule=None`). Trigger them manually from the Astro UI, the Astro CLI, or the Airflow REST API.
- Read the list of Variables or Connections from a Dag `param` (`type="array"`) that you can edit in the trigger form, so no code change is needed to adjust coverage.
- Check one item per mapped task through dynamic task mapping (`.expand`), so you see exactly which item failed.
- Report every result, then fail. Each check returns a structured result instead of raising, so one missing Variable does not hide the others. A final `summarize` task logs a pass/fail table and fails the run once if any check failed.
- Use no retries (`retries=0`). A canary must reflect real state, and a retry would mask a transient failure.

### Secrets backends

`Variable.get()` and `BaseHook.get_connection()` walk the standard resolution
chain: the configured secrets backend, then environment variables, then the
metadata database. Because of this, the canaries exercise the secrets backend as
a side effect.
This is basic retrievability: it confirms that a value comes back, not which
source served it.

### Connection connectivity

By default, `connection_testing` only confirms that each Connection resolves.
Confirming that the destination system is reachable requires the
`AIRFLOW__CORE__TEST_CONNECTION` environment variable to be set to `Enabled`.
Airflow disables connection testing by default for security, so this variable is
mandatory: without it, the canary can only confirm resolution. When it is set, the
canary also tests each Connection's destination through its hook.

Connectivity testing exercises the network path from your Astro Deployment to your
own infrastructure, so run it on a Deployment, not locally. Because it reaches
external systems, enable it temporarily, run the canary, then set the variable back
to `Disabled` or remove it. Each project README gives the exact steps. Connectivity
testing also requires the provider package for each `conn_type` and network access
from the workers to the destination.

## Customize for your environment

This repository is a starting point. Copy or fork it, then
edit the default lists so the canaries check the Variables and Connections your
own environment depends on:

- Connections: edit `DEFAULT_CONNECTIONS` in `dags/connection_testing_dag.py`.
- Variables: edit `DEFAULT_VARIABLES` in `dags/variable_testing_dag.py`.

Make these edits in the project that matches your Airflow major version
(`airflow-2/` or `airflow-3/`). The default lists are placeholders; replace them
with your real connection and variable names. You can also override either list
per run in the trigger form without changing code, but editing the defaults makes
the canary reflect your environment every time it runs.

## Work with a project

Run the canaries on an Astro Deployment. They check the Variables, Connections, and
destinations that a real environment depends on, so running them locally only
confirms that the Dags parse and pass their structure tests; it does not reach any
real resources.

Push the project that matches your Deployment's Astro Runtime:

```bash
astro deploy   # from inside airflow-2/ or airflow-3/
```

Trigger a canary on your Deployment and choose what it checks:

1. In the Astro UI, open your Deployment, then open the Airflow UI.
2. Trigger the `variable_testing` or `connection_testing` Dag with config. Edit the `variables` or `connections` field to set what the canary checks, or leave it unchanged to check the default list.
3. Open the `summarize` task's logs to read the results. `summarize` logs a pass/fail table for every check and fails the run if any check failed, so the run passes only when every item resolves.

To confirm the Dags parse and pass their structure tests locally:

```bash
astro dev parse     # validate that Dags parse with no import errors
astro dev pytest    # run the Dag-integrity tests
```
