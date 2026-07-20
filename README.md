# gsd-canary-dags

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
| `airflow-3/` | 3.1-17 | 3.1.8 | Task SDK (`airflow.sdk`) |

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

By default, `connection_testing` only confirms that each Connection resolves. To
also confirm that the destination system is reachable, the canary calls
`Connection.test_connection()` for every Connection when the
`AIRFLOW__CORE__TEST_CONNECTION` environment variable is set to `Enabled`.

Connectivity testing is off by default because it reaches external systems, and
Airflow disables connection testing by default for security. Enable it
temporarily, run the canary, then remove the variable. Each project README
describes how to set the variable locally and on a Deployment. Connectivity
testing also requires the provider package for each `conn_type` and network access
from the workers to the destination.

## Work with a project

Each folder is a standard Astro project. From inside `airflow-2/` or `airflow-3/`:

```bash
astro dev parse     # validate that Dags parse with no import errors
astro dev start     # run Airflow locally
astro dev pytest    # run the Dag-integrity tests
astro deploy        # push code to an Astro Deployment on the matching Astro Runtime
```

Trigger a canary on your Deployment and choose what it checks:

1. In the Astro UI, open your Deployment, then open the Airflow UI.
2. Trigger the `variable_testing` or `connection_testing` Dag with config. Edit the `variables` or `connections` field to set what the canary checks, or leave it unchanged to check the default list.
3. Open the `summarize` task's logs to read the results. `summarize` logs a pass/fail table for every check and fails the run if any check failed, so the run passes only when every item resolves.
