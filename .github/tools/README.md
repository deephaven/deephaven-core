# GitHub Workflow Tools

## Nightly Benchmark Metrics and Dashboarding

Location: `.github/tools/metrics/benchmark_capture.py`

The benchmark capture tool has been created to run after the nightly benchmark job in an effort to capture result data and publish it to GCP Monitoring for historical reference. This script consumes the CSV files that the benchmark gradle jobs publish to the `tmp/logs/` folder at the root of the project during the `Nightly Benchmark` workflow execution.

### Benchmark File Format
Due to constraints from the GCP API we are limited to 10 labels per metric. This is a limitation imposed by GCP and to accomodate this we have a check to only process the first 9 fields, the `Benchmark` field is skipped as it is used as the metric's base name.

#### Baseline Fields
- Benchmark: Name of the benchmark, used as the base of the metric name.
- Score: Value used as the metric point.
- Run & Iteration are not used in the dashboarding and the mean average of each metric combination is redered as a single data point.

#### Dynamic Fields
Any fields beyond the "Benchmark", "Score", "Run" and "Iteration" are added as labels to allow for the filtering and aggregation of the various permutations for all benchmark results.

### Dashboard Creation

For each CSV file processed by the tooling a dashboard will be generated to render the metrics. Each dashboard is limited to rendering a total of 40 metrics, this is a customizable value from the commandline but has been chosen as a default in line with the upper limit from GCP for metrics per dashboard.

The dashboard title is generated from the filename basename and postfixed by the dashboard rowSet, in the even there are greater than 40 metrics total then you will see multiple dashboards created to support this.

### GitHub Actions Integration

Location: `.github/workflows/nightly-benchmarks.yml`

The `nightly-benchmarks.yml` file includes a `benchmarks` section which can be extended to expand the scope of benchmarks run and published, for each benchmark step defined the CSV files will be consumed in the later publish-metrics step as well as be archived to GitHub artifacts by the archive step.

The following template can be used to create additional benchmark steps within the `benchmark` job definition.

```
- name: Benchmark - [BENCHMARK_NAME]
uses: burrunan/gradle-cache-action@v1
with:
    job-id: benchmark
    arguments: [GRADLE_COMMAND]
    gradle-version: wrapper
```