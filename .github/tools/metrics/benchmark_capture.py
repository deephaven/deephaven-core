#!/usr/bin/env python3

import time
import click
import os
import csv
import requests
import json
import logging

from pprint import pprint
from datetime import timedelta

from jinja2 import Environment, FileSystemLoader

from google.api_core.exceptions import NotFound, InternalServerError

from google.cloud import monitoring_v3
from google.cloud import monitoring_dashboard_v1

# Lable limit of 10 is a hard API limit
GCP_LABEL_LIMIT = 10

# Dashboard limits, 10 is an organic floor, 40 is a hard API limit
DASHBOARD_METRIC_FLOOR = 10
DASHBOARD_METRIC_CEILING = 40

# Setup logger
logger = logging.getLogger("benchmark_capture")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

logger.addHandler(ch)

# Capture time for metrics
now = time.time()

# Setup GCP clients
monitoring_client = monitoring_v3.MetricServiceClient()
dashboard_client = monitoring_dashboard_v1.DashboardsServiceClient()


def record_metric(project_name, metric):
    """Create TS entry for captured metric"""

    global logger
    global now
    global monitoring_client

    series = monitoring_v3.TimeSeries()

    series.resource.type = "global"
    series.metric.type = f"custom.googleapis.com/{metric['Benchmark']}"

    # Required to maintain uniqueness of each permutation
    series.metric.labels["Run"] = metric["Run"]
    series.metric.labels["Iteration"] = metric["Iteration"]

    # Populate dynamic labels, GCP limit is 10 labels per descriptor
    for key in list(metric.keys()):
        if key not in ["Benchmark", "Run", "Iteration", "Score"]:
            series.metric.labels[key] = metric[key]

        if len(series.metric.labels) > GCP_LABEL_LIMIT:
            logger.warn(
                f"Exiting metric label loop, limit of {GCP_LABEL_LIMIT} labels."
            )
            break  # Break out, we have hit limit on labels

    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)

    interval = monitoring_v3.TimeInterval(
        {"end_time": {"seconds": seconds, "nanos": nanos}}
    )

    point = monitoring_v3.Point(
        {"interval": interval, "value": {"double_value": float(metric["Score"])}}
    )

    series.points = [point]

    logger.info(
        f"Publishing {series.resource.type}/{series.metric.type}: {metric['Score']}"
    )

    try:
        monitoring_client.create_time_series(name=project_name, time_series=[series])
    except InternalServerError:
        logger.error(
            f"Failed to publish metric {series.metric.type}, this may be because the metric descriptor has been recently created. Will retry on the next run."
        )


def get_dashboard(project_name, title, index=1):
    """Attempt to retrieve a dashboard and return the JSON"""

    global logger

    dashboard_request = monitoring_dashboard_v1.types.GetDashboardRequest(
        name=f"{project_name}/dashboards/{title}-{index}"
    )

    try:
        dashboard = dashboard_client.get_dashboard(request=dashboard_request)

        logger.info(f"Found dashboard {project_name}/dashboards/{title}-{index}.")

        return dashboard
    except NotFound:
        logger.info(
            f"Dashboard {project_name}/dashboards/{title}-{index} does not exist."
        )

        return None


def generate_dashboard(
    template_path, project_name, title, metrics, filter_keys, index=1
):
    """Generate JSON template and return Python object representation of template for later processing."""

    global logger

    logger.info(
        f"Generating dashboard template {project_name}/dashboards/{title}-{index} with {len(metrics)} metrics."
    )

    file_loader = FileSystemLoader(template_path)
    env = Environment(loader=file_loader)

    template = env.get_template("benchmark_dashboard.json.j2")

    dashboard_template = json.loads(
        template.render(
            dashboard_path=f"{project_name}/dashboards/{title}-{index}",
            title=f"{title} ({index})",
            metrics=metrics,
            filter_keys=filter_keys,
            group_by_keys=["Iteration"],
        )
    )

    return dashboard_template


def publish_dashboards(project_name, title, dashboard_templates):
    """Populate JSON dashboard template and use it to create/update a GCP Dashboard in project."""

    global logger

    for idx, dashboard_template in enumerate(dashboard_templates):
        # Create Dashboard PB
        dashboard = monitoring_dashboard_v1.Dashboard(dashboard_template)

        # Fetch dashboard to see if we need to create or update in place
        existing_dashboard = get_dashboard(project_name, title, idx + 1)

        if existing_dashboard is None:  # Create new dashboard
            dashboard_request = monitoring_dashboard_v1.types.CreateDashboardRequest(
                parent=project_name, dashboard=dashboard
            )

            logger.info(
                f"Publishing new dashboard {project_name}/dashboards/{title}-{idx + 1}."
            )

            dashboard_client.create_dashboard(dashboard_request)

        else:  # Update existing dashboard
            # Ensure we target returned version of the dashboard
            dashboard.etag = existing_dashboard.etag  # <-- Broke everything :(

            dashboard_request = monitoring_dashboard_v1.types.UpdateDashboardRequest(
                dashboard=dashboard
            )

            logger.info(
                f"Updating dashboard {project_name}/dashboards/{title}-{idx + 1}."
            )

            dashboard_client.update_dashboard(dashboard_request)


def get_metadata(path):
    """Get GCP metadata object for requested path"""

    global logger

    logger.debug(f"Querying {path} from instance metadata service.")

    url = f"http://metadata.google.internal/{path}"
    headers = {"Metadata-Flavor": "Google"}

    r = requests.get(url, headers=headers)

    if r.status_code != 404:
        return r.text
    else:
        return None


def get_project_id():
    """Retrieve GCP project from the instance metadata"""

    global logger

    logger.info("Attempting to query project ID from instance metadata service.")

    return get_metadata("/computeMetadata/v1/project/numeric-project-id")


@click.command(
    help="Read in provided glob of FILES and generate custom metrics for historical benchmark data capture."
)
@click.option(
    "--project-id", envvar="GCP_PROJECT_ID", default=None, help="Numeric GCP project ID"
)
@click.option(
    "--metrics-per-dashboard",
    default=40,
    help="Maximum number of metrics per dashboard",
)
@click.option("--template-path", default="templates", help="Root of template path")
@click.argument("files", nargs=-1)
def main(project_id, metrics_per_dashboard, template_path, files):
    """Read in CSV and push custom metrics to project"""

    global logger

    logger.info("Starting metrics capture and dashboard creation.")

    if project_id is None:
        project_id = get_project_id()

    project_name = f"projects/{project_id}"

    logger.info(f'Targeting GCP project "{project_name}"')

    for f in files:
        metrics = []
        metric_keys = []

        with open(f, "r") as data:
            logger.info(f"Reading {f}...")
            for metric in csv.DictReader(data):
                # We only need a single iteration of each run to aggregate keys for widgets
                if metric["Iteration"] == "1":
                    # Append keys to listing of keys to be used later in dashboard creation
                    metric_keys += metric.keys()
                    # Again don't need every iteration to create the widgets
                    metrics.append(metric)

                # Commit the metric timeseries to GCP services
                record_metric(project_name, metric)

        # Extract Dashboard name from filename
        dashboard_title = os.path.basename(f).split("-")[0]

        # Squash key list
        metric_keys = set(metric_keys)
        # Remove keys that will NOT be used for creating metric filter in the dashboard genneration
        filter_keys = list(
            metric_keys - set(["Benchmark", "Run", "Iteration", "Score"])
        )[
            :9
        ]  # Limit to first 10 keys

        if metrics_per_dashboard > DASHBOARD_METRIC_CEILING:
            logger.warning(
                "Metrics per dashboard can not exceed 40 per GCP API limitations. Reset value to 40."
            )
            metrics_per_dashboard = DASHBOARD_METRIC_CEILING

        elif metrics_per_dashboard < DASHBOARD_METRIC_FLOOR:
            logger.warning("Metrics per dashboard below 10. Reset value to 10.")
            metrics_per_dashboard = DASHBOARD_METRIC_FLOOR

        # Generate dashboard templates
        dashboard_templates = []
        windows_size = len(metrics) + (metrics_per_dashboard - 1)
        windows = windows_size // metrics_per_dashboard
        for i in range(windows):
            metrics_slice = metrics[
                (i * metrics_per_dashboard) : (i + 1) * metrics_per_dashboard
            ]

            dashboard_templates.append(
                generate_dashboard(
                    template_path,
                    project_name,
                    dashboard_title,
                    metrics_slice,
                    filter_keys,
                    index=i + 1,
                )
            )

        # Publish dashboards to GCP
        publish_dashboards(project_name, dashboard_title, dashboard_templates)

    logger.info("Completed metrics capture and dashboard creation.")


if __name__ == "__main__":
    main()
