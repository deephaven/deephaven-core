---
title: Crash Course Overview
description: A crash course in Deephaven that covers tables, time and calendars, query strings, plots, data I/O, and configuration.
hide_table_of_contents: true
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

<div className="comment-title">

This crash course takes you from starting a Deephaven server to building real-time queries and plots. Each chapter builds on the ones before it, so work through them in order.

</div>

<hr className="margin-bottom--lg" />

<div className="row">

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/get-started/">

## Get started

Start a Deephaven server with Docker, open the IDE, and get familiar with the console and workspace.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/architecture-overview/">

## Architecture overview

Learn how Deephaven's tables, table operations, engine, and user interface fit together, starting from the central idea that everything is a table.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/create-tables/">

## Create tables

Learn to create static tables from scratch, read CSV data from a file or URL into a table, create ticking tables that update in real time, and replay static data as a ticking table.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/table-ops/">

## Table operations

Learn the table operations that appear in almost every query: add, select, and drop columns; filter and sort rows; group and aggregate data; and merge and join tables.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/time-calendars/">

## Time and calendars

Work with date-time types, time zones, time arithmetic, and business calendars.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/query-strings/">

## Query strings

Learn the query-string syntax that table operations use to filter rows and compute new columns, and how to call built-in and user-defined functions from inside a query string.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/vectorization-vs-loops/">

## Recipes, not loops!

Understand why Deephaven uses declarative recipes instead of loops. Learn how a recipe lets query results update in real time as new data arrives, without re-running your code.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/plots/">

## Real-time plots

Whether your data is static or updating in real time, Deephaven can plot it. Build basic plots, plots with multiple axes, and subplots with the built-in plotting API.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/export-data/">

## Data I/O

Read data into Deephaven and write it back out in common formats and systems, including CSV, Parquet, Kafka, and Iceberg.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/configure/">

## Configure your Deephaven instance

Change authentication, choose a deployment, install Java packages, set memory limits, and keep your instance up to date.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/getting-started/crash-course/crash-course-wrap-up/">

## Wrapping up

Find out where to go next in the documentation and where to get help from the Deephaven community.

</CoreTutorialCard>

</div>
