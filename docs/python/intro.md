---
title: Community Core Docs
description: Learn how to use the Deephaven real-time query engine. Find getting-started tutorials, practical how-to guides, deep conceptual explanations, detailed API references, and helpful cheat sheets.
hide_title: true
slug: /docs
---

import { TaglineCarouselTyping } from '@site/src/pages/fragments/\_TaglineCarousel';
import { DocsSectionCount } from '@theme/deephaven/core-docs-components';
import { QuickstartCTA, TutorialCTA, HelpCTA } from '@theme/deephaven/CTA';

<h3 className="text--primary margin-bottom--sm margin-top--md">Learn Deephaven</h3>

<!-- Headlines rotate through at top; each is expected to be split on two lines -->

<TaglineCarouselTyping rows={2} taglines={[
'and build real-time\n applications',
'to join and\n transform streams',
'to explore data\n in real time',
'and query streaming\n dataframes',
]}
/>

<div className="comment-title">

Deephaven Community Core is a real-time, time-series, column-oriented query engine with relational database features. [Read the overview](./conceptual/deephaven-overview.md) for a more detailed introduction.

</div>

<div className="padding-vert--sm"></div>
<hr />
<div className="padding-vert--md"></div>

<div className="row">
<QuickstartCTA /><TutorialCTA/>
</div>

<HelpCTA/>

<div className="row padding-vert--lg">
<div className="col">

<DocsSectionCount id={"core"} path={"docs/how-to-guides"}>

## How-to guides

</DocsSectionCount>

Step-by-step guides to help you achieve a specific goal. Most useful when you're trying to get something done for the first time.

</div>
<div className="col col--4 popular-articles-list">

Popular guides

- [Use filters](./how-to-guides/use-filters.md)
- [Group and ungroup data](./how-to-guides/grouping-data.md)
- [Read Parquet files](./how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](./how-to-guides/data-import-export/parquet-export.md)
- [Connect to Kafka](./how-to-guides/data-import-export/kafka-stream.md)
- [Install Python packages](./how-to-guides/install-and-use-python-packages.md)

</div>
</div>

<div className="row padding-vert--lg">
<div className="col">
<DocsSectionCount id={"core"} path={"docs/conceptual"}>

## Conceptual guides

</DocsSectionCount>

Clear explanations of core concepts to improve your fundamental understanding of Deephaven building blocks. Most useful for gaining mastery of a topic.

</div>
<div className="col col--4 popular-articles-list">

Important concepts

- [Deephaven overview](./conceptual/deephaven-overview.md)
- [Deephaven's design](./conceptual/deephaven-design.md)
- [Designing the Core API](./conceptual/deephaven-core-api.md)
- [Incremental update model](./conceptual/table-update-model.md)
- [Docker bind mounts](./conceptual/docker-data-volumes.md)

</div>
</div>

<div className="row padding-vert--lg">
<div className="col">
<DocsSectionCount id={"core"} path={"docs/reference"}>

## Reference guides

</DocsSectionCount>

Descriptions of Deephaven's methods, functions, and classes, with examples. Most useful when you need detailed information about a specific Deephaven API. For the full signature of every class and method, use the generated API documentation.

</div>
<div className="col col--4 popular-articles-list">

Generated API documentation

- [Pydoc (Python server)](/core/pydoc)
- [Pydoc (Python client)](/core/client-api/python)
- [Javadoc (Java client/server)](/core/javadoc)
- [JavaScript (client)](/core/client-api/javascript/modules/dh.html)
- [Godoc (Go client)](https://pkg.go.dev/github.com/deephaven/deephaven-core/go)
- [C++ (client)](/core/client-api/cpp)
- [C++ (client examples)](/core/client-api/cpp-examples)
- [R (client)](/core/client-api/r)

</div>
</div>

<div className="row padding-vert--lg">
<div className="col">
<DocsSectionCount id={"core"} path={"docs/reference/cheat-sheets"}>

## Cheat sheets

</DocsSectionCount>

Short snippets and example queries. Most useful to keep open in a browser tab while first learning to write queries.

</div>
<div className="col col--4 popular-articles-list">

Useful cheat sheets

- [Ultimate cheat sheet](./reference/cheat-sheets/cheat-sheet.md)
- [Table constructors](./reference/cheat-sheets/simple-table-constructors.md)
- [Time operations](./reference/cheat-sheets/time-cheat-sheet.md)

</div>
</div>
