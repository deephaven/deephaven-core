---
title: Servers and Clients
---

Deephaven offers server-side and client-side APIs in multiple languages. These APIs allow you to build applications that distribute workloads across multiple machines, share data, and provide seamless user experiences.

When should you use server-side APIs? When should you use client-side APIs? When might you need multiple servers? This guide answers these questions by:

- Explaining the differences between server-client and peer-to-peer architectures.
- Covering how Deephaven's server- and client-side APIs work.
- Providing guidance on choosing the right architecture for your application.

## Software architecture

Platforms generally support two software architectures: Server-client and peer-to-peer.

### Server-client software architecture

Server-client architecture in modern software development follows a model where:

- A centralized process, called the server, provides services to users.
- Decentralized processes, called clients, send requests to the server to access services.

### Peer-to-peer software architecture

Peer-to-peer architecture in software development follows a model where every process is both a client and a server. Multiple instances of Deephaven can act as peers, allowing for distributed processing across machines. In this model:

- Each Deephaven instance can both provide and access services.
- Data and processing can be distributed across multiple nodes.
- Communication happens directly between peers rather than through a central server.

## Deephaven servers and clients

Deephaven servers and clients follow the standard server-client software architecture. When you connect a client to a Deephaven server, you establish a communication channel to that server. The server processes your requests and sends responses back to the client. For ticking tables, Deephaven uses its Barrage protocol to efficiently stream data: the client receives an initial snapshot followed by incremental updates containing only the changes. The heavy lifting is done by the server itself; that's where the processing happens. The client merely sends and receives requests and consumes the responses.

Key characteristics of Deephaven's server-client model:

- **Server-side processing**: The vast majority of computational work happens on the server, where data is stored and transformed.
- **Lightweight clients**: Clients do little to no real data processing. They send requests to the server and receive responses with the requested information.
- **Ticking table support**: Some of Deephaven's client APIs (Python, JavaScript, Java) support subscribing to ticking tables and receiving real-time updates. Other client APIs may receive static snapshots of table data at the time of the request.
- **Language agnostic**: Clients are agnostic to the server-side API language being used. A Python client can connect to a server running Groovy code, and vice versa.

## Deephaven's language support

Deephaven supports multiple languages for different purposes:

### Server-side scripting languages

Server-side scripts run directly on the Deephaven server and have full access to the table API for creating, transforming, and managing tables:

- [Python](https://deephaven.io/core/pydoc/)
- [Groovy](https://deephaven.io/core/javadoc/)

### Client APIs

Client APIs allow applications written in various languages to connect to a Deephaven server, send requests, and receive table data. Client applications can also send server-side scripts to be executed on the server:

- [Python](https://docs.deephaven.io/core/client-api/python/)
- [Java](https://deephaven.io/core/javadoc/)
- [C++](https://docs.deephaven.io/core/client-api/cpp/)
- [R](https://docs.deephaven.io/core/client-api/r/)
- [JavaScript](https://docs.deephaven.io/core/client-api/javascript/modules/dh.html)
- [Go](https://pkg.go.dev/github.com/deephaven/deephaven-core/go)

## Server vs client considerations

One of the challenges of building a distributed application is to decide where to perform different aspects of the computation. In the client-server model, you typically want data-intensive operations such as aggregations and streaming joins to be done on the server side. On the other hand, client-side operations are best for visualizations and simple sorting and filtering of already-processed data.

## Architecture planning considerations

When designing a Deephaven application, several factors influence whether you need one server or many, and how resources should be allocated. The following subsections cover the most important considerations.

### Data intensity

Data intensity depends on multiple factors that interact in complex ways:

- **Row volume**: The number of rows processed per unit of time.
- **Column count**: Tables with many columns require more memory and processing power.
- **Data types**: Numeric data (integers, doubles) is compact and fast to process. String data, especially long strings or those with high cardinality, consumes significantly more memory. Complex types (arrays, nested structures) add further overhead.
- **Data retention**: How long data needs to be kept in memory affects total capacity requirements.
- **Query complexity**: Simple aggregations have different performance characteristics than complex joins or window operations.
- **Update frequency**: High-frequency updates to ticking tables create more work than batch updates.

A single server might easily handle billions of rows of compact numeric data with simple queries, while struggling with millions of rows containing large strings and complex joins. Capacity planning requires analyzing your specific workload rather than relying on general row-count thresholds.

### Computational resources

Your server architecture depends on matching computational resources to workload requirements:

- **Memory (RAM)**: Deephaven keeps active tables in memory for fast access. Your memory needs depend on the size of your tables (rows × columns × data type sizes), the number of concurrent tables, and any intermediate results from queries. Running out of memory forces the JVM to garbage collect more frequently, degrading performance.
- **CPU cores**: Query operations, joins, aggregations, and formula evaluations benefit from multiple cores. Higher core counts improve throughput for complex queries and allow multiple operations to run in parallel.
- **Disk I/O**: While Deephaven is primarily in-memory, disk performance matters for operations like loading historical data, writing logs, or persisting snapshots. SSD storage is preferred for these operations.
- **Network bandwidth**: For applications sharing data between servers or serving many clients, network capacity can become a bottleneck.

When your workload exceeds what a single server can handle efficiently, distributing across multiple servers becomes necessary. The decision point depends on your specific resource constraints and performance requirements, not arbitrary thresholds.

### Specialized workloads

Consider using multiple servers when your application has distinct processing requirements:

- **Data segregation**: When different data sets need to be processed separately for security or compliance reasons.
- **Resource optimization**: When some workloads are CPU-intensive, while others are memory-intensive.
- **Specialized processing**: When different parts of your application require different libraries or configurations.

For example, you might dedicate one server to real-time data ingestion and another to complex analytical queries, allowing each to be optimized for its specific task.

### Scalability

As your application grows, consider how it will scale:

- **Vertical scaling**: Adding more resources (RAM, CPU) to a single server has limits and may become cost-prohibitive.
- **Horizontal scaling**: Adding more servers distributes the workload and provides more flexibility.

Deephaven's architecture supports horizontal scaling through its peer-to-peer capabilities. This approach typically requires less engineering effort than complex performance tuning of a single server and provides better fault tolerance.

When designing your application, consider future growth patterns and build with scalability in mind from the beginning.

### Data security

Your architecture choices have significant implications for data security:

- **Single server**: Simplifies security management with a single point of control but creates a single point of failure.
- **Multiple servers**: Allows for data segregation and compartmentalization but requires careful security configuration between servers.
- **Server-client model**: Enables fine-grained access control but requires secure communication channels between clients and servers.

Common security aspects that require careful thought when designing application architecture include:

- **Authentication**: How users will identify themselves to the system.
- **Authorization**: What data and operations each user can access.
- **Data encryption**: Whether data needs to be encrypted at rest and/or in transit.
- **Network isolation**: Whether servers should be isolated on separate networks.
- **Audit logging**: How user actions will be tracked and monitored.

Address these concerns early in your architecture planning, as they often influence decisions about server topology, network configuration, and client deployment.

## Example architectures

The following examples demonstrate how the planning considerations discussed above lead to different architectural choices.

### Example: Server-client application

Consider a portfolio risk monitoring system for a trading desk. Here's how a Deephaven server-client architecture enables real-time risk management:

**Server side:**

- A Deephaven server runs on a dedicated machine with access to market data feeds and position data.
- Ingests real-time market data (prices, volumes, volatility measures) and continuously updates position valuations.
- Performs complex calculations: Greeks for options positions, Value at Risk (VaR), portfolio exposure by sector/geography, and stress testing scenarios.
- Maintains historical tables of positions, P&L, and risk metrics for compliance and reporting.
- Generates real-time alerts when risk limits are breached.

**Client side:**

- Portfolio managers access risk dashboards from their workstations via Python clients.
- Traders on the desk can query specific position details and hypothetical scenarios from their terminals.
- Risk officers can request custom reports and filtered views (e.g., only high-risk positions or specific asset classes).
- Compliance teams can access audit trails and historical snapshots from separate client applications.
- Multiple users can simultaneously access and analyze the same underlying data without impacting server performance.

This architecture centralizes intensive real-time calculations on the server while allowing diverse users to access the data they need through lightweight clients.

### Example: Multi-server application

Multi-server applications distribute workloads across multiple machines to handle high-volume data processing or specialized tasks. This example shows a financial trading application that uses Deephaven's peer-to-peer architecture:

**Server A (Data Ingestion & Storage):**

- Dedicated to high-speed market data ingestion from multiple sources:
  - Real-time stock market feeds
  - Cryptocurrency exchanges
  - Futures markets
  - Options data
- Maintains connections to historical databases
- Performs initial data cleaning and normalization
- Exposes ticking tables via URIs for other servers to access

**Server B (Analysis & Client Services):**

- Connects to Server A using [URIs](../how-to-guides/use-uris.md) to access live market data
- Performs complex calculations:
  - Risk analytics
  - Trading signals
  - Portfolio optimization
- Serves processed data to client applications
- Executes automated trading strategies based on signals

**Benefits of this architecture:**

- Separation of concerns allows each server to be optimized for its specific task
- Data ingestion continues uninterrupted even during intensive analysis operations
- System can scale by adding specialized servers for specific markets or strategies
- Fault tolerance improves as the failure of one component doesn't bring down the entire system

This implementation demonstrates the [peer-to-peer](#peer-to-peer-software-architecture) software architecture, where each Deephaven instance functions as both a client and a server.

## Choosing your architecture

Determining the right architecture for your Deephaven application is challenging because it depends on the specific characteristics of your workload. There's no formula that takes your requirements as input and produces a server configuration as output. Instead, consider this iterative approach:

1. **Start simple**: Begin with a single server running your workload. This provides a baseline for understanding your actual resource consumption and performance characteristics.

2. **Measure and profile**: Monitor memory usage, CPU utilization, query latency, and update throughput under realistic conditions. These measurements reveal actual bottlenecks rather than theoretical concerns.

3. **Identify constraints**: Determine which resource (memory, CPU, network, disk I/O) becomes the limiting factor first. This guides whether you need more resources, different resources, or multiple servers.

4. **Scale incrementally**: When a single server becomes insufficient, consider whether vertical scaling (more RAM/CPU) or horizontal scaling (additional servers) better addresses your specific constraint.

The examples and considerations throughout this document provide a framework for thinking about architecture decisions, but testing with your actual data and workload remains the most reliable way to determine appropriate infrastructure.

## Related documentation

- [Core API design](./deephaven-core-api.md)
- [Use URIs to share tables](../how-to-guides/use-uris.md)
