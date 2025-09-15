---
title: Servers & Clients
---

Deephaven offers both server-side and client-side APIs in multiple languages. These APIs allow you to build applications in a multitude of ways that distribute workloads across multiple machines, share data, and provide seamless user experiences.

When should you use server-side APIs? When should you use client-side APIs? When might you need multiple servers? This guide answers these questions by:

- Explaining the differences between server-client and peer-to-peer architectures.
- Helping you build a mental model of servers and clients.
- Covering how Deephaven's server- and client-side APIs work.
- Providing guidance on choosing the right architecture for your application.

## Software architecture

Platforms that offer both server-side and client-side APIs generally support two software architectures. Deephaven is no different.

### Server-client software architecture

Server-client architecture in modern software development follows a model where:

- A centralized process, called the server, provides services to users.
- Decentralized processes, called clients, send requests to the server to access services.

### Peer-to-peer software architecture

Peer-to-peer architecture in software development follows a model where every process is both a client and a server. Multiple instances of Deephaven can act as peers, allowing for distributed processing across machines. In this model:

- Each Deephaven instance can both provide and consume services
- Data and processing can be distributed across multiple nodes
- Communication happens directly between peers rather than through a central server

### A mental model of servers and clients

Consider a restaurant serving food to customers. In this analogy, the restaurant is the server, and the customers are clients.

Customers enter the restaurant, get seated, and look at the menu. After some time, they place orders for food and/or drinks. Those orders are taken by the wait staff to the kitchen and/or bar, where food and drinks are prepared and brought back to the customers.

The roles in this scenario are analogous:

- Customer -> Client
- Kitchen -> Server
- Wait staff -> Communication channel

You could draw more analogies, but these three form the basis of the server-client model.

This analogy helps build a mental model of how servers and clients work in software, and Deephaven is no different. When you connect a client to the server, you establish a communication channel to that server. From there, your requests get sent across that channel to the server, which processes your requests and sends responses back in the form of snapshots of tables or other data. The heavy lifting is done by the server itself; that's where the processing happens. The client merely sends and receives requests and consumes the responses.

### Deephaven servers and clients

Deephaven servers and clients follow the standard server-client software architecture:

- The vast majority of work happens on the server.
- Clients do little to no real data processing. They merely send requests to a server and receive responses with the requested information.

A couple of extra details that are worth noting include:

- Not all of Deephaven's client APIs support ticking data like the server does. When the client does not support ticking data, it receives snapshots of live tables at the time they are requested.
- Clients are agnostic to the server-side API language being used.

## An example server-client application

Consider an at-home golf simulator with a launch monitor that captures data about each golf shot. Here's how a Deephaven server-client architecture could enhance this setup:

**Server side:**

- A Deephaven server runs on the computer connected to the launch monitor
- It ingests real-time shot data as it happens
- Processes the data to calculate metrics (ball speed, spin rate, distance, etc.)
- Creates and maintains tables with player statistics and performance trends
- Handles data storage and retrieval

**Client side:**

- Multiple clients running on separate devices can connect to the server
- Clients can request filtered shot information (e.g., only drives or only a specific player's shots)
- Users can request new calculations or metrics through the client
- Clients can visualize shot data through charts and graphs
- Multiple players can access their statistics simultaneously from different devices

This architecture centralizes the data processing while allowing flexible access from various devices.

## An example multi-server application

Multi-server applications distribute workloads across multiple machines to handle high-volume data processing or specialized tasks. Let's examine a financial trading application that uses Deephaven's peer-to-peer architecture:

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

## Server and client considerations

The first steps to application development should always be planning. The following subsections cover the most important considerations for Deephaven application design.

### Data intensity

If your application deals with low to moderate volumes of data, a single server is likely sufficient. For example, if you process one million rows per day, a single server should have no problem handling it.

However, consider an application that processes 100 million rows a day. A single server will likely have no issue for a while, but after some time, it may not be enough.

Additionally, what type of data are you working with? If you work with lots of numeric data, it might not be much of an issue, but if there's lots of large string data, memory management becomes an important consideration.

### Computational resources

The number of servers you need is dependent on your available computational resources.

If you can only dedicate 8GB of RAM to Deephaven, your application may need more than a single server to run effectively. Similarly, you may not need multiple servers if you can dedicate 32GB of RAM to a single server instance.

### Latency requirements

If your application requires very low latency, consider where processing should occur:

- **Server-side processing**: Minimizes latency for data-intensive operations since no data transmission is required between components. This is ideal for time-sensitive calculations.

- **Client-side processing**: Introduces some network latency as data must be transmitted from server to client. This may be acceptable for visualization or less time-sensitive operations.

Packaging critical operations to run directly on a Deephaven server can significantly reduce latency. The trade-off is that servers typically require more resources than lightweight clients.

### Specialized workloads

Consider using multiple servers when your application has distinct processing requirements:

- **Data segregation**: When different data sets need to be processed separately for security or compliance reasons
- **Resource optimization**: When some workloads are CPU-intensive while others are memory-intensive
- **Specialized processing**: When different parts of your application require different libraries or configurations

For example, you might dedicate one server to real-time data ingestion and another to complex analytical queries, allowing each to be optimized for its specific task.

### Scalability

As your application grows, consider how it will scale:

- **Vertical scaling**: Adding more resources (RAM, CPU) to a single server has limits and may become cost-prohibitive
- **Horizontal scaling**: Adding more servers distributes the workload and provides more flexibility

Deephaven's architecture supports horizontal scaling through its peer-to-peer capabilities. This approach typically requires less engineering effort than complex performance tuning of a single server and provides better fault tolerance.

When designing your application, consider future growth patterns and build with scalability in mind from the beginning.

### Data security

Your architecture choices have significant implications for data security:

- **Single server**: Simplifies security management with a single point of control but creates a single point of failure
- **Multiple servers**: Allows for data segregation and compartmentalization but requires careful security configuration between servers
- **Server-client model**: Enables fine-grained access control but requires secure communication channels between clients and servers

Common security aspects that require careful thought when designing application architecture include:

- **Authentication**: How users will identify themselves to the system
- **Authorization**: What data and operations each user can access
- **Data encryption**: Whether data needs to be encrypted at rest and/or in transit
- **Network isolation**: Whether servers should be isolated on separate networks
- **Audit logging**: How user actions will be tracked and monitored

Deephaven provides different authentication mechanisms that can be configured according to your security requirements.

## Deephaven's APIs

Deephaven offers two server-side APIs and several client-side APIs.

### Server-side

- [Python](https://deephaven.io/core/pydoc/)
- [Groovy](https://deephaven.io/core/javadoc/)

### Client-side

- [Python](https://docs.deephaven.io/core/client-api/python/)
- [Java](https://deephaven.io/core/javadoc/)
- [C++](https://docs.deephaven.io/core/client-api/cpp/)
- [R](https://docs.deephaven.io/core/client-api/r/)
- [JavaScript](https://docs.deephaven.io/core/client-api/javascript/modules/dh.html)
- [Go](https://pkg.go.dev/github.com/deephaven/deephaven-core/go)

## Choose the right architecture

When designing your Deephaven application, consider these key questions:

1. **Data volume and complexity**: How much data will you process? How complex are your calculations?
2. **Performance requirements**: What are your latency requirements? Do you need real-time updates?
3. **Resource constraints**: What hardware resources are available? What are your budget constraints?
4. **User access patterns**: How many users need access? From what types of devices?
5. **Scalability needs**: How will your application grow over time?

Start with the simplest architecture that meets your needs, then scale as required. Many applications begin with a single server and add clients or additional servers as they mature.

## Related documentation

- [Core API design](./deephaven-core-api.md)
- [URIs in Deephaven](/core/docs/how-to-guides/use-uris/)
