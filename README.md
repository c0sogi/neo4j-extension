# neo4j-extension

A Python library that provides higher-level abstractions and utilities for working with [Neo4j](https://neo4j.com/) databases. It wraps the official Neo4j Python driver to simplify both synchronous and asynchronous operations, offers object-like handling of Nodes and Relationships, and includes a system for dealing with Neo4j types (dates, times, durations, spatial data, etc.) in a Pythonic way.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
  - [Basic Connection](#basic-connection)
  - [Retrieving the Graph Schema](#retrieving-the-graph-schema)
  - [Working with Nodes and Relationships](#working-with-nodes-and-relationships)
  - [Synchronous vs. Asynchronous](#synchronous-vs-asynchronous)
- [Neo4j Types](#neo4j-types)
- [License](#license)

---

## Features

1. **Neo4jConnection**  
   - A high-level connection class that manages the underlying `neo4j.Driver` (for sync) or `neo4j.AsyncDriver` (for async).  
   - Supports environment variables to configure host, port, user, and password:
     - `NEO4J_HOST` (default `"localhost"`)
     - `NEO4J_BOLT_PORT` (default `"7687"`)
     - `NEO4J_USER` (default `"neo4j"`)
     - `NEO4J_PASSWORD` (default `""`)

2. **Schema Extraction**  
   - Quickly retrieve the database schema (labels, relationship types, properties, and indexes/constraints) via `graph_schema` and `formatted_graph_schema`.

3. **Transaction Decorators**  
   - Decorators for simpler read-write or read-only transactions, both sync and async:
     - `@with_session.readwrite_transaction`
     - `@with_session.readonly_transaction`
     - `@with_async_session.readwrite_transaction`
     - `@with_async_session.readonly_transaction`

4. **Graph Model Classes**  
   - `Graph`, `Node`, `Relationship` classes to represent and manipulate graph elements in Python.
   - `upsert_node`, `upsert_relationship`, and `upsert_graph` methods to persist changes to Neo4j.

5. **Neo4jType System**  
   - Abstract base class `Neo4jType` and concrete classes (e.g. `Neo4jString`, `Neo4jInteger`, `Neo4jBoolean`, `Neo4jPoint`, etc.) represent Neo4j values with serialization/deserialization into Cypher syntax.
   - Functions for converting back and forth between native Python objects and Neo4jType objects.

---

## Installation

```bash
git clone https://github.com/c0sogi/neo4j-extension.git
cd neo4j_extension
pip install .
```

---

## Quick Start

Below is a minimal example of establishing a connection, creating nodes, and reading the schema.

```python
from neo4j_extension.connection import Neo4jConnection
from neo4j_extension.graph import Node, Relationship, Graph

# Initialize connection (uses defaults or environment variables)
conn = Neo4jConnection()

# Connect to Neo4j (driver is lazily loaded, but you can force it here)
driver = conn.connect()

# Clear all data from the database (dangerous in production!)
conn.clear_all()

# Create some nodes and relationships
graph = Graph()
node1 = Node(uniqueId="1", properties={"name": "Alice", "age": 30}, labels={"Person"})
node2 = Node(uniqueId="2", properties={"name": "Bob", "age": 25}, labels={"Person"})
rel = Relationship(uniqueId="3", properties={"since": 2020}, rel_type="KNOWS", start_node=node1, end_node=node2)

graph.add_node(node1)
graph.add_node(node2)
graph.add_relationship(rel)

# Upsert entire graph into Neo4j
conn.upsert_graph(graph=graph)

# Print the schema discovered from the database
print(conn.formatted_graph_schema)

# Close the connection when done
conn.close()
```

---

## Usage Examples

### Basic Connection

```python
from neo4j_extension.connection import Neo4jConnection

# Provide credentials directly
conn = Neo4jConnection(
    host="my_neo4j_host",
    port="7687",
    user="neo4j",
    password="secret_password"
)

# or use environment variables:
# NEO4J_HOST, NEO4J_BOLT_PORT, NEO4J_USER, NEO4J_PASSWORD

driver = conn.connect()  # Establish the driver (sync)
conn.close()             # Close when done
```

### Retrieving the Graph Schema

```python
# Retrieve a dictionary of node properties, relationship properties, relationships, and metadata
schema = conn.graph_schema
print(schema)

# Or get a human-readable string
print(conn.formatted_graph_schema)
```

### Working with Nodes and Relationships

```python
from neo4j_extension.graph import Node, Relationship, Graph

# Build up a small graph in Python
graph = Graph()

node_alice = Node(uniqueId="1", properties={"name": "Alice"}, labels={"Person"})
node_bob   = Node(uniqueId="2", properties={"name": "Bob"},   labels={"Person"})
relation   = Relationship(
    uniqueId="3",
    properties={"since": 2021},
    rel_type="KNOWS",
    start_node=node_alice,
    end_node=node_bob
)

graph.add_node(node_alice)
graph.add_node(node_bob)
graph.add_relationship(relation)

# Upsert this graph into the DB in one transaction
conn.upsert_graph(graph)
```

### Synchronous vs. Asynchronous

- **Synchronous** (uses `@with_session` decorators):  
  ```python
  from neo4j_extension.connection import Neo4jConnection, with_session
  from neo4j import ManagedTransaction

  class MyNeo4j(Neo4jConnection):
      @with_session.readwrite_transaction
      def create_person(self, tx: ManagedTransaction, name: str):
          query = "CREATE (p:Person {name: $name}) RETURN p"
          tx.run(query, name=name)

  my_conn = MyNeo4j()
  my_conn.create_person("Alice")
  ```
  
- **Asynchronous** (uses `@with_async_session` decorators):  
  ```python
  import asyncio
  from neo4j_extension.connection import Neo4jConnection, with_async_session
  from neo4j import AsyncManagedTransaction

  class MyAsyncNeo4j(Neo4jConnection):
      @with_async_session.readwrite_transaction
      async def create_person_async(self, tx: AsyncManagedTransaction, name: str):
          query = "CREATE (p:Person {name: $name}) RETURN p"
          await tx.run(query, name=name)

  async def main():
      my_conn = MyAsyncNeo4j()
      await my_conn.create_person_async("Alice")
      await my_conn.aclose()

  asyncio.run(main())
  ```

---

## Neo4j Types

This library defines an extensive set of classes to represent Neo4j data types in Python. These classes inherit from the abstract base `Neo4jType` and implement methods like `to_cypher()` and `from_cypher()` for serialization/deserialization:

- **Primitives**: `Neo4jNull`, `Neo4jBoolean`, `Neo4jInteger`, `Neo4jFloat`, `Neo4jString`
- **Containers**: `Neo4jList`, `Neo4jMap`
- **Spatial**: `Neo4jPoint` (with `PointValue`)
- **Temporal**: `Neo4jDate`, `Neo4jLocalTime`, `Neo4jLocalDateTime`, `Neo4jZonedTime`, `Neo4jZonedDateTime`, `Neo4jDuration`
- **Binary**: `Neo4jByteArray`

You can convert a Cypher expression string to a `Neo4jType` using `convert_cypher_to_neo4j(expr)`, and convert between Python native values and `Neo4jType` via `ensure_neo4j_type(value)` or `convert_neo4j_to_python(value)`.

---

## License

MIT License

---

**Enjoy using `neo4j_extension` to simplify your Neo4j workflows!**
