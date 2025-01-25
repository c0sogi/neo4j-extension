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
  - [Some Handy Utility Methods](#some-handy-utility-methods)
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
   - Quickly retrieve the database schema (labels, relationship types, properties, and indexes/constraints) via `get_graph_schema()` or `get_formatted_graph_schema()`.

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
   - Functions for converting back and forth between native Python objects and these Neo4jType classes.

---

## Installation

```bash
pip install neo4j-extension@git+https://github.com/c0sogi/neo4j-extension.git
```

---

## Quick Start

Below is a minimal example of establishing a connection, creating nodes/relationships, and reading the schema.

```python
from neo4j_extension import Neo4jConnection, Graph, Node, Relationship

# Initialize connection (can also rely on environment variables)
conn = Neo4jConnection(
    host="localhost",
    port="7687",
    user="neo4j",
    password="secret"
)

# Connect to Neo4j (driver is lazily loaded; you can force initialization here)
driver = conn.connect()

# Clear all data from the database (dangerous in production!)
conn.clear_all()

# Create a small graph
g = Graph()
node1 = Node(properties={"name": "Alice", "age": 30}, labels={"Person"}, globalId="node1")
node2 = Node(properties={"name": "Bob", "age": 25}, labels={"Person"}, globalId="node2")
rel   = Relationship(properties={"since": 2020}, rel_type="KNOWS", start_node=node1, end_node=node2, globalId="rel1")

g.add_node(node1)
g.add_node(node2)
g.add_relationship(rel)

# Upsert the entire Graph into Neo4j
conn.upsert_graph(g)

# Print the schema discovered from the database
print(conn.get_formatted_graph_schema())

# Close the connection
conn.close()
```

---

## Usage Examples

### Basic Connection

```python
from neo4j_extension import Neo4jConnection

# Provide credentials directly
conn = Neo4jConnection(
    host="my_neo4j_host",
    port="7687",
    user="neo4j",
    password="secret_password"
)

# Or rely on environment variables (NEO4J_HOST, NEO4J_BOLT_PORT, etc.)
with Neo4jConnection() as conn:
  ...  # Use the connection here

# You can also use the async context manager
async with Neo4jConnection() as conn:
  ...  # Use the async connection here
```

### Retrieving the Graph Schema

```python
# Retrieve a structured dictionary with node props, rel props, relationships, and metadata
schema = conn.get_graph_schema()
print(schema)

# Or get a human-readable formatted output
print(conn.get_formatted_graph_schema())
```

### Working with Nodes and Relationships

```python
from neo4j_extension import Graph, Node, Relationship

# Build up a small graph in memory
g = Graph()

node_alice = Node(properties={"name": "Alice"}, labels={"Person"}, globalId="alice")
node_bob   = Node(properties={"name": "Bob"},   labels={"Person"}, globalId="bob")
rel        = Relationship(
    properties={"since": 2021},
    rel_type="KNOWS",
    start_node=node_alice,
    end_node=node_bob,
    globalId="rel_alice_knows_bob"
)

g.add_node(node_alice)
g.add_node(node_bob)
g.add_relationship(rel)

# Use upsert_graph to write everything in a single transaction
conn.upsert_graph(g)
```

### Some Handy Utility Methods

In addition to creating/upserting `Node` and `Relationship` objects, `Neo4jConnection` includes many convenience methods for everyday tasks:

- **Node / Relationship Lookups**  
  - `find_node_by_global_id(gid)` : Get a single node by its `globalId`  
  - `match_nodes(label, property_filters=None, limit=None)` : Find nodes by label and optional property filters  
  - `match_relationships(rel_type, property_filters=None, limit=None)` : Same pattern for relationships  
  - `find_nodes_in_relationship(rel_type, property_filters=None, limit=None)` : Returns `(start_node, relationship, end_node)` tuples

- **Deletion**  
  - `delete_node_by_global_id(gid)` : Detach and delete a single node  
  - `delete_nodes_by_label(label, property_filters=None)` : Detach and delete multiple nodes  
  - `delete_relationship_by_global_id(gid)` : Delete a single relationship  
  - `delete_relationships_by_type(rel_type, property_filters=None)` : Delete multiple relationships

- **Updating Node / Relationship Data**  
  - `update_node_properties(gid, new_properties)` : Add or overwrite properties on a node  
  - `remove_node_property(gid, property_key)` : Remove one property  
  - `add_labels_to_node(gid, labels)` / `remove_labels_from_node(gid, labels)` : Manage labels  
  - `update_relationship_properties(gid, new_properties)` : Add or overwrite properties on a relationship

- **Linking Nodes**  
  - `link_nodes(start_gid, end_gid, rel_type, properties=None)` : Merge or create a relationship between two existing nodes

- **Inspecting Existing Data**  
  - `get_node_properties(gid)` / `get_relationship_properties(gid)` : Return a dict of properties  
  - `count_nodes(label)` / `count_relationships(rel_type)` : Quickly get counts of labeled nodes or typed relationships

These utilities all use the same transaction decorators behind the scenes, so you can focus on your logic rather than boilerplate Cypher queries.

### Synchronous vs. Asynchronous

- **Synchronous** (uses `@with_session` decorators):
  ```python
  from neo4j_extension import Neo4jConnection, with_session
  from neo4j import ManagedTransaction

  class MyNeo4j(Neo4jConnection):
      @with_session.readwrite_transaction
      def create_person(self, tx: ManagedTransaction, name: str):
          query = "CREATE (p:Person {name: $name}) RETURN p"
          tx.run(query, name=name)

  my_conn = MyNeo4j()
  my_conn.create_person("Alice")
  my_conn.close()
  ```

- **Asynchronous** (uses `@with_async_session` decorators):
  ```python
  import asyncio
  from neo4j_extension import Neo4jConnection, with_async_session
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

This library defines an extensive set of classes to represent Neo4j data types in Python. They inherit from the abstract base `Neo4jType` and implement methods like `to_cypher()` and `from_cypher()` for serialization/deserialization:

- **Primitives**: `Neo4jNull`, `Neo4jBoolean`, `Neo4jInteger`, `Neo4jFloat`, `Neo4jString`
- **Containers**: `Neo4jList`, `Neo4jMap`
- **Spatial**: `Neo4jPoint` (with `PointValue`)
- **Temporal**: `Neo4jDate`, `Neo4jLocalTime`, `Neo4jLocalDateTime`, `Neo4jZonedTime`, `Neo4jZonedDateTime`, `Neo4jDuration`
- **Binary**: `Neo4jByteArray`

You can convert a Cypher expression string to a `Neo4jType` with `convert_cypher_to_neo4j(expr)` and convert between Python native values and Neo4jType objects via `ensure_neo4j_type(value)` or `convert_neo4j_to_python(value)`.

---

## License

MIT License

---

**Enjoy using `neo4j_extension` to simplify your Neo4j workflows!**