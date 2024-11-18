# DAG Execution Server

A flexible server for executing Directed Acyclic Graphs (DAGs) with support for caching, parallel execution, and parameterized nodes.

## Features

- ✨ Dynamic DAG execution
- 🔄 Node output caching
- ⚡️ Parallel execution of independent nodes
- 🎛️ Parameterized node execution
- 🔍 Cycle detection and validation
- 🎯 Selective output nodes

## API

### Execute DAG

`POST /compose`

Execute a DAG with specified nodes and dependencies.

```
# 1. Basic DAG - shows how nodes depend on each other
curl -X POST http://localhost:3000/compose -H "Content-Type: application/json" -d '{
  "nodes": {
    "A": {
      "depends_on": []
    },
    "B": {
      "depends_on": ["A"]
    }
  }
}'
# Response: {"request_id":"uuid","outputs":{"A":{"value":2},"B":{"value":4}}}


# 2. Using parameters - shows how to modify node behavior
curl -X POST http://localhost:3000/compose -H "Content-Type: application/json" -d '{
  "nodes": {
    "A": {
      "depends_on": [],
      "params": {
        "multiplier": 3.0
      }
    }
  }
}'
# Response: {"request_id":"uuid","outputs":{"A":{"value":6}}}


# 3. Caching workflow - shows how to cache and reuse results
# First request - cache the results
curl -X POST http://localhost:3000/compose -H "Content-Type: application/json" -d '{
  "nodes": {
    "A": {
      "depends_on": [],
      "cache_output": true
    },
    "B": {
      "depends_on": ["A"],
      "cache_output": true
    }
  }
}'
# Response: {"request_id":"123-abc","outputs":{"A":{"value":2},"B":{"value":4}}}

# Second request - use cached results
curl -X POST http://localhost:3000/compose -H "Content-Type: application/json" -d '{
  "request_id": "123-abc",
  "nodes": {
    "A": {
      "depends_on": [],
      "use_cached_inputs": true
    },
    "C": {
      "depends_on": ["A"]
    }
  }
}'
# Response: {"request_id":"123-abc","outputs":{"A":{"value":2},"C":{"value":7}}}


# 4. Selective outputs - only return specific nodes
curl -X POST http://localhost:3000/compose -H "Content-Type: application/json" -d '{
  "nodes": {
    "A": {"depends_on": []},
    "B": {"depends_on": ["A"]},
    "C": {"depends_on": ["A"]}
  },
  "output_nodes": ["B"]
}'
# Response: {"request_id":"uuid","outputs":{"B":{"value":4}}}
```

### Validate DAG

`POST /validate`

```
# 1. Valid DAG - should return 200 OK
curl -X POST http://localhost:3000/validate -H "Content-Type: application/json" -d '{
  "nodes": {
    "A": {
      "depends_on": []
    },
    "B": {
      "depends_on": ["A"]
    }
  }
}'
# Response: "Valid DAG configuration"


# 2. Cyclic dependency - should return 400 Bad Request
curl -X POST http://localhost:3000/validate -H "Content-Type: application/json" -d '{
  "nodes": {
    "A": {
      "depends_on": ["B"]
    },
    "B": {
      "depends_on": ["A"]
    }
  }
}'
# Response: "Cycle detected involving node A"


# 3. Missing dependency - should return 400 Bad Request
curl -X POST http://localhost:3000/validate -H "Content-Type: application/json" -d '{
  "nodes": {
    "A": {
      "depends_on": []
    },
    "B": {
      "depends_on": ["C"]
    }
  }
}'
# Response: "Node B depends on non-existent node C"


# 4. Empty DAG - should return 400 Bad Request
curl -X POST http://localhost:3000/validate -H "Content-Type: application/json" -d '{
  "nodes": {}
}'
# Response: "empty dag"
```

### Hitting Node Endpoints

`POST /endpoint/:node_name`

```
# 1. Node A - basic handler
curl -X POST http://localhost:3000/endpoint/A -H "Content-Type: application/json" -d '{
  "inputs": {},
  "params": {}
}'
# Response: {"value": 2}


# 2. Node A with multiplier parameter
curl -X POST http://localhost:3000/endpoint/A -H "Content-Type: application/json" -d '{
  "inputs": {},
  "params": {
    "multiplier": 3.0
  }
}'
# Response: {"value": 6}


# 3. Node B - doubles its input
curl -X POST http://localhost:3000/endpoint/B -H "Content-Type: application/json" -d '{
  "inputs": {
    "A": 5
  }
}'
# Response: {"value": 10}


# 4. Node C - adds 5 to its input
curl -X POST http://localhost:3000/endpoint/C -H "Content-Type: application/json" -d '{
  "inputs": {
    "A": 3
  }
}'
# Response: {"value": 8}


# 5. Node D - multiplies its inputs
curl -X POST http://localhost:3000/endpoint/D -H "Content-Type: application/json" -d '{
  "inputs": {
    "B": 4,
    "C": 7
  }
}'
# Response: {"value": 28}


# 6. Error case - wrong input type
curl -X POST http://localhost:3000/endpoint/B -H "Content-Type: application/json" -d '{
  "inputs": {
    "A": "not a number"
  }
}'
# Response: {"error": "expects numeric input"}
```