```
    ______
   /      \
  |  SOAP  |
   \______/
   o   O
 o   O
   o
```
# soap (snakes on a plane)

An embedded Python server in Rust.

You create a Rust `Snake` that handles your Python script, add it to the router, and it'll be available as an HTTP endpoint.

This combines some flexible iteration speed in Python in a Rust execution environment.

## Why do this?

If you just are handling Python 100% of the time, there's no reason to do this.

But if you are sometimes handling things in Rust (for performance reasons), yet want to experiment with Python, this might be for you.

## Installation

Handling Python is a bit painful. See the `.cargo/config.toml` for details.

## Using it

### Version Handling

You can:
- Request a specific version: `"version": "1.2.1"`
- Request latest version: omit version field or set to `null`
- Request major version only: `"version": "1"`

The response will tell you which version was actually used:

```json
{
  "version": {
    "actual": "1.2.1",    // version that was used
    "requested": "1.2.1"  // version that was requested
  }
}
```

### Example Available Endpoints

1. `/onehot` - One-hot encoding
```bash
curl -s -X POST http://localhost:3000/endpoint/onehot -H "Content-Type: application/json" -d '{
  "inputs": {
    "data": ["dog", "cat"]
  }}' | jq
{
  "value": {
    "metrics": {
      "gil_duration_ms": 0,
      "lock_duration_ms": 0,
      "total_duration_ms": 0
    },
    "result": [
      [
        0,
        3
      ],
      [
        3,
        0
      ]
    ],
    "version": {
      "actual": "1.3.0",
      "requested": null
    }
  },
  "error": null
}

```

2. `/feature_engineering` - Feature engineering
```bash
curl -X POST http://127.0.0.1:3000/feature_engineering \
     -H "Content-Type: application/json" \
     -d '{"data": [1.0, 2.0, 3.0, 4.0]}'
```

3. `/compose` - Chains onehot and feature engineering
```bash
curl -s -X POST http://localhost:3000/compose \
-H "Content-Type: application/json" \
-d '{
  "nodes": {
    "onehot": {
      "depends_on": [],
      "params": {
        "version": "1.0.0"
      }
    },
    "feature_engineering": {
      "depends_on": ["onehot"],
      "params": {
        "version": "1.0.0"
      }
    }
  },
  "inputs": {
    "data": ["dog", "cat", "mouse"]
  },
  "output_nodes": ["feature_engineering"]
}' | jq
{
  "request_id": "0525d85c-35a9-4836-a982-77976872fc12",
  "outputs": {
    "feature_engineering": {
      "value": {
        "metrics": {
          "gil_duration_ms": 0,
          "lock_duration_ms": 0,
          "total_duration_ms": 0
        },
        "result": [
          0.0,
          1.0,
          0.0,
          1.0,
          0.0,
          0.0,
          0.0,
          0.0,
          1.0
        ],
        "version": {
          "actual": "0.0.1",
          "requested": "1.0.0"
        }
      },
      "error": null
    }
  }
}

```

### Load Testing

There is a simple load test tool included:

```bash
# Start the server
cargo run --bin soap -- --num-interpreters 4 --scripts-dir ./scripts

# Run load test
cargo run --bin load_test -- --target-tps 100 --duration-secs 10
```

## Development

Scripts are hot-reloaded when modified. Place your Python scripts in the `scripts` directory with the naming format:
```
{script_name}_v{version}.py
```

For example:
- `one_hot_v1.0.0.py`
- `feature_engineering_v2.1.0.py`