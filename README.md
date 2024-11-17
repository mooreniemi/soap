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

An embeded Python server in Rust.

You create a Rust `Snake` that handles your Python script, add it to the router, and it'll be available as an HTTP endpoint.

This combines some flexibile iteration speed in Python in a Rust execution environment.

## Why do this?

If you just are handling Python 100% of the time, there's no reason to do this.

But if you are sometimes handling things in Rust (for performance reasons), yet want to experiment with Python, this might be for you.

## Installation

Handling Python is a bit painful. See the `.cargo/config.toml` for details.

## Using it

### sample request

```
curl -s -X POST http://127.0.0.1:3000/onehot \
     -H "Content-Type: application/json" \
     -d '{"categories": ["cat", "dog", "cat", "mouse", "elephant"]}' \
| jq '.result |= map(join(", "))'
```

### sample response

```
{
  "metrics": {
    "gil_duration_ms": 0,
    "lock_duration_ms": 0,
    "total_duration_ms": 0
  },
  "result": [
    "3, 0, 0, 0",
    "0, 3, 0, 0",
    "3, 0, 0, 0",
    "0, 0, 0, 3",
    "0, 0, 3, 0"
  ],
  "version": {
    "actual": "1.3.0",
    "requested": null
  }
}
```

You can ask for different versions:

```
curl -s -X POST http://127.0.0.1:3000/onehot \
     -H "Content-Type: application/json" \
     -d '{"categories": ["cat", "dog", "cat", "mouse", "elephant"], "version": "1.2.1"}' \
| jq '.result |= map(join(", "))'
```

And those will be used, if they exist, and if not, the latest version will be used:

```
{
  "version": {
    "actual": "1.2.1",
    "requested": "1.2.1"
  }
}
```

### load test

There is an extremely simple load test in `src/load_test.rs`.

It is just pointing at the `onehot` endpoint.

```
cargo run --bin soap # the server
cargo run --bin load_test
```
