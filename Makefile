.PHONY: test-server test integration-test kill-test-server setup-test-deps server load-test

# Kill any running test servers
kill-test-server:
	@echo "Killing any running test servers on port 3039..."
	-@lsof -ti:3039 | xargs kill -9 2>/dev/null || true
	@echo "Done"

# Build and start the test server
test-server:
	cargo run --manifest-path tests/test_server/Cargo.toml

# Run Python tests
test:
	python3 -m pytest tests/integration/

# Run integration tests (starts server, runs tests, cleans up)
integration-test: setup-test-deps kill-test-server
	@echo "Starting test server..."
	@cargo run --manifest-path tests/test_server/Cargo.toml &
	@echo "Waiting for server to start..."
	@sleep 2
	@echo "Running tests..."
	@python3 -m pytest tests/integration/test_dag.py -v
	@echo "Cleaning up..."
	@$(MAKE) kill-test-server

# Install Python dependencies (for integration tests)
setup-test-deps:
	pip3 install -r tests/integration/requirements.txt --break-system-packages

# Run the main server with proper configuration
server:
	@echo "Starting main server..."
	RUST_LOG=soap=debug,hyper=off \
	SCRIPTS_DIR=$(shell pwd)/scripts \
	NUM_INTERPRETERS=4 \
	cargo run --release --bin soap

# Default values for load test
TARGET_TPS ?= 128
DURATION_SECS ?= 5

# Run load test against the server
load-test:
	@echo "Running load test with ${TARGET_TPS} TPS for ${DURATION_SECS} seconds..."
	cargo run --release --bin load_test -- --target-tps ${TARGET_TPS} --duration-secs ${DURATION_SECS}