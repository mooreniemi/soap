import json
import requests
import pytest

class DagTester:
    def __init__(self, base_url="http://localhost:3000"):
        self.base_url = base_url

    def execute_dag(self, nodes=None, request_id=None, output_nodes=None, inputs=None):
        """Execute a DAG with the given configuration"""
        config = {
            "nodes": {}
        }

        # Handle nodes parameter
        if isinstance(nodes, dict):
            if "nodes" in nodes:
                config["nodes"] = nodes["nodes"]
            else:
                config["nodes"] = nodes

        # Add other configuration
        if request_id is not None:
            config["request_id"] = request_id
        if output_nodes is not None:
            config["output_nodes"] = output_nodes
        if inputs is not None:
            config["inputs"] = inputs

        print(f"\nExecuting DAG with config: {json.dumps(config, indent=2)}")
        response = requests.post(f"{self.base_url}/compose", json=config)

        if response.status_code != 200:
            raise Exception(f"Failed with status {response.status_code}: {response.text}")

        response_data = response.json()
        result = {
            "request_id": response_data["request_id"]
        }

        # Extract values from NodeOutput objects
        for node, output in response_data["outputs"].items():
            result[node] = output["value"]

        print(f"Received result: {json.dumps(result, indent=2)}")
        return result

def test_simple_dag(tester):
    """Test a simple DAG execution"""
    nodes = {
        "A": {
            "depends_on": []
        },
        "B": {
            "depends_on": ["A"]
        },
        "C": {
            "depends_on": ["A"]
        },
        "D": {
            "depends_on": ["B", "C"]
        }
    }
    result = tester.execute_dag(nodes=nodes)
    assert result["D"] == 28  # Expected value based on handlers

def test_dag_with_params(tester):
    """Test DAG execution with parameters"""
    nodes = {
        "A": {
            "depends_on": [],
            "params": {
                "return_string": True
            }
        },
        "B": {
            "depends_on": ["A"]
        },
        "C": {
            "depends_on": ["A"]
        },
        "D": {
            "depends_on": ["B", "C"]
        }
    }
    with pytest.raises(Exception) as exc_info:
        tester.execute_dag(nodes=nodes)
    assert "expects numeric input" in str(exc_info.value)

def test_invalid_dag(tester):
    """Test validation of invalid DAG"""
    nodes = {
        "A": {
            "depends_on": ["B"]  # Cycle: A depends on B
        },
        "B": {
            "depends_on": ["A"]  # Cycle: B depends on A
        }
    }
    with pytest.raises(Exception) as exc_info:
        tester.execute_dag(nodes=nodes)
    assert "Cycle detected" in str(exc_info.value)

def test_missing_dependency(tester):
    """Test handling of missing dependency"""
    nodes = {
        "A": {
            "depends_on": []
        },
        "B": {
            "depends_on": ["C"]  # C doesn't exist
        }
    }
    with pytest.raises(Exception) as exc_info:
        tester.execute_dag(nodes=nodes)
    assert "depends on non-existent node" in str(exc_info.value)

def test_caching(tester):
    """Test caching functionality"""
    nodes1 = {
        "A": {
            "depends_on": [],
            "cache_output": True
        },
        "B": {
            "depends_on": ["A"],
            "cache_output": True
        }
    }
    result1 = tester.execute_dag(nodes=nodes1)
    request_id = result1["request_id"]

    nodes2 = {
        "A": {
            "depends_on": [],
            "use_cached_inputs": True
        },
        "B": {
            "depends_on": ["A"],
            "use_cached_inputs": True
        }
    }
    result2 = tester.execute_dag(nodes=nodes2, request_id=request_id)
    assert result2["B"] == result1["B"]

def test_parallel_execution(tester):
    """Test parallel execution of independent nodes"""
    nodes = {
        "A": {
            "depends_on": []
        },
        "B": {
            "depends_on": ["A"]
        },
        "C": {
            "depends_on": ["A"]
        }
    }
    result = tester.execute_dag(nodes=nodes)
    assert "B" in result and "C" in result

def test_error_propagation(tester):
    """Test error propagation from node handlers"""
    nodes = {
        "A": {
            "depends_on": [],
            "params": {
                "return_string": True  # This will cause B to fail
            }
        },
        "B": {
            "depends_on": ["A"]
        }
    }
    with pytest.raises(Exception) as exc_info:
        tester.execute_dag(nodes=nodes)
    assert "expects numeric input" in str(exc_info.value)

def test_request_id_and_cached_inputs(tester):
    """Test that nodes can use cached inputs from a previous request"""
    tester = DagTester()

    # First request - normal execution
    response = tester.execute_dag({
        "nodes": {
            "A": {"depends_on": []},
            "B": {"depends_on": ["A"]},
        }
    })

    # Second request - B should use cached A
    response = tester.execute_dag({
        "nodes": {
            "A": {
                "depends_on": [],
                "cache_output": True
            },
            "B": {
                "depends_on": ["A"],
                "use_cached_inputs": True
            }
        }
    })
    assert "B" in response
    assert response["B"] == 4 # B doubles A's output of 5

def test_node_specific_cache_override(tester):
    """Test that cache settings can be overridden per node"""
    tester = DagTester()

    # Execute with cache disabled for specific node
    config = {
        "nodes": {
            "A": {
                "depends_on": [],
                "cache_output": False  # Disable cache for A
            },
            "B": {
                "depends_on": ["A"],
                "use_cached_inputs": True
            }
        }
    }

    # Should fail because A's output wasn't cached
    with pytest.raises(Exception, match="Node A requires cached input from A, but A has caching disabled"):
        tester.execute_dag(config)

def test_invalid_parameter_types(tester):
    """Test handling of invalid parameter types"""
    nodes = {
        "A": {
            "depends_on": [],
            "params": {"multiplier": "not_a_number"}  # Should be a number
        }
    }
    with pytest.raises(Exception) as exc_info:
        tester.execute_dag(nodes=nodes)
    assert "invalid parameter" in str(exc_info.value).lower()

def test_request_id_reuse_different_nodes(tester):
    """Test reusing request ID with different node set"""
    nodes1 = {
        "A": {
            "depends_on": [],
            "cache_output": True
        },
        "B": {
            "depends_on": ["A"],
            "cache_output": True
        }
    }
    result1 = tester.execute_dag(nodes=nodes1)
    request_id = result1["request_id"]

    # Try to use cached A with a new node D
    nodes2 = {
        "A": {
            "depends_on": [],
            "use_cached_inputs": True
        },
        "D": {
            "depends_on": ["A"]
        }
    }
    result2 = tester.execute_dag(nodes=nodes2, request_id=request_id)
    assert "D" in result2, "Should be able to use cached A with new node D"
    assert result2["A"] == result1["A"], "A should have same value from cache"

def test_error_in_middle_execution(tester):
    """Test handling error in middle of DAG execution"""
    nodes = {
        "A": {
            "depends_on": [],
            "cache_output": True
        },
        "B": {
            "depends_on": ["A"],
            "params": {"return_string": True}  # This will make B return a string
        },
        "C": {
            "depends_on": ["A"]
        },
        "D": {
            "depends_on": ["B", "C"]  # Should fail when B returns string
        }
    }
    with pytest.raises(Exception) as exc_info:
        tester.execute_dag(nodes=nodes)
    assert "numeric input" in str(exc_info.value)

def test_partial_cached_execution(tester):
    """Test executing DAG with mix of cached and new nodes"""
    # First run to cache some results
    nodes1 = {
        "A": {
            "depends_on": [],
            "cache_output": True
        },
        "B": {
            "depends_on": ["A"],
            "cache_output": True
        }
    }
    result1 = tester.execute_dag(nodes=nodes1)
    request_id = result1["request_id"]

    # Second run using some cached results with new computation
    nodes2 = {
        "A": {
            "depends_on": [],
            "use_cached_inputs": True
        },
        "B": {
            "depends_on": ["A"],
            "use_cached_inputs": True
        },
        "C": {
            "depends_on": ["A"],  # New computation based on cached A
        },
        "D": {
            "depends_on": ["B", "C"]  # Mix of cached and new results
        }
    }
    result2 = tester.execute_dag(nodes=nodes2, request_id=request_id)
    assert result2["A"] == result1["A"], "A should be cached"
    assert result2["B"] == result1["B"], "B should be cached"
    assert "C" in result2, "C should be computed"
    assert "D" in result2, "D should be computed"

def test_standalone_endpoints(tester):
    """Test direct endpoint access"""
    tester = DagTester()

    # Test node A (returns 3.0)
    response = requests.post(
        f"{tester.base_url}/endpoint/A",
        json={
            "inputs": {},
            "params": {"multiplier": 3.0}
        }
    )
    assert response.status_code == 200
    assert response.json()["value"] == 6  # 2 * 3.0

    # Test node B (doubles input)
    response = requests.post(
        f"{tester.base_url}/endpoint/B",
        json={
            "inputs": {"B": 5},
            "params": {}
        }
    )
    assert response.status_code == 200
    assert response.json()["value"] == 5

def test_output_filtering(tester):
    """Test that output_nodes parameter correctly filters results"""
    nodes = {
        "A": {
            "depends_on": []
        },
        "B": {
            "depends_on": ["A"]
        },
        "C": {
            "depends_on": ["A"]
        },
        "D": {
            "depends_on": ["B", "C"]
        }
    }

    # Request only specific nodes in output
    result = tester.execute_dag(nodes=nodes, output_nodes=["A", "D"])

    # Should only contain requested nodes
    assert set(result.keys()) == {"request_id", "A", "D"}
    assert "B" not in result
    assert "C" not in result

    # Values should still be correct
    assert result["A"] == 2
    assert result["D"] == 28

def test_validate_endpoint(tester):
    """Test the /validate endpoint"""
    # Test valid DAG
    valid_dag = {
        "nodes": {
            "A": {
                "depends_on": []
            },
            "B": {
                "depends_on": ["A"]
            }
        }
    }
    response = requests.post(f"{tester.base_url}/validate", json=valid_dag)
    assert response.status_code == 200
    assert "Valid DAG configuration" in response.text

    # Test invalid DAG (cycle)
    invalid_dag = {
        "nodes": {
            "A": {
                "depends_on": ["B"]
            },
            "B": {
                "depends_on": ["A"]
            }
        }
    }
    response = requests.post(f"{tester.base_url}/validate", json=invalid_dag)
    assert response.status_code == 400
    assert "Cycle detected" in response.text

    # Test invalid DAG (missing dependency)
    invalid_dag = {
        "nodes": {
            "A": {
                "depends_on": ["C"]  # C doesn't exist
            }
        }
    }
    response = requests.post(f"{tester.base_url}/validate", json=invalid_dag)
    assert response.status_code == 400
    assert "depends on non-existent node" in response.text

def test_dag_with_external_inputs(tester):
    """Test executing a DAG with external inputs instead of source nodes"""
    nodes = {
        "B": {
            "depends_on": ["A"]
        },
        "C": {
            "depends_on": ["A"]
        }
    }

    # Execute DAG with external input for node A
    result = tester.execute_dag(
        nodes=nodes,
        request_id=None,
        output_nodes=None,
        inputs={"A": 5}  # Provide external input
    )

    # Verify results
    assert result["B"] == 10  # B doubles input: 5 * 2
    assert result["C"] == 10  # C adds 5: 5 + 5

@pytest.fixture
def tester():
    return DagTester()

if __name__ == "__main__":
    pytest.main([__file__])