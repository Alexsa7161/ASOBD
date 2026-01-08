

import os
import socket
import time
import pytest
import requests
import psycopg2

RESULT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "result")
INTEGRATION_RESULT_DIR = os.path.join(RESULT_DIR, "integration")
os.makedirs(INTEGRATION_RESULT_DIR, exist_ok=True)


def detect_hosts():
    """Автоопределение хостов: docker-сеть или localhost"""
    localhost_hosts = {
        "postgres": "localhost",
        "airflow": "localhost",
        "clickhouse": "localhost",
        "prometheus": "localhost",
        "grafana": "localhost"
    }
    
    docker_hosts = {
        "postgres": "postgres",
        "airflow": "airflow",
        "clickhouse": "clickhouse",
        "prometheus": "prometheus",
        "grafana": "grafana"
    }
    
    try:
        socket.gethostbyname("postgres")
        print("docker-compose network detected")
        return docker_hosts
    except socket.gaierror:
        print("localhost mode")
        return localhost_hosts


def wait_for_tcp(host, port, timeout=10, interval=1):
    print(f"TCP {host}:{port}")
    import socket
    start = time.time()
    attempts = 0
    while time.time() - start < timeout:
        attempts += 1
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"TCP {host}:{port} OK (attempt {attempts})")
                return True
        except OSError:
            print(f"TCP attempt {attempts}: failed")
        time.sleep(interval)
    print(f"TCP {host}:{port} timeout")
    return False


def wait_for_http(url, timeout=10, interval=1):
    print(f"HTTP {url}")
    start = time.time()
    attempts = 0
    while time.time() - start < timeout:
        attempts += 1
        try:
            r = requests.get(url, timeout=3)
            print(f"HTTP attempt {attempts}: {r.status_code}")
            if r.status_code == 200:
                print(f"HTTP {url} OK")
                return True
        except Exception as e:
            print(f"HTTP attempt {attempts}: {type(e).__name__}: {e}")
        time.sleep(interval)
    print(f"HTTP {url} timeout")
    return False


@pytest.mark.integration
def test_clickstream_full_stack():
    print("integration test started")
    
    HOSTS = detect_hosts()
    print(f"hosts: {HOSTS}")
    

    assert wait_for_tcp(HOSTS["postgres"], 5432), f"postgres {HOSTS['postgres']}:5432 unavailable"
    

    assert wait_for_http(f"http://{HOSTS['airflow']}:8080"), f"airflow {HOSTS['airflow']}:8080 unavailable"
    

    assert wait_for_http(f"http://{HOSTS['clickhouse']}:8123/ping"), f"clickhouse {HOSTS['clickhouse']}:18123 unavailable"
    

    assert wait_for_http(f"http://{HOSTS['prometheus']}:9090/-/ready"), f"prometheus unavailable"
    

    assert wait_for_http(f"http://{HOSTS['grafana']}:3000/api/health"), f"grafana unavailable"
    
    print("integration test passed")
