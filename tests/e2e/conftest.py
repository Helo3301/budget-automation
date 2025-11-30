"""Playwright E2E test fixtures."""
import os
import pytest
import subprocess
import time
import socket
from pathlib import Path


def is_port_in_use(port: int) -> bool:
    """Check if a port is in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


@pytest.fixture(scope="session")
def web_server():
    """Start the web server for E2E tests."""
    # Check if server already running
    if is_port_in_use(5000):
        yield "http://localhost:5000"
        return

    # Start the web server with uvicorn
    project_root = Path(__file__).parent.parent.parent
    env = {**os.environ, "PYTHONPATH": str(project_root.parent)}
    server_proc = subprocess.Popen(
        ["python3", "-m", "uvicorn", "web.api:app", "--host", "127.0.0.1", "--port", "5000"],
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env
    )

    # Wait for server to start
    max_wait = 10
    for _ in range(max_wait * 10):
        if is_port_in_use(5000):
            break
        time.sleep(0.1)
    else:
        server_proc.kill()
        raise RuntimeError("Web server failed to start")

    yield "http://localhost:5000"

    # Cleanup
    server_proc.terminate()
    server_proc.wait(timeout=5)


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Configure browser context for tests."""
    return {
        **browser_context_args,
        "viewport": {"width": 1280, "height": 720},
        "ignore_https_errors": True,
    }
