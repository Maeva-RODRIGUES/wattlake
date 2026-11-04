"""Capture README screenshots of the Nessie and Trino web interfaces.

Steps: run the smoke test, open port-forwards to Nessie and Trino, take the
screenshots with a headless Chromium into docs/images/, close the port-forwards.
Each screenshot is taken only once the expected content is on screen.

Usage: make screenshots
"""

import json
import socket
import subprocess
import sys
import time
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

REPO_ROOT = Path(__file__).resolve().parents[2]
IMAGES_DIR = REPO_ROOT / "docs" / "images"

KUBE_CONTEXT = "k3d-wattlake"
NAMESPACE = "wattlake"
# Unusual local ports, to avoid clashing with port-forwards opened by hand.
TRINO_PORT = 18080
NESSIE_PORT = 29120
TRINO_URL = f"http://127.0.0.1:{TRINO_PORT}"
NESSIE_URL = f"http://127.0.0.1:{NESSIE_PORT}"

TRINO_USER = "wattlake"
SMOKE_TABLE = "smoke_test.readings"
VIEWPORT = {"width": 1440, "height": 900}
TIMEOUT_MS = 30_000


def run_smoke_test() -> str:
    """Run the smoke test and return its UTC start time (ISO 8601, seconds)."""
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print("==> Running make smoke-test")
    subprocess.run(["make", "smoke-test"], cwd=REPO_ROOT, check=True)
    return started_at


def get_json(url: str, headers: dict[str, str] | None = None):
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_for_port(port: int, process: subprocess.Popen, timeout_s: float = 30) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read().decode() if process.stderr else ""
            raise RuntimeError(f"port-forward on {port} exited early: {stderr.strip()}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError(f"port {port} not reachable after {timeout_s}s")


@contextmanager
def port_forwards(*forwards: tuple[str, int, int]):
    """Open one kubectl port-forward per (service, local_port, remote_port), always closed on exit."""
    processes = []
    try:
        for service, local_port, remote_port in forwards:
            print(f"==> Port-forward svc/{service} to 127.0.0.1:{local_port}")
            process = subprocess.Popen(
                [
                    "kubectl", "--context", KUBE_CONTEXT, "--namespace", NAMESPACE,
                    "port-forward", "--address", "127.0.0.1",
                    f"svc/{service}", f"{local_port}:{remote_port}",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            processes.append(process)
            wait_for_port(local_port, process)
        yield
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        print("==> Port-forwards closed")


def latest_smoke_query_id() -> str:
    """Id of the latest finished smoke test assertion query, from the Trino REST API."""
    queries = get_json(f"{TRINO_URL}/v1/query", headers={"X-Trino-User": TRINO_USER})
    matches = [
        q for q in queries
        if q["state"] == "FINISHED" and "data_check" in q["query"]
    ]
    if not matches:
        raise RuntimeError("no finished smoke test query found in Trino")
    return max(matches, key=lambda q: q["queryStats"]["createTime"])["queryId"]


def page_box(page: Page, selector_or_locator) -> dict:
    """Bounding box of an element in page coordinates, whatever the scroll position."""
    locator = page.locator(selector_or_locator) if isinstance(selector_or_locator, str) else selector_or_locator
    return locator.evaluate(
        "e => { const r = e.getBoundingClientRect();"
        " return {x: r.x + scrollX, y: r.y + scrollY, width: r.width, height: r.height}; }"
    )


def capture_nessie(page: Page, started_at: str) -> Path:
    """Commit log of branch main, cropped to the commits made by this smoke test run."""
    history = get_json(f"{NESSIE_URL}/api/v2/trees/main/history")["logEntries"]
    messages = [
        e["commitMeta"]["message"] for e in history
        if e["commitMeta"]["commitTime"][:19] >= started_at
    ]
    if not messages:
        raise RuntimeError(f"no Nessie commit on main since {started_at}Z")

    page.goto(f"{NESSIE_URL}/commits/main")
    rows = page.locator(".commitLog__body")
    rows.nth(len(messages) - 1).wait_for(timeout=TIMEOUT_MS)
    # The UI must list the same commits, in the same order, as the API.
    for index, message in enumerate(messages):
        if message not in rows.nth(index).inner_text():
            raise RuntimeError(f"Nessie UI row {index} does not show [{message}]")

    # Crop right at the bottom border of the last commit of this run, so no older commit shows.
    last_row = page_box(page, rows.nth(len(messages) - 1))
    path = IMAGES_DIR / "nessie-commits.png"
    page.screenshot(
        path=path,
        full_page=True,
        clip={"x": 0, "y": 0, "width": VIEWPORT["width"], "height": last_row["y"] + last_row["height"]},
    )
    return path


def capture_trino(page: Page) -> Path:
    """Details of the smoke test data_check query, cropped to its SQL and stages."""
    page.goto(f"{TRINO_URL}/ui/")
    # Without TLS, Trino only asks for a user name.
    page.fill("#usernameInput", TRINO_USER)
    page.get_by_role("button", name="LOG IN").click()
    page.wait_for_url("**/#/dashboard", timeout=TIMEOUT_MS)

    query_id = latest_smoke_query_id()
    page.goto(f"{TRINO_URL}/ui/#/queries/{query_id}")
    page.get_by_text("FINISHED").first.wait_for(timeout=TIMEOUT_MS)
    page.get_by_text("data_check").first.wait_for(timeout=TIMEOUT_MS)
    stages = page.get_by_text("Stages", exact=True)
    stages.wait_for(timeout=TIMEOUT_MS)
    page.wait_for_load_state("networkidle")

    query_title = page_box(page, page.get_by_text("Query", exact=True))
    page_height = page.evaluate("document.documentElement.scrollHeight")
    margin = 16
    path = IMAGES_DIR / "trino-query.png"
    page.screenshot(
        path=path,
        full_page=True,
        clip={
            "x": query_title["x"] - margin,
            "y": query_title["y"] - margin,
            "width": query_title["width"] + 2 * margin,
            "height": page_height - query_title["y"] + margin,
        },
    )
    return path


def main() -> int:
    started_at = run_smoke_test()
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    with port_forwards(("nessie", NESSIE_PORT, 19120), ("trino", TRINO_PORT, 8080)):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport=VIEWPORT)
                print(f"==> Saved {capture_nessie(page, started_at).relative_to(REPO_ROOT)}")
                print(f"==> Saved {capture_trino(page).relative_to(REPO_ROOT)}")
            finally:
                browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
