"""Capture the README screenshots of the stack's web interfaces.

Steps: run the smoke test, open port-forwards to Nessie, Trino, Argo CD and
Grafana, take the screenshots with a headless Chromium into docs/images/, close
the port-forwards. Each screenshot is taken only once the expected content is
on screen. The Argo CD and Grafana passwords are read from their Secrets and
only typed into the login forms: they are never printed or written to disk.

Usage: make screenshots
"""

import base64
import html
import json
import socket
import subprocess
import sys
import time
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

REPO_ROOT = Path(__file__).resolve().parents[2]
IMAGES_DIR = REPO_ROOT / "docs" / "images"

KUBE_CONTEXT = "k3d-wattlake"
NAMESPACE = "wattlake"
# Unusual local ports, to avoid clashing with port-forwards opened by hand.
TRINO_PORT = 18080
NESSIE_PORT = 29120
ARGOCD_PORT = 18082
GRAFANA_PORT = 13000
TRINO_URL = f"http://127.0.0.1:{TRINO_PORT}"
NESSIE_URL = f"http://127.0.0.1:{NESSIE_PORT}"
ARGOCD_URL = f"http://127.0.0.1:{ARGOCD_PORT}"
GRAFANA_URL = f"http://127.0.0.1:{GRAFANA_PORT}"

# Every Argo CD Application of the stack (infra/argocd/), all expected Synced and Healthy.
ARGOCD_APPS = [
    "alloy",
    "airflow",
    "kafka",
    "loki",
    "monitoring",
    "nessie",
    "postgres",
    "seaweedfs",
    "streaming",
    "strimzi",
    "trino",
    "wattlake",
]
REALTIME_TABLE = "iceberg.bronze.eco2mix_national_realtime"

TRINO_USER = "wattlake"
SMOKE_TABLE = "smoke_test.readings"
VIEWPORT = {"width": 1440, "height": 900}
TIMEOUT_MS = 30_000


def run_smoke_test() -> str:
    """Run the smoke test and return its UTC start time (ISO 8601, seconds)."""
    started_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
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
def port_forwards(*forwards: tuple[str, str, int, int]):
    """Open one kubectl port-forward per (namespace, service, local_port, remote_port), always closed on exit."""
    processes = []
    try:
        for namespace, service, local_port, remote_port in forwards:
            print(f"==> Port-forward {namespace}/svc/{service} to 127.0.0.1:{local_port}")
            process = subprocess.Popen(
                [
                    "kubectl",
                    "--context",
                    KUBE_CONTEXT,
                    "--namespace",
                    namespace,
                    "port-forward",
                    "--address",
                    "127.0.0.1",
                    f"svc/{service}",
                    f"{local_port}:{remote_port}",
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
    matches = [q for q in queries if q["state"] == "FINISHED" and "data_check" in q["query"]]
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
    messages = [e["commitMeta"]["message"] for e in history if e["commitMeta"]["commitTime"][:19] >= started_at]
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


def read_secret(namespace: str, name: str, key: str) -> str:
    """Value of a Secret key, decoded. Kept in memory only."""
    encoded = subprocess.run(
        [
            "kubectl",
            "--context",
            KUBE_CONTEXT,
            "--namespace",
            namespace,
            "get",
            "secret",
            name,
            "--output",
            f"jsonpath={{.data.{key}}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if not encoded:
        raise RuntimeError(f"Secret {namespace}/{name} has no key {key}")
    return base64.b64decode(encoded).decode()


def trino_query(sql: str) -> tuple[list[str], list[list]]:
    """Run sql through Trino's REST API and return the column names and rows."""
    request = urllib.request.Request(
        f"{TRINO_URL}/v1/statement", data=sql.encode(), headers={"X-Trino-User": TRINO_USER}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    columns, rows = [], []
    while True:
        if "error" in result:
            raise RuntimeError(f"Trino query failed: {result['error']['message']}")
        columns = columns or [c["name"] for c in result.get("columns", [])]
        rows.extend(result.get("data", []))
        if "nextUri" not in result:
            return columns, rows
        result = get_json(result["nextUri"], headers={"X-Trino-User": TRINO_USER})


def login_argocd(page: Page) -> None:
    page.goto(f"{ARGOCD_URL}/login")
    page.locator("input[name=username]").fill("admin")
    page.locator("input[name=password]").fill(read_secret("argocd", "argocd-initial-admin-secret", "password"))
    page.get_by_role("button", name="SIGN IN").click()
    page.wait_for_url("**/applications", timeout=TIMEOUT_MS)


def capture_argocd_apps(page: Page) -> Path:
    """Argo CD's list of Applications, once every one shows Synced and Healthy."""
    # List view, every Application on one page (the default shows 10 per page).
    page.goto(f"{ARGOCD_URL}/applications?view=list")
    page.get_by_text("Items per page", exact=False).first.click()
    page.get_by_text("all", exact=True).first.click()
    for app in ARGOCD_APPS:
        page.get_by_text(app, exact=True).first.wait_for(timeout=TIMEOUT_MS)
    page.wait_for_function(
        "n => ['Healthy', 'Synced'].every(s => document.body.innerText.split(s).length - 1 >= n)",
        arg=len(ARGOCD_APPS),
        timeout=TIMEOUT_MS,
    )
    path = IMAGES_DIR / "argocd-applications.png"
    page.screenshot(path=path, full_page=True)
    return path


def capture_argocd_streaming(page: Page) -> Path:
    """Resource tree of the streaming Application: producer and writer Deployments and their pods."""
    page.goto(f"{ARGOCD_URL}/applications/argocd/streaming?view=tree")
    for name in ("producer", "writer"):
        page.get_by_text(name, exact=True).first.wait_for(timeout=TIMEOUT_MS)
    page.wait_for_load_state("networkidle")
    path = IMAGES_DIR / "argocd-streaming.png"
    page.screenshot(path=path, full_page=True)
    return path


def capture_grafana(page: Page) -> Path:
    """The WattLake dashboard over the last 24 hours, in kiosk mode (no menus)."""
    page.goto(f"{GRAFANA_URL}/login")
    page.locator("input[name=user]").fill("admin")
    page.locator("input[name=password]").fill(read_secret("monitoring", "grafana-admin", "admin-password"))
    page.get_by_role("button", name="Log in").click()
    page.wait_for_url(lambda url: "/login" not in url, timeout=TIMEOUT_MS)

    # Grafana only renders the panels inside the viewport, and pins its kiosk
    # footer to the bottom of it: the viewport holds the whole dashboard.
    page.set_viewport_size({"width": VIEWPORT["width"], "height": 1550})
    try:
        page.goto(f"{GRAFANA_URL}/d/wattlake/wattlake?orgId=1&from=now-24h&to=now&kiosk")
        page.get_by_text("Retard du writer", exact=True).first.wait_for(timeout=TIMEOUT_MS)
        page.wait_for_load_state("networkidle")
        # Charts and log lines are drawn after the queries return.
        page.locator("canvas").first.wait_for(state="attached", timeout=TIMEOUT_MS)
        page.wait_for_timeout(5000)
        path = IMAGES_DIR / "grafana-wattlake.png"
        page.screenshot(path=path)
    finally:
        page.set_viewport_size(VIEWPORT)
    return path


def capture_realtime_versions(page: Page) -> Path:
    """The versions of one quarter-hour in the real-time bronze table, as Trino returns them.

    Trino's web interface shows queries, not their results: the rows are read
    through its REST API and laid out in a plain HTML table.
    """
    sql = (
        "SELECT date_heure, consommation, nucleaire, eolien, solaire, _source_updated_at, _kafka_offset\n"
        f"FROM {REALTIME_TABLE}\n"
        f"WHERE date_heure = (SELECT max(date_heure) FROM {REALTIME_TABLE}\n"
        "                    WHERE date_heure <= current_timestamp - INTERVAL '2' HOUR)\n"
        "ORDER BY _kafka_offset"
    )
    columns, rows = trino_query(sql)
    if len(rows) < 2:
        raise RuntimeError(f"expected several versions of the quarter-hour, got {len(rows)} row(s)")

    head = "".join(f"<th>{html.escape(c)}</th>" for c in columns)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>" for row in rows)
    page.set_content(
        "<html><body style='font-family: sans-serif; margin: 0'>"
        "<div id='result' style='display: inline-block; padding: 16px'>"
        f"<pre style='background:#f4f4f4; padding:12px; font-size:14px'>{html.escape(sql)}</pre>"
        "<table style='border-collapse: collapse; font-size: 14px'>"
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
        "<style>th, td {border: 1px solid #ccc; padding: 6px 10px; text-align: right} th {background:#eaeaea}</style>"
        f"<p style='color:#555'>{len(rows)} lignes, renvoyées par Trino ({TRINO_USER}).</p>"
        "</div></body></html>"
    )
    path = IMAGES_DIR / "trino-realtime-versions.png"
    page.locator("#result").screenshot(path=path)
    return path


def main() -> int:
    started_at = run_smoke_test()
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    with (
        port_forwards(
            (NAMESPACE, "nessie", NESSIE_PORT, 19120),
            (NAMESPACE, "trino", TRINO_PORT, 8080),
            ("argocd", "argocd-server", ARGOCD_PORT, 80),
            ("monitoring", "monitoring-grafana", GRAFANA_PORT, 80),
        ),
        sync_playwright() as playwright,
    ):
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport=VIEWPORT)
            captures = [
                lambda: capture_nessie(page, started_at),
                lambda: capture_trino(page),
                lambda: capture_realtime_versions(page),
                lambda: (login_argocd(page), capture_argocd_apps(page))[1],
                lambda: capture_argocd_streaming(page),
                lambda: capture_grafana(page),
            ]
            for capture in captures:
                print(f"==> Saved {capture().relative_to(REPO_ROOT)}")
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
