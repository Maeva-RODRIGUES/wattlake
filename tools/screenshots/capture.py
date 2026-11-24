"""Capture the README screenshots of the stack's web interfaces.

Steps: run the smoke test, open port-forwards to Nessie, Trino, Airflow,
Argo CD and Grafana, take the screenshots with a headless Chromium into
docs/images/, close the port-forwards. Each screenshot is taken only once the
expected content is on screen. The Airflow, Argo CD and Grafana passwords are
read from their Secrets and only typed into the login forms: they are never
printed or written to disk.

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
from urllib.parse import quote

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
AIRFLOW_PORT = 18081
TRINO_URL = f"http://127.0.0.1:{TRINO_PORT}"
NESSIE_URL = f"http://127.0.0.1:{NESSIE_PORT}"
ARGOCD_URL = f"http://127.0.0.1:{ARGOCD_PORT}"
GRAFANA_URL = f"http://127.0.0.1:{GRAFANA_PORT}"
AIRFLOW_URL = f"http://127.0.0.1:{AIRFLOW_PORT}"

# Every Argo CD Application of the stack (infra/argocd/), all expected Synced and Healthy.
ARGOCD_APPS = [
    "alloy",
    "airflow",
    "coredns",
    "kafka",
    "loki",
    "monitoring",
    "nessie",
    "nessie-gc",
    "postgres",
    "seaweedfs",
    "streaming",
    "strimzi",
    "trino",
    "wattlake",
]
REALTIME_TABLE = "iceberg.bronze.eco2mix_national_realtime"
MONTHLY_GOLD_TABLE = "iceberg.gold.renewable_share_monthly"
DBT_DAG = "dbt_transformations"
# Its tasks, in order: the Write-Audit-Publish chain, then the daily compaction.
DBT_DAG_TASKS = ["create_audit_branch", "dbt_build", "publish", "is_daily_compaction", "compact_silver_and_gold"]
# Runs shown in the Airflow grid: a day of hourly runs, the compaction once.
AIRFLOW_GRID_RUNS = "25"

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
    # Taller than the default: the tree scrolls inside the page, and every
    # deployment adds a ReplicaSet (up to 10 kept per Deployment).
    page.set_viewport_size({"width": VIEWPORT["width"], "height": 1050})
    try:
        page.goto(f"{ARGOCD_URL}/applications/argocd/streaming?view=tree")
        for name in ("producer", "writer"):
            page.get_by_text(name, exact=True).first.wait_for(timeout=TIMEOUT_MS)
        page.wait_for_load_state("networkidle")
        # The status panel shows the author of the synced commit, e-mail address
        # included: no address may appear in the project's files.
        page.locator(".application-status-panel__item__row").filter(has_text="Author:").evaluate_all(
            "rows => rows.forEach(row => row.style.display = 'none')"
        )
        path = IMAGES_DIR / "argocd-streaming.png"
        page.screenshot(path=path, full_page=True)
    finally:
        page.set_viewport_size(VIEWPORT)
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


def format_cell(value) -> str:
    """A result value as Trino's CLI shows it: NULL as a dash, booleans in lower case."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return str(value).lower()
    return html.escape(str(value))


def render_query_result(page: Page, sql: str, columns: list[str], rows: list[list]) -> None:
    """Lay out a query and its result rows in a plain HTML table, in the #result element."""
    head = "".join(f"<th>{html.escape(c)}</th>" for c in columns)
    body = "".join("<tr>" + "".join(f"<td>{format_cell(v)}</td>" for v in row) + "</tr>" for row in rows)
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

    render_query_result(page, sql, columns, rows)
    path = IMAGES_DIR / "trino-realtime-versions.png"
    page.locator("#result").screenshot(path=path)
    return path


def capture_renewable_share(page: Page) -> Path:
    """The yearly share of renewables in the monthly gold table, as Trino returns it.

    The strict share (statistical conventions of the SDES) is NULL in 2012:
    the technology detail it needs is published from 2013 only.
    """
    sql = (
        "SELECT year(month) AS annee,\n"
        "       round(100 * sum(renewable_mwh) / sum(production_mwh), 1) AS part_large_pct,\n"
        "       round(100 * sum(renewable_strict_mwh) / sum(production_with_detail_mwh), 1) AS part_stricte_pct,\n"
        "       bool_or(is_partial) AS annee_en_cours\n"
        f"FROM {MONTHLY_GOLD_TABLE}\n"
        "GROUP BY 1 ORDER BY 1"
    )
    columns, rows = trino_query(sql)
    if len(rows) < 10:
        raise RuntimeError(f"expected one row per year since 2012, got {len(rows)}")
    render_query_result(page, sql, columns, rows)
    path = IMAGES_DIR / "gold-renewable-share.png"
    page.locator("#result").screenshot(path=path)
    return path


def login_airflow(page: Page) -> None:
    page.goto(f"{AIRFLOW_URL}/auth/login/")
    page.locator("input[name=username]").fill("admin")
    page.locator("input[name=password]").fill(read_secret(NAMESPACE, "airflow-credentials", "admin-password"))
    page.locator("input[type=submit]").click()
    page.wait_for_url(lambda url: "/auth/login" not in url, timeout=TIMEOUT_MS)


def latest_compaction_run(page: Page) -> str:
    """Id of the latest successful dbt run that also compacted silver and gold, from Airflow's REST API.

    The browser session, once logged in, authenticates the API calls.
    """
    api = f"{AIRFLOW_URL}/api/v2/dags/{DBT_DAG}/dagRuns"
    runs = page.request.get(f"{api}?state=success&order_by=-run_after&limit=50")
    if not runs.ok:
        raise RuntimeError(f"Airflow API: HTTP {runs.status} on the DAG runs")
    for run in runs.json()["dag_runs"]:
        tasks = page.request.get(f"{api}/{run['dag_run_id']}/taskInstances").json()["task_instances"]
        if {t["task_id"]: t["state"] for t in tasks}.get("compact_silver_and_gold") == "success":
            return run["dag_run_id"]
    raise RuntimeError("no successful dbt run with a compaction among the last 50")


def capture_airflow_graph(page: Page) -> Path:
    """Graph of the latest dbt run that compacted: Write-Audit-Publish, then the compaction, all succeeded."""
    run_id = latest_compaction_run(page)
    # A short viewport: the graph, centred in its canvas, sits right under
    # the header (DAG and run id), without empty canvas around it.
    page.set_viewport_size({"width": VIEWPORT["width"], "height": 330})
    try:
        page.goto(f"{AIRFLOW_URL}/dags/{DBT_DAG}/runs/{quote(run_id, safe='')}")
        page.get_by_role("button", name="Afficher le graphe (Appuyez sur g)").click()
        page.get_by_role("button", name="Replier le panneau des détails").click()
        # Each task node shows its name and its state.
        for task in DBT_DAG_TASKS:
            page.locator(".react-flow__node", has_text=task).filter(has_text="success").wait_for(timeout=TIMEOUT_MS)
        # Without the minimap and the zoom buttons, which would cover the last task.
        page.add_style_tag(content=".react-flow__minimap, .react-flow__controls, .react-flow__panel {display: none}")
        # No tooltip: the pointer rests on the empty canvas.
        page.mouse.move(VIEWPORT["width"] / 2, 320)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1000)
        path = IMAGES_DIR / "airflow-dbt-graph.png"
        page.screenshot(path=path)
    finally:
        page.set_viewport_size(VIEWPORT)
    return path


def capture_airflow_grid(page: Page) -> Path:
    """Grid of the last runs of the dbt DAG: one column per hourly run, one row per task."""
    # Narrow: the grid is drawn against the right edge, this keeps the task
    # names close to their runs.
    page.set_viewport_size({"width": 900, "height": VIEWPORT["height"]})
    try:
        page.goto(f"{AIRFLOW_URL}/dags/{DBT_DAG}")
        # The UI remembers the last view: the graph, after capture_airflow_graph.
        page.get_by_role("button", name="Afficher la grille (Appuyez sur g)").click()
        page.get_by_role("button", name="Replier le panneau des détails").click()
        page.get_by_role("button", name="Options").click()
        # The "number of runs" select, 10 by default.
        page.get_by_test_id("display-dag-run-options").get_by_role("combobox").click()
        page.get_by_role("option", name=AIRFLOW_GRID_RUNS, exact=True).click()
        page.keyboard.press("Escape")
        page.evaluate("document.activeElement && document.activeElement.blur()")
        # No tooltip: the pointer rests on the empty page below the grid.
        page.mouse.move(450, VIEWPORT["height"] - 10)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)
        last_row = page_box(page, page.get_by_text(DBT_DAG_TASKS[-1], exact=True).first)
        margin = 16
        path = IMAGES_DIR / "airflow-dbt-grid.png"
        page.screenshot(
            path=path,
            clip={"x": 64, "y": 0, "width": 900 - 64, "height": last_row["y"] + last_row["height"] + margin},
        )
    finally:
        page.set_viewport_size(VIEWPORT)
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
            (NAMESPACE, "airflow-api-server", AIRFLOW_PORT, 8080),
        ),
        sync_playwright() as playwright,
    ):
        browser = playwright.chromium.launch(headless=True)
        try:
            # Airflow shows dates in the browser's time zone, and its labels in
            # the browser's language: UTC like the run ids, and French like the
            # README. The Airflow captures find their buttons by these labels.
            # "fr", not "fr-FR": Airflow falls back to English for the latter.
            page = browser.new_page(viewport=VIEWPORT, timezone_id="UTC", locale="fr")
            captures = [
                lambda: capture_nessie(page, started_at),
                lambda: capture_trino(page),
                lambda: capture_realtime_versions(page),
                lambda: capture_renewable_share(page),
                lambda: (login_airflow(page), capture_airflow_graph(page))[1],
                lambda: capture_airflow_grid(page),
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
