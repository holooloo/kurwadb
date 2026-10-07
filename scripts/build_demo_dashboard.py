#!/usr/bin/env python3
"""Builds the site's shared top menu and its demo dashboard.

    python3 scripts/build_demo_dashboard.py

* docs/dashboard.html is priv/dashboard/index.html - the page every node
  serves - with the menu on top and demo mode forced, so the published demo
  and the real dashboard are the same code and cannot drift.
* docs/index.html and docs/article.html get the same menu, between the
  <!-- site-nav:start --> and <!-- site-nav:end --> markers (put right after
  <body> the first time).

Run it after changing the dashboard or the menu, and commit what it writes.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
REPO = "https://github.com/holooloo/kurwadb"

LINKS = [
    ("home", "index.html", "Home"),
    ("article", "article.html", "Article"),
    ("dashboard", "dashboard.html", "Dashboard"),
    ("performance", f"{REPO}/blob/main/PERFORMANCE.md", "Performance"),
]

GITHUB_ICON = (
    '<svg viewBox="0 0 16 16" width="18" height="18" fill="currentColor" aria-hidden="true"><path d="M8 0C3.58 0 0 3.58 0 8c0 '
    "3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28"
    "-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87"
    ".31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27s1.36.09 2 .27c1.53-1.04 2.2-.82 2.2-.82.44"
    " 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15"
    '.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z"/></svg>'
)

# The menu brings its own palette - the site's, light and dark - so it looks
# the same on the cream pages and on the dashboard.
STYLE = """
<style>
  .knav { --kn-bg: rgba(255, 248, 234, 0.82); --kn-ink: #2b2118; --kn-muted: #6f6154; --kn-rule: #ead9bd;
    --kn-accent: #e2761b; --kn-accent-soft: rgba(226, 118, 27, 0.12); --kn-panel: #fffaf0;
    position: sticky; top: 0; z-index: 50; background: var(--kn-bg); border-bottom: 1px solid var(--kn-rule);
    -webkit-backdrop-filter: saturate(160%) blur(14px); backdrop-filter: saturate(160%) blur(14px);
    font: 500 14.5px/1.2 "IBM Plex Sans", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) .knav { --kn-bg: rgba(42, 32, 24, 0.85); --kn-ink: #fdf3e3; --kn-muted: #c3b09a;
      --kn-rule: #4d3d2c; --kn-accent: #f59a3c; --kn-accent-soft: rgba(245, 154, 60, 0.14); --kn-panel: #362a1f; }
  }
  :root[data-theme="dark"] .knav { --kn-bg: rgba(42, 32, 24, 0.85); --kn-ink: #fdf3e3; --kn-muted: #c3b09a;
    --kn-rule: #4d3d2c; --kn-accent: #f59a3c; --kn-accent-soft: rgba(245, 154, 60, 0.14); --kn-panel: #362a1f; }
  .knav * { box-sizing: border-box; }
  .knav-in { max-width: 1180px; margin: 0 auto; padding: 0 clamp(16px, 4vw, 40px); height: 58px;
    display: flex; align-items: center; gap: 18px; }
  .knav a { color: var(--kn-muted); text-decoration: none; font-weight: 500; }
  .knav a:hover { color: var(--kn-ink); }
  .knav-brand { display: flex; align-items: center; gap: 9px; color: var(--kn-ink) !important; font-weight: 700 !important;
    font-size: 16px; letter-spacing: -0.01em; white-space: nowrap; }
  .knav-brand img { width: 26px; height: 26px; border-radius: 7px; }
  .knav-links { display: flex; gap: 20px; margin-left: auto; white-space: nowrap; }
  .knav-links a[aria-current="page"], .knav-menu a[aria-current="page"] { color: var(--kn-accent); }
  .knav-links a[aria-current="page"] { box-shadow: inset 0 -2px 0 var(--kn-accent); padding-bottom: 3px; }
  .knav-gh { display: inline-flex; align-items: center; justify-content: center; width: 34px; height: 34px; flex: none;
    border-radius: 9px; border: 1px solid var(--kn-rule); }
  .knav-demo { display: inline-flex; align-items: center; gap: 8px; height: 34px; padding: 0 13px; border-radius: 10px;
    flex: none; white-space: nowrap; font-size: 13.5px; font-weight: 600 !important; color: var(--kn-accent) !important;
    border: 1px solid var(--kn-accent); background: var(--kn-accent-soft); }
  .knav-demo .live { width: 7px; height: 7px; border-radius: 50%; background: var(--kn-accent); box-shadow: 0 0 9px var(--kn-accent);
    animation: knav-pulse 2.4s ease-in-out infinite; }
  @keyframes knav-pulse { 50% { opacity: .35; } }
  @media (prefers-reduced-motion: reduce) { .knav-demo .live { animation: none; } }
  .knav-menu { display: none; position: relative; margin-left: auto; }
  .knav-menu summary { list-style: none; cursor: pointer; width: 38px; height: 34px; border-radius: 9px;
    border: 1px solid var(--kn-rule); display: flex; align-items: center; justify-content: center; color: var(--kn-ink); }
  .knav-menu summary::-webkit-details-marker { display: none; }
  .knav-menu[open] summary { border-color: var(--kn-accent); }
  .knav-menu .knav-drop { position: absolute; right: 0; top: 44px; min-width: 200px; background: var(--kn-panel);
    border: 1px solid var(--kn-rule); border-radius: 12px; padding: 8px; box-shadow: 0 12px 30px rgba(0, 0, 0, .18);
    display: flex; flex-direction: column; }
  .knav-menu .knav-drop a { padding: 10px 12px; border-radius: 8px; }
  .knav-menu .knav-drop a:hover { background: var(--kn-accent-soft); }
  @media (max-width: 760px) {
    .knav-links, .knav-gh { display: none; }
    .knav-menu { display: block; }
    .knav-demo { margin-left: 0; }
  }
  @media (max-width: 380px) { .knav-demo span.txt { display: none; } }
</style>
"""


def nav(current):
    def mark(key):
        return ' aria-current="page"' if key == current else ""

    links = "\n".join(f'      <a href="{href}"{mark(key)}>{label}</a>' for key, href, label in LINKS)
    menu = "\n".join(f'          <a href="{href}"{mark(key)}>{label}</a>' for key, href, label in LINKS)
    return f"""<!-- site-nav:start -->
<nav class="knav" aria-label="Site">
  <div class="knav-in">
    <a class="knav-brand" href="index.html"><img src="favicon.svg" alt="">kurwadb</a>
    <div class="knav-links">
{links}
    </div>
    <a class="knav-gh" href="{REPO}" aria-label="Source on GitHub" title="Source on GitHub">{GITHUB_ICON}</a>
    <details class="knav-menu">
      <summary aria-label="Menu"><svg viewBox="0 0 20 20" width="18" height="18" aria-hidden="true"><path d="M3 5h14M3 10h14M3 15h14" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg></summary>
      <div class="knav-drop">
{menu}
          <a href="{REPO}">GitHub</a>
      </div>
    </details>
    <a class="knav-demo" href="dashboard.html"><span class="live"></span><span class="txt">Live demo</span></a>
  </div>
</nav>{STYLE}<!-- site-nav:end -->"""


MARKERS = re.compile(r"<!-- site-nav:start -->.*?<!-- site-nav:end -->", re.S)


def with_nav(html, current):
    block = nav(current)
    if MARKERS.search(html):
        return MARKERS.sub(lambda _: block, html, count=1)
    return html.replace("<body>", "<body>\n" + block, 1)


def dashboard():
    html = (ROOT / "priv" / "dashboard" / "index.html").read_text()
    head = """<title>kurwadb · live dashboard demo</title>
<meta name="description" content="kurwadb's cluster dashboard on a simulated three-node cluster: requests moving from clients through every protocol frontend to the replicas and shards.">
<link rel="icon" href="favicon.svg" type="image/svg+xml">
<link rel="canonical" href="https://kurwadb.dev/dashboard.html">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&display=swap">
<!-- Generated by scripts/build_demo_dashboard.py from priv/dashboard/index.html: edit that, not this. -->
<script>window.KURWA_DEMO = true;</script>"""
    html, n = re.subn(r"<title>.*?</title>", lambda _: head, html, count=1)
    assert n == 1, "no <title> in the dashboard"
    return with_nav(html, "dashboard")


def main():
    (DOCS / "dashboard.html").write_text(dashboard())
    for page, current in (("index.html", "home"), ("article.html", "article")):
        path = DOCS / page
        path.write_text(with_nav(path.read_text(), current))
    print("wrote docs/dashboard.html; menu in docs/index.html, docs/article.html")


if __name__ == "__main__":
    main()
