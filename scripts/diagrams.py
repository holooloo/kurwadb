#!/usr/bin/env python3
"""Generates the ADR diagrams in docs/adr/*.svg.

Geometry is computed rather than hand-placed, so a label change cannot silently
push text out of its box. Colours are baked in with an explicit background, so the
same file reads correctly on a light or a dark page (GitHub serves the SVG through
an image proxy, where a prefers-color-scheme rule would not be reliable).

    python3 scripts/diagrams.py
"""

import html
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "docs" / "adr"

SANS = "ui-sans-serif,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,'Liberation Mono',monospace"

BG = "#fcfcfb"
BOX = "#ffffff"
BAND = "#f4f4f5"
LINE = "#a1a1aa"
EDGE = "#d4d4d8"
INK = "#18181b"
MUTED = "#71717a"
CORE = "#b45309"
CORE_BG = "#fffbeb"
STORE = "#0f766e"
STORE_BG = "#f0fdfa"
OK = "#15803d"
DOWN = "#b91c1c"


def esc(s):
    return html.escape(str(s), quote=True)


class Svg:
    def __init__(self, width, height, title):
        self.w, self.h = width, height
        self.parts = []
        self.title = title

    def rect(self, x, y, w, h, fill=BOX, stroke=EDGE, rx=8, sw=1, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{d}/>'
        )

    def text(self, x, y, s, size=13, fill=INK, anchor="middle", weight="400", mono=False,
             italic=False, halo=False):
        fam = MONO if mono else SANS
        st = ' font-style="italic"' if italic else ""
        if halo:
            # Rough advance width is good enough for a plate that only has to
            # hide a rule running under the text.
            w = len(str(s)) * size * (0.54 if not mono else 0.60)
            x0 = {"middle": x - w / 2, "start": x - 4, "end": x - w}[anchor]
            self.parts.append(
                f'<rect x="{x0 - 6}" y="{y - size}" width="{w + 12}" height="{size + 8}" '
                f'rx="3" fill="{BG}"/>'
            )
        self.parts.append(
            f'<text x="{x}" y="{y}" font-family="{fam}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{st}>{esc(s)}</text>'
        )

    def line(self, x1, y1, x2, y2, stroke=LINE, sw=1.4, dash=None, arrow=True):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        a = ' marker-end="url(#a)"' if arrow else ""
        self.parts.append(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" '
            f'stroke-width="{sw}"{d}{a}/>'
        )

    def path(self, d, stroke=LINE, sw=1.4, dash=None, arrow=True, fill="none"):
        da = f' stroke-dasharray="{dash}"' if dash else ""
        a = ' marker-end="url(#a)"' if arrow else ""
        self.parts.append(
            f'<path d="{d}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{da}{a}/>'
        )

    def box(self, x, y, w, h, lines, fill=BOX, stroke=EDGE, accent=None):
        """A box whose lines are (text, size, colour, weight, mono) tuples, centred."""
        self.rect(x, y, w, h, fill=fill, stroke=stroke)
        if accent:
            self.parts.append(
                f'<path d="M {x} {y + 8} q 0 -8 8 -8 l {w - 16} 0 q 8 0 8 8 l 0 3 '
                f'l -{w} 0 z" fill="{accent}"/>'
            )
        total = sum(l[1] + 6 for l in lines) - 6
        cy = y + h / 2 - total / 2
        for txt, size, colour, weight, mono in lines:
            cy += size
            self.text(x + w / 2, cy, txt, size=size, fill=colour, weight=weight, mono=mono)
            cy += 6

    def render(self):
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
            f'viewBox="0 0 {self.w} {self.h}" role="img" aria-label="{esc(self.title)}">\n'
            f"<title>{esc(self.title)}</title>\n"
            '<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
            'markerHeight="7" orient="auto-start-reverse">'
            f'<path d="M 0 0 L 10 5 L 0 10 z" fill="{LINE}"/></marker></defs>\n'
            f'<rect width="{self.w}" height="{self.h}" fill="{BG}"/>\n'
            + "\n".join(self.parts)
            + "\n</svg>\n"
        )

    def write(self, name):
        (OUT / name).write_text(self.render(), encoding="utf-8")
        print(f"wrote docs/adr/{name} ({self.w}x{self.h})")


def L(t, size=13, colour=INK, weight="400", mono=False):
    return (t, size, colour, weight, mono)


# ----------------------------------------------------------------- components
def components():
    W = 900
    s = Svg(W, 690, "kurwadb component stack")
    s.text(W / 2, 30, "How the components fit together", 17, INK, weight="600")
    s.text(W / 2, 50, "every cost measured, not estimated · one laptop, three nodes sharing a CPU",
           11.5, MUTED)

    # clients
    s.box(70, 72, 370, 46, [L("HTTP client", 13.5, INK, "500"),
                            L("PUT / GET / DELETE  /k/:key", 11, MUTED, mono=True)])
    s.box(460, 72, 370, 46, [L("9P mount", 13.5, INK, "500"),
                             L("ls · cat · stat — no client library", 11, MUTED)])

    s.line(255, 118, 255, 140)
    s.line(645, 118, 645, 140)

    # frontends
    s.box(70, 140, 370, 44, [L("Gateway.Router", 13, INK, "500", mono=True)])
    s.box(460, 140, 370, 44, [L("NineP.Server", 13, INK, "500", mono=True)])
    s.text(W / 2, 133, "thin adapters — no logic of their own", 11.5, MUTED, italic=True)

    # converge
    s.path("M 255 184 L 255 196 L 450 196 L 450 214", arrow=True)
    s.path("M 645 184 L 645 196 L 450 196", arrow=False)

    # api
    s.box(200, 214, 500, 48, [L("Kurwa  /  Kurwa.Namespace", 13.5, INK, "500", mono=True),
                              L("keys, named sets, union & intersection", 11, MUTED)])
    s.line(450, 262, 450, 284)

    # extractor
    s.box(200, 284, 500, 72, [L("Kurwa.Extractor", 13.5, INK, "500", mono=True),
                              L("cache + single-flight", 11.5, MUTED),
                              L("cache hit ≈ 0.3 µs · 1000 concurrent misses → 1 quorum read",
                                10.5, MUTED)])
    s.line(450, 356, 450, 374)

    # coordinator
    s.rect(130, 376, 640, 116, fill=CORE_BG, stroke=CORE)
    s.text(150, 398, "Kurwa.Coordinator", 13.5, CORE, anchor="start", weight="600", mono=True)
    s.text(150, 414, "leaderless — any node coordinates any key", 11, MUTED, anchor="start")

    s.box(146, 424, 190, 54, [L("Placement", 12, INK, "500", mono=True),
                              L("who owns the key,", 10.5, MUTED),
                              L("who is reachable · 0.3 µs", 10.5, MUTED)])
    s.box(352, 424, 190, 54, [L("Quorum", 12, INK, "500", mono=True),
                              L("first 2 of 3 to answer", 10.5, MUTED),
                              L("1.8 µs", 10.5, MUTED)])
    s.box(558, 424, 196, 54, [L("Handoff", 12, INK, "500", mono=True),
                              L("what a down replica", 10.5, MUTED),
                              L("missed, per node", 10.5, MUTED)])

    # fan out
    for x in (190, 450, 710):
        s.path(f"M 450 492 L 450 512 L {x} 512 L {x} 542", arrow=True)
    s.text(W / 2, 508, "Erlang distribution — never 9P, never HTTP", 11, MUTED, italic=True,
           halo=True)

    # replicas
    for x, label in [(70, "node A"), (330, "node B"), (590, "node C")]:
        s.rect(x, 542, 240, 104, fill=STORE_BG, stroke=STORE)
        s.text(x + 120, 564, label, 12.5, STORE, weight="600")
        s.box(x + 16, 574, 208, 60, [L("Kurwa.Store", 12, INK, "500", mono=True),
                                     L("shard = phash2(key)", 10.5, MUTED),
                                     L("ETS + WAL · put 1.5 µs / get 0.3 µs", 10, MUTED)])
    s.text(W / 2, 670, "a key is its own payload, so the replica-local answer is one :ets.lookup",
           11.5, MUTED, italic=True)
    s.write("components.svg")


# ----------------------------------------------------------------- write path
def write_path():
    W = 900
    s = Svg(W, 560, "kurwadb write path")
    s.text(W / 2, 30, "A write, with one replica down", 17, INK, weight="600")
    s.text(W / 2, 50, "add(\"order:1029\")   ·   n = 3, w = 2", 12, MUTED, mono=True)

    # lanes sit right of the numbered steps, so step text never crosses a lifeline
    lanes = [("coordinator", 250, INK), ("node A", 440, STORE), ("node B", 620, STORE),
             ("node C", 800, DOWN)]
    for name, x, colour in lanes:
        s.rect(x - 78, 74, 156, 28, fill=BAND, stroke=EDGE, rx=6)
        s.text(x, 93, name, 12, colour, weight="600")
        s.line(x, 102, x, 492, stroke=EDGE, sw=1, dash="3 4", arrow=False)
    s.text(800, 118, "unreachable", 10.5, DOWN, italic=True)

    def step(n, y, note, sub=None):
        s.text(20, y + 4, f"{n}", 12, CORE, anchor="start", weight="700")
        s.text(36, y + 4, note, 11.5, INK, anchor="start")
        if sub:
            s.text(36, y + 19, sub, 10.5, MUTED, anchor="start")

    step(1, 140, "Clock.tick()", "a Lamport stamp, not wall-clock time")
    step(2, 176, "Placement.targets(key, 3)", "primaries A B C · up A B · down C")
    step(3, 212, "one record, stamped once", "every replica stores identical bytes")

    # fan out
    s.line(250, 250, 440, 250, stroke=CORE)
    s.text(345, 244, "put", 10.5, CORE, mono=True)
    s.line(250, 276, 620, 276, stroke=CORE)
    s.text(435, 270, "put", 10.5, CORE, mono=True)

    # no arrowhead: nothing is delivered to a node that is down
    s.line(250, 302, 740, 302, stroke=EDGE, dash="4 4", arrow=False)
    s.text(772, 306, "✕", 13, DOWN)
    s.text(495, 296, "skipped — nothing to send to a node that is down", 10.5, MUTED, italic=True,
           halo=True)

    # acks
    s.line(440, 328, 250, 328, stroke=OK)
    s.text(345, 322, "ok", 10.5, OK, mono=True)
    s.line(620, 348, 250, 348, stroke=OK)
    s.text(435, 342, "ok", 10.5, OK, mono=True)

    s.rect(60, 366, 520, 38, fill="#f0fdf4", stroke=OK, rx=6)
    s.text(76, 390, "2 of 2 acked — w satisfied  →  :ok to the client", 12, OK,
           anchor="start", weight="500")

    # handoff
    s.line(250, 428, 790, 428, stroke=CORE, dash="5 3")
    s.text(520, 422, "Handoff.store(node C, record)", 10.5, CORE, mono=True, halo=True)
    s.text(520, 440, "C owes this write, and the coordinator remembers", 10.5, MUTED,
           italic=True, halo=True)

    s.rect(60, 460, 780, 64, fill=CORE_BG, stroke=CORE, rx=6)
    s.text(76, 482, "later — node C comes back", 12, CORE, anchor="start", weight="600")
    s.text(76, 500, "Cluster publishes it as reachable → Handoff kicks → the record replays → queue empty.",
           11, INK, anchor="start")
    s.text(76, 516, "No read, no operator step, no repair tool. Placement never moved, so C was still responsible.",
           10.5, MUTED, anchor="start")
    s.write("write-path.svg")


# ------------------------------------------------------------------ read path
def read_path():
    W = 900
    s = Svg(W, 560, "kurwadb read path")
    s.text(W / 2, 30, "A read, and how a stale replica gets fixed", 17, INK, weight="600")
    s.text(W / 2, 50, "member?(\"order:1029\")   ·   n = 3, r = 2", 12, MUTED, mono=True)

    # cache decision
    s.box(60, 74, 300, 52, [L("Extractor: cached?", 12.5, INK, "500", mono=True)])
    s.path("M 360 100 L 556 100", arrow=True)
    s.text(458, 92, "hit", 10.5, OK, weight="600")
    s.box(556, 80, 284, 40, [L("true / false · 0.3 µs, no network", 11.5, OK, "500", mono=True)],
          fill="#f0fdf4", stroke=OK)

    s.path("M 210 126 L 210 152", arrow=True)
    s.text(224, 143, "miss", 10.5, MUTED, anchor="start")
    s.box(60, 152, 300, 52, [L("single-flight", 12.5, INK, "500", mono=True),
                             L("one caller resolves, the rest wait", 10.5, MUTED)])
    s.path("M 210 204 L 210 230", arrow=True)

    # quorum read
    s.rect(60, 230, 780, 152, fill=BAND, stroke=EDGE)
    s.text(78, 252, "Quorum.run([A, B, C], get, need = 2)", 12.5, INK, anchor="start", mono=True,
           weight="500")

    rows = [
        ("node A", "{alive, lamport 41}", OK, "counted"),
        ("node B", "{alive, lamport 41}", OK, "counted"),
        ("node C", "{alive, lamport 12}", DOWN, "stale — answered, and behind"),
    ]
    y = 272
    for name, payload, colour, note in rows:
        s.text(96, y + 14, name, 11.5, INK, anchor="start", weight="500")
        s.text(176, y + 14, payload, 11.5, colour, anchor="start", mono=True)
        s.text(352, y + 14, note, 10.5, MUTED, anchor="start", italic=True)
        y += 32

    # merge
    s.path("M 450 382 L 450 406", arrow=True)
    s.box(210, 406, 480, 50, [L("merge — highest {lamport, node} wins", 12.5, INK, "500", mono=True),
                              L("a total order, so no siblings and nothing to ask the caller",
                                10.5, MUTED)])

    # read repair: hooks up the right-hand side into the row that was behind,
    # so it crosses no label
    s.path("M 640 406 L 640 350 L 600 350", stroke=CORE, arrow=True)
    s.text(664, 342, "read repair", 11, CORE, anchor="start", weight="600")
    s.text(664, 358, "pushes the winner back", 10.5, MUTED, anchor="start")

    # outcomes
    s.path("M 450 456 L 450 484", stroke=OK, arrow=True)
    s.box(310, 484, 280, 42, [L("true", 12.5, OK, "600", mono=True)], fill="#f0fdf4", stroke=OK)
    s.box(60, 484, 230, 42,
          [L("quorum not reachable", 11, DOWN, "500"), L("→ 503, never a false", 11, DOWN, "500")],
          fill="#fef2f2", stroke=DOWN)
    s.text(725, 500, "cached for next time —", 10.5, MUTED, italic=True)
    s.text(725, 514, "positive and negative TTLs differ", 10.5, MUTED, italic=True)
    s.write("read-path.svg")


if __name__ == "__main__":
    components()
    write_path()
    read_path()
