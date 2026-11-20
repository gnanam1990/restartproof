"""M06 — Lab console: accessible static viewer for run evidence.

Renders run cards side by side — verdict, declared boundary, target
provenance, effect rows, attempts, coverage seal, journal timeline and
quarantine — in a single static HTML file with no scripts and no
external assets, so it stays readable headless, offline, and at 200%
browser zoom (T-014).

Every card names its observed boundary and provenance (INV-06): a
HOST_FIXTURE card is labeled as such and never presented as QNX proof;
a qnx-profile card shows the guest-reported uname captured by the
operator bridge, or explicitly says the target run never happened.
"""

from __future__ import annotations

import html
import json
import os

from . import __version__
from .runner import run_dir
from .util import utc_now
from .verdict import build_bundle

_CSS = """
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { font-family: system-ui, sans-serif; margin: 0; padding: 1rem;
       max-width: 100%; }
.skip { position: absolute; left: -9999px; }
.skip:focus { left: 0; background: Canvas; color: CanvasText;
              padding: .5rem; z-index: 1; }
.grid { display: grid; gap: 1rem;
        grid-template-columns: repeat(auto-fit, minmax(22rem, 1fr)); }
.card { border: 2px solid CanvasText; border-radius: .5rem;
        padding: 1rem; min-width: 0; }
.verdict { font-size: 1.25rem; font-weight: 700; padding: .4rem .7rem;
           border: 2px solid CanvasText; border-radius: .4rem;
           display: inline-block; margin: .3rem 0; }
.v-PASS { border-style: double; }
.v-FAIL_DUPLICATE, .v-FAIL_MISSING { border-width: 4px; }
.v-REVIEW, .v-SETUP { border-style: dashed; }
table { border-collapse: collapse; width: 100%; margin: .5rem 0; }
th, td { border: 1px solid CanvasText; padding: .3rem .5rem;
         text-align: left; word-break: break-word; }
caption { text-align: left; font-weight: 600; padding-bottom: .3rem; }
dl { display: grid; grid-template-columns: max-content 1fr; gap: .2rem .8rem; }
dt { font-weight: 600; }
dd { margin: 0; word-break: break-word; }
.warn { border: 2px dashed CanvasText; padding: .6rem; margin: 1rem 0; }
code { word-break: break-all; }
footer { margin-top: 1.5rem; font-size: .85rem; opacity: .8; }
"""

_VERDICT_TEXT = {
    "PASS": "PASS — expected effect count observed at the sealed boundary",
    "FAIL_DUPLICATE": "FAIL — duplicate durable effects",
    "FAIL_MISSING": "FAIL — no effect inside a closed empty interval",
    "REVIEW": "REVIEW — evidence incomplete; cannot classify",
    "SETUP": "SETUP — unsupported or not run",
}


def _esc(v) -> str:
    return html.escape("" if v is None else str(v))


def _kv(pairs) -> str:
    rows = "".join(f"<dt>{_esc(k)}</dt><dd>{_esc(v)}</dd>"
                   for k, v in pairs if v is not None)
    return f"<dl>{rows}</dl>"


def _effects_table(bundle: dict, run_id: str) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{_esc(r.get('seq'))}</td>"
        f"<td>{_esc(r.get('op_id'))}</td>"
        f"<td>{_esc(r.get('attempt_id'))}</td>"
        f"<td><code>{_esc(str(r.get('payload_digest'))[:24])}…</code></td>"
        "</tr>"
        for r in bundle["effects"]["attributable"])
    caption = (f"Attributable effect rows for {run_id} "
               f"({bundle['effects']['attributableCount']} distinct; "
               f"{bundle['effects']['duplicateReadsIgnored']} duplicate "
               "reads ignored)")
    extra = ""
    if bundle["effects"]["conflicted"]:
        extra = ("<p class='warn'>Conflicting receipt copies were "
                 "quarantined: "
                 + _esc(", ".join(sorted(
                     bundle["effects"]["conflicted"].keys())))
                 + "</p>")
    empty = ("<tr><td colspan=4>no attributable "
             "effects observed</td></tr>")
    return (f"<table><caption>{_esc(caption)}</caption>"
            "<thead><tr><th scope='col'>seq</th><th scope='col'>op</th>"
            "<th scope='col'>attempt</th><th scope='col'>payload digest"
            "</th></tr></thead>"
            f"<tbody>{rows or empty}</tbody></table>" + extra)


def _attempts_table(bundle: dict) -> str:
    attempts = []
    for op in bundle["operations"]:
        for a in op.get("attempts", []):
            attempts.append({**a, "opId": op["opId"]})
    if not attempts:
        return ""
    rows = "".join(
        "<tr>"
        f"<td>{_esc(a.get('attemptId'))}</td>"
        f"<td>{_esc(a.get('sendResult'))}</td>"
        f"<td>{_esc(a.get('generation'))}</td>"
        f"<td>{_esc(a.get('receiptSeq'))}</td>"
        "</tr>" for a in attempts)
    return ("<table><caption>Client attempts (raw send outcomes; not "
            "effect evidence by themselves)</caption>"
            "<thead><tr><th scope='col'>attempt</th>"
            "<th scope='col'>send result</th><th scope='col'>generation"
            "</th><th scope='col'>receipt seq</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>")


def _journal_timeline(bundle: dict) -> str:
    items = "".join(
        f"<li><code>{_esc(step)}</code> (gen {entry.get('controlGeneration')})"
        f" — {_esc(json.dumps({k: v for k, v in entry.items()
                               if k != 'controlGeneration'},
                              sort_keys=True)[:240])}</li>"
        for step, entry in bundle["journal"].items())
    return (f"<details><summary>Journal timeline "
            f"({len(bundle['journal'])} steps)</summary>"
            f"<ol>{items}</ol></details>")


def render_card(store, run, bundle_link: str) -> str:
    bundle = build_bundle(store, run)
    v = bundle["verdict"]
    code = v["code"]
    target = bundle.get("target") or {}
    ident = bundle.get("targetIdentity") or {}
    uname = ident.get("uname")
    prov = bundle["provenance"]
    incomplete = " (observation incomplete)" if v.get("incomplete") else ""
    quarantine = ""
    if bundle["quarantine"]:
        quarantine = ("<p class='warn'>Quarantined evidence: "
                      f"{len(bundle['quarantine'])} entr"
                      f"{'y' if len(bundle['quarantine']) == 1 else 'ies'}"
                      " — see the exported bundle.</p>")
    rel = bundle_link
    return f"""
<section class="card" aria-labelledby="h-{_esc(run['run_id'])}">
  <h2 id="h-{_esc(run['run_id'])}">Run <code>{_esc(run['run_id'])}</code></h2>
  <p><span class="verdict v-{_esc(code)}">{_esc(code)}</span>
     <span>{_esc(_VERDICT_TEXT.get(code, code))}{_esc(incomplete)}</span></p>
  <p><strong>Reason:</strong> {_esc(v["reason"])}</p>
  {_kv([
      ("Observed boundary", bundle["effectBoundary"]),
      ("Target provenance", prov),
      ("Guest-reported identity (uname)",
       uname or ("not captured — no operator-bridge attach"
                 if bundle.get("target", {}).get("qnx") else None)),
      ("Sink mode", run.get("sink_mode")),
      ("Scenario", bundle.get("scenarioId") or run.get("scenario_id")),
      ("Manifest digest", (bundle.get("manifestDigest") or "")[:24] + "…"),
      ("Coverage seal",
       "closed & complete" if (bundle.get("coverage") or {})
                            .get("coverage") == "complete"
       else _esc((bundle.get("coverage") or {}).get("coverage")
                 or "withheld / absent")),
      ("Live sink read", bundle["evidenceSources"]["liveSinkRead"]),
  ])}
  {_effects_table(bundle, run["run_id"])}
  {_attempts_table(bundle)}
  {_journal_timeline(bundle)}
  {quarantine}
  <p><a href="{_esc(rel)}">Export: evidence bundle (JSON)</a></p>
</section>"""


def render_page(store, runs, out_path: str,
                bundle_links: dict) -> str:
    cards = "".join(render_card(store, r, bundle_links[r["run_id"]])
                    for r in runs)
    has_fixture = any((r.get("manifest") or {})
                      .get("target", {}).get("label") == "HOST_FIXTURE"
                      for r in runs)
    notice = ""
    if has_fixture:
        notice = ("<p class='warn'><strong>Provenance notice:</strong> "
                  "cards labeled HOST_FIXTURE are portable host runs — "
                  "they are not proof of execution on a licensed QNX "
                  "target and cannot satisfy the QNX release gate.</p>")
    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RestartProof lab console</title>
<style>{_CSS}</style>
</head>
<body>
<a class="skip" href="#main">Skip to results</a>
<header>
  <h1>RestartProof lab console</h1>
  <p>{len(runs)} run{'s' if len(runs) != 1 else ''} — every verdict names
     its observed boundary and target provenance.</p>
</header>
{notice}
<main id="main" class="grid">
{cards}
</main>
<footer>
  Generated {utc_now()} by rp {__version__}. Effect rows are exported
  verbatim from the same bundle each card links to.
</footer>
</body>
</html>"""
    os.makedirs(os.path.dirname(os.path.abspath(out_path)),
                exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w") as fh:
        os.chmod(tmp, 0o600)
        fh.write(doc)
    os.replace(tmp, out_path)
    return out_path


def console(store, runs, out_path: str | None = None,
            write_bundles: bool = True) -> str:
    """Render the console and (for collected runs) its evidence JSON.

    Returns the HTML path. Bundle files go to <home>/runs/<id>/
    evidence.json so the console's relative links resolve when the
    output lives at <home>/console.html (the default).
    """
    if write_bundles:
        from .verdict import _write_json, build_bundle as _bb
        for r in runs:
            if r["status"] in ("collected", "reported"):
                _write_json(os.path.join(
                    run_dir(store.home, r["run_id"]),
                    "evidence.json"), _bb(store, r))
    path = out_path or os.path.join(store.home, "console.html")
    home_abs = os.path.abspath(store.home)
    out_abs = os.path.abspath(path)
    inside_home = out_abs.startswith(home_abs + os.sep)
    links = {}
    from .verdict import _write_json, build_bundle as _bb
    for r in runs:
        if inside_home:
            links[r["run_id"]] = os.path.join(
                "runs", r["run_id"], "evidence.json")
        else:
            # console outside the run home: place a bundle copy next to
            # it so the link works without leaking the private layout
            link = f"evidence-{r['run_id']}.json"
            if r["status"] in ("collected", "reported"):
                _write_json(os.path.join(
                    os.path.dirname(out_abs), link), _bb(store, r))
            links[r["run_id"]] = link
    return render_page(store, runs, path, links)
