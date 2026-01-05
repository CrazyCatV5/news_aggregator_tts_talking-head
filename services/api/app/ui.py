from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>DFO News Aggregator — Dashboard</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;max-width:1100px;margin:24px auto;padding:0 16px}
    .card{border:1px solid #ddd;border-radius:12px;padding:16px;margin:12px 0}
    button{padding:10px 14px;border-radius:10px;border:1px solid #333;background:#111;color:#fff;cursor:pointer}
    button:disabled{opacity:.5;cursor:not-allowed}
    code{background:#f6f6f6;padding:2px 6px;border-radius:6px}
    .row{display:flex;gap:12px;flex-wrap:wrap}
    .grow{flex:1}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
    table{border-collapse:collapse;width:100%}
    th,td{border-bottom:1px solid #eee;padding:8px 6px;text-align:left}
    th{font-size:12px;color:#444}
    .state{font-weight:600}
    .ok{color:#0a7}
    .err{color:#b00}
  </style>
</head>
<body>
  <h1>DFO Business News Aggregator</h1>

  <div class="card">
    <div class="row">
      <div class="grow">
        <div><b>Status:</b> <span id="status" class="mono">idle</span></div>
        <div><b>Job:</b> <span id="job" class="mono">—</span></div>
        <div><b>Sources:</b> <span id="sources" class="mono">—</span></div>
        <div><b>Ingested:</b> <span id="ingested" class="mono">—</span></div>
        <div><b>Errors:</b> <span id="errors" class="mono">—</span></div>
        <div><b>Message:</b> <span id="message" class="mono">—</span></div>
      </div>
      <div>
        <button id="run">Run ingest</button>
      </div>
    </div>
  </div>

  <div class="card">
    <h3 style="margin-top:0">Per-source progress</h3>
    <table>
      <thead>
        <tr>
          <th>Source</th><th>State</th><th>Links</th><th>Articles OK</th><th>Inserted</th><th>Errors</th>
        </tr>
      </thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>

  <div class="card">
    <div>Useful endpoints:</div>
    <ul>
      <li><code>/news</code> — selected items</li>
      <li><code>/digest</code> — digest text</li>
      <li><code>/jobs/{id}</code> — job status</li>
      <li><code>/jobs/{id}/detail</code> — per-source + errors</li>
      <li><code>/docs</code> — Swagger</li>
    </ul>
  </div>

<script>
let currentJob = null;
const el = (id)=>document.getElementById(id);

function renderJob(d){
  el("status").textContent = d.status || "—";
  el("job").textContent = d.job_id || currentJob || "—";
  const done = (d.done_sources ?? "—");
  const total = (d.total_sources ?? "—");
  el("sources").textContent = `${done}/${total}`;
  el("ingested").textContent = d.ingested ?? "—";
  el("errors").textContent = d.errors_count ?? "—";
  el("message").textContent = d.message ?? "—";
}

function renderSources(sources){
  const tbody = el("tbody");
  tbody.innerHTML = "";
  const names = Object.keys(sources || {});
  names.sort();
  for(const name of names){
    const s = sources[name];
    const tr = document.createElement("tr");
    const state = s.state || "—";
    tr.innerHTML = `
      <td class="mono">${name}</td>
      <td class="state ${state==="done" ? "ok" : (state==="error" ? "err" : "")}">${state}</td>
      <td class="mono">${s.links ?? 0}</td>
      <td class="mono">${s.articles_ok ?? 0}</td>
      <td class="mono">${s.inserted ?? 0}</td>
      <td class="mono">${s.errors ?? 0}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function poll(){
  if(!currentJob) return;
  const r1 = await fetch(`/jobs/${currentJob}`);
  const j = await r1.json();
  renderJob(j);

  const r2 = await fetch(`/jobs/${currentJob}/detail`);
  const d = await r2.json();
  renderSources(d.sources || {});

  if(j.status === "running" || j.status === "queued") setTimeout(poll, 800);
}

el("run").addEventListener("click", async ()=>{
  el("run").disabled = true;
  const r = await fetch("/ingest/run?limit_per_html_source=20", {method:"POST"});
  const d = await r.json();
  currentJob = d.job_id;
  renderJob({status:"queued", job_id: currentJob, done_sources: 0, total_sources: "—", message:"Enqueued"});
  el("run").disabled = false;
  setTimeout(poll, 300);
});

</script>
</body>
</html>"""

@router.get("/ui", response_class=HTMLResponse)
def ui():
    return HTMLResponse(HTML)
