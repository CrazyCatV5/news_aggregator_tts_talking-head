(function(){
  function el(id){ return document.getElementById(id); }
  function nz(v, fb){ return (v === undefined || v === null) ? fb : v; }

  function log(msg){
    var box = el('jslog');
    if(!box) return;
    var nl = String.fromCharCode(10);
    var cur = box.textContent || '';
    box.textContent = cur ? (cur + nl + String(msg)) : String(msg);
  }

  // -------------------------
  // Tabs + Catalog UI state
  // -------------------------
  var activeTab = 'dashboard';
  var catalogOffset = 0;
  var catalogLimit = 50;

  function setTab(name){
    activeTab = name;
    var dash = el('tab-dashboard');
    var cat = el('tab-catalog');
    var llm = el('tab-llm');
    var dig = el('tab-digests');
    var tts = el('tab-tts');
    var vid = el('tab-video');
    if(dash) dash.classList.toggle('hidden', name !== 'dashboard');
    if(cat) cat.classList.toggle('hidden', name !== 'catalog');
    if(llm) llm.classList.toggle('hidden', name !== 'llm');
    if(dig) dig.classList.toggle('hidden', name !== 'digests');
    if(tts) tts.classList.toggle('hidden', name !== 'tts');
    if(vid) vid.classList.toggle('hidden', name !== 'video');
    if(name === 'llm'){ try{ loadLlmItems(); }catch(e){} }
    if(name === 'digests'){ try{ initDigestsTab(true); }catch(e){} }
    if(name === 'tts'){ try{ initTtsTab(true); }catch(e){} }
    if(name === 'video'){ try{ initVideoTab(true); }catch(e){} }
    var btns = document.querySelectorAll('.tab-btn');
    for(var i=0;i<btns.length;i++){
      btns[i].classList.toggle('active', (btns[i].dataset && btns[i].dataset.tab) === name);
    }
  }

  // API base from /ui or /api/ui
  var uiPath = (window.location && window.location.pathname) ? window.location.pathname : '/ui';
  var apiBase = '';
  if(uiPath.length >= 3 && uiPath.substr(uiPath.length-3) === '/ui'){
    apiBase = uiPath.substr(0, uiPath.length-3);
  } else if(uiPath.length >= 4 && uiPath.substr(uiPath.length-4) === '/ui/'){
    apiBase = uiPath.substr(0, uiPath.length-4);
  }
  // Allow explicit API base override via <meta name="api-base" content="/api"> or window.API_BASE.
  var metaApiBase = (document.querySelector('meta[name="api-base"]') || {}).content;
  if (metaApiBase && metaApiBase.trim()) {
    apiBase = metaApiBase.trim().replace(/\/+$/, '');
  } else if (window.API_BASE && String(window.API_BASE).trim()) {
    apiBase = String(window.API_BASE).trim().replace(/\/+$/, '');
  }
  function apiUrl(path){
    if(!path) path = '/';
    if(path.charAt(0) !== '/') path = '/' + path;
    return apiBase + path;
  }

  // Small fetch helpers used by some tabs (e.g., TTS).
  function apiGet(path){
    return fetch(apiUrl(path)).then(function(r){
      if(!r.ok){
        return r.text().then(function(t){
          throw new Error('HTTP ' + r.status + ': ' + (t || '').slice(0, 500));
        });
      }
      return r.json();
    });
  }
  function apiPost(path, body){
    return fetch(apiUrl(path), {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body || {}),
    }).then(function(r){
      if(!r.ok){
        return r.text().then(function(t){
          throw new Error('HTTP ' + r.status + ': ' + (t || '').slice(0, 500));
        });
      }
      return r.json();
    });
  }

  function fmtTs(ts){
    if(!ts) return '—';
    try{
      var d = new Date(ts * 1000);
      return d.toISOString().replace('T',' ').slice(0,19) + 'Z';
    }catch(e){
      return String(ts);
    }
  }

  function renderJob(d, currentJob){
    el('status').textContent  = d.status  || '—';
    el('job').textContent     = d.job_id  || currentJob || '—';
    el('created').textContent = fmtTs(d.created_at);
    el('updated').textContent = fmtTs(d.updated_at);
    var done  = nz(d.done_sources, '—');
    var total = nz(d.total_sources, '—');
    el('sources').textContent  = String(done) + '/' + String(total);
    el('ingested').textContent = nz(d.ingested, '—');
    el('errors').textContent   = nz(d.errors_count, '—');
    el('message').textContent  = nz(d.message, '—');
  }

  function renderSources(sources){
    var tbody = el('tbody');
    tbody.innerHTML = '';
    sources = sources || {};
    var names = [];
    for(var k in sources){ if(Object.prototype.hasOwnProperty.call(sources,k)) names.push(k); }
    names.sort();
    for(var i=0;i<names.length;i++){
      var name = names[i];
      var s = sources[name] || {};
      var tr = document.createElement('tr');
      var state = s.state || '—';
      var cls = (state === 'done') ? 'ok' : ((state === 'error') ? 'err' : '');
      tr.innerHTML =
        '<td class="mono">' + name + '</td>' +
        '<td class="state ' + cls + '">' + state + '</td>' +
        '<td class="mono">' + nz(s.links, 0) + '</td>' +
        '<td class="mono">' + nz(s.articles_ok, 0) + '</td>' +
        '<td class="mono">' + nz(s.inserted, 0) + '</td>' +
        '<td class="mono">' + nz(s.errors, 0) + '</td>';
      tbody.appendChild(tr);
    }
  }

  function setCurrentJob(jobId){
    currentJob = jobId || null;
    if(el('jobId')) el('jobId').value = currentJob || '';
    if(currentJob){
      try{ window.location.hash = 'job=' + currentJob; }catch(e){}
      renderJob({status:'—', job_id: currentJob, message:'Loading...'}, currentJob);
      setTimeout(poll, 50);
    }
  }

  function renderJobsList(items){
    var tbody = el('jobsTbody');
    if(!tbody) return;
    tbody.innerHTML = '';
    items = items || [];
    for(var i=0;i<items.length;i++){
      var j = items[i] || {};
      var jid = j.job_id || '';
      var status = j.status || '—';
      var cls = (status === 'done') ? 'ok' : ((status === 'error' || status === 'failed') ? 'err' : '');
      var tr = document.createElement('tr');
      tr.innerHTML =
        '<td class="mono"><a href="#" data-job="' + jid + '">' + jid + '</a></td>' +
        '<td class="state ' + cls + '">' + status + '</td>' +
        '<td class="mono">' + fmtTs(j.created_at) + '</td>' +
        '<td class="mono">' + fmtTs(j.updated_at) + '</td>' +
        '<td class="mono">' + nz(j.ingested, '—') + '</td>' +
        '<td class="mono">' + nz(j.errors_count, '—') + '</td>';
      tbody.appendChild(tr);
    }

    var links = tbody.querySelectorAll('a[data-job]');
    for(var k=0;k<links.length;k++){
      links[k].addEventListener('click', function(ev){
        ev.preventDefault();
        var jid = this.getAttribute('data-job');
        setCurrentJob(jid);
      });
    }
  }

  function loadJobs(){
    return fetch(apiUrl('/jobs?limit=20&offset=0'))
      .then(function(r){ return r.json(); })
      .then(function(d){ renderJobsList(d.items || []); })
      .catch(function(e){ log('loadJobs failed: ' + e); });
  }

  function escapeHtml(s){
    s = (s || '');
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  // Alias used in LLM digest UI rendering.
  function esc(s){
    return escapeHtml(s);
  }

  // ----- Full text modal (article stored in DB) -----
  var modalEl = el('modal');
  var modalBackdropEl = el('modalBackdrop');
  var modalCloseEl = el('modalClose');
  var modalTitleEl = el('modalTitle');
  var modalMetaEl = el('modalMeta');
  var modalBodyEl = el('modalBody');

  function isModalOpen(){
    return modalEl && !modalEl.classList.contains('hidden');
  }

  function closeModal(){
    if(!modalEl) return;
    modalEl.classList.add('hidden');
    modalEl.setAttribute('aria-hidden','true');
    if(modalTitleEl) modalTitleEl.textContent = '';
    if(modalMetaEl) modalMetaEl.innerHTML = '';
    if(modalBodyEl) modalBodyEl.innerHTML = '';
  }

  function openModal(){
    if(!modalEl) return;
    modalEl.classList.remove('hidden');
    modalEl.setAttribute('aria-hidden','false');
  }

  if(modalBackdropEl){ modalBackdropEl.addEventListener('click', closeModal); }
  if(modalCloseEl){ modalCloseEl.addEventListener('click', closeModal); }
  document.addEventListener('keydown', function(e){
    if(e.key === 'Escape' && isModalOpen()) closeModal();
  });

  function formatBodyToHtml(text){
    // Body is stored as plain text; render safely with basic paragraphing.
    var safe = escapeHtml(text || '');
    // Normalize newlines
    safe = safe.replace(/\r\n/g,'\n');
    // Split into paragraphs by blank lines
    var parts = safe.split(/\n\s*\n/g);
    var out = [];
    for(var i=0;i<parts.length;i++){
      var p = parts[i].trim();
      if(!p) continue;
      out.push('<p>' + p.replace(/\n/g,'<br>') + '</p>');
    }
    return out.join('');
  }

  function viewFullText(itemId){
    if(!itemId) return;
    if(modalTitleEl) modalTitleEl.textContent = 'Loading...';
    if(modalMetaEl) modalMetaEl.innerHTML = '';
    if(modalBodyEl) modalBodyEl.innerHTML = '<div class="small">Loading article from DB...</div>';
    openModal();

    fetch(apiUrl('/items/' + encodeURIComponent(itemId)))
      .then(function(r){ return r.json(); })
      .then(function(d){
        if(!d || !d.ok || !d.item){
          if(modalTitleEl) modalTitleEl.textContent = 'Not found';
          if(modalBodyEl) modalBodyEl.innerHTML = '<div class="small">Item not found in DB.</div>';
          return;
        }
        var it = d.item;
        if(modalTitleEl) modalTitleEl.textContent = (it.title || '(no title)');

        var meta = [];
        if(it.source_name) meta.push('<span class="badge">' + escapeHtml(it.source_name) + '</span>');
        if(it.published_at) meta.push('<span class="small">published: ' + escapeHtml(it.published_at) + '</span>');
        if(!it.published_at && it.fetched_at) meta.push('<span class="small">fetched: ' + escapeHtml(it.fetched_at) + '</span>');
        if(it.url) meta.push('<a class="small" href="' + escapeHtml(it.url) + '" target="_blank" rel="noopener">Open source</a>');
        if(modalMetaEl) modalMetaEl.innerHTML = meta.join(' ');

        var body = it.body || '';
        if(!body || (body.trim && !body.trim())){
          if(modalBodyEl) modalBodyEl.innerHTML = '<div class="small">Body is empty for this item (stored as empty string).</div>';
          return;
        }
        if(modalBodyEl) modalBodyEl.innerHTML = formatBodyToHtml(body);
      })
      .catch(function(e){
        if(modalTitleEl) modalTitleEl.textContent = 'Error';
        if(modalBodyEl) modalBodyEl.innerHTML = '<div class="small">Failed to load item: ' + escapeHtml(String(e)) + '</div>';
      });
  }

  // Backward-compatible alias used by older renderers.
  // Some UI components call viewItem(id) instead of viewFullText(id).
  function viewItem(itemId){
    return viewFullText(itemId);
  }

  // -------------------------
  // Catalog
  // -------------------------
  function buildCatalogParams(){
    var p = new URLSearchParams();
    // Catalog control ids are defined in static/index.html
    var q = (el('catQuery')||{}).value || '';
    var source = (el('catSource')||{}).value || '';
    var pf = (el('catPublishedFrom')||{}).value || '';
    var pt = (el('catPublishedTo')||{}).value || '';
    var bizMin = (el('catBizMin')||{}).value || '';
    var dfoMin = (el('catDfoMin')||{}).value || '';
    var hasCompany = (el('catHasCompany')||{}).checked;
    var excludeWar = (el('catExcludeWar')||{}).checked;
    var sort = (el('catSort')||{}).value || '';

    if(q.trim()) p.set('q', q.trim());
    if(source) p.set('source', source);
    if(pf) p.set('published_from', new Date(pf).toISOString());
    if(pt) p.set('published_to', new Date(pt).toISOString());
    if(bizMin) p.set('biz_min', bizMin);
    if(dfoMin) p.set('dfo_min', dfoMin);
    if(hasCompany) p.set('has_company', '1');
    if(excludeWar) p.set('exclude_war', 'true');
    if(sort) p.set('sort', sort);

    p.set('limit', String(catalogLimit));
    p.set('offset', String(catalogOffset));
    return p;
  }

  function renderCatalog(items){
    var root = el('catList');
    if(!root) return;
    root.innerHTML = '';
    items = items || [];

    if(!items.length){
      root.innerHTML = '<div class="small">No items for selected filters.</div>';
      return;
    }

    items.forEach(function(it){
      var card = document.createElement('div');
      card.className = 'row';
      card.style.alignItems = 'center';
      card.style.gap = '10px';
      card.style.padding = '8px 0';
      card.style.borderBottom = '1px solid #eee';

      var published = it.published_at ? new Date(it.published_at).toLocaleString() : '';
      var left = document.createElement('div');
      left.className = 'grow';
      left.innerHTML =
        '<div><b>' + escapeHtml(it.title || '(no title)') + '</b></div>' +
        '<div class="small mono">' +
          (it.source_name ? escapeHtml(it.source_name) : '') +
          (published ? (' — ' + escapeHtml(published)) : '') +
          (it.url ? (' — <a href="' + escapeHtml(it.url) + '" target="_blank" rel="noopener">open</a>') : '') +
        '</div>';

      var btnOpen = document.createElement('button');
      btnOpen.className = 'ghost';
      btnOpen.textContent = 'text';
      btnOpen.addEventListener('click', function(){ viewFullText(it.id); });

      var btnDel = document.createElement('button');
      btnDel.className = 'ghost';
      btnDel.textContent = 'delete';
      btnDel.addEventListener('click', function(){
        if(confirm('Delete item #' + it.id + '?')) deleteItem(it.id);
      });

      card.appendChild(left);
      card.appendChild(btnOpen);
      card.appendChild(btnDel);
      root.appendChild(card);
    });
  }

  async function loadCatalog(reset){
    if(reset) catalogOffset = 0;
    var status = el('catSummary');
    if(status) status.textContent = 'Loading...';

    try{
      var params = buildCatalogParams();
      var resp = await fetch(apiUrl('/api/items?' + params.toString()));
      var data = await resp.json();
      if(!resp.ok) throw new Error((data && data.detail) ? data.detail : ('HTTP ' + resp.status));

      renderCatalog(data.items || []);

      var prevBtn = el('catPrev');
      var nextBtn = el('catNext');
      var total = typeof data.total === 'number' ? data.total : null;

      if(prevBtn) prevBtn.disabled = catalogOffset <= 0;
      if(nextBtn) nextBtn.disabled = !data.has_more;

      if(status){
        if(total !== null){
          status.textContent = 'Showing ' + (data.offset + 1) + '–' + (data.offset + (data.items||[]).length) + ' of ' + total;
        } else {
          status.textContent = 'Loaded ' + (data.items||[]).length + ' items';
        }
      }
    }catch(err){
      if(status) status.textContent = 'Error: ' + err.message;
      log('catalog error: ' + err.message);
    }
  }

  async function initCatalog(){
    var sel = el('catSource');
    if(sel){
      try{
        var r = await fetch(apiUrl('/api/items/sources'));
        var d = await r.json();
        var sources = (d && d.sources) ? d.sources : [];
        sel.innerHTML = '<option value="">All sources</option>' + sources.map(function(s){
          return '<option value="' + escapeHtml(s) + '">' + escapeHtml(s) + '</option>';
        }).join('');
      }catch(e){
        // ignore; dropdown will stay empty
      }
    }

    var btn = el('catSearch');
    if(btn) btn.addEventListener('click', function(){ loadCatalog(true); });

    var reset = el('catReset');
    if(reset) reset.addEventListener('click', function(){
      ['catQuery','catPublishedFrom','catPublishedTo','catBizMin','catDfoMin'].forEach(function(id){
        var x = el(id); if(x) x.value = '';
      });
      if(el('catSource')) el('catSource').value = '';
      if(el('catHasCompany')) el('catHasCompany').checked = false;
      if(el('catExcludeWar')) el('catExcludeWar').checked = false;
      if(el('catSort')) el('catSort').value = '';
      loadCatalog(true);
    });

    var prev = el('catPrev');
    if(prev) prev.addEventListener('click', function(){
      catalogOffset = Math.max(0, catalogOffset - catalogLimit);
      loadCatalog(false);
    });

    var next = el('catNext');
    if(next) next.addEventListener('click', function(){
      catalogOffset = catalogOffset + catalogLimit;
      loadCatalog(false);
    });

    ['catQuery','catPublishedFrom','catPublishedTo','catBizMin','catDfoMin','catHasCompany','catExcludeWar','catSort','catSource'].forEach(function(id){
      var x = el(id);
      if(!x) return;
      x.addEventListener('keydown', function(e){
        if(e.key === 'Enter') loadCatalog(true);
      });
      x.addEventListener('change', function(){
        // light UX: keep results in sync when toggling checkbox / select
        loadCatalog(true);
      });
    });
  }

  function initTabs(){
    document.querySelectorAll('.tab-btn').forEach(function(b){
      b.addEventListener('click', function(){
        setTab(b.getAttribute('data-tab') || 'dashboard');
      });
    });
    setTab(activeTab);
  }

  function renderNewsByDay(grouped){
    var root = el('newsByDay');
    if(!root) return;
    root.innerHTML = '';
    grouped = grouped || {};
    var days = [];
    for(var k in grouped){ if(Object.prototype.hasOwnProperty.call(grouped,k)) days.push(k); }
    if(!days.length){
      root.innerHTML = '<div class="small">No items for selected filters.</div>';
      return;
    }
    days.sort(); days.reverse();

    for(var i=0;i<days.length;i++){
      var day = days[i];
      var items = (grouped[day] || []).slice();

      // UI-only sorting.
      var sortKey = (el('newsSort') && el('newsSort').value) ? el('newsSort').value : 'time_desc';
      if(sortKey === 'source_asc'){
        items.sort(function(a,b){
          var as = (a && a.source_name) ? String(a.source_name) : '';
          var bs = (b && b.source_name) ? String(b.source_name) : '';
          var c = as.localeCompare(bs, 'ru');
          if(c !== 0) return c;
          // fallback to time desc
          var at = (a && (a.published_at || a.fetched_at)) ? String(a.published_at || a.fetched_at) : '';
          var bt = (b && (b.published_at || b.fetched_at)) ? String(b.published_at || b.fetched_at) : '';
          return bt.localeCompare(at);
        });
      }
      var block = document.createElement('div');
      block.className = 'card';
      block.style.margin = '12px 0';
      var listId = 'day_' + day;

      block.innerHTML =
        '<div class="row" style="align-items:center">' +
          '<div class="grow"><b class="mono">' + day + '</b> <span class="small">(' + items.length + ')</span></div>' +
          '<div><button type="button" data-toggle="' + day + '">Toggle</button></div>' +
        '</div>' +
        '<div id="' + listId + '" style="margin-top:10px"></div>';

      root.appendChild(block);

      var list = document.getElementById(listId);
      var ul = document.createElement('ul');
      ul.style.margin = '0';

      for(var j=0;j<items.length;j++){
        var it = items[j] || {};
        var li = document.createElement('li');
        var title = escapeHtml(it.title || '(no title)');
        var src = escapeHtml(it.source_name || '');
        var url = it.url || '';
        var id = it.id || '';
        li.innerHTML =
          title + ' <span class="small">(' + src + ')</span> — ' +
          '<a href="' + url + '" target="_blank" rel="noreferrer">open</a>' +
          (id ? ' — <a href="#" data-view="' + id + '">text</a>' : '') +
          (id ? ' — <a href="#" data-del="' + id + '">delete</a>' : '');
        ul.appendChild(li);
      }

      if(list) list.appendChild(ul);

      // toggle
      var btns = block.querySelectorAll('button[data-toggle]');
      if(btns && btns.length){
        btns[0].addEventListener('click', (function(listId){
          return function(){
            var d = document.getElementById(listId);
            if(!d) return;
            d.style.display = (d.style.display === 'none') ? 'block' : 'none';
          };
        })(listId));
      }

      // delete links (delegate)
      ul.addEventListener('click', function(ev){
        var t = ev.target;
        if(!t) return;
        var vid = t.getAttribute ? t.getAttribute('data-view') : null;
        var did = t.getAttribute ? t.getAttribute('data-del') : null;
        if(vid){
          ev.preventDefault();
          viewItem(vid);
          return;
        }
        if(did){
          ev.preventDefault();
          deleteItem(did);
        }
      });
    }
  }

  var currentJob = null;

  function poll(){
    if(!currentJob) return;

    fetch(apiUrl('/jobs/' + currentJob))
      .then(function(r){ return r.json(); })
      .then(function(j){
        renderJob(j, currentJob);
        return fetch(apiUrl('/jobs/' + currentJob + '/detail'));
      })
      .then(function(r){ return r.json(); })
      .then(function(d){
        renderSources(d.sources || {});
        if(d && d.job && (d.job.status === 'running' || d.job.status === 'queued')){
          setTimeout(poll, 800);
        }
      })
      .catch(function(e){ log('poll failed: ' + e); });
  }

  function runIngest(){
    var btn = el('run');
    if(btn) btn.disabled = true;

    fetch(apiUrl('/ingest/run?limit_per_html_source=500'), {method:'POST'})
      .then(function(r){ return r.json(); })
      .then(function(d){
        currentJob = d.job_id;
        renderJob({status:'queued', job_id: currentJob, message:'Enqueued'}, currentJob);
        if(btn) btn.disabled = false;
        setTimeout(poll, 300);
      })
      .catch(function(e){
        if(btn) btn.disabled = false;
        log('runIngest failed: ' + e);
      });
  }

  function loadNewsByDay(){
    var days = parseInt((el('days') && el('days').value) ? el('days').value : '7', 10);
    var minBiz = parseInt((el('minBiz') && el('minBiz').value) ? el('minBiz').value : '2', 10);
    var minDfo = parseInt((el('minDfo') && el('minDfo').value) ? el('minDfo').value : '2', 10);
    var require_company = !!(el('reqCompany') && el('reqCompany').checked);
    var exclude_war = !!(el('excludeWar') && el('excludeWar').checked);

    var url = apiUrl('/news/by-day?days=' + days +
                     '&min_business=' + minBiz +
                     '&min_dfo=' + minDfo +
                     '&require_company=' + require_company +
                     '&exclude_war=' + exclude_war +
                     '&limit_per_day=50');

    return fetch(url)
      .then(function(r){ return r.json(); })
      .then(function(d){
        // Sorting is done on the client (more flexible for the dashboard).
        renderNewsByDay(d.items || {});
      })
      .catch(function(e){
        var root = el('newsByDay');
        if(root) root.innerHTML = '<div class="small" style="color:#b00">Failed to load news.</div>';
        log('loadNewsByDay failed: ' + e);
      });
  }

  function deleteItem(id){
    if(!id) return;
    fetch(apiUrl('/items/' + id), {method:'DELETE'})
      .then(function(r){ return r.json(); })
      .then(function(d){
        log('delete item ' + id + ': ' + JSON.stringify(d));
        loadNewsByDay();
    initLlmTab();
      })
      .catch(function(e){ log('deleteItem failed: ' + e); });
  }

  
// ---------------------------------------------------------------------------
// LLM Digest tab
// ---------------------------------------------------------------------------

function renderLlmList(items, total){
  var root = el('llmList');
  var cnt = el('llmCount');
  if(cnt) cnt.textContent = (total != null ? String(total) : '—');
  if(!root) return;
  root.innerHTML = '';
  items = items || [];
  if(!items.length){
    root.innerHTML = '<div class="small">No analyzed items yet. Click "Enqueue" and wait for llm-worker.</div>';
    return;
  }

  for(var i=0;i<items.length;i++){
    var it = items[i];
    var wrap = document.createElement('div');
    wrap.className = 'item';

    var dt = (it.published_at || it.fetched_at || '').replace('T',' ').replace('Z','');
    var head = document.createElement('div');
    head.innerHTML = '<div class="row"><div class="grow"><b>' + esc(it.title_short || it.title) + '</b></div>' +
      '<div class="mono small">interest=' + esc(String(it.interest_score)) + '</div></div>' +
      '<div class="small muted">' + esc(it.source_name || '') + ' • ' + esc(dt) + '</div>';
    wrap.appendChild(head);

    var b = document.createElement('div');
    b.className = 'small';
    b.textContent = it.bulletin || it.summary || '';
    wrap.appendChild(b);

    var actions = document.createElement('div');
    actions.className = 'row';
    actions.style.marginTop = '6px';

    var a1 = document.createElement('a');
    a1.className = 'link';
    a1.href = it.url || '#';
    a1.target = '_blank';
    a1.rel = 'noreferrer';
    a1.textContent = 'open source';
    actions.appendChild(a1);

    var btn = document.createElement('button');
    btn.className = 'btn';
    btn.type = 'button';
    btn.textContent = 'details';
    (function(itemId){
      btn.addEventListener('click', function(){ loadLlmItem(itemId); });
    })(it.item_id || it.id);
    actions.appendChild(btn);

    wrap.appendChild(actions);
    root.appendChild(wrap);
  }
}

function loadLlmItems(){
  var only = true;
  var cb = el('llmOnlyDfoBusiness');
  if(cb) only = !!cb.checked;
  var q = '/llm/items?limit=200&only_dfo_business=' + (only ? 'true' : 'false');
  fetch(apiUrl(q))
    .then(function(r){ return r.json(); })
    .then(function(d){
      if(!d || !d.ok) throw new Error('bad response');
      renderLlmList(d.items || [], d.total);
    })
    .catch(function(e){
      log('loadLlmItems failed: ' + e);
    });
}

function loadLlmItem(itemId){
  if(!itemId) return;
  fetch(apiUrl('/llm/items/' + encodeURIComponent(itemId)))
    .then(function(r){ return r.json(); })
    .then(function(d){
      if(!d || !d.ok) throw new Error('bad response');
      var it = d.item || {};
      alert(
        (it.title_short || it.title || '') + '\n\n' +
        'interest=' + (it.interest_score || 0) + ', is_dfo_business=' + (it.is_dfo_business || 0) + '\n' +
        'tags=' + (it.tags || '') + '\n\n' +
        (it.summary || '') + '\n\n' +
        'WHY: ' + (it.why || '')
      );
    })
    .catch(function(e){ log('loadLlmItem failed: ' + e); });
}

function initLlmTab(){
  var b1 = el('llmEnqueueBtn');
  var b2 = el('llmRefreshBtn');
  if(b1){
    b1.addEventListener('click', function(){
      fetch(apiUrl('/llm/enqueue?window_hours=0&min_business=2&min_dfo=2&exclude_war=true'), {method:'POST'})
        .then(function(r){ return r.json(); })
        .then(function(d){
          log('llm enqueue: ' + JSON.stringify(d));
          loadLlmItems();
        })
        .catch(function(e){ log('llm enqueue failed: ' + e); });
    });
  }
  if(b2){
    b2.addEventListener('click', function(){ loadLlmItems(); });
  }
}


// ---------------------------------------------------------------------------
// Daily digests UI
// ---------------------------------------------------------------------------

var _digestsInit = false;

function _todayIso(){
  try{ return new Date().toISOString().slice(0,10); }catch(e){ return ''; }
}

function _getDay(){
  var i = el('digDay');
  return (i && i.value) ? i.value : '';
}

function _setDay(v){
  var i = el('digDay');
  if(i) i.value = v;
}

function _renderDigestMeta(d){
  var meta = el('digMeta');
  if(!meta) return;
  if(!d){ meta.textContent = '—'; return; }
  meta.textContent = 'day=' + (d.day||'—') + ', status=' + (d.status||'—') + ', items=' + String(d.items_count||0) + ', updated=' + (d.updated_at||'—');
}

function _renderDiagnostics(d){
  var diag = el('digDiag');
  if(!diag){ return; }
  if(!d || !d.diagnostics || !d.diagnostics.counts){ diag.textContent='—'; return; }
  var c = d.diagnostics.counts;
  diag.textContent = 'candidates_total=' + c.candidates_total + ', prefer_bucket=' + c.prefer_bucket + ', fallback_bucket=' + c.fallback_bucket;
}

function _renderItems(d){
  var root = el('digItems');
  if(!root) return;
  root.innerHTML = '';
  if(!d || !d.items || !d.items.length){
    root.innerHTML = '<div class="small">No items in digest (yet).</div>';
    return;
  }
  for(var i=0;i<d.items.length;i++){
    var it = d.items[i] || {};
    var wrap = document.createElement('div');
    wrap.className = 'item';
    var dt = (it.published_at || it.fetched_at || '').replace('T',' ').replace('Z','');
    wrap.innerHTML =
      '<div class="row"><div class="grow"><b>#' + esc(String(it.rank)) + ' ' + esc(it.title_short || it.title || '') + '</b></div>' +
      '<div class="mono small">interest=' + esc(String(nz(it.interest_score,0))) + '</div></div>' +
      '<div class="small muted">' + esc(it.source_name || '') + ' • ' + esc(dt) + ' • biz=' + esc(String(nz(it.business_score,0))) + ' dfo=' + esc(String(nz(it.dfo_score,0))) + '</div>' +
      (it.bulletin ? ('<div class="small" style="margin-top:6px">' + esc(it.bulletin) + '</div>') : '') +
      (it.why ? ('<div class="small muted" style="margin-top:6px">why: ' + esc(it.why) + '</div>') : '') +
      (it.url ? ('<div style="margin-top:6px"><a class="link" href="' + esc(it.url) + '" target="_blank" rel="noreferrer">open source</a></div>') : '');
    root.appendChild(wrap);
  }
}

function _segmentsToText(segments){
  if(!segments || !segments.length) return '';
  var out = [];
  for(var i=0;i<segments.length;i++){
    var s = segments[i] || {};
    if(s.type === 'intro' || s.type === 'outro'){
      out.push((s.text||'').trim());
    } else if(s.type === 'item'){
      var head = (s.rank ? ('#' + s.rank + ': ') : '');
      out.push(head + ((s.text||'').trim()));
      if(s.transition){
        out.push('↳ ' + String(s.transition).trim());
      }
    }
    out.push('');
  }
  return out.join('\n').trim();
}

function _renderScript(d){
  var meta = el('digScriptMeta');
  var txt = el('digScriptText');
  var raw = el('digScriptJson');
  if(meta) meta.textContent = '—';
  if(txt) txt.textContent = '—';
  if(raw) raw.textContent = '—';
  if(!d){ return; }

  var segs = null;
  if(d.script && typeof d.script === 'object') segs = d.script;
  if(!segs && d.script_json){
    try{ segs = JSON.parse(d.script_json); }catch(e){ segs = null; }
  }

  var has = !!(segs && segs.length);
  if(meta) meta.textContent = has ? ('segments=' + segs.length + ', model=' + (d.script_model||'') + ', created=' + (d.script_created_at||'—')) : 'no script yet';
  if(txt) txt.textContent = has ? _segmentsToText(segs) : '—';
  if(raw) raw.textContent = has ? JSON.stringify(segs, null, 2) : '—';
}

function renderDigest(d){
  _renderDigestMeta(d);
  _renderDiagnostics(d);
  _renderItems(d);
  _renderScript(d);
}

function renderDigestsList(list){
  var root = el('digHistory');
  var cnt = el('digHistoryCount');
  if(cnt) cnt.textContent = '—';
  if(!root) return;
  root.innerHTML = '';
  list = list || [];
  if(cnt) cnt.textContent = String(list.length);
  if(!list.length){
    root.innerHTML = '<div class="small">No digests yet.</div>';
    return;
  }
  for(var i=0;i<list.length;i++){
    var d = list[i] || {};
    var day = d.day || '';
    var wrap = document.createElement('div');
    wrap.className = 'item';
    wrap.innerHTML =
      '<div class="row"><div class="grow"><b class="mono">' + esc(day) + '</b></div>' +
      '<div class="mono small">status=' + esc(d.status||'') + ', items=' + esc(String(nz(d.items_count,0))) + '</div></div>' +
      '<div class="small muted">created=' + esc(d.created_at||'') + ' • updated=' + esc(d.updated_at||'') + '</div>' +
      '<div style="margin-top:6px"><a href="#" data-day="' + esc(day) + '">load</a></div>';
    root.appendChild(wrap);
  }
}

function loadDigestsList(){
  fetch(apiUrl('/digests?limit=50&offset=0'))
    .then(function(r){ return r.json(); })
    .then(function(d){
      if(!d || !d.ok) throw new Error('bad response');
      renderDigestsList(d.items || []);
    })
    .catch(function(e){ log('loadDigestsList failed: ' + e); });
}

function loadDigest(day){
  if(!day) return;
  fetch(apiUrl('/digests/daily?day=' + encodeURIComponent(day)))
    .then(function(r){ return r.json(); })
    .then(function(d){
      if(!d || !d.ok) throw new Error('bad response');
      if(!d.exists){ renderDigest(null); return; }
      renderDigest(d.digest || null);
    })
    .catch(function(e){ log('loadDigest failed: ' + e); });
}

function createDigest(day, force){
  if(!day) return;
  var minInterest = parseInt((el('digMinInterest')||{}).value || '5', 10);
  var preferDays = parseInt((el('digPreferDays')||{}).value || '2', 10);
  var lookback = parseInt((el('digLookback')||{}).value || '60', 10);
  if(isNaN(minInterest)) minInterest = 5;
  if(isNaN(preferDays)) preferDays = 2;
  if(isNaN(lookback)) lookback = 60;
  var excludeWar = !!(el('digExcludeWar') && el('digExcludeWar').checked);
  var onlyDfoBiz = !!(el('digOnlyDfoBiz') && el('digOnlyDfoBiz').checked);
  var q = '/digests/daily/create?day=' + encodeURIComponent(day) +
    '&min_interest=' + encodeURIComponent(String(minInterest)) +
    '&prefer_days=' + encodeURIComponent(String(preferDays)) +
    '&max_lookback_days=' + encodeURIComponent(String(lookback)) +
    '&exclude_war=' + (excludeWar ? 'true' : 'false') +
    '&min_business=2&min_dfo=2&only_dfo_business=' + (onlyDfoBiz ? 'true' : 'false') +
    '&refill=true&force=' + (force ? 'true' : 'false');
  fetch(apiUrl(q), {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(d){
      if(!d || !d.ok) throw new Error('bad response');
      renderDigest(d.digest || null);
      loadDigestsList();
    })
    .catch(function(e){ log('createDigest failed: ' + e); });
}

function generateScript(day, force){
  if(!day) return;
  // Give immediate feedback: script generation can take time (LLM call).
  log('generateScript: start day=' + day + ' force=' + (force ? 'true' : 'false'));
  var meta = el('digScriptMeta');
  if(meta) meta.textContent = 'generating...';

  // Add a client-side timeout so the UI doesn't look "dead" forever.
  // Server-side timeouts are handled by the API/LLM services.
  var controller = null;
  try{ controller = new AbortController(); }catch(e){ controller = null; }
  var didTimeout = false;
  var tmo = setTimeout(function(){
    if(controller){
      didTimeout = true;
      try{ controller.abort(); }catch(e){}
    }
  }, 210000); // 210s

  // If it takes longer than a few seconds, tell the user it's still running.
  var slow = setTimeout(function(){
    log('generateScript: still running (waiting for LLM)...');
  }, 6000);

  fetch(
      apiUrl('/digests/daily/script?day=' + encodeURIComponent(day) + '&force=' + (force ? 'true' : 'false')),
      {method:'POST', signal: controller ? controller.signal : undefined}
    )
    .then(function(r){
      // Surface non-JSON errors clearly.
      if(!r.ok){
        return r.text().then(function(t){
          throw new Error('HTTP ' + r.status + ': ' + (t || '').slice(0, 300));
        });
      }
      return r.json();
    })
    .then(function(d){
      if(!d || !d.ok) throw new Error((d && d.detail) ? d.detail : 'bad response');
      renderDigest(d.digest || null);
      log('generateScript: ok');
    })
    .catch(function(e){
      if(didTimeout){
        log('generateScript: timeout (client-side). The server may still be working. Try again or check API logs.');
      } else {
        log('generateScript failed: ' + e);
      }
      // Reset meta if we failed.
      try{ _renderScript(null); }catch(_e){}
    })
    .finally(function(){
      try{ clearTimeout(tmo); }catch(e){}
      try{ clearTimeout(slow); }catch(e){}
    });
}

function initDigestsTab(autoLoad){
  if(!_digestsInit){
    _digestsInit = true;
    if(el('digDay') && !el('digDay').value) el('digDay').value = _todayIso();

    var bLoad = el('digLoadBtn');
    if(bLoad) bLoad.addEventListener('click', function(){ loadDigest(_getDay()); });
    var bCreate = el('digCreateBtn');
    if(bCreate) bCreate.addEventListener('click', function(){ createDigest(_getDay(), false); });
    var bForce = el('digForceBtn');
    if(bForce) bForce.addEventListener('click', function(){ createDigest(_getDay(), true); });
    var bScript = el('digScriptBtn');
    if(bScript) bScript.addEventListener('click', function(){ generateScript(_getDay(), false); });
    var bScriptForce = el('digScriptForceBtn');
    if(bScriptForce) bScriptForce.addEventListener('click', function(){ generateScript(_getDay(), true); });
    var bList = el('digRefreshListBtn');
    if(bList) bList.addEventListener('click', function(){ loadDigestsList(); });

    var listRoot = el('digHistory');
    if(listRoot) listRoot.addEventListener('click', function(ev){
      var t = ev.target;
      if(!t) return;
      var day = t.getAttribute ? t.getAttribute('data-day') : null;
      if(day){
        ev.preventDefault();
        _setDay(day);
        loadDigest(day);
      }
    });
  }
  loadDigestsList();
  if(autoLoad){
    var day2 = _getDay();
    if(day2) loadDigest(day2);
  }
}


  function deleteDay(day){
    if(!day) return;
    fetch(apiUrl('/items/by-day?day=' + encodeURIComponent(day)), {method:'DELETE'})
      .then(function(r){ return r.json(); })
      .then(function(d){
        log('delete day ' + day + ': ' + JSON.stringify(d));
        loadNewsByDay();
    initLlmTab();
      })
      .catch(function(e){ log('deleteDay failed: ' + e); });
  }

  function purge(days){
    if(!days) return;
    fetch(apiUrl('/items/purge?days=' + encodeURIComponent(days)), {method:'DELETE'})
      .then(function(r){ return r.json(); })
      .then(function(d){
        log('purge days=' + days + ': ' + JSON.stringify(d));
        loadNewsByDay();
    initLlmTab();
      })
      .catch(function(e){ log('purge failed: ' + e); });
  }

  window.addEventListener('DOMContentLoaded', function(){
    var btnRun = el('run');
    if(btnRun) btnRun.addEventListener('click', runIngest);

    var btnLoad = el('loadNews');
    if(btnLoad) btnLoad.addEventListener('click', loadNewsByDay);

    var ns = el('newsSort');
    if(ns) ns.addEventListener('change', loadNewsByDay);

    var ew = el('excludeWar');
    if(ew) ew.addEventListener('change', loadNewsByDay);

    var b1 = el('delOne');
    if(b1) b1.addEventListener('click', function(){
      var v = el('delId').value;
      if(v) deleteItem(v);
    });

    var b2 = el('delByDay');
    if(b2) b2.addEventListener('click', function(){
      var v = el('delDay').value;
      if(v) deleteDay(v);
    });

    var b3 = el('purgeBtn');
    if(b3) b3.addEventListener('click', function(){
      var v = el('purgeDays').value;
      if(v) purge(v);
    });

    var bj = el('refreshJobs');
    if(bj) bj.addEventListener('click', loadJobs);

    var bo = el('openJob');
    if(bo) bo.addEventListener('click', function(){
      var v = (el('jobId') && el('jobId').value) ? el('jobId').value.trim() : '';
      if(v) setCurrentJob(v);
    });

    initTabs();
    initCatalog();
    log('UI loaded. API base = ' + (apiBase || '(root)'));
    loadJobs();
    loadNewsByDay();
    initLlmTab();

    // Open job from hash #job=<id>
    try{
      var h = (window.location && window.location.hash) ? window.location.hash : '';
      if(h && h.indexOf('job=') >= 0){
        var jid = h.replace(/^#/, '').split('job=')[1] || '';
        jid = jid.split('&')[0].trim();
        if(jid) setCurrentJob(jid);
      }
    }catch(e){}
  });
  // Export minimal helpers for legacy blocks that live outside this closure.
  // (Keeps the rest of the UI scoped, while still enabling the TTS tab logic below.)
  window.el = el;
  window.log = log;
  window.apiUrl = apiUrl;
  window.apiGet = apiGet;
  window.apiPost = apiPost;
  window.setTab = setTab;
})();// ---------------- TTS (Daily Digest Audio) ----------------
var ttsInited = false;
  var videoInited = false;

function ttsSetStatus(s){
  var st = el('ttsStatus'); if(st) st.textContent = s || '—';
}
function ttsClearOutput(){
  var a = el('ttsDownload');
  if(a){ a.href = '#'; a.style.display = 'none'; }
  var p = el('ttsPlayer');
  if(p){ p.src = ''; p.style.display = 'none'; }
  var m = el('ttsMeta'); if(m) m.textContent = '';
}
function ttsApplyResult(data){
  if(!data){ ttsSetStatus('—'); return; }

  // error passthrough
  if(data.detail){
    ttsSetStatus('error');
    if(el('ttsMeta')) el('ttsMeta').textContent = JSON.stringify(data, null, 2);
    return;
  }

  // status
  if(data.exists === false) ttsSetStatus('no audio');
  else if(data.ok) ttsSetStatus('ok');
  else ttsSetStatus('—');

  if(el('ttsMeta')) el('ttsMeta').textContent = JSON.stringify(data, null, 2);

  // Fill human-friendly fields.
  if(el('ttsFile')) el('ttsFile').textContent = (data.file_name || '—');
  if(el('ttsVoiceOut')) el('ttsVoiceOut').textContent = (data.voice_wav || '—');
  if(el('ttsLangOut')) el('ttsLangOut').textContent = (data.language || '—');

  if(data.download_url){
    var url = apiUrl(data.download_url);
    var a = el('ttsDownload');
    if(a){ a.href = url; a.style.display = ''; }
    var p = el('ttsPlayer');
    if(p){ p.src = url; p.style.display = ''; }
  }
}

function ttsGetDay(){
  var d = el('ttsDay'); return d ? d.value : '';
}
function ttsGetParams(){
  // API expects: language=ru, voice_wav=<path|null>, force_script=true|false
  var voice_wav = el('ttsVoice') ? el('ttsVoice').value.trim() : '';
  var lang  = el('ttsLang') ? el('ttsLang').value.trim() : '';
  var force_script = el('ttsForce') ? el('ttsForce').checked : false;

  // Keep UI flexible: if language is empty, default to 'ru' (backend default).
  if(!lang) lang = 'ru';

  var qs = [];
  if(force_script) qs.push('force_script=true');
  if(voice_wav) qs.push('voice_wav=' + encodeURIComponent(voice_wav));
  if(lang) qs.push('language=' + encodeURIComponent(lang));
  return qs.length ? ('?' + qs.join('&')) : '';
}

async function ttsCheck(){
  var day = ttsGetDay();
  if(!day){ log('TTS: day is empty'); return; }
  ttsSetStatus('loading');
  ttsClearOutput();
  try{
    // Status endpoint accepts language query parameter
    var qs = '';
    var lang = el('ttsLang') ? el('ttsLang').value.trim() : '';
    if(!lang) lang = 'ru';
    qs = '?language=' + encodeURIComponent(lang);
    var data = await apiGet('/tts/daily/' + encodeURIComponent(day) + qs);
    ttsApplyResult(data);
  }catch(e){
    ttsSetStatus('error');
    log('TTS check error: ' + (e && e.message ? e.message : e));
  }
}

async function ttsRender(){
  var day = ttsGetDay();
  if(!day){ log('TTS: day is empty'); return; }
  ttsSetStatus('rendering');
  ttsClearOutput();
  var qs = ttsGetParams();
  try{
    var data = await apiPost('/tts/daily/' + encodeURIComponent(day) + '/render' + qs, {});
    ttsApplyResult(data);
  }catch(e){
    ttsSetStatus('error');
    log('TTS render error: ' + (e && e.message ? e.message : e));
  }
}

function initTtsTab(setDefaults){
  if(!ttsInited){
    var b1 = el('ttsCheckBtn'); if(b1) b1.addEventListener('click', function(){ ttsCheck(); });
    var b2 = el('ttsRenderBtn'); if(b2) b2.addEventListener('click', function(){ ttsRender(); });
    var b3 = el('ttsOpenDigestBtn'); if(b3) b3.addEventListener('click', function(){
      // open digests tab for the same day
      var day = ttsGetDay();
      if(el('digDay') && day) el('digDay').value = day;
      setTab('digests');
      try{ initDigestsTab(false); }catch(e){}
    });
    ttsInited = true;
  }
  if(setDefaults){
    var d = el('ttsDay');
    if(d && !d.value){
      // default to today (local)
      try{
        var now = new Date();
        var yyyy = now.getFullYear();
        var mm = String(now.getMonth()+1).padStart(2,'0');
        var dd = String(now.getDate()).padStart(2,'0');
        d.value = yyyy + '-' + mm + '-' + dd;
      }catch(e){}
    }
  }
  // lightweight auto check on open
  try{ ttsCheck(); }catch(e){}
}


// ---------------- Video (SadTalker Talking Head) ----------------
var videoInited = false;

function videoGetDay(){
  var d = el('videoDay');
  return d ? (d.value || '').trim() : '';
}
function videoGetLang(){
  var l = el('videoLang');
  return l ? (l.value || 'ru').trim() : 'ru';
}
function videoGetImage(){
  var i = el('videoImage');
  return i ? (i.value || '').trim() : '';
}
function videoGetForceTts(){
  var c = el('videoForceTts');
  return !!(c && c.checked);
}

function videoSetStatus(s){
  var x = el('videoStatus');
  if(x) x.textContent = s || '—';
}

function videoApplyResult(data){
  data = data || {};
  var exists = !!data.exists || !!data.file_name;
  var file = data.file_name || '—';
  var img = data.image_path || data.image || '—';
  var aud = data.audio_file_name || '—';

  if(exists){
    videoSetStatus('ok');
  } else {
    videoSetStatus('no video');
  }

  var vf = el('videoFile'); if(vf) vf.textContent = file;
  var vi = el('videoImageOut'); if(vi) vi.textContent = img;
  var va = el('videoAudioOut'); if(va) va.textContent = aud;

  var a = el('videoDownload');
  if(a && data.download_url){
    a.href = apiUrl(data.download_url);
    a.style.display = '';
  } else if(a){
    a.href = '#';
    a.style.display = 'none';
  }

  var meta = el('videoMeta');
  if(meta){
    var txt = JSON.stringify(data, null, 2);
    meta.textContent = txt;
  }
}

async function videoCheck(){
  var day = videoGetDay();
  if(!day){ videoSetStatus('no day'); return; }
  var lang = encodeURIComponent(videoGetLang());
  videoSetStatus('loading...');
  try{
    var data = await apiGet('/video/daily/' + encodeURIComponent(day) + '?language=' + lang);
    videoApplyResult(data);
  }catch(e){
    videoSetStatus('error');
    log('Video check error: ' + (e && e.message ? e.message : e));
  }
}

async function videoRender(){
  var day = videoGetDay();
  if(!day){ videoSetStatus('no day'); return; }
  var lang = encodeURIComponent(videoGetLang());
  var force = videoGetForceTts();
  var img = videoGetImage();
  var qs = '?language=' + lang + (force ? '&force_tts=true' : '');
  if(img){
    qs += '&image=' + encodeURIComponent(img);
  }
  videoSetStatus('rendering...');
  try{
    var data = await apiPost('/video/daily/' + encodeURIComponent(day) + '/render' + qs, {});
    videoApplyResult(data);
  }catch(e){
    videoSetStatus('error');
    log('Video render error: ' + (e && e.message ? e.message : e));
  }
}

function initVideoTab(setDefaults){
  if(!videoInited){
    var b1 = el('videoCheckBtn'); if(b1) b1.addEventListener('click', function(){ videoCheck(); });
    var b2 = el('videoRenderBtn'); if(b2) b2.addEventListener('click', function(){ videoRender(); });
    videoInited = true;
  }
  if(setDefaults){
    var d = el('videoDay');
    if(d && !d.value){
      try{
        var now = new Date();
        var yyyy = now.getFullYear();
        var mm = String(now.getMonth()+1).padStart(2,'0');
        var dd = String(now.getDate()).padStart(2,'0');
        d.value = yyyy + '-' + mm + '-' + dd;
      }catch(e){}
    }
  }
  try{ videoCheck(); }catch(e){}
}
