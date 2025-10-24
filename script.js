(() => {
  function init(){
    // Column positions from the manifest sheets
    const COL_SO = 4, COL_FP = 10, COL_CH = 11, COL_FL = 12;

    let loadingXLSX = null;
    async function ensureXLSX(){
      if (typeof XLSX !== 'undefined') return true;
      if (!loadingXLSX){
        loadingXLSX = new Promise((resolve, reject)=>{
          const script = document.createElement('script');
          script.src = 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
          script.async = true;
          script.crossOrigin = 'anonymous';
          script.referrerPolicy = 'no-referrer';
          script.dataset.fallback = 'xlsx';
          script.onload = ()=> resolve(true);
          script.onerror = (err)=> reject(err || new Error('Failed to load fallback XLSX parser'));
          document.head.appendChild(script);
        });
      }
      try{
        await loadingXLSX;
      }catch(err){
        console.error('Unable to load XLSX fallback', err);
        loadingXLSX = null;
        return false;
      }
      return typeof XLSX !== 'undefined';
    }
    const supabaseConfig = window.SUPABASE_CONFIG || null;
    const supabase = (typeof window.supabase !== 'undefined' && supabaseConfig?.url && supabaseConfig?.anonKey)
      ? window.supabase.createClient(supabaseConfig.url, supabaseConfig.anonKey)
      : null;
    const SUPABASE_ENABLED = !!supabase;

    const logoutBtn = document.getElementById('auth_logout');

    const MANIFEST_HEADERS = ['Run','Drop','Zone','Date','Sales Order','Name','Address','Suburb','Postcode','Phone Number','FP','CH','FL','Weight','Type'];
    const normSO = v => (v == null ? '' : String(v).trim().toUpperCase());
    const coerceCount = v => {
      if (v == null || v === '') return 0;
      const num = Number(v);
      if (Number.isFinite(num)) return Math.max(0, num);
      const parsed = parseInt(String(v), 10);
      return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
    };

    function manifestRowToCells(row){
      const cells = Array.isArray(row) ? [...row] : [];
      if (cells.length < MANIFEST_HEADERS.length){
        while(cells.length < MANIFEST_HEADERS.length) cells.push('');
      }else if(cells.length > MANIFEST_HEADERS.length){
        cells.length = MANIFEST_HEADERS.length;
      }
      return cells.map(cell => (cell ?? '') === '' ? '-' : String(cell));
    }

    function computeManifestData(table){
      const generated = {};
      const rowLookup = {};
      table.slice(1).forEach((row, idx)=>{
        const so = normSO(row[COL_SO]);
        if(!so) return;
        (rowLookup[so] ||= []).push(idx);
        const total = coerceCount(row[COL_FP]) + coerceCount(row[COL_CH]) + coerceCount(row[COL_FL]);
        if(total<=0) return;
        const arr = (generated[so] ||= []);
        const start = arr.length;
        for(let i=1;i<=total;i++) arr.push(`${so}${String(start+i).padStart(3,'0')}`);
      });
      return { generated, rowLookup };
    }

    function computeScheduleEntries(table){
      const headers = (table[0] || []).map(v => String(v ?? '').trim().toLowerCase());
      const createdIdx = headers.findIndex(h => h === 'created from');
      const entries = [];
      table.slice(1).forEach(row=>{
        const so = normSO(row[COL_SO]);
        if(!so) return;
        const created = createdIdx !== -1 ? row[createdIdx] : row[COL_SO];
        entries.push({ createdFrom: created ?? '', so });
      });
      return entries;
    }

    async function readWorkbookFile(file){
      const buf = await file.arrayBuffer();
      const wb  = XLSX.read(buf,{type:'array'});
      const ws  = wb.Sheets[wb.SheetNames[0]];
      return XLSX.utils.sheet_to_json(ws,{header:1});
    }

    function showToast(msg,type='info'){
      const el=document.createElement('div');
      el.textContent=msg;
      el.role='status';
      Object.assign(el.style,{
        position:'fixed',left:'50%',top:'16px',transform:'translateX(-50%)',
        padding:'10px 14px',borderRadius:'10px',zIndex:9999,fontSize:'14px',
        border:'1px solid rgba(148,163,184,.3)',backdropFilter:'blur(8px)',
        boxShadow:'0 10px 24px rgba(2,6,23,.4)',color:'#e2e8f0',background:'rgba(56,189,248,.12)'
      });
      if(type==='error'){el.style.background='rgba(248,113,113,.22)'; el.style.color='#fecaca';}
      if(type==='success'){el.style.background='rgba(74,222,128,.22)'; el.style.color='#bbf7d0';}
      document.body.appendChild(el);
      setTimeout(()=>el.remove(),1600);
    }

    async function storeFinalDataForUser(userId, tableData, filesMeta, manifestData){
      const manifest = manifestData || computeManifestData(tableData);
      if (SUPABASE_ENABLED){
        const { error } = await supabase
          .from('depot_manifests')
          .insert({
            depot_id: userId,
            kind: 'final',
            payload: {
              tableData,
              filesMeta,
              generated: manifest.generated,
              rowLookup: manifest.rowLookup
            },
            uploaded_by: currentUser?.id || 'admin'
          });
        if (error) throw error;
      } else {
        const base = suffix => `drm_${userId}_final_${suffix}`;
        localStorage.setItem(base('table_v2'), JSON.stringify(tableData));
        localStorage.setItem(base('generated_v2'), JSON.stringify(manifest.generated));
        localStorage.setItem(base('rowlookup_v2'), JSON.stringify(manifest.rowLookup));
        localStorage.setItem(base('files_meta_v2'), JSON.stringify(filesMeta));
        localStorage.setItem(base('scanned_v2'), JSON.stringify({}));
      }
    }

    async function storeGlueDataForUser(userId, scheduleEntries, filesMeta, options = {}){
      const { tableData = [], manifest = null } = options || {};
      const generated = manifest?.generated || {};
      const rowLookup = manifest?.rowLookup || {};
      const payload = {
        scheduleEntries,
        filesMeta,
        tableData,
        generated,
        rowLookup
      };
      if (SUPABASE_ENABLED){
        const { error } = await supabase
          .from('depot_manifests')
          .insert({
            depot_id: userId,
            kind: 'glue',
            payload,
            uploaded_by: currentUser?.id || 'admin'
          });
        if (error) throw error;
      } else {
        const base = suffix => `drm_${userId}_glue_${suffix}`;
        localStorage.setItem(base('schedule_v1'), JSON.stringify(scheduleEntries));
        localStorage.setItem(base('files_meta_v2'), JSON.stringify(filesMeta));
        localStorage.setItem(base('table_v2'), JSON.stringify(tableData));
        localStorage.setItem(base('generated_v2'), JSON.stringify(generated));
        localStorage.setItem(base('rowlookup_v2'), JSON.stringify(rowLookup));
        localStorage.setItem(base('scanned_v2'), JSON.stringify({}));
      }
    }

    const REPORTS_KEY = 'drm_admin_reports_v1';
    const loadReportsLocal = () => {
      try{
        return JSON.parse(localStorage.getItem(REPORTS_KEY) || '[]');
      }catch{
        return [];
      }
    };
    const saveReportsLocal = (list) => {
      localStorage.setItem(REPORTS_KEY, JSON.stringify(list));
    };

    async function loadReports(){
      if (SUPABASE_ENABLED){
        const { data, error } = await supabase
          .from('depot_reports')
          .select('*')
          .order('created_at', { ascending:false });
        if (error) throw error;
        return data || [];
      }
      return loadReportsLocal();
    }

    async function addReport(report){
      if (SUPABASE_ENABLED){
        const { error } = await supabase.from('depot_reports').insert({
          id: report.id,
          depot_id: report.depotId,
          depot_name: report.depotName,
          kind: report.kind,
          rows: report.rows,
          filename: report.filename,
          csv: report.csv
        });
        if (error) throw error;
        window.dispatchEvent(new CustomEvent('drm:reports-updated'));
        return;
      }
      const reports = loadReportsLocal();
      reports.push(report);
      saveReportsLocal(reports);
      window.dispatchEvent(new CustomEvent('drm:reports-updated'));
    }

    async function removeReport(reportId){
      if (SUPABASE_ENABLED){
        const { error } = await supabase
          .from('depot_reports')
          .delete()
          .eq('id', reportId);
        if (error) throw error;
        window.dispatchEvent(new CustomEvent('drm:reports-updated'));
        return;
      }
      const reports = loadReportsLocal().filter(r => r.id !== reportId);
      saveReportsLocal(reports);
      window.dispatchEvent(new CustomEvent('drm:reports-updated'));
    }
    function encodeCSV(str){
      try{
        if (window.TextEncoder){
          const bytes = new TextEncoder().encode(str);
          let binary = '';
          bytes.forEach(b => binary += String.fromCharCode(b));
          return btoa(binary);
        }
      }catch{}
      return btoa(unescape(encodeURIComponent(str)));
    }
    function decodeCSV(b64){
      try{
        const binary = atob(b64);
        if (window.TextDecoder){
          const bytes = Uint8Array.from(binary, c => c.charCodeAt(0));
          return new TextDecoder().decode(bytes);
        }
        return decodeURIComponent(escape(binary));
      }catch{
        try{
          return decodeURIComponent(escape(atob(b64)));
        }catch{
          return atob(b64);
        }
      }
    }

    const USERS = [
      { id:'albury',   name:'Albury Depot', role:'depot' },
      { id:'melbourne',name:'Melbourne Depot', role:'depot' },
      { id:'sydney',   name:'Sydney Depot', role:'depot' },
      { id:'brisbane', name:'Brisbane Depot', role:'depot' },
      { id:'perth',    name:'Perth Depot', role:'depot' },
      { id:'glueline', name:'Glueline Team', role:'glue' },
      { id:'admin',    name:'Administrator', role:'admin' }
    ];
    const PASSWORDS = {
      default: 'Knowles40',
      admin: '40knowles'
    };
    const AUTH_KEY = 'drm_auth_user_v1';
    let currentUser = null;
    let appStarted = false;

    function setupAuth(onReady){
      let resolved = false;
      const overlay = document.getElementById('auth_overlay');
      const form = document.getElementById('auth_form');
      const userSelect = document.getElementById('auth_user');
      const passInput = document.getElementById('auth_pass');
      const errorEl = document.getElementById('auth_error');
      const loginBtn = document.getElementById('auth_login');
      if (!overlay || !form || !userSelect || !passInput || !errorEl || !loginBtn){
        resolved = true;
        onReady({ id:'anonymous', name:'Anonymous User' });
        return;
      }

      if (!userSelect.dataset.populated){
        USERS.forEach(user=>{
          const option = document.createElement('option');
          option.value = user.id;
          option.textContent = user.name;
          userSelect.appendChild(option);
        });
        userSelect.dataset.populated = 'true';
      }

      function setError(msg){
        errorEl.textContent = msg || '';
        errorEl.style.display = msg ? 'block' : 'none';
      }

      function showOverlay(){
        document.body.classList.add('auth-locked');
        overlay.classList.add('show');
        overlay.setAttribute('aria-hidden','false');
        setError('');
        passInput.value='';
        loginBtn.disabled = false;
        requestAnimationFrame(()=> userSelect.focus());
        if (logoutBtn) logoutBtn.style.display = 'none';
      }

      function hideOverlay(){
        overlay.classList.remove('show');
        overlay.setAttribute('aria-hidden','true');
        document.body.classList.remove('auth-locked');
        setError('');
        passInput.value='';
      }

      function complete(user){
        currentUser = user;
        hideOverlay();
        localStorage.setItem(AUTH_KEY, JSON.stringify({ id:user.id, name:user.name, role:user.role }));
        if (!resolved){
          resolved = true;
          onReady(user);
        }
      }

      form.addEventListener('submit', (event)=>{
        event.preventDefault();
        const selected = userSelect.value;
        const password = passInput.value.trim();
        if (!selected){ setError('Select your depot.'); userSelect.focus(); return; }
        const record = USERS.find(u=>u.id===selected) || { id:selected, name:selected, role: selected === 'admin' ? 'admin' : 'depot' };
        const role = record.role ?? (record.id === 'admin' ? 'admin' : 'depot');
        const expected = (PASSWORDS[record.id] !== undefined)
          ? PASSWORDS[record.id]
          : (role === 'admin' ? PASSWORDS.admin : PASSWORDS.default);
        if (password !== expected){ setError('Incorrect password.'); passInput.value=''; passInput.focus(); return; }
        const user = { id: record.id, name: record.name ?? record.id, role };
        complete(user);
      });

      passInput.addEventListener('input', ()=> setError(''));
      userSelect.addEventListener('change', ()=> setError(''));

      const stored = localStorage.getItem(AUTH_KEY);
      if (stored){
        try{
          const parsed = JSON.parse(stored);
          if (parsed?.id){
            const base = USERS.find(u=>u.id===parsed.id);
            const user = base ? { ...base } : { id: parsed.id, name: parsed.name ?? parsed.id, role: parsed.role ?? (parsed.id === 'admin' ? 'admin' : 'depot') };
            complete(user);
            return;
          }
        }catch{
          localStorage.removeItem(AUTH_KEY);
        }
      }

      showOverlay();
    }

    function MarkingModule(prefix){
      const fileEl         = document.getElementById(prefix + '_file');
      const fileMeta       = document.getElementById(prefix + '_file_meta');
      const scheduleFileEl = document.getElementById(prefix + '_schedule_file');
      const scheduleMeta   = document.getElementById(prefix + '_schedule_meta');
      const scanEl         = document.getElementById(prefix + '_scan');
      const clearEl        = document.getElementById(prefix + '_clear');
      const exportEl       = document.getElementById(prefix + '_export');
      const tableWrap      = document.getElementById(prefix + '_table');
      const scheduleWrap   = document.getElementById(prefix + '_schedule_table');
      const summaryEl      = document.getElementById(prefix + '_scanned_summary');
      const filterClearEl  = document.getElementById(prefix + '_filter_clear');
      const canUpload      = currentUser?.role === 'admin';

      const hasRunsheetUI = Boolean(fileEl && fileMeta && tableWrap);
      const hasScheduleUI = Boolean(scheduleFileEl && scheduleMeta && scheduleWrap);

      if (!scanEl || !clearEl || !exportEl) {
        return { focus: () => {} };
      }
      if (!hasRunsheetUI && !hasScheduleUI) {
        return { focus: () => {} };
      }

      const baseKey = (suffix)=>{
        const user = currentUser?.id || 'anon';
        return `drm_${user}_${prefix}_${suffix}`;
      };
      const KEYS = {
        table:   baseKey('table_v2'),
        gen:     baseKey('generated_v2'),
        scanned: baseKey('scanned_v2'),
        lookup:  baseKey('rowlookup_v2'),
        files:   baseKey('files_meta_v2'),
        schedule:baseKey('schedule_v1'),
      };

      let tableData = [];
      let generated = {};
      let scanned   = {};
      let rowLookup = {};
      let statusEl  = null;
      let loadedFiles = [];
      let scheduleEntries = [];
      let filteredSO = null;
      let lastScanInfo = null;
      let autoScanTimer = null;
      const AUTOSCAN_DELAY = 120;
      const MIN_BARCODE_LENGTH = 11;

      const extractSO = v => {
        const upper = String(v ?? '').toUpperCase();
        const match = upper.match(/SO\d+/);
        return match ? match[0] : '';
      };

      if (filterClearEl) filterClearEl.style.display = 'none';
      if (summaryEl) summaryEl.style.display = 'none';

      if (!canUpload){
        if (fileEl){
          fileEl.disabled = true;
          fileEl.style.display = 'none';
        }
        if (scheduleFileEl){
          scheduleFileEl.disabled = true;
          scheduleFileEl.style.display = 'none';
        }
      }

      function ensureStatus(){
        if (statusEl) return statusEl;
        statusEl = document.createElement('div');
        Object.assign(statusEl.style,{
          marginTop:'0.75rem',border:'1px solid rgba(148,163,184,.25)',borderRadius:'12px',
          padding:'12px 14px',background:'rgba(56,189,248,.08)',boxShadow:'inset 0 1px 0 rgba(255,255,255,.04)',
          fontSize:'1rem',letterSpacing:'0.01em'
        });
        const card = fileEl.closest('.card');
        const controls = card?.querySelector('.controls');
        if (card && controls) card.insertBefore(statusEl, controls.nextSibling);
        return statusEl;
      }

      function setStatus({so, run, drop, scannedCount, total}){
        const el = ensureStatus();
        const runText = run && run !== '-' ? run : '-';
        const dropText = drop && drop !== '-' ? drop : '-';
        const hasRoute = runText !== '-' || dropText !== '-';
        const routeText = hasRoute ? `Run ${runText} / Drop ${dropText}` : 'Not routed';
        let html = `
          <strong>Sales Order:</strong> <span>${so}</span>&nbsp;&middot;&nbsp;
          <strong>Route:</strong> <span style="font-size:1.2rem;font-weight:700;letter-spacing:.02em">${routeText}</span>
        `;
        if (hasRunsheetUI) {
          html += `&nbsp;&middot;&nbsp;<strong>Progress:</strong> <span>${scannedCount}/${total}</span>`;
        }
        el.innerHTML = html;
      }

      const toast = showToast;

      function updateFileMeta(){
        if (!fileMeta) return;
        if (!loadedFiles.length) {
          fileMeta.textContent = canUpload ? 'No runsheet loaded.' : 'Awaiting admin upload.';
          return;
        }
        const totalRows = loadedFiles.reduce((sum,file)=>sum+file.rows,0);
        fileMeta.textContent = `${loadedFiles.length} file(s) merged - ${totalRows.toLocaleString()} rows`;
      }

      function updateScheduleMeta(){
        if (!scheduleMeta) return;
        if (!scheduleEntries.length) {
          scheduleMeta.textContent = canUpload ? 'No production schedule loaded.' : 'Awaiting admin upload.';
          return;
        }
        const matched = scheduleEntries.reduce((sum, entry)=> sum + (rowLookup[entry.so]?.length ? 1 : 0), 0);
        let text = `${scheduleEntries.length} production order(s) loaded - ${matched} matched to runsheet.`;
        if (filteredSO){
          const visible = scheduleEntries.filter(entry => entry.so === filteredSO).length;
          text += ` Showing ${visible} for ${filteredSO}.`;
        }
        scheduleMeta.textContent = text;
      }

      function updateScanAvailability(){
        if (!scanEl) return;
        const hasGenerated = Object.values(generated).some(arr => Array.isArray(arr) && arr.length > 0);
        const shouldEnable = hasGenerated;
        const wasDisabled = scanEl.disabled;
        scanEl.disabled = !shouldEnable;
        if (shouldEnable && wasDisabled) focusScan();
      }

      function updateFilterUI(){
        if (!filterClearEl) return;
        filterClearEl.style.display = filteredSO ? '' : 'none';
      }

      function updateSummaryDisplay(){
        if (!summaryEl) return;
        if (!lastScanInfo || (hasScheduleUI && !scheduleEntries.length)){
          summaryEl.style.display = 'none';
          summaryEl.innerHTML = '';
          return;
        }
        const { so, run, drop } = lastScanInfo;
        const runText = run && run !== '-' ? run : '-';
        const dropText = drop && drop !== '-' ? drop : '-';
        const hasRoute = runText !== '-' || dropText !== '-';
        const routeText = hasRoute ? `Run ${runText} / Drop ${dropText}` : 'Not routed';
        summaryEl.style.display = '';
        summaryEl.innerHTML = `
          <strong>Scanned:</strong> <span>${so}</span>&nbsp;&middot;&nbsp;
          <strong>Route:</strong> <span>${routeText}</span>
        `;
      }

      function recalcManifest(){
        const manifest = computeManifestData(tableData);
        generated = manifest.generated;
        rowLookup = manifest.rowLookup;
      }

      function applyScheduleFilter(so){
        if (!hasScheduleUI) return;
        filteredSO = so;
        renderScheduleTable();
        updateScheduleMeta();
        updateScanAvailability();
        updateSummaryDisplay();
      }

      function clearScheduleFilter(){
        if (!hasScheduleUI) return;
        filteredSO = null;
        renderScheduleTable();
        updateScheduleMeta();
        updateScanAvailability();
        focusScan();
        updateSummaryDisplay();
      }

      function save(){
        try{
          const plainScanned = {};
          Object.entries(scanned).forEach(([k,v])=>plainScanned[k]=Array.from(v));
          localStorage.setItem(KEYS.scanned, JSON.stringify(plainScanned));
          if (hasRunsheetUI){
            localStorage.setItem(KEYS.table, JSON.stringify(tableData));
            localStorage.setItem(KEYS.gen, JSON.stringify(generated));
            localStorage.setItem(KEYS.lookup, JSON.stringify(rowLookup));
            localStorage.setItem(KEYS.files, JSON.stringify(loadedFiles));
            if (typeof window !== 'undefined'){
              window.dispatchEvent(new CustomEvent('drm:runsheet-updated', { detail: { prefix } }));
            }
          }
          if (hasScheduleUI){
            localStorage.setItem(KEYS.schedule, JSON.stringify(scheduleEntries));
          }
        }catch{}
      }

      function load(){
        try{
          const storedScanned = localStorage.getItem(KEYS.scanned);
          scanned = {};
          if (storedScanned){
            const plain = JSON.parse(storedScanned) || {};
            Object.entries(plain).forEach(([k,arr])=>scanned[k]=new Set(arr||[]));
          }

          if (hasRunsheetUI){
            const t = localStorage.getItem(KEYS.table);
            const g = localStorage.getItem(KEYS.gen);
            const l = localStorage.getItem(KEYS.lookup);
            const f = localStorage.getItem(KEYS.files);
            if (t){
              tableData = JSON.parse(t) || [];
              generated = g ? JSON.parse(g) || {} : {};
              rowLookup = l ? JSON.parse(l) || {} : {};
              loadedFiles = f ? JSON.parse(f) : [];
              if ((!g || !l) && tableData.length){
                const manifest = computeManifestData(tableData);
                generated = manifest.generated;
                rowLookup = manifest.rowLookup;
              }
              if (tableData.length && tableWrap){
                renderTable();
                Object.keys(rowLookup).forEach(updateRowHighlight);
              } else if (tableWrap){
                tableWrap.innerHTML = '<div class="table-scroll"></div>';
                scanEl.disabled = true;
              }
            } else {
              tableData = []; generated = {}; rowLookup = {}; loadedFiles = [];
              if (tableWrap) tableWrap.innerHTML = '<div class="table-scroll"></div>';
              scanEl.disabled = true;
            }
          } else {
            tableData = []; generated = {}; rowLookup = {}; loadedFiles = [];
            scanEl.disabled = true;
          }

          if (hasScheduleUI){
            const sched=localStorage.getItem(KEYS.schedule);
            scheduleEntries = sched ? JSON.parse(sched) : [];
          }else{
            scheduleEntries = [];
          }

          updateFileMeta();
          refreshSchedule({ fetchRemote: SUPABASE_ENABLED && !hasRunsheetUI });
          updateScanAvailability();
        }catch{
          tableData=[]; generated={}; rowLookup={}; loadedFiles=[];
          if (hasRunsheetUI && tableWrap) tableWrap.innerHTML='<div class="table-scroll"></div>';
          scanEl.disabled = true;
          if (hasScheduleUI) scheduleEntries = [];
          scanned = {};
          updateFileMeta();
          refreshSchedule({ fetchRemote: SUPABASE_ENABLED && !hasRunsheetUI });
          updateScanAvailability();
        }
      }

      function reset(clear=false){
        tableData=[]; generated={}; scanned={}; rowLookup={}; loadedFiles=[];
        if (hasScheduleUI) scheduleEntries=[];
        filteredSO = null;
        lastScanInfo = null;
        if (autoScanTimer){ clearTimeout(autoScanTimer); autoScanTimer=null; }
        if (hasRunsheetUI && tableWrap) tableWrap.innerHTML='<div class="table-scroll"></div>';
        if (hasScheduleUI && scheduleWrap) scheduleWrap.innerHTML='<div class="table-scroll"></div>';
        scanEl.disabled = true;
        updateFilterUI();
        updateSummaryDisplay();
        if(clear){
          if (hasRunsheetUI){
            localStorage.removeItem(KEYS.table);
            localStorage.removeItem(KEYS.gen);
            localStorage.removeItem(KEYS.lookup);
            localStorage.removeItem(KEYS.files);
          }
          if (hasScheduleUI){
            localStorage.removeItem(KEYS.schedule);
          }
          localStorage.removeItem(KEYS.scanned);
        }
        updateFileMeta();
        refreshSchedule({ fetchRemote: false });
        updateScanAvailability();
      }

      function renderTable(){
        if (!hasRunsheetUI || !tableWrap) return;
        const headers = MANIFEST_HEADERS;
        let html = '<div class="table-scroll"><table><thead><tr>';
        headers.forEach(h=> html += `<th>${h}</th>`);
        html += '</tr></thead><tbody>';
        tableData.slice(1).forEach((row, idx)=>{
          html += `<tr id="${prefix}-row-${idx}">`;
          const cells = manifestRowToCells(row);
          cells.forEach(cell=> html += `<td>${cell}</td>`);
          html += '</tr>';
        });
        html += '</tbody></table></div>';
        tableWrap.innerHTML = html;
      }

      function renderScheduleTable(){
        if (!hasScheduleUI || !scheduleWrap) return;
        const headers = MANIFEST_HEADERS;
        const entries = filteredSO ? scheduleEntries.filter(entry => entry.so === filteredSO) : scheduleEntries;
        if (!entries.length){
          const message = filteredSO
            ? `No production orders found for ${filteredSO}.`
            : '';
          const body = message ? `<div style="padding:1rem;text-align:center;">${message}</div>` : '';
          scheduleWrap.innerHTML = `<div class="table-scroll">${body}</div>`;
          updateFilterUI();
          return;
        }
        let html = '<div class="table-scroll"><table><thead><tr>';
        headers.forEach(h=> html += `<th>${h}</th>`);
        html += '</tr></thead><tbody>';
        entries.forEach(entry=>{
          const idxs = rowLookup[entry.so] || [];
          const route = firstRunDrop(entry.so);
          const runText = route.run && route.run !== '-' ? route.run : '-';
          const dropText = route.drop && route.drop !== '-' ? route.drop : '-';
          if (idxs.length){
            idxs.forEach(idx=>{
              const row = tableData[idx+1] || [];
              const cells = manifestRowToCells(row);
              html += '<tr>' + cells.map(cell=>`<td>${cell}</td>`).join('') + '</tr>';
            });
          }else{
            const cells = new Array(headers.length).fill('-');
            cells[0] = runText;
            cells[1] = dropText;
            cells[4] = entry.so || '-';
            cells[5] = entry.createdFrom || '-';
            html += '<tr>' + cells.map(cell=>`<td>${cell}</td>`).join('') + '</tr>';
          }
        });
        html += '</tbody></table></div>';
        scheduleWrap.innerHTML = html;
        updateFilterUI();
      }

      function loadFromLocalStorage(){
        try{
          const t = localStorage.getItem(KEYS.table);
          const g = localStorage.getItem(KEYS.gen);
          const l = localStorage.getItem(KEYS.lookup);
          const f = localStorage.getItem(KEYS.files);
          const sched = localStorage.getItem(KEYS.schedule);
          const scannedRaw = localStorage.getItem(KEYS.scanned);
          if (t) tableData = JSON.parse(t) || tableData;
          if (g) generated = JSON.parse(g) || generated;
          if (l) rowLookup = JSON.parse(l) || rowLookup;
          if (f) loadedFiles = JSON.parse(f) || loadedFiles;
          if (sched){
            const parsed = JSON.parse(sched);
            if (Array.isArray(parsed)) scheduleEntries = parsed;
          }
          if (scannedRaw){
            const plain = JSON.parse(scannedRaw) || {};
            scanned = {};
            Object.entries(plain).forEach(([key, arr])=>{
              scanned[key] = new Set(arr || []);
            });
          }
        }catch{
          // ignore malformed localStorage values
        }
      }

      async function loadInitialData({ fetchRemote = SUPABASE_ENABLED, isInitial = false } = {}){
        tableData = [];
        generated = {};
        rowLookup = {};
        loadedFiles = [];
        scheduleEntries = [];
        filteredSO = null;
        lastScanInfo = null;
        scanned = {};

        // fallback local data first
        loadFromLocalStorage();

        if (fetchRemote && SUPABASE_ENABLED){
          try{
            const kind = prefix === 'final' ? 'final' : 'glue';
            const { data, error } = await supabase
              .from('depot_manifests')
              .select('payload, created_at')
              .eq('depot_id', currentUser?.id || 'unknown')
              .eq('kind', kind)
              .order('created_at', { ascending:false })
              .limit(1)
              .maybeSingle();
            if (error) throw error;
            if (data?.payload){
              const payload = data.payload || {};
              if (Array.isArray(payload.tableData)) tableData = payload.tableData;
              if (Array.isArray(payload.filesMeta)) loadedFiles = payload.filesMeta;
              if (payload.generated) generated = payload.generated;
              if (payload.rowLookup) rowLookup = payload.rowLookup;
              if (Array.isArray(payload.scheduleEntries)) scheduleEntries = payload.scheduleEntries;
              else if (Array.isArray(payload.entries)) scheduleEntries = payload.entries;
            }
          }catch(err){
            console.error('Supabase fetch error', err);
            if (!isInitial){
              showToast('Unable to fetch latest data from server. Using local copy.', 'error');
            }
          }
        }

        if (prefix === 'final' && tableData.length){
          if (!Object.keys(generated).length || !Object.keys(rowLookup).length){
            const manifest = computeManifestData(tableData);
            generated = manifest.generated;
            rowLookup = manifest.rowLookup;
          }
        }

        if (prefix === 'glue' && !scheduleEntries.length && tableData.length){
          scheduleEntries = computeScheduleEntries(tableData);
        }

        if (!hasRunsheetUI){
          try{
            localStorage.setItem(KEYS.table, JSON.stringify(tableData));
            localStorage.setItem(KEYS.gen, JSON.stringify(generated));
            localStorage.setItem(KEYS.lookup, JSON.stringify(rowLookup));
            localStorage.setItem(KEYS.files, JSON.stringify(loadedFiles));
            localStorage.setItem(KEYS.schedule, JSON.stringify(scheduleEntries));
          }catch{
            // ignore storage failures
          }
        }

        scanEl.disabled = (prefix === 'final') && tableData.length === 0;

        updateFileMeta();
        updateScheduleMeta();
        renderTable();
        renderScheduleTable();
        updateScanAvailability();
        updateSummaryDisplay();
      }

      function refreshSchedule(options = {}){
        if (!hasScheduleUI) return;
        const { fetchRemote = false, isInitial = false } = options || {};
        loadInitialData({ fetchRemote, isInitial }).catch(err => console.error(err));
      }

      function updateRowHighlight(so){
        if (!hasRunsheetUI) return;
        const idxs=rowLookup[so];
        if(!idxs) return;
        const tot = generated[so]?.length ?? 0;
        const scn = scanned[so]?.size ?? 0;
        idxs.forEach(i=>{
          const tr = document.getElementById(`${prefix}-row-${i}`);
          if(!tr) return;
          tr.classList.remove('partial','completed');
          if(tot>0 && scn>=tot) tr.classList.add('completed');
          else if(scn>0) tr.classList.add('partial');
        });
      }

      function firstRunDrop(so){
        const idxs=rowLookup[so];
        if(!idxs?.length) return {run:'-',drop:'-'};
        const row = tableData[idxs[0]+1] || [];
        return { run: String(row[0] ?? '-'), drop: String(row[1] ?? '-') };
      }

      function focusScan(){
        if(!scanEl.disabled) requestAnimationFrame(()=>scanEl.focus());
      }

      function shakeInput(){
        scanEl.style.transition='transform 0.08s ease';
        scanEl.style.transform='translateX(0)';
        let i=0;
        const t=setInterval(()=>{
          scanEl.style.transform=`translateX(${i%2===0?'-6px':'6px'})`;
          if(++i>6){clearInterval(t); scanEl.style.transform='translateX(0)';}
        },50);
      }

      async function handleFiles(fileList){
        if (!canUpload || !hasRunsheetUI) return;
        if(!fileList || !fileList.length) return;
        if (!(await ensureXLSX())){
          toast('Excel parser not available. Check your connection and try again.', 'error');
          if (fileEl) fileEl.value='';
          return;
        }
        try{
          const files = Array.from(fileList);
          const results = await Promise.all(files.map(f => readWorkbookFile(f).then(rows => ({ name:f.name, rows }))));
          if (!results.length || !results[0].rows?.length){
            toast('The selected workbook appears to be empty.', 'error');
            return;
          }

          let base = tableData.length ? tableData[0] : results[0].rows[0] || [];
          let merged = [ base ];
          let addedCount = 0;
          let newFilesMeta = [];

          if (tableData.length > 1) {
            merged = merged.concat(tableData.slice(1));
            addedCount += (tableData.length - 1);
          }

          results.forEach(r => {
            const body = (r.rows || []).slice(1);
            if (body.length) {
              merged = merged.concat(body);
              addedCount += body.length;
              newFilesMeta.push({ name: r.name, rows: body.length });
            }
          });

          tableData = merged;
          loadedFiles = (loadedFiles || []).concat(newFilesMeta);
          filteredSO = null;
          lastScanInfo = null;
          updateSummaryDisplay();
          updateFilterUI();

          recalcManifest();
          renderTable();
          scanEl.value = '';
          Object.keys(rowLookup).forEach(updateRowHighlight);
          refreshSchedule({ fetchRemote: false });
          updateFileMeta();
          updateScanAvailability();
          save();
          focusScan();
          toast(`Merged ${newFilesMeta.length} file(s), ${addedCount.toLocaleString()} rows.`, 'success');
        }catch(err){
          console.error(err);
          toast('Unable to read the selected workbook. Please verify the file format.', 'error');
        }finally{
          fileEl.value = '';
        }
      }

      async function handleSchedule(fileList){
        if (!canUpload || !hasScheduleUI) return;
        if(!fileList || !fileList.length) return;
        if (!(await ensureXLSX())){
          toast('Excel parser not available. Check your connection and try again.', 'error');
          if (scheduleFileEl) scheduleFileEl.value='';
          return;
        }
        try{
          const file = fileList[0];
          const rows = await readWorkbookFile(file);
          if(!rows.length){
            toast('The production schedule appears to be empty.', 'error');
            scheduleEntries = [];
            refreshSchedule({ fetchRemote: false });
            save();
            return;
          }
          const headers = (rows[0] || []).map(v => String(v ?? '').trim().toLowerCase());
          const createdIdx = headers.findIndex(h => h === 'created from');
          if (createdIdx === -1){
            toast('Could not find a "Created From" column in the production schedule.', 'error');
            return;
          }
          const seen = new Set();
          const entries = [];
          rows.slice(1).forEach(row=>{
            const raw = row[createdIdx];
            if (raw == null || raw === '') return;
            const so = extractSO(raw);
            if (!so || seen.has(so)) return;
            seen.add(so);
            entries.push({ createdFrom: String(raw), so });
          });
          if (!entries.length){
            toast('No sales orders found in the production schedule.', 'error');
            filteredSO = null;
            scheduleEntries = [];
            refreshSchedule({ fetchRemote: false });
            save();
            return;
          }
          filteredSO = null;
          scheduleEntries = entries;
          lastScanInfo = null;
          updateSummaryDisplay();
          updateFilterUI();
          refreshSchedule({ fetchRemote: false });
          updateScanAvailability();
          save();
          toast(`Loaded ${entries.length} production order(s).`,'success');
        }catch(err){
          console.error(err);
          toast('Unable to read the production schedule.', 'error');
        }finally{
          scheduleFileEl.value = '';
        }
      }

      function handleScan(raw){
        const s = raw ? String(raw).trim() : '';
        if(!s) return false;
        if (s.length < MIN_BARCODE_LENGTH){ toast(`Barcode must be ${MIN_BARCODE_LENGTH} characters.`, 'error'); shakeInput(); return false; }
        const code = s.toUpperCase();
        const so = code.slice(0,-3);
        const known = generated[so];
        if(!known || !known.includes(code)){ toast('Sales Order not found or barcode invalid.','error'); shakeInput(); return false; }
        if(!scanned[so]) scanned[so]=new Set();
        const preventDuplicates = hasRunsheetUI;
        if (preventDuplicates && scanned[so].has(code)){
          toast('This barcode has already been scanned.','info');
          focusScan();
          return false;
        }
        scanned[so].add(code);
        const scannedCount = scanned[so].size;
        const total = known.length;
        const {run, drop} = firstRunDrop(so);
        setStatus({so, run, drop, scannedCount, total});
        if (hasScheduleUI && !hasRunsheetUI) applyScheduleFilter(so);
        updateRowHighlight(so);
        lastScanInfo = { so, run, drop };
        updateSummaryDisplay();
        if (autoScanTimer){ clearTimeout(autoScanTimer); autoScanTimer=null; }
        save();
        focusScan();
        const statusText = hasRunsheetUI
          ? `Marked 1 / ${total} for ${so}`
          : `Run ${run || '-'} / Drop ${drop || '-'} for ${so}`;
        toast(statusText,'success');
        return true;
      }

      scanEl.addEventListener('keydown', (e)=>{
        if(e.key==='Enter'){
          const ok = handleScan(e.target.value);
          if(ok) e.target.value='';
        }
      });

      scanEl.addEventListener('input', ()=>{
        if (scanEl.disabled) return;
        const value = scanEl.value ? scanEl.value.trim() : '';
        if (!value){
          if (autoScanTimer){ clearTimeout(autoScanTimer); autoScanTimer=null; }
          return;
        }
        if (value.length < MIN_BARCODE_LENGTH) return;
        if (autoScanTimer) clearTimeout(autoScanTimer);
        autoScanTimer = setTimeout(()=>{
          autoScanTimer = null;
          const ok = handleScan(value);
          if (ok) scanEl.value='';
        }, AUTOSCAN_DELAY);
      });

      if (canUpload && fileEl) fileEl.addEventListener('change', (e)=>{ handleFiles(e.target.files); });
      if (canUpload && scheduleFileEl) scheduleFileEl.addEventListener('change', (e)=>{ handleSchedule(e.target.files); });
      if (filterClearEl){
        filterClearEl.addEventListener('click', ()=>{ clearScheduleFilter(); });
        filterClearEl.addEventListener('keydown', (event)=>{
          if (event.key === 'Enter' || event.key === ' '){
            event.preventDefault();
            clearScheduleFilter();
          }
        });
      }

      clearEl.addEventListener('click', ()=>{
        if(!confirm('Clear this marking tab?')) return;
        reset(true);
        scanEl.value='';
        if (hasRunsheetUI && fileEl) fileEl.value='';
        if (hasScheduleUI && scheduleFileEl) scheduleFileEl.value='';
        toast('Cleared.','success');
      });

      if (hasScheduleUI && !hasRunsheetUI && typeof window !== 'undefined'){
        window.addEventListener('drm:runsheet-updated', event=>{
          const prefixDetail = event?.detail?.prefix;
          const shouldFetch = SUPABASE_ENABLED && (prefixDetail === 'admin' || prefixDetail === 'glue');
          refreshSchedule({ fetchRemote: shouldFetch });
        });
      }

      exportEl.addEventListener('click', async ()=>{
        if(!tableData.length){
          toast('Nothing to report yet.', 'error');
          return;
        }
        const esc = v => {
          const s = (v??'')==='' ? '-' : String(v);
          return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
        };

        let headers = MANIFEST_HEADERS;
        let dataRows = [];
        if (prefix === 'final'){
          if (tableData.length <= 1){
            toast('No manifest entries yet.', 'info');
            return;
          }
          headers = [...MANIFEST_HEADERS, 'Status'];
          const statusBySo = {};
          Object.keys(generated).forEach(so=>{
            const expected = generated[so]?.length ?? 0;
            const counted = scanned[so]?.size ?? 0;
            statusBySo[so] = expected > 0 && counted >= expected ? 'Complete' : 'Not Complete';
          });
          dataRows = tableData.slice(1).map(row=>{
            const cells = manifestRowToCells(row);
            const so = normSO(row[COL_SO]);
            cells.push(statusBySo[so] || 'Not Complete');
            return cells;
          });
        }else{
          if (tableData.length <= 1){
            toast('No entries in the schedule yet.', 'info');
            return;
          }
          dataRows = tableData.slice(1).map(row => manifestRowToCells(row));
        }

        let csv = headers.join(',') + '\n';
        dataRows.forEach(cells => { csv += cells.slice(0, headers.length).map(esc).join(',') + '\n'; });

        const report = {
          id: `rep_${Date.now()}_${Math.random().toString(16).slice(2,10)}`,
          depotId: currentUser?.id || 'unknown',
          depotName: currentUser?.name || 'Unknown Depot',
          kind: prefix,
          created: new Date().toISOString(),
          rows: dataRows.length,
          filename: `${prefix}_${currentUser?.id || 'unknown'}_${Date.now()}.csv`,
          csv: encodeCSV(csv)
        };
        try{
          await addReport(report);
          toast('Report sent to admin.', 'success');
        }catch(err){
          console.error('Failed to send report', err);
          toast('Failed to send report.', 'error');
        }
      });

      loadInitialData({ fetchRemote: SUPABASE_ENABLED, isInitial: true }).catch(err => console.error(err));

      return { focus: () => { if(!scanEl.disabled) scanEl.focus(); } };
    }

    function AdminModule(){
      const uploadEl = document.getElementById('admin_upload');
      const metaEl   = document.getElementById('admin_meta');
      const pushFinalEl = document.getElementById('admin_push_final');
      const pushGlueEl  = document.getElementById('admin_push_glue');
      const previewWrap = document.getElementById('admin_preview');
      const targetsWrap = document.getElementById('admin_targets');
      const reportsMeta = document.getElementById('admin_reports_meta');
      const reportsTable= document.getElementById('admin_reports_table');
      if (!uploadEl || !metaEl || !pushFinalEl || !pushGlueEl || !previewWrap || !targetsWrap || !reportsMeta || !reportsTable){
        return { focus: () => {} };
      }

      let tableData = [];
      let fileName = '';
      let reportsCache = [];
      let reportsLoading = false;

      function renderPreview(){
        if (!tableData.length){
          previewWrap.innerHTML = '<div class="table-scroll"></div>';
          return;
        }
        let html = '<div class="table-scroll"><table><thead><tr>';
        MANIFEST_HEADERS.forEach(h => html += `<th>${h}</th>`);
        html += '</tr></thead><tbody>';
        tableData.slice(1).forEach(row => {
          const cells = manifestRowToCells(row);
          html += '<tr>' + cells.map(cell => `<td>${cell}</td>`).join('') + '</tr>';
        });
        html += '</tbody></table></div>';
        previewWrap.innerHTML = html;
      }

      function renderTargets(){
        targetsWrap.innerHTML = '';
        USERS.filter(u => u.role === 'depot' || u.role === 'glue').forEach(user=>{
          const label = document.createElement('label');
          const checkbox = document.createElement('input');
          checkbox.type = 'checkbox';
          checkbox.value = user.id;
          checkbox.checked = true;
          const span = document.createElement('span');
          span.textContent = user.name;
          label.appendChild(checkbox);
          label.appendChild(span);
          targetsWrap.appendChild(label);
        });
      }

      function selectedDepotIds(){
        return Array.from(targetsWrap.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
      }

      async function renderReports(){
        reportsLoading = true;
        reportsMeta.textContent = 'Loading reports…';
        reportsTable.innerHTML = '<div class="table-scroll"></div>';
        try{
          const reports = await loadReports();
          const normalized = Array.isArray(reports) ? reports.map(report => {
            const createdValue = report.created || report.created_at || report.createdAt || null;
            const depotId = report.depot_id || report.depotId || 'unknown';
            const depotName = report.depot_name || report.depotName || depotId;
            return {
              id: report.id,
              depotId,
              depotName,
              kind: report.kind,
              rows: report.rows,
              filename: report.filename,
              csv: report.csv,
              created: createdValue
            };
          }) : [];
          reportsCache = normalized;
          if (!normalized.length){
            reportsMeta.textContent = 'No reports submitted.';
            reportsTable.innerHTML = '<div class="table-scroll"></div>';
            return;
          }
          reportsMeta.textContent = `${normalized.length} report(s) awaiting review.`;
          let html = '<div class="table-scroll"><table><thead><tr>';
          html += '<th>Depot</th><th>Type</th><th>Rows</th><th>Submitted</th><th>Actions</th>';
          html += '</tr></thead><tbody>';
          normalized.forEach(report => {
            let submitted = 'Unknown';
            if (report.created){
              const date = new Date(report.created);
              if (!Number.isNaN(date.getTime())){
                submitted = date.toLocaleString();
              }
            }
            const kindLabel = report.kind === 'final' ? 'Final' : 'Glueline';
            html += `<tr data-report-id="${report.id}">` +
                    `<td>${report.depotName || report.depotId}</td>` +
                    `<td>${kindLabel}</td>` +
                    `<td>${report.rows ?? '-'}</td>` +
                    `<td>${submitted}</td>` +
                    '<td>' +
                    `<button type="button" class="report-download" data-report="${report.id}">Download</button>` +
                    `<button type="button" class="report-remove" data-report="${report.id}">Remove</button>` +
                    '</td></tr>';
          });
          html += '</tbody></table></div>';
          reportsTable.innerHTML = html;
        }catch(err){
          console.error('Failed to load reports', err);
          reportsCache = [];
          reportsMeta.textContent = 'Unable to load reports.';
          reportsTable.innerHTML = '<div class="table-scroll"><div style="padding:1rem;text-align:center;">Error loading reports.</div></div>';
        }finally{
          reportsLoading = false;
        }
      }

      async function downloadReportById(id){
        if (!id) return;
        if (reportsLoading){
          showToast('Reports are still loading. Please wait.', 'info');
          return;
        }
        const report = reportsCache.find(r => r.id === id);
        if (!report){
          showToast('Report not found.', 'error');
          return;
        }
        try{
          const csv = decodeCSV(report.csv);
          const blob = new Blob([csv], { type:'text/csv;charset=utf-8' });
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = report.filename || `${report.kind}_report.csv`;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          setTimeout(()=>URL.revokeObjectURL(a.href),0);
        }catch(err){
          console.error('Failed to download report', err);
          showToast('Unable to download report.', 'error');
        }
      }

      async function handleReportAction(event){
        const btn = event.target.closest('button');
        if (!btn) return;
        const id = btn.dataset.report;
        if (!id) return;
        if (btn.classList.contains('report-download')){
          await downloadReportById(id);
        } else if (btn.classList.contains('report-remove')){
          try{
            await removeReport(id);
            showToast('Report removed.', 'info');
          }catch(err){
            console.error('Failed to remove report', err);
            showToast('Unable to remove report.', 'error');
          }
        }
      }

      function updateMeta(){
        if (!tableData.length){
          metaEl.textContent = 'No shared manifest uploaded.';
        } else {
          metaEl.textContent = `${fileName || 'Shared Upload'} - ${Math.max(0, tableData.length - 1)} rows`;
        }
      }

      uploadEl.addEventListener('change', async event => {
        const file = event.target.files?.[0];
        if (!file) return;
        if (!(await ensureXLSX())){
          showToast('Excel parser not available. Check your connection and try again.', 'error');
          uploadEl.value = '';
          return;
        }
        try {
          const rows = await readWorkbookFile(file);
          if (!rows.length){
            showToast('Uploaded workbook is empty.', 'error');
            tableData = [];
            renderPreview();
            updateMeta();
            return;
          }
          tableData = rows;
          fileName = file.name;
          renderPreview();
          updateMeta();
          showToast(`Loaded admin workbook (${rows.length - 1} rows).`, 'success');
        } catch (err) {
          console.error(err);
          showToast('Unable to read admin workbook.', 'error');
        } finally {
          uploadEl.value = '';
        }
      });

      function ensureData(){
        if (!tableData.length){
          showToast('Upload a manifest before pushing.', 'error');
          return false;
        }
        return true;
      }

      function currentFilesMeta(){
        return [{
          name: fileName || 'Shared Upload',
          rows: Math.max(0, tableData.length - 1),
          pushedBy: currentUser?.name || 'Admin',
          pushedAt: new Date().toISOString()
        }];
      }

      async function pushFinal(){
        if (!ensureData()) return;
        const targets = selectedDepotIds();
        if (!targets.length){
          showToast('Select at least one depot.', 'error');
          return;
        }
        const meta = currentFilesMeta();
        const manifest = computeManifestData(tableData);
        pushFinalEl.disabled = true;
        pushGlueEl.disabled = true;
        try{
          await Promise.all(targets.map(userId => {
            return storeFinalDataForUser(userId, tableData, meta, manifest);
          }));
          window.dispatchEvent(new CustomEvent('drm:runsheet-updated', { detail: { prefix: 'admin' } }));
          showToast(`Pushed manifest to ${targets.length} depot(s).`, 'success');
        }catch(err){
          console.error('Failed to push final manifest', err);
          showToast('Failed to push manifest to depot(s).', 'error');
        }finally{
          pushFinalEl.disabled = false;
          pushGlueEl.disabled = false;
        }
      }

      async function pushGlue(){
        if (!ensureData()) return;
        const entries = computeScheduleEntries(tableData);
        if (!entries.length){
          showToast('No sales orders found to push.', 'error');
          return;
        }
        const targets = selectedDepotIds();
        if (!targets.length){
          showToast('Select at least one depot.', 'error');
          return;
        }
        const meta = currentFilesMeta();
        const manifest = computeManifestData(tableData);
        pushGlueEl.disabled = true;
        pushFinalEl.disabled = true;
        try{
          await Promise.all(targets.map(userId => {
            return storeGlueDataForUser(userId, entries, meta, { tableData, manifest });
          }));
          window.dispatchEvent(new CustomEvent('drm:runsheet-updated', { detail: { prefix: 'admin', kind: 'glue' } }));
          showToast(`Pushed schedule to ${targets.length} depot(s).`, 'success');
        }catch(err){
          console.error('Failed to push schedule', err);
          showToast('Failed to push schedule to depot(s).', 'error');
        }finally{
          pushGlueEl.disabled = false;
          pushFinalEl.disabled = false;
        }
      }

      pushFinalEl.addEventListener('click', pushFinal);
      pushGlueEl.addEventListener('click', pushGlue);
      reportsTable.addEventListener('click', event=>{
        handleReportAction(event).catch(err => console.error(err));
      });
      window.addEventListener('drm:reports-updated', ()=>{
        renderReports().catch(err => console.error(err));
      });

      renderTargets();
      renderReports().catch(err => console.error(err));

      return {
        focus: () => uploadEl.focus()
      };
    }

    function startApp(user){
      if (appStarted) return;
      appStarted = true;
      currentUser = user;

      if (logoutBtn){
        logoutBtn.style.display = 'inline-flex';
        logoutBtn.textContent = `Logout (${user.name})`;
        if (!logoutBtn.dataset.bound){
          logoutBtn.addEventListener('click', ()=>{
            localStorage.removeItem(AUTH_KEY);
            location.reload();
          });
          logoutBtn.dataset.bound = 'true';
        }
      }

      const finalModule = MarkingModule('final');
      let glueModule  = null;
      let adminModule   = null;

      const glueTabBtn = document.getElementById('tab-glue');
      if (user.role === 'admin' || user.role === 'glue'){
        if (glueTabBtn) glueTabBtn.style.display = '';
        glueModule = MarkingModule('glue');
      } else if (glueTabBtn){
        glueTabBtn.style.display = 'none';
      }

      const adminTabBtn = document.getElementById('tab-admin');
      if (user.role === 'admin' && adminTabBtn){
        adminTabBtn.style.display = '';
        adminModule = AdminModule();
      } else if (adminTabBtn){
        adminTabBtn.style.display = 'none';
      }

      let activeTab = glueModule ? (user.role === 'glue' ? 'glue' : 'final') : 'final';
      if (adminModule) activeTab = 'admin';

      function activate(which){
        activeTab = which;
        const map = {
          final: { tab:'#tab-final', panel:'#panel-final', focus: finalModule.focus }
        };
        if (glueModule){
          map.glue = { tab:'#tab-glue', panel:'#panel-glue', focus: glueModule.focus };
        }
        if (adminModule){
          map.admin = { tab:'#tab-admin', panel:'#panel-admin', focus: adminModule.focus };
        }
        const finalTabEl = document.getElementById('tab-final');
        const glueTabEl  = document.getElementById('tab-glue');
        const adminTabEl = document.getElementById('tab-admin');
        const finalPanel = document.getElementById('panel-final');
        const gluePanel  = document.getElementById('panel-glue');
        const adminPanel = document.getElementById('panel-admin');
        if (!finalTabEl || !glueTabEl || !finalPanel || !gluePanel) return;
        finalTabEl.setAttribute('aria-selected','false');
        glueTabEl.setAttribute('aria-selected','false');
        finalPanel.classList.remove('active');
        gluePanel.classList.remove('active');
        if (adminTabEl && adminPanel){
          adminTabEl.setAttribute('aria-selected','false');
          adminPanel.classList.remove('active');
        }
        const conf = map[which];
        if (!conf) return;
        document.querySelector(conf.tab)?.setAttribute('aria-selected','true');
        document.querySelector(conf.panel)?.classList.add('active');
        conf.focus();
      }

      document.getElementById('tab-final')?.addEventListener('click', ()=>activate('final'));
      if (glueModule) document.getElementById('tab-glue')?.addEventListener('click',  ()=>activate('glue'));
      if (adminModule && adminTabBtn){
        adminTabBtn.addEventListener('click', ()=>activate('admin'));
      }

      activate(activeTab);

      window.addEventListener('focus', ()=>{
        if (activeTab === 'final') finalModule.focus();
        else if (activeTab === 'glue' && glueModule) glueModule.focus();
        else if (activeTab === 'admin' && adminModule) adminModule.focus();
      });
    }

    setupAuth(startApp);
  }

  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', init, { once: true });
    } else {
      init();
    }
  }
})();











