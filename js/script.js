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

    // Handle Supabase email confirmation links (magic links) on redirect
    // Some environments may be configured to send clickable confirmation links
    // instead of 6‑digit OTP codes. When the user clicks the link, Supabase
    // redirects back with tokens in the URL (e.g. #access_token=…&type=signup).
    // We detect that case, finalize profile creation using any pending
    // registration info we stored, then sign out and return to the login screen.
    async function handleAuthRedirectIfPresent(){
      if (!SUPABASE_ENABLED) return;
      try{
        const hash = (typeof location !== 'undefined') ? (location.hash || '') : '';
        const search = (typeof location !== 'undefined') ? (location.search || '') : '';
        const params = new URLSearchParams(hash && hash.startsWith('#') ? hash.slice(1) : (search || ''));
        const type = params.get('type');
        if (!type) return; // No auth redirect signals

        // If Supabase already established a session from the URL, we can get the user
        const { data } = await supabase.auth.getUser();
        const authUser = data?.user || null;
        if (!authUser) return;

        // Use pending registration details saved before redirect (if present)
        let pending = null;
        try{ pending = JSON.parse(localStorage.getItem('drm_pending_reg') || 'null'); }catch{ pending = null; }
        if (pending && pending.email && (String(pending.email).toLowerCase() === String(authUser.email || '').toLowerCase())){
          try{
            await supabase
              .from('profiles')
              .upsert({ user_id: authUser.id, full_name: pending.fullName || authUser.email || 'User', username: pending.username || (authUser.email ? authUser.email.split('@')[0] : authUser.id), depot_id: pending.depotId || null }, { onConflict: 'user_id' });
          }catch(err){ console.error('Profile upsert (link) failed', err); }
        }

        // Clean the URL of tokens to avoid confusion on reload
        try{ history.replaceState({}, document.title, location.pathname + location.search.replace(/([?&])type=[^&#]*(&|$)/,'$1').replace(/[?&]$/,'')); }catch{}
        try{ if (location.hash) history.replaceState({}, document.title, location.pathname + location.search); }catch{}

        // Notify and sign out to enforce admin approval flow
        try{ alert('Email verified. Your account is pending admin approval.'); }catch{}
        try{ localStorage.removeItem('drm_pending_reg'); }catch{}
        try{ await supabase.auth.signOut(); }catch{}
      }catch(err){
        console.warn('Auth redirect handling error', err);
      }
    }

    // Ensure the admin Supabase status updates immediately on load
    // so it does not remain stuck at the initial "Checking Supabase..." label
    ;(function initialSupabaseStatus(){
      try{
        const el = (typeof document !== 'undefined') ? document.getElementById('admin_supabase_status') : null;
        if (!el) return;
        if (!SUPABASE_ENABLED){
          el.textContent = 'Supabase: Not configured';
          return;
        }
        const now = (typeof performance !== 'undefined' && performance.now) ? ()=>performance.now() : ()=>Date.now();
        const started = now();
        const finish = (ok, msg)=>{
          const ms = Math.max(0, Math.round(now() - started));
          if (ok){
            el.textContent = `Supabase: Connected (${ms} ms)`;
          }else{
            el.textContent = `Supabase: Error - ${msg || 'Unknown error'}`;
          }
        };
        const timeoutMs = 7000;
        let timeoutId = setTimeout(()=>{ try{ finish(false,'timeout'); }finally{ timeoutId = null; } }, timeoutMs);
        const clearT = ()=>{ if (timeoutId){ clearTimeout(timeoutId); timeoutId = null; } };

        // Try a lightweight head query against two known tables
        supabase
          .from('glueline_scans')
          .select('id', { head:true, count:'exact' })
          .limit(1)
          .then(res => {
            if (!res || res.error){
              return supabase
                .from('depot_manifests')
                .select('id', { head:true, count:'exact' })
                .limit(1)
                .then(res2 => {
                  clearT();
                  if (res2 && !res2.error) finish(true);
                  else finish(false, (res2?.error?.message || res?.error?.message || 'Unknown error'));
                });
            }
            clearT();
            finish(true);
          })
          .catch(err => { clearT(); console.error('Supabase connectivity quick check failed', err); finish(false, err?.message || 'Network error'); });
      }catch(err){
        console.error('Supabase connectivity quick check exception', err);
      }
    })();

    // Proactively process any auth redirect in the URL (magic link flow)
    // before showing login overlays, so users who clicked the email link
    // get a clear success path even if OTP is not enabled.
    handleAuthRedirectIfPresent().catch(()=>{});

    const logoutBtn = document.getElementById('auth_logout');

    const MANIFEST_HEADERS = ['Run','Drop','Zone','FP','Type','Sales Order','Name','Address','Suburb','Postcode','CH','FL','Weight','Date'];
    const DISPLAY_INDEX_MAP = [0,1,2,10,14,4,5,6,7,8,11,12,13,3];
    const normSO = v => (v == null ? '' : String(v).trim().toUpperCase());
    const coerceCount = v => {
      if (v == null || v === '') return 0;
      const num = Number(v);
      if (Number.isFinite(num)) return Math.max(0, num);
      const parsed = parseInt(String(v), 10);
      return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
    };

    function manifestRowToCells(row){
      const source = Array.isArray(row) ? row : [];
      return DISPLAY_INDEX_MAP.map(idx=>{
        const value = source[idx];
        return (value == null || value === '') ? '-' : String(value);
      });
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

    function rowHasMeaningfulData(row){
      if (!Array.isArray(row)) return false;
      return row.some(cell=>{
        if (cell == null) return false;
        if (typeof cell === 'number') return !Number.isNaN(cell);
        return String(cell).trim() !== '';
      });
    }

    function sanitizeTableData(table){
      if (!Array.isArray(table) || !table.length) return [];
      const header = Array.isArray(table[0]) ? table[0] : [];
      const body = table.slice(1).filter(rowHasMeaningfulData);
      return [header, ...body];
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

    function escapeHTML(value){
      return String(value ?? '')
        .replace(/&/g,'&amp;')
        .replace(/</g,'&lt;')
        .replace(/>/g,'&gt;')
        .replace(/"/g,'&quot;')
        .replace(/'/g,'&#39;');
    }

    async function storeFinalDataForUser(userId, tableData, filesMeta, manifestData){
      // Merge new data on top of any existing payload for this user,
      // so multiple runs (e.g., Perth + Adelaide) can be worked together.
      const incoming = sanitizeTableData(tableData);
      let mergedTable = incoming;
      let mergedFiles = Array.isArray(filesMeta) ? filesMeta : [];

      if (SUPABASE_ENABLED){
        try{
          const { data, error } = await supabase
            .from('depot_manifests')
            .select('payload, created_at')
            .eq('depot_id', userId)
            .eq('kind', 'final')
            .order('created_at', { ascending:false })
            .limit(1);
          if (!error && Array.isArray(data) && data.length && data[0]?.payload?.tableData){
            const existing = data[0].payload;
            const header = (existing.tableData?.[0]?.length ? existing.tableData[0] : (incoming[0] || []));
            const existingBody = (existing.tableData || []).slice(1).filter(rowHasMeaningfulData);
            const newBody = incoming.slice(1).filter(rowHasMeaningfulData);
            mergedTable = sanitizeTableData([header, ...existingBody, ...newBody]);
            const existingFiles = Array.isArray(existing.filesMeta) ? existing.filesMeta : [];
            mergedFiles = existingFiles.concat(mergedFiles);
          }
        }catch(fetchErr){
          console.error('Merge fetch failed, pushing incoming only', fetchErr);
          mergedTable = incoming;
          mergedFiles = Array.isArray(filesMeta) ? filesMeta : [];
        }

        const manifest = manifestData || computeManifestData(mergedTable);
        const { error: insertError } = await supabase
          .from('depot_manifests')
          .insert({
            depot_id: userId,
            kind: 'final',
            payload: {
              tableData: mergedTable,
              filesMeta: mergedFiles,
              generated: manifest.generated,
              rowLookup: manifest.rowLookup
            },
            uploaded_by: currentUser?.id || 'admin'
          });
        if (insertError) throw insertError;
      } else {
        // Local fallback: merge with any existing cached final for that user.
        const base = suffix => `drm_final_${suffix}`;
        try{
          const existingRaw = localStorage.getItem(base('table_v2'));
          const existingFilesRaw = localStorage.getItem(base('files_meta_v2'));
          if (existingRaw){
            const existingTable = JSON.parse(existingRaw) || [];
            const header = (existingTable?.[0]?.length ? existingTable[0] : (incoming[0] || []));
            const existingBody = (existingTable || []).slice(1).filter(rowHasMeaningfulData);
            const newBody = incoming.slice(1).filter(rowHasMeaningfulData);
            mergedTable = sanitizeTableData([header, ...existingBody, ...newBody]);
            const existingFiles = existingFilesRaw ? (JSON.parse(existingFilesRaw) || []) : [];
            mergedFiles = (existingFiles || []).concat(mergedFiles);
          }
        }catch(parseErr){
          console.error('Local merge failed, replacing with incoming', parseErr);
          mergedTable = incoming;
          mergedFiles = Array.isArray(filesMeta) ? filesMeta : [];
        }

        const manifest = manifestData || computeManifestData(mergedTable);
        localStorage.setItem(base('table_v2'), JSON.stringify(mergedTable));
        localStorage.setItem(base('generated_v2'), JSON.stringify(manifest.generated));
        localStorage.setItem(base('rowlookup_v2'), JSON.stringify(manifest.rowLookup));
        localStorage.setItem(base('files_meta_v2'), JSON.stringify(mergedFiles));
        // Preserve any existing scans; do not reset scanned_v2 here.
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
        const payload = {
          id: report.id,
          depot_id: report.depotId,
          depot_name: report.depotName,
          kind: report.kind,
          rows: report.rows,
          filename: report.filename,
          csv: report.csv
        };
        if (report.created){
          payload.created_at = report.created;
        }
        const { error } = await supabase.from('depot_reports').insert(payload);
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
    function generateClientUuid(){
      if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'){
        return crypto.randomUUID();
      }
      const hex = [];
      for (let i = 0; i < 8; i++){
        hex.push(((Math.random() * 0xffff) | 0).toString(16).padStart(4, '0'));
      }
      return `${hex[0]}${hex[1]}-${hex[2]}-${hex[3]}-${hex[4]}-${hex[5]}${hex[6]}${hex[7]}`;
    }

    const CLIENT_INSTANCE_ID = generateClientUuid();

    const AUTH_KEY = 'drm_auth_user_v1';
    let currentUser = null;
    let appStarted = false;

    // Get password verification utility
    const verifyPassword = window.AuthUtils?.verifyPassword || (async () => {
      throw new Error('AuthUtils not loaded. Ensure auth-utils.js is loaded before script.js');
    });

    async function checkTableExists(){
      if (!SUPABASE_ENABLED) return false;
      try {
        // Try a simple query to see if table exists
        const { error } = await supabase
          .from('depots')
          .select('id')
          .limit(1);
        
        // If we get a "relation does not exist" error, table doesn't exist
        if (error && error.code === 'PGRST116') {
          return false;
        }
        return true;
      } catch {
        return false;
      }
    }

    async function fetchUsersFromDatabase(){
      if (!SUPABASE_ENABLED) {
        console.warn('Supabase not enabled, cannot fetch users from database');
        console.warn('Please check your Supabase configuration in js/config.js');
        return [];
      }
      
      // First check if table exists
      const tableExists = await checkTableExists();
      if (!tableExists) {
        console.error('âŒ Users table does not exist!');
        console.error('ðŸ“‹ To fix this:');
        console.error('   1. Open your Supabase dashboard');
        console.error('   2. Go to SQL Editor');
        console.error('   3. Copy and run the contents of database/setup.sql');
        console.error('   4. Refresh this page');
        return [];
      }
      
      try {
        console.log('Fetching users from Supabase...');
        let data = [];
        // Try to fetch extended columns if schema supports them
        let res = await supabase
          .from('depots')
          .select('id, name, role, depot_id, approved')
          .order('name');
        if (res.error) {
          console.warn('Extended users select failed, falling back to minimal columns', res.error);
          const fallback = await supabase
            .from('depots')
            .select('id, name, role')
            .order('name');
          if (fallback.error) {
            console.error('Error fetching users:', fallback.error);
            return [];
          }
          data = fallback.data || [];
        } else {
          data = res.data || [];
        }

        // Filter to only approved non-depot users when 'approved' present
        try{
          data = (data || []).filter(u => {
            const isDepot = u.role === 'depot';
            const hasApproval = (typeof u.approved === 'boolean') ? u.approved : true;
            return isDepot || hasApproval;
          });
        }catch{}

        console.log(`âœ… Successfully fetched ${data?.length || 0} users from database`);
        if (data.length === 0) {
          console.warn('âš ï¸ No users found in database!');
          console.warn('ðŸ“‹ Run node database/generate_password_hash.js to generate INSERT statements');
          console.warn('ðŸ“‹ Then run those INSERT statements in Supabase SQL Editor');
        }
        return data || [];
      } catch (err) {
        console.error('Exception fetching users:', err);
        return [];
      }
    }

    function passwordMeetsPolicy(pw){
      if (typeof pw !== 'string') return false;
      if (pw.length < 8) return false;
      const hasUpper = /[A-Z]/.test(pw);
      const hasSpecial = /[^A-Za-z0-9]/.test(pw);
      return hasUpper || hasSpecial;
    }

    async function verifyUserPassword(userId, password){
      if (!SUPABASE_ENABLED) {
        console.warn('Supabase not enabled, cannot verify password');
        return false;
      }
      try {
        const { data, error } = await supabase
          .from('users')
          .select('password_hash, salt, iterations')
          .eq('id', userId)
          .single();
        
        if (error || !data) {
          console.error('Error fetching user password data:', error);
          return false;
        }

        // Ensure we're using the correct function from AuthUtils
        if (!window.AuthUtils || !window.AuthUtils.verifyPassword) {
          console.error('AuthUtils.verifyPassword function not available');
          console.error('Make sure auth-utils.js is loaded before script.js');
          return false;
        }

        // Enable debug mode for password verification
        window.DEBUG_PASSWORD_VERIFICATION = true;
        
        console.log('ðŸ” Verifying password for user:', userId);
        console.log('Password entered:', '"' + password + '"', '(length:', password.length + ')');
        console.log('Hash from DB length:', data.password_hash?.length || 0, '(expected: 128 for 64 bytes)');
        console.log('Salt from DB length:', data.salt?.length || 0, '(expected: 64 for 32 bytes)');
        console.log('Salt from DB (first 16 chars):', data.salt?.substring(0, 16) + '...');
        console.log('Iterations:', data.iterations || 100000, '(expected: 100000)');
        console.log('Hash from DB (first 32 chars):', data.password_hash?.substring(0, 32) + '...');
        
        // Validate data before verification
        if (!data.password_hash || data.password_hash.length !== 128) {
          console.error('âŒ Invalid hash length! Expected 128 characters (64 bytes in hex)');
          console.error('Current hash length:', data.password_hash?.length || 0);
          console.error('Please regenerate password hashes using: node database/generate_password_hash.js');
          return false;
        }
        
        if (!data.salt || data.salt.length !== 64) {
          console.error('âŒ Invalid salt length! Expected 64 characters (32 bytes in hex)');
          console.error('Current salt length:', data.salt?.length || 0);
          console.error('Please regenerate password hashes using: node database/generate_password_hash.js');
          return false;
        }
        
        const isValid = await window.AuthUtils.verifyPassword(
          password,
          data.password_hash,
          data.salt,
          data.iterations || 100000
        );

        if (!isValid) {
          console.error('âŒ Password verification failed!');
          console.error('Possible causes:');
          console.error('1. Password in database was hashed with different parameters');
          console.error('2. Salt or hash format mismatch');
          console.error('3. Wrong password entered');
          console.error('');
          console.error('ðŸ“‹ To fix: Regenerate passwords with: node database/generate_password_hash.js');
          console.error('   Then update your database with the new hashes');
        } else {
          console.log('âœ… Password verification successful!');
        }

        return isValid;
      } catch (err) {
        console.error('Exception verifying password:', err);
        return false;
      }
    }

    function setupAuth(onReady){
      let resolved = false;
      let overlay = document.getElementById('auth_overlay');
      let form = document.getElementById('auth_form');
      let emailInput = document.getElementById('auth_email');
      let passInput = document.getElementById('auth_pass');
      let errorEl = document.getElementById('auth_error');
      let loginBtn = document.getElementById('auth_login');
      if (!overlay || !form || !emailInput || !passInput || !errorEl || !loginBtn){
        // Build a minimal, working sign-in overlay dynamically
        try{
          const wrap = document.createElement('div');
          wrap.id = 'auth_overlay';
          wrap.className = 'auth-overlay show';
          wrap.setAttribute('role','dialog');
          wrap.setAttribute('aria-modal','true');
          wrap.setAttribute('aria-hidden','false');
          wrap.innerHTML = (
            '<form class="auth-card" id="auth_form">' +
              '<h2 id="auth_title">Sign in</h2>' +
              '<label>Email<input type="email" id="auth_email" placeholder="you@example.com" autocomplete="email" required></label>' +
              '<label>Password<input type="password" id="auth_pass" placeholder="Enter password" autocomplete="current-password" required></label>' +
              '<label>Depot (select Admin to manage)' +
                '<select id="auth_depot"><option value="" disabled selected>Select depot or Admin…</option><option value="__admin__">Admin</option></select>' +
              '</label>' +
              '<label id="auth_admin_code_row" style="display:none;">Admin code<input type="password" id="auth_admin_code" placeholder="Enter admin code" autocomplete="off"></label>' +
              '<div id="auth_error" class="auth-error" role="alert"></div>' +
              '<button id="auth_login" type="submit">Sign In</button>' +
            '</form>'
          );
          document.body.appendChild(wrap);
          overlay = document.getElementById('auth_overlay');
          form = document.getElementById('auth_form');
          emailInput = document.getElementById('auth_email');
          passInput = document.getElementById('auth_pass');
          errorEl = document.getElementById('auth_error');
          loginBtn = document.getElementById('auth_login');
          document.body.classList.add('auth-locked');
        }catch(err){
          console.error('Unable to build login overlay', err);
          resolved = true;
          onReady({ id:'anonymous', name:'Anonymous User' });
          return;
        }
      }

      // Supabase Auth is used; no user list needed

      function setError(msg){
        errorEl.textContent = msg || '';
        errorEl.style.display = msg ? 'block' : 'none';
      }

      // Email OTP is only used during registration (Supabase email verification); not for login

      async function showOverlay(){
        document.body.classList.add('auth-locked');
        overlay.classList.add('show');
        overlay.setAttribute('aria-hidden','false');
        setError('');
        passInput.value='';
        loginBtn.disabled = false;
        try{ await populateLoginDepots(); }catch{}
        requestAnimationFrame(()=> emailInput.focus());
        if (logoutBtn) logoutBtn.style.display = 'none';
      }

      // Registration flow wiring
      const regOverlay = document.getElementById('reg_overlay');
      const regForm = document.getElementById('reg_form');
      const regDepot = document.getElementById('reg_depot');
      const regUser = document.getElementById('reg_username');
      const regFull = document.getElementById('reg_fullname');
      const regPass = document.getElementById('reg_pass');
      const regConfirm = document.getElementById('reg_confirm');
      const regError = document.getElementById('reg_error');
      const regSubmit = document.getElementById('reg_submit');
      const regCancel = document.getElementById('reg_cancel');
      const regLink = document.getElementById('auth_register_link');

      function setRegError(msg){
        if (!regError) return;
        regError.textContent = msg || '';
        regError.style.display = msg ? 'block' : 'none';
      }

      async function populateDepotsForRegistration(){
        if (!regDepot) return;
        regDepot.innerHTML = '<option value="" disabled selected>Loading depotsâ€¦</option>';
        try{
          const users = await fetchUsersFromDatabase();
          const depots = (users || []).filter(u => u.role === 'depot');
          regDepot.innerHTML = '';
          if (depots.length){
            depots.forEach(d => {
              const opt = document.createElement('option');
              opt.value = d.id;
              opt.textContent = d.name || d.id;
              regDepot.appendChild(opt);
            });
          } else {
            // Fallback to a known static list so registration isn't blocked
            const fallback = [
              { id:'glueline', name:'Glueline' },
              { id:'albury', name:'Albury' },
              { id:'sydney', name:'Sydney' },
              { id:'brisbane', name:'Brisbane' },
              { id:'melbourne', name:'Melbourne' },
              { id:'perth', name:'Perth' }
            ];
            fallback.forEach(d => {
              const opt = document.createElement('option');
              opt.value = d.id;
              opt.textContent = d.name;
              regDepot.appendChild(opt);
            });
          }
        }catch(err){
          console.error('Failed to load depots for registration', err);
          regDepot.innerHTML = '<option value="" disabled selected>Error loading depots</option>';
        }
      }

      function showRegistration(){
        if (!SUPABASE_ENABLED){
          setError('Registration requires server connection.');
          return;
        }
        if (!regOverlay) return;
        setRegError('');
        const regEmailEl = document.getElementById('reg_email');
        if (regEmailEl) regEmailEl.value = '';
        regUser.value = '';
        regFull.value = '';
        regPass.value = '';
        regConfirm.value = '';
        populateDepotsForRegistration();
        regOverlay.classList.add('show');
        regOverlay.setAttribute('aria-hidden','false');
      }
      function hideRegistration(){
        if (!regOverlay) return;
        regOverlay.classList.remove('show');
        regOverlay.setAttribute('aria-hidden','true');
      }
      if (regLink){ regLink.addEventListener('click', showRegistration); }
      if (regCancel){ regCancel.addEventListener('click', hideRegistration); }
      if (regForm){
        // Registration code verification UI
        const regCodeSection = document.getElementById('reg_code_section');
        const regCodeInput = document.getElementById('reg_code');
        const regVerifyBtn = document.getElementById('reg_verify_code');
        const regCodeInfo = document.getElementById('reg_code_info');
        let pendingReg = null; // { email, username, fullName, depotId }

        regForm.addEventListener('submit', async (e)=>{
          e.preventDefault();
          setRegError('');
          if (!SUPABASE_ENABLED){ setRegError('Registration unavailable.'); return; }
          const depotId = regDepot?.value?.trim();
          const email = (document.getElementById('reg_email')?.value || '').trim();
          const username = (regUser?.value || '').trim();
          const fullName = (regFull?.value || '').trim();
          const pw = regPass?.value || '';
          const pc = regConfirm?.value || '';
          if (!depotId){ setRegError('Select a depot.'); return; }
          if (!email){ setRegError('Enter your email.'); return; }
          if (!username){ setRegError('Enter a username.'); return; }
          if (!/^[a-z0-9_\-.]{3,}$/i.test(username)){ setRegError('Username must be 3+ letters/numbers.'); return; }
          if (!fullName){ setRegError('Enter your full name.'); return; }
          if (pw !== pc){ setRegError('Passwords do not match.'); return; }
          if (!passwordMeetsPolicy(pw)){ setRegError('Password does not meet policy.'); return; }
          try{
            regSubmit.disabled = true;
            // Ensure username not taken in profiles
            try{
              const { data: exists } = await supabase.from('profiles').select('username').eq('username', username).limit(1);
              if (Array.isArray(exists) && exists.length){ setRegError('Username already exists.'); regSubmit.disabled = false; return; }
            }catch{}
            // Persist pending registration so magic-link confirmations can complete profile
            try{ localStorage.setItem('drm_pending_reg', JSON.stringify({ email, username, fullName, depotId })); }catch{}
            // Create auth user via Supabase Auth (not confirmed yet)
            const redirectTo = (typeof location !== 'undefined') ? (location.origin + location.pathname) : undefined;
            const { data: signRes, error: signErr } = await supabase.auth.signUp({ email, password: pw, options: { data: { full_name: fullName, username, depot_id: depotId }, emailRedirectTo: redirectTo } });
            if (signErr){ setRegError(signErr.message || 'Failed to create account.'); regSubmit.disabled = false; return; }
            pendingReg = { email, username, fullName, depotId };
            // Send OTP code to email (ensure Auth settings use Email OTP for signup)
            const { error: otpErr } = await supabase.auth.resend({ type: 'signup', email });
            if (otpErr){ setRegError(otpErr.message || 'Failed to send verification code.'); regSubmit.disabled = false; return; }
            if (regCodeSection){ regCodeSection.style.display = ''; }
            if (regCodeInfo){ regCodeInfo.textContent = 'We sent a 6-digit code to your email. Enter it below to verify.'; }
            try{ regCodeInput?.focus(); }catch{}
          }catch(err){
            console.error('Registration error', err);
            setRegError('Unexpected error. Please try again.');
          }finally{
            regSubmit.disabled = false;
          }
        });
        if (regVerifyBtn){
          regVerifyBtn.addEventListener('click', async ()=>{
            setRegError('');
            if (!SUPABASE_ENABLED){ setRegError('Server unavailable.'); return; }
            const token = (regCodeInput?.value || '').trim();
            const email = pendingReg?.email || (document.getElementById('reg_email')?.value || '').trim();
            if (!email){ setRegError('Missing email.'); return; }
            if (!token || !/^[0-9]{6}$/.test(token)){ setRegError('Enter the 6-digit code.'); regCodeInput?.focus(); return; }
            try{
              regVerifyBtn.disabled = true;
              // Verify the email OTP for signup
              const { data, error } = await supabase.auth.verifyOtp({ email, token, type: 'signup' });
              if (error || !data?.user){ setRegError(error?.message || 'Invalid or expired code.'); regVerifyBtn.disabled = false; return; }
              // Upsert profile after verification
              // Persist depot selection from registration
              const depotIdFinal = pendingReg?.depotId || regDepot?.value?.trim() || null;
              const usernameFinal = pendingReg?.username || (regUser?.value || '').trim();
              const fullNameFinal = pendingReg?.fullName || (regFull?.value || '').trim();
              const { error: profErr } = await supabase
                .from('profiles')
                .upsert({ user_id: data.user.id, full_name: fullNameFinal, username: usernameFinal, depot_id: depotIdFinal }, { onConflict: 'user_id' });
              if (profErr){ console.error('Profile upsert failed', profErr); }
              try{ localStorage.removeItem('drm_pending_reg'); }catch{}
              alert('Email verified. Your account is pending admin approval.');
              // Sign out and return to login
              try{ await supabase.auth.signOut(); }catch{}
              if (regCodeSection){ regCodeSection.style.display = 'none'; }
              hideRegistration();
              showOverlay();
            }catch(err){
              console.error('Verify code error', err);
              setRegError('Unable to verify code.');
            }finally{
              regVerifyBtn.disabled = false;
            }
          });
        }
        // Optional: support "Resend code" button if present
        const regResend = document.getElementById('reg_resend_code');
        if (regResend){
          regResend.addEventListener('click', async ()=>{
            setRegError('');
            const email = pendingReg?.email || (document.getElementById('reg_email')?.value || '').trim();
            if (!email){ setRegError('Enter your email first.'); return; }
            try{
              regResend.disabled = true;
              const { error } = await supabase.auth.resend({ type: 'signup', email });
              if (error){ setRegError(error.message || 'Failed to resend code.'); }
              else { setRegError('A new code was sent. Check your email.'); }
            }catch(err){ setRegError('Unable to resend code.'); }
            finally{ regResend.disabled = false; }
          });
        }
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
        // Hide the sign-in overlay immediately on successful auth
        try{ hideOverlay(); }catch{}
    if (currentUser?.role === 'admin'){
      try{
        const savedCtx = localStorage.getItem('drm_admin_depot_context');
        if (savedCtx){ currentUser.depot_id = savedCtx; }
      }catch{}
    }
      
        // âœ… Ensure Admin tab becomes visible if role is admin
        const adminTab = document.getElementById('tab-admin');
        const adminPanel = document.getElementById('panel-admin');
        if (user?.role === 'admin') {
          if (adminTab) adminTab.style.display = 'inline-block';
          if (adminPanel) adminPanel.style.display = 'block';
          // Use plain ASCII to avoid mojibake in some consoles
          console.log('Admin privileges detected - showing admin panel.');
        } else {
          if (adminTab) adminTab.style.display = 'none';
          if (adminPanel) adminPanel.style.display = 'none';
        }
      
        // Do not persist user identity locally; keep user data only in Supabase

        if (!resolved){
          resolved = true;
          onReady(user);
        }
      }

      // Admin-mode toggle
      let depotSelect = document.getElementById('auth_depot');
      let adminCodeRow = document.getElementById('auth_admin_code_row');
      let adminCodeInput = document.getElementById('auth_admin_code');
      // If the HTML does not include the depot/admin selector or admin code row, create them dynamically
      try{
        if (!depotSelect && form && errorEl){
          const label = document.createElement('label');
          label.innerHTML = 'Depot (select Admin to manage)\n<select id="auth_depot"><option value="" disabled selected>Select depot or Admin…</option><option value="__admin__">Admin</option></select>';
          form.insertBefore(label, errorEl);
          depotSelect = label.querySelector('#auth_depot');
        }
        if (!adminCodeRow && form && errorEl){
          const label = document.createElement('label');
          label.id = 'auth_admin_code_row';
          label.style.display = 'none';
          label.innerHTML = 'Admin code\n<input type="password" id="auth_admin_code" placeholder="Enter admin code" autocomplete="off">';
          form.insertBefore(label, errorEl);
          adminCodeRow = label;
          adminCodeInput = label.querySelector('#auth_admin_code');
        }
      }catch{}
      async function populateLoginDepots(){
        if (!depotSelect) return;
        depotSelect.innerHTML = '<option value="" disabled selected>Loading depots…</option>';
        try{
          const { data, error } = await supabase.from('depots').select('id, name, role').order('name');
          let depots = [];
          if (!error && Array.isArray(data)){
            depots = data.filter(d => d.role === 'depot').map(d => ({ id:d.id, name:d.name || d.id }));
          }
          depotSelect.innerHTML = '';
          // Placeholder first so user explicitly chooses
          const placeholder = document.createElement('option');
          placeholder.value = '';
          placeholder.disabled = true;
          placeholder.selected = true;
          placeholder.textContent = 'Select depot or Admin…';
          depotSelect.appendChild(placeholder);
          const adminOpt = document.createElement('option');
          adminOpt.value = '__admin__'; adminOpt.textContent = 'Admin';
          depotSelect.appendChild(adminOpt);
          if (!depots.length){
            depots = [
              { id:'glueline', name:'Glueline' },
              { id:'albury', name:'Albury' },
              { id:'sydney', name:'Sydney' },
              { id:'brisbane', name:'Brisbane' },
              { id:'melbourne', name:'Melbourne' },
              { id:'perth', name:'Perth' }
            ];
          }
          depots.forEach(d=>{ const opt=document.createElement('option'); opt.value=d.id; opt.textContent=d.name; depotSelect.appendChild(opt); });
        }catch{
          depotSelect.innerHTML = '';
          const ph = document.createElement('option'); ph.value=''; ph.disabled=true; ph.selected=true; ph.textContent='Select depot or Admin…'; depotSelect.appendChild(ph);
          const onlyAdmin = document.createElement('option'); onlyAdmin.value='__admin__'; onlyAdmin.textContent='Admin'; depotSelect.appendChild(onlyAdmin);
        }
        const updateAdminCodeVisibility = ()=>{
          const isAdminSel = depotSelect.value === '__admin__';
          if (adminCodeRow){ adminCodeRow.style.display = isAdminSel ? '' : 'none'; }
          if (isAdminSel) requestAnimationFrame(()=> adminCodeInput?.focus());
        };
        depotSelect.addEventListener('change', updateAdminCodeVisibility);
        // Initialize visibility in case Admin is preselected (e.g., only Admin option present)
        updateAdminCodeVisibility();
      }
      populateLoginDepots();

      async function elevateToAdmin(adminCode){
        try{
          const { data, error } = await supabase.rpc('admin_check_and_elevate', { p_code: adminCode });
          if (error){ throw error; }
          return { ok: !!data, error: null };
        }catch(err){
          const msg = err?.message || err?.hint || err?.details || 'RPC failed';
          console.error('Admin elevate failed:', err);
          return { ok:false, error: msg };
        }
      }

      form.addEventListener('submit', async (event)=>{
        event.preventDefault();
        const email = (emailInput.value || '').trim();
        const password = passInput.value.trim();
        if (!email){ setError('Enter your email.'); emailInput.focus(); return; }
        const selectedDepot = depotSelect ? depotSelect.value : '';
        if (!selectedDepot){ setError('Select a depot (or Admin).'); depotSelect?.focus(); return; }
        if (!SUPABASE_ENABLED){ setError('Server unavailable. Check Supabase config.'); return; }
        
        loginBtn.disabled = true;
        setError('Signing in...');

        try {
          const { data: signIn, error: signErr } = await supabase.auth.signInWithPassword({ email, password });
          if (signErr || !signIn?.user){
            setError(signErr?.message || 'Invalid credentials.');
            passInput.value='';
            passInput.focus();
            loginBtn.disabled = false;
            return;
          }
          const authedUser = signIn.user;
          let profile = null;
          try{
            const { data: prof, error: profErr } = await supabase
              .from('profiles')
              .select('full_name, depot_id, role, approved')
              .eq('user_id', authedUser.id)
              .single();
            if (!profErr && prof) profile = prof;
          }catch{}

          // If profile is missing, create a minimal one so login can proceed
          if (!profile){
            try{
              const meta = authedUser.user_metadata || {};
              const full_name = meta.full_name || authedUser.email || 'User';
              const username = meta.username || (authedUser.email ? authedUser.email.split('@')[0] : authedUser.id);
              await supabase
                .from('profiles')
                .upsert({ user_id: authedUser.id, full_name, username, depot_id: null }, { onConflict: 'user_id' });
              const { data: prof2 } = await supabase
                .from('profiles')
                .select('full_name, depot_id, role, approved')
                .eq('user_id', authedUser.id)
                .single();
              if (prof2) profile = prof2;
            }catch(err){
              console.warn('Unable to create default profile; continuing with fallback', err);
            }
          }
          if (!profile){
            // Fallback local profile to avoid blocking sign-in
            profile = { full_name: authedUser.email || 'User', depot_id: null, role: 'user', approved: true };
          }
          let finalRole = profile?.role || 'user';
          if (selectedDepot === '__admin__'){
            const code = (adminCodeInput?.value || '').trim();
            if (!code){ setError('Enter the admin code.'); adminCodeInput?.focus(); loginBtn.disabled = false; return; }
            const result = await elevateToAdmin(code);
            if (!result.ok){ setError(result.error || 'Invalid admin code.'); loginBtn.disabled = false; return; }
            const { data: prof2 } = await supabase
              .from('profiles')
              .select('full_name, depot_id, role, approved')
              .eq('user_id', authedUser.id)
              .single();
            if (prof2){ profile = prof2; finalRole = prof2.role || 'admin'; }
          } else {
            // Act as selected depot for this session
            finalRole = 'depot';
            profile.depot_id = selectedDepot;
            try{ localStorage.setItem('drm_admin_depot_context', selectedDepot); }catch{}
            // Persist depot selection so RLS (current_depot_id) matches
            try{
              await supabase
                .from('profiles')
                .update({ depot_id: selectedDepot })
                .eq('user_id', authedUser.id);
            }catch(e){ console.warn('Unable to persist depot selection', e); }
          }

          if (!profile){
            setError('No profile found. Contact admin.');
            await supabase.auth.signOut();
            loginBtn.disabled = false;
            return;
          }
          if (typeof profile.approved === 'boolean' && !profile.approved){
            setError('Your account is pending approval by an admin.');
            await supabase.auth.signOut();
            passInput.value='';
            passInput.focus();
            loginBtn.disabled = false;
            return;
          }

          const appUser = { id: authedUser.id, name: profile.full_name || authedUser.email, role: finalRole, depot_id: profile.depot_id || null };
          console.log('Signing in as', appUser.role, 'depot_id=', appUser.depot_id || '(none)');
          complete(appUser);
        } catch (err) {
          console.error('Authentication error:', err);
          setError('Authentication error. Please try again.');
          passInput.value='';
          passInput.focus();
          loginBtn.disabled = false;
        } finally {
          // Safety: if still on the login overlay, re-enable button
          const stillVisible = overlay?.classList?.contains('show');
          if (stillVisible && !resolved){ loginBtn.disabled = false; }
        }
      });

      passInput.addEventListener('input', ()=> setError(''));
      emailInput.addEventListener('input', ()=> setError(''));

      // If a Supabase Auth session exists, use it; else show login
      (async ()=>{
        try{
          const { data } = await supabase.auth.getUser();
          const authUser = data?.user || null;
          if (authUser){
            const { data: prof } = await supabase
              .from('profiles')
              .select('full_name, depot_id, role, approved')
              .eq('user_id', authUser.id)
              .single();
            if (prof && (typeof prof.approved !== 'boolean' || prof.approved)){
              complete({ id: authUser.id, name: prof.full_name || authUser.email, role: prof.role || 'user', depot_id: prof.depot_id || null });
              return;
            }
            await supabase.auth.signOut().catch(()=>{});
          }
        }catch{}
        showOverlay();
      })();
    }

    function MarkingModule(prefix){
      const fileEl         = document.getElementById(prefix + '_file');
      const fileMeta       = document.getElementById(prefix + '_file_meta');
      const scheduleFileEl = document.getElementById(prefix + '_schedule_file');
      const scheduleMeta   = document.getElementById(prefix + '_schedule_meta');
      const scanEl         = document.getElementById(prefix + '_scan');
      const clearEl        = document.getElementById(prefix + '_clear');
      const exportEl       = document.getElementById(prefix + '_export');
      const exportTopEl    = document.getElementById(prefix + '_export_top');
      const tableWrap      = document.getElementById(prefix + '_table');
      const scheduleWrap   = document.getElementById(prefix + '_schedule_table');
      const summaryEl      = document.getElementById(prefix + '_scanned_summary');
      const filterClearEl  = document.getElementById(prefix + '_filter_clear');
      const isGlueline     = currentUser?.id === 'glueline';
      const isDepotUser    = (currentUser?.role === 'depot') || !!currentUser?.depot_id;
      const currentDepotId = currentUser?.depot_id || currentUser?.id || 'unknown';
      const remoteScansEnabled = Boolean(SUPABASE_ENABLED && isDepotUser);
      const displayOnly    = (currentUser?.role === 'admin') && !currentUser?.depot_id;
      const isFinalModule  = prefix === 'final';
      const topBar         = document.querySelector('.top-bar');
      const gluelineLogWrap = isGlueline ? document.createElement('div') : null;
      let gluelineLogEntries = [];
      let gluelineLogBody = null;
      let gluelineRealtimeChannel = null;
      const GLUELINE_LOG_LIMIT = 200;
      const SCAN_LOG_TABLE = 'glueline_scans';
      const soLogWrap = (isFinalModule && !isGlueline) ? document.getElementById(prefix + '_so_log') : null;
      const soLogBody = soLogWrap ? soLogWrap.querySelector('.so-log-body') : null;
      const soLogTitle = soLogWrap ? soLogWrap.querySelector('.so-log-title') : null;
      const soLogMeta = soLogWrap ? soLogWrap.querySelector('.so-log-meta') : null;
      const routeDisplayWrap = (isFinalModule && !isGlueline) ? document.getElementById(prefix + '_route_display') : null;
      const routeSoEl = routeDisplayWrap ? routeDisplayWrap.querySelector('.route-value--so') : null;
      const routeRunEl = routeDisplayWrap ? routeDisplayWrap.querySelector('.route-value--run') : null;
      const routeDropEl = routeDisplayWrap ? routeDisplayWrap.querySelector('.route-value--drop') : null;
      const routeStatusEl = routeDisplayWrap ? routeDisplayWrap.querySelector('.route-display-status') : null;
      const runStatusWrap = (isFinalModule && !isGlueline) ? document.getElementById(prefix + '_run_status') : null;
      const runTilesEl = runStatusWrap ? runStatusWrap.querySelector('.run-tiles') : null;
      const runStatusMetaEl = runStatusWrap ? runStatusWrap.querySelector('.run-status-meta') : null;
      let gluelineRealtimeInitialized = false;
      let gluelineRealtimeInitializing = false;
      let gluelineBeforeUnloadBound = false;
      const filtersDisabled = isFinalModule;
      const runFilterEl    = filtersDisabled ? null : document.getElementById(prefix + '_run_filter');
      // Uploads are restricted to the Admin tab only; Final Marking never uploads
      const canUpload      = false;

      const hasRunsheetUI = Boolean(fileEl && fileMeta && tableWrap);
      const hasScheduleUI = Boolean(scheduleFileEl && scheduleMeta && scheduleWrap);

      if (!scanEl || !clearEl || !exportEl) {
        return { focus: () => {} };
      }
      if (!hasRunsheetUI && !hasScheduleUI) {
        return { focus: () => {} };
      }

      if (exportTopEl && exportEl){
        exportTopEl.addEventListener('click', ()=> exportEl.click());
      }

      const baseKey = (suffix)=>{
        // Do not include username in local storage keys
        return `drm_${prefix}_${suffix}`;
      };
      const KEYS = {
        table:   baseKey('table_v2'),
        gen:     baseKey('generated_v2'),
        scanned: baseKey('scanned_v2'),
        lookup:  baseKey('rowlookup_v2'),
        files:   baseKey('files_meta_v2'),
        schedule:baseKey('schedule_v1'),
        notes:   baseKey('notes_v1'),
      };

      let tableData = [];
      let generated = {};
      let scanned   = {};
      let rowLookup = {};
      let statusEl  = null;
      let loadedFiles = [];
      let scheduleEntries = [];
      let filteredSO = null;
      let runFilter = 'all';
      // multi-select runs removed; using single runFilter only
      let lastScanInfo = null;
      let autoScanTimer = null;
      let notes = {};
      let runSOMap = new Map();
      const AUTOSCAN_DELAY = 120;
      const MIN_BARCODE_LENGTH = 11; // e.g., SO252101001
      const AUTO_ENTER_ON_LENGTH = true; // auto-enter when value length matches exactly

      const extractSO = v => {
        const upper = String(v ?? '').toUpperCase();
        const match = upper.match(/SO\d+/);
        return match ? match[0] : '';
      };

      function rebuildRunSOMap(){
        runSOMap = new Map();
        if (!Array.isArray(tableData) || tableData.length <= 1) return;
        tableData.slice(1).forEach(row => {
          const runVal = String(row?.[0] ?? '').trim();
          const soVal = normSO(row?.[COL_SO]);
          if (!runVal || !soVal) return;
          const key = runVal.toUpperCase();
          let set = runSOMap.get(key);
          if (!set){ set = new Set(); runSOMap.set(key, set); }
          set.add(soVal);
        });
      }

      function computeRunProgress(runKey){
        const sos = runSOMap.get(runKey) || new Set();
        let total = 0;
        let scannedCount = 0;
        sos.forEach(so => {
          total += (generated[so]?.length ?? 0);
          scannedCount += (scanned[so]?.size ?? 0);
        });
        return { total, scanned: scannedCount };
      }

      function renderRunStatus(){
        if (!runStatusWrap || !runTilesEl) return;
        const runs = Array.from(runSOMap.keys()).sort((a,b)=> a.localeCompare(b, undefined, { numeric:true, sensitivity:'base' }));
        if (!runs.length){
          runTilesEl.innerHTML = '';
          if (runStatusMetaEl) runStatusMetaEl.textContent = 'No runs loaded.';
          return;
        }
        const activeUpper = (runFilter || 'all').trim().toUpperCase();
        const tiles = [];
        tiles.push(`<div class="run-tile${activeUpper==='ALL' ? ' run-tile--active' : ''}" title="Show all runs" aria-label="Show all runs">All</div>`);
        runs.forEach(run => {
          const { total, scanned } = computeRunProgress(run);
          let cls = 'run-tile';
          if (total > 0 && scanned >= total) cls += ' run-tile--complete';
          else if (scanned > 0) cls += ' run-tile--partial';
          if (activeUpper === String(run).toUpperCase()) cls += ' run-tile--active';
          tiles.push(`<div class="${cls}" title="Run ${escapeHTML(run)}: ${scanned}/${total} scanned" aria-label="Run ${escapeHTML(run)} ${scanned} of ${total} scanned">${escapeHTML(run)}</div>`);
        });
        runTilesEl.innerHTML = tiles.join('');
        // No checkbox enhancement; simple clickable tiles only
        if (runStatusMetaEl) {
          const activeUpper = (runFilter || 'all').trim().toUpperCase();
          const which = activeUpper==='ALL' ? 'Showing all runs' : `Showing run ${activeUpper}`;
          runStatusMetaEl.textContent = `${which}. Green = complete, Yellow = partial`;
        }
        renderRunSummary();
      }

      function computeRunSummary(runUpper){
        const upper = String(runUpper || '').toUpperCase();
        let totalWeight = 0;
        let totalFP = 0;
        const drops = new Set();
        tableData.slice(1).forEach(row => {
          const runVal = String(row?.[0] ?? '').trim().toUpperCase();
          if (!upper || runVal !== upper) return;
          const cells = manifestRowToCells(row);
          const dropVal = cells[1];
          if (dropVal && dropVal !== '-') drops.add(String(dropVal));
          const fpVal = coerceCount(cells[3]);
          totalFP += fpVal;
          const wRaw = String(cells[12] ?? '').trim();
          const wNum = parseFloat(wRaw.replace(/[^0-9.\-]/g, ''));
          if (!Number.isNaN(wNum)) totalWeight += wNum;
        });
        return { weight: totalWeight, drops: drops.size, fp: totalFP };
      }

      function renderRunSummary(){
        if (!runStatusWrap) return;
        let summaryEl = runStatusWrap.querySelector('.run-summary');
        if (!summaryEl){
          summaryEl = document.createElement('div');
          summaryEl.className = 'run-summary';
          if (runTilesEl && runTilesEl.parentElement === runStatusWrap){
            runStatusWrap.insertBefore(summaryEl, runTilesEl);
          } else if (runStatusMetaEl && runStatusMetaEl.parentElement === runStatusWrap){
            runStatusWrap.insertBefore(summaryEl, runStatusMetaEl.nextSibling);
          } else {
            runStatusWrap.appendChild(summaryEl);
          }
        }
        const activeUpper = (runFilter || 'all').trim().toUpperCase();
        if (activeUpper === 'ALL' || tableData.length <= 1){
          summaryEl.style.display = 'none';
          summaryEl.textContent = '';
          return;
        }
        const { weight, drops, fp } = computeRunSummary(activeUpper);
        summaryEl.style.display = '';
        let wText = String(weight);
        try{ wText = Number.isFinite(weight) ? weight.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '0'; }catch{}
        summaryEl.innerHTML = `
          <div class=\"summary-chip summary-chip--weight\"><span class=\"chip-label\">Weight</span><span class=\"chip-value\">${wText}</span></div>
          <div class=\"summary-chip summary-chip--drops\"><span class=\"chip-label\">Drops</span><span class=\"chip-value\">${drops}</span></div>
          <div class=\"summary-chip summary-chip--fp\"><span class=\"chip-label\">FP</span><span class=\"chip-value\">${fp}</span></div>
        `;
      }

      if (runTilesEl && !runTilesEl.dataset.bound){
        runTilesEl.addEventListener('click', (event)=>{
          const tile = event.target.closest('.run-tile');
          if (!tile) return;
          const label = String(tile.textContent || '').trim();
          const upper = label.toUpperCase();
          runFilter = (upper === 'ALL') ? 'all' : label;
          renderTable();
          renderScheduleTable();
          renderRunStatus();
          if (hasRunsheetUI) focusScan();
        });
        runTilesEl.dataset.bound = 'true';
      }

      async function fetchLatestDepotManifest(depotId){
        if (!SUPABASE_ENABLED || !depotId) return null;
        const { data, error } = await supabase
          .from('depot_manifests')
          .select('payload, created_at')
          .eq('depot_id', depotId)
          .eq('kind', 'final')
          .order('created_at', { ascending:false })
          .limit(1);
        if (error) throw error;
        return Array.isArray(data) && data.length ? data[0] : null;
      }

      function hydrateStateFromPayload(payload){
        if (!payload || typeof payload !== 'object') return false;
        let hydrated = false;
        if (Array.isArray(payload.tableData)){
          tableData = payload.tableData;
          hydrated = true;
        }
        if (Array.isArray(payload.filesMeta)) loadedFiles = payload.filesMeta;
        if (payload.generated && typeof payload.generated === 'object') generated = payload.generated;
        if (payload.rowLookup && typeof payload.rowLookup === 'object') rowLookup = payload.rowLookup;
        if (Array.isArray(payload.scheduleEntries)) scheduleEntries = payload.scheduleEntries;
        else if (Array.isArray(payload.entries)) scheduleEntries = payload.entries;
        return hydrated;
      }

      if (filterClearEl){
        filterClearEl.style.display = 'none';
        if (filtersDisabled) filterClearEl.remove();
      }
      if (summaryEl) summaryEl.style.display = 'none';

      if (gluelineLogWrap){
        gluelineLogWrap.className = 'glueline-log';
        gluelineLogWrap.innerHTML = '<div class="glueline-log-title">Scan Log</div><div class="glueline-log-body"><div class="glueline-log-empty">No scans yet.</div></div>';
        gluelineLogBody = gluelineLogWrap.querySelector('.glueline-log-body');
      }

      const parentCard = scanEl.closest('.card');
      if (isGlueline){
        const logoutBtnEl = document.getElementById('auth_logout');
        if (tableWrap) tableWrap.style.display = 'none';
        if (scheduleWrap) scheduleWrap.style.display = 'none';
        if (fileMeta) fileMeta.style.display = 'none';
        if (scheduleMeta) scheduleMeta.style.display = 'none';
        if (fileEl){
          fileEl.disabled = true;
          fileEl.style.display = 'none';
        }
        if (exportEl){
          exportEl.style.display = 'none';
          exportEl.tabIndex = -1;
          exportEl.setAttribute('aria-hidden','true');
        }
        if (exportTopEl){
          exportTopEl.style.display = 'none';
          exportTopEl.tabIndex = -1;
          exportTopEl.setAttribute('aria-hidden','true');
        }
        if (runFilterEl){
          const group = runFilterEl.closest('.run-filter-group');
          if (group) group.style.display = 'none';
        }
        if (clearEl){
          clearEl.style.display = 'none';
          clearEl.setAttribute('aria-hidden','true');
          clearEl.tabIndex = -1;
        }
        let topClearBtn = document.getElementById('glueline_clear_top');
        if (!topClearBtn){
          topClearBtn = document.createElement('button');
          topClearBtn.type = 'button';
          topClearBtn.id = 'glueline_clear_top';
          topClearBtn.className = 'glueline-clear-btn';
          topClearBtn.textContent = 'Clear Final';
          topClearBtn.addEventListener('click', ()=>{ clearEl?.click(); });
        }
        if (topBar){
          if (!topClearBtn.isConnected){
            const topRightActions = document.querySelector('.top-right-actions') || topBar;
            if (logoutBtnEl && logoutBtnEl.parentElement === topRightActions){
              topRightActions.insertBefore(topClearBtn, logoutBtnEl);
            }else{
              topRightActions.appendChild(topClearBtn);
            }
          }
          topClearBtn.style.display = 'inline-flex';
        }
        if (parentCard){
          parentCard.classList.add('glueline-condensed');
          if (gluelineLogWrap && !gluelineLogWrap.isConnected){
            parentCard.appendChild(gluelineLogWrap);
          }
        }
      }else if (displayOnly){
        // Admin viewing Final Marking: show runs only (no scanning, no uploads)
        if (tableWrap) tableWrap.style.display = '';
        // Admin viewing Final Marking: show runs only (no scanning, no uploads)
        if (tableWrap) tableWrap.style.display = '';
        if (scheduleWrap) scheduleWrap.style.display = '';
        if (fileMeta) fileMeta.style.display = '';
        if (scheduleMeta) scheduleMeta.style.display = '';
        if (fileEl){ fileEl.disabled = true; fileEl.style.display = 'none'; }
        if (scheduleFileEl){ scheduleFileEl.disabled = true; scheduleFileEl.style.display = 'none'; }
        if (exportEl){ exportEl.style.display = 'none'; exportEl.tabIndex = -1; exportEl.setAttribute('aria-hidden','true'); }
        if (exportTopEl){ exportTopEl.style.display = 'none'; exportTopEl.tabIndex = -1; exportTopEl.setAttribute('aria-hidden','true'); }
        if (clearEl){ clearEl.style.display = 'none'; clearEl.setAttribute('aria-hidden','true'); clearEl.tabIndex = -1; }
        if (scanEl){ scanEl.disabled = true; scanEl.style.display = 'none'; }
        // Render depot selection UI inside Final Marking for admins
        if (tableWrap){
          const host = tableWrap.querySelector('.table-scroll');
          if (host){
            host.innerHTML = '<div style=\'padding:1rem;\'><label style=\'display:block;margin-bottom:8px;\'>Select depot to view as</label><select id=\'admin_final_depot\' class=\'run-filter\'></select><button id=\'admin_final_depot_apply\' style=\'margin-left:8px;\'>Enter</button><div class=\'file-meta\' style=\'margin-top:8px;\'>Select a depot to view and scan as that depot.</div></div>';
            (async ()=>{
              const sel = document.getElementById('admin_final_depot');
              try{
                let depots = [];
                const { data } = await supabase.from('depots').select('id, name, role').order('name');
                depots = (data || []).map(d => ({ id:d.id, name:d.name || d.id }));
                if (!depots.length){
                  depots = [
                    { id:'glueline', name:'Glueline' },
                    { id:'albury', name:'Albury' },
                    { id:'sydney', name:'Sydney' },
                    { id:'brisbane', name:'Brisbane' },
                    { id:'melbourne', name:'Melbourne' },
                    { id:'perth', name:'Perth' }
                  ];
                }
                depots.forEach(d=>{ const opt=document.createElement('option'); opt.value=d.id; opt.textContent=d.name; sel.appendChild(opt); });
              }catch(err){ console.warn('Unable to load depots for final selection', err); }
              const btn = document.getElementById('admin_final_depot_apply');
              if (btn){ btn.addEventListener('click', ()=>{
                const val = sel.value || '';
                if (!val){ alert('Please select a depot.'); return; }
                try{ localStorage.setItem('drm_admin_depot_context', val); }catch{}
                showToast('Depot selected. Reloading…','info');
                setTimeout(()=> location.reload(), 300);
              }); }
            })();
          }
        }
        if (scheduleWrap) scheduleWrap.style.display = '';
        if (fileMeta) fileMeta.style.display = '';
        if (scheduleMeta) scheduleMeta.style.display = '';
        if (fileEl){ fileEl.disabled = true; fileEl.style.display = 'none'; }
        if (scheduleFileEl){ scheduleFileEl.disabled = true; scheduleFileEl.style.display = 'none'; }
        if (exportEl){ exportEl.style.display = 'none'; exportEl.tabIndex = -1; exportEl.setAttribute('aria-hidden','true'); }
        if (exportTopEl){ exportTopEl.style.display = 'none'; exportTopEl.tabIndex = -1; exportTopEl.setAttribute('aria-hidden','true'); }
        if (clearEl){ clearEl.style.display = 'none'; clearEl.setAttribute('aria-hidden','true'); clearEl.tabIndex = -1; }
        if (scanEl){ scanEl.disabled = true; scanEl.style.display = 'none'; }
      }else{
        if (tableWrap) tableWrap.style.display = '';
        if (scheduleWrap) scheduleWrap.style.display = '';
        if (fileMeta) fileMeta.style.display = '';
        if (scheduleMeta) scheduleMeta.style.display = '';
        if (fileEl) fileEl.style.display = '';
        if (exportEl){
          exportEl.style.display = '';
          exportEl.tabIndex = 0;
          exportEl.removeAttribute('aria-hidden');
        }
        if (exportTopEl){
          exportTopEl.style.display = 'inline-flex';
          exportTopEl.tabIndex = 0;
          exportTopEl.removeAttribute('aria-hidden');
        }
        if (runFilterEl){
          const group = runFilterEl.closest('.run-filter-group');
          if (group) group.style.display = '';
        }
        const topClearBtn = document.getElementById('glueline_clear_top');
        if (topClearBtn){
          topClearBtn.remove();
        }
        if (parentCard){
          parentCard.classList.remove('glueline-condensed');
        }
        if (gluelineLogWrap && gluelineLogWrap.isConnected){
          gluelineLogWrap.remove();
        }
        gluelineLogEntries = [];
        gluelineLogBody = null;
      }

      const clearTopBtn = isFinalModule ? document.getElementById(prefix + '_clear_top') : null;
      if (isFinalModule){
        if (isGlueline){
          if (clearTopBtn) clearTopBtn.style.display = 'none';
        }else{
          if (clearTopBtn){
            clearTopBtn.style.display = 'inline-flex';
            if (!clearTopBtn.dataset.bound){
              clearTopBtn.addEventListener('click', ()=>{ clearEl?.click(); });
              clearTopBtn.dataset.bound = 'true';
            }
          }
          if (clearEl){
            clearEl.style.display = 'none';
            clearEl.setAttribute('aria-hidden','true');
            clearEl.tabIndex = -1;
          }
        }
      }else if (clearTopBtn){
        clearTopBtn.style.display = 'none';
      }

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

      if (runFilterEl){
        runFilterEl.addEventListener('change', event=>{
          const value = event.target.value || 'all';
          runFilter = value === 'all' ? 'all' : value.trim();
          renderTable();
          renderScheduleTable();
          if (hasRunsheetUI) focusScan();
        });
      }

      function ensureStatus(){
        if (statusEl) return statusEl;
        statusEl = document.createElement('div');
        statusEl.className = 'status-card';
        const card = fileEl.closest('.card');
        const controls = card?.querySelector('.controls');
        if (card && controls){
          if (isGlueline){
            controls.appendChild(statusEl);
          }else{
            card.insertBefore(statusEl, controls.nextSibling);
          }
        }
        return statusEl;
      }

      function setStatus({so, run, drop, scannedCount, total, statusMessage}){
        const el = ensureStatus();
        const isGlueline = currentUser?.id === 'glueline';
        el.classList.toggle('status-card--glueline', !!isGlueline);
        const routeExists = (run && run !== '-') || (drop && drop !== '-');
        const runPart = run && run !== '-' ? String(run) : '';
        const dropPart = drop && drop !== '-' ? String(drop) : '';
        const combinedRoute = `${runPart}${dropPart}`;
        const safeSO = escapeHTML(so || '-');
        let routeHTML;
        if (statusMessage){
          routeHTML = `<div class="status-route status-route--missing">${escapeHTML(statusMessage)}</div>`;
        } else if (routeExists){
          routeHTML = `<div class="status-route">${escapeHTML(combinedRoute || '-')}</div>`;
        } else {
          routeHTML = `<div class="status-route status-route--missing">Not routed</div>`;
        }
        const progressHTML = (hasRunsheetUI && !isGlueline)
          ? `<div class="status-meta"><span class="status-label">Progress</span><span class="status-progress">${escapeHTML(String(scannedCount ?? 0))}/${escapeHTML(String(total ?? 0))}</span></div>`
          : '';
        el.innerHTML = `
          <div class="status-row">
            <span class="status-label">Sales Order</span>
            <span class="status-value">${safeSO}</span>
          </div>
          ${routeHTML}
          ${progressHTML}
        `;
        if (isFinalModule && soLogWrap){
          if (statusMessage){
            updateSoLogDisplay(so, { statusMessage });
          } else {
            updateSoLogDisplay(so);
          }
        }
        if (routeDisplayWrap){
          if (statusMessage){
            updateRouteDisplay({ so, run, drop, statusMessage });
          } else {
            updateRouteDisplay({ so, run, drop });
          }
        }
      }

      const toast = showToast;

      function updateGluelineLog(){
        if (!gluelineLogWrap || !gluelineLogBody) return;
        if (!gluelineLogEntries.length){
          gluelineLogBody.innerHTML = '<div class="glueline-log-empty">No scans yet.</div>';
          return;
        }
        const entriesHTML = gluelineLogEntries.map(entry=>{
          const runText = entry.run && entry.run !== '-' ? `Run ${entry.run}` : 'Run -';
          const dropText = entry.drop && entry.drop !== '-' ? `Drop ${entry.drop}` : 'Drop -';
          const timeText = entry.time || '';
          return `
            <div class="glueline-log-entry">
              <div class="glueline-log-row">
                <span class="glueline-log-so">${escapeHTML(entry.so || '-')}</span>
                <span class="glueline-log-time">${escapeHTML(timeText)}</span>
              </div>
              <div class="glueline-log-meta">${escapeHTML(runText)} Â· ${escapeHTML(dropText)}</div>
              <div class="glueline-log-code">${escapeHTML(entry.code || '-')}</div>
            </div>
          `;
        }).join('');
        gluelineLogBody.innerHTML = entriesHTML;
      }

      function recordGluelineScan({ code, so, run, drop, time }, options = {}){
        if (!isGlueline || !gluelineLogWrap) return;
        const silent = options?.silent === true;
        gluelineLogEntries.unshift({
          code,
          so,
          run: run || '-',
          drop: drop || '-',
          time: time || new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        });
        if (gluelineLogEntries.length > GLUELINE_LOG_LIMIT){
          gluelineLogEntries.length = GLUELINE_LOG_LIMIT;
        }
        if (!silent){
          updateGluelineLog();
        }
      }

      function rebuildGluelineLogFromStoredScans(){
        if (!isGlueline || !gluelineLogWrap || !scanned) return;
        const rebuilt = [];
        Object.entries(scanned).forEach(([so, set])=>{
          if (!set || typeof set.forEach !== 'function') return;
          const { run, drop } = firstRunDrop(so);
          Array.from(set).forEach(code=>{
            rebuilt.push({
              code,
              so,
              run: run || '-',
              drop: drop || '-',
              time: '--'
            });
          });
        });
        if (!rebuilt.length) return;
        rebuilt.sort((a, b)=>{
          const soCompare = String(a.so).localeCompare(String(b.so), undefined, { sensitivity:'base', numeric:true });
          if (soCompare !== 0) return soCompare;
          return String(a.code).localeCompare(String(b.code), undefined, { sensitivity:'base', numeric:true });
        });
        gluelineLogEntries = rebuilt;
        updateGluelineLog();
      }

      function formatGluelineLogTime(value, fallback = ''){
        if (!value) return fallback;
        try{
          const date = value instanceof Date ? value : new Date(value);
          if (!Number.isFinite(date?.getTime())) return fallback;
          return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }catch{
          return fallback;
        }
      }

      function addGluelineScannedCode(so, code){
        const normalizedSO = normSO(so);
        const normalizedCode = String(code || '').trim().toUpperCase();
        if (!normalizedSO || !normalizedCode) return { added:false, so: normalizedSO, code: normalizedCode };
        const set = scanned[normalizedSO] || new Set();
        const before = set.size;
        set.add(normalizedCode);
        scanned[normalizedSO] = set;
        if (set.size !== before){
          updateRowHighlight(normalizedSO);
          return { added:true, so: normalizedSO, code: normalizedCode };
        }
        return { added:false, so: normalizedSO, code: normalizedCode };
      }

      function applyGluelineScanUpdate(record, options = {}){
        if (!remoteScansEnabled || !record) return;
        const recordDepot = record.depot_id || record.depotId || record.depot || null;
        if (recordDepot){
          if (recordDepot !== currentDepotId) return;
        } else if (currentDepotId !== 'glueline'){
          return;
        }
        const updateStatus = options.updateStatus !== false;
        const recordLog = options.recordLog !== false;
        const persist = options.persist !== false;
        const deferLogRender = options.deferLogRender === true;
        const so = normSO(record.so);
        const code = String(record.code || '').trim().toUpperCase();
        if (!so || !code) return;
        const route = firstRunDrop(so);
        const run = record.run || route.run || '-';
        const drop = record.drop || route.drop || '-';
        const time = formatGluelineLogTime(record.created_at || record.createdAt || record.created || record.time, record.time);
        const { added } = addGluelineScannedCode(so, code);
        const total = generated[so]?.length ?? 0;
        const scannedCount = scanned[so]?.size ?? 0;
        if (updateStatus){
          setStatus({ so, run, drop, scannedCount, total });
          lastScanInfo = { so, run, drop };
          updateSummaryDisplay();
        }
        if (hasScheduleUI && !hasRunsheetUI){
          applyScheduleFilter(so);
        }
        if (recordLog){
          recordGluelineScan({ code, so, run, drop, time }, { silent: deferLogRender });
        }
        if (persist && added){
          save();
        }
      }

      function ensureGluelineUnloadBinding(){
        if (!remoteScansEnabled || gluelineBeforeUnloadBound) return;
        gluelineBeforeUnloadBound = true;
        window.addEventListener('beforeunload', teardownGluelineRealtime);
      }

      function teardownGluelineRealtime(){
        if (!gluelineRealtimeChannel) return;
        try{
          gluelineRealtimeChannel.unsubscribe();
        }catch(err){
          console.error('Failed to unsubscribe glueline realtime channel', err);
        }
        if (SUPABASE_ENABLED && typeof supabase.removeChannel === 'function'){
          try{
            supabase.removeChannel(gluelineRealtimeChannel);
          }catch{}
        }
        gluelineRealtimeChannel = null;
      }

      function subscribeToGluelineRealtime(){
        if (!remoteScansEnabled) return;
        if (gluelineRealtimeChannel) return;
        const channelName = `public:${SCAN_LOG_TABLE}:${currentDepotId}`;
        const realtimeFilter = currentDepotId ? `depot_id=eq.${currentDepotId}` : undefined;
        const changeConfig = {
          event: 'INSERT',
          schema: 'public',
          table: SCAN_LOG_TABLE
        };
        if (realtimeFilter) changeConfig.filter = realtimeFilter;
        gluelineRealtimeChannel = supabase
          .channel(channelName)
          .on('postgres_changes', changeConfig, payload=>{
            const record = payload?.new;
            if (!record) return;
            if (record.client_id && record.client_id === CLIENT_INSTANCE_ID) return;
            applyGluelineScanUpdate(record);
          });
        gluelineRealtimeChannel.subscribe(status=>{
          if (status === 'CLOSED' || status === 'CHANNEL_ERROR' || status === 'TIMED_OUT'){
            gluelineRealtimeChannel = null;
          }
        });
      }

      function syncGluelineScanRemote(payload){
        if (!remoteScansEnabled) return Promise.resolve();
        const so = normSO(payload?.so);
        const code = String(payload?.code || '').trim().toUpperCase();
        if (!so || !code) return Promise.resolve();
        const run = payload?.run || null;
        const drop = payload?.drop || null;
        return supabase
          .from(SCAN_LOG_TABLE)
          .insert({
            id: generateClientUuid(),
            so,
            code,
            run,
            drop,
            depot_id: currentDepotId,
            client_id: CLIENT_INSTANCE_ID
          })
          .then(({ error })=>{
            if (error) throw error;
          })
          .catch(err=>{
            console.error('Failed to sync scan to Supabase', err);
          });
      }

      async function hydrateGluelineLogFromRemote(){
        if (!remoteScansEnabled) return;
        try{
          let builder = supabase
            .from(SCAN_LOG_TABLE)
            .select('id, so, code, run, drop, depot_id, client_id, created_at');
          if (currentDepotId){
            builder = builder.eq('depot_id', currentDepotId);
          }
          const { data, error } = await builder
            .order('created_at', { ascending:false })
            .limit(GLUELINE_LOG_LIMIT);
          if (error) throw error;
          if (!Array.isArray(data)) return;
          gluelineLogEntries = [];
          data.slice().reverse().forEach(row=>{
            applyGluelineScanUpdate(row, {
              updateStatus: false,
              persist: false,
              deferLogRender: true
            });
          });
          if (gluelineLogEntries.length){
            const latest = gluelineLogEntries[0];
            const latestSO = normSO(latest.so);
            const total = latestSO ? (generated[latestSO]?.length ?? 0) : 0;
            const scannedCount = latestSO ? (scanned[latestSO]?.size ?? 0) : 0;
            setStatus({ so: latestSO, run: latest.run, drop: latest.drop, scannedCount, total });
            lastScanInfo = { so: latestSO, run: latest.run, drop: latest.drop };
          }
          updateGluelineLog();
          updateSummaryDisplay();
          save();
        }catch(err){
          console.error('Failed to load glueline scan log', err);
        }
      }

      async function ensureGluelineRealtime(){
        if (!remoteScansEnabled) return;
        if (gluelineRealtimeInitialized || gluelineRealtimeInitializing) return;
        gluelineRealtimeInitializing = true;
        try{
          await hydrateGluelineLogFromRemote();
        }catch(err){
          console.error('Unable to hydrate glueline log from Supabase', err);
        }finally{
          gluelineRealtimeInitializing = false;
        }
        subscribeToGluelineRealtime();
        ensureGluelineUnloadBinding();
        gluelineRealtimeInitialized = true;
      }

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
        if (displayOnly){ if (scanEl) scanEl.disabled = true; return; }
        if (!scanEl) return;
        const hasGenerated = Object.values(generated).some(arr => Array.isArray(arr) && arr.length > 0);
        const shouldEnable = hasGenerated;
        const wasDisabled = scanEl.disabled;
        scanEl.disabled = !shouldEnable;
        if (shouldEnable && wasDisabled) focusScan();
      }

      function updateFilterUI(){
        if (!filterClearEl || filtersDisabled) return;
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

      function resetSoLogDisplay(message){
        if (!soLogBody) return;
        if (soLogTitle) soLogTitle.textContent = 'Scan Log';
        if (soLogMeta) soLogMeta.textContent = message || 'Scan a barcode to view consignments for that sales order.';
        soLogBody.innerHTML = '<div class="so-log-empty">No sales order selected.</div>';
        if (routeDisplayWrap) resetRouteDisplay(message);
        if (runStatusWrap){
          rebuildRunSOMap();
          renderRunStatus();
        }
      }

      function updateSoLogDisplay(so, { statusMessage } = {}){
        if (!soLogBody) return;
        const normalizedSO = normSO(so);
        if (!normalizedSO){
          resetSoLogDisplay();
          if (routeDisplayWrap) resetRouteDisplay();
          return;
        }
        const consignments = generated[normalizedSO] || [];
        if (soLogTitle) soLogTitle.textContent = `Sales Order ${escapeHTML(normalizedSO)}`;
        if (statusMessage){
          if (soLogMeta) soLogMeta.textContent = statusMessage;
          soLogBody.innerHTML = `<div class="so-log-empty">${escapeHTML(statusMessage)}</div>`;
          return;
        }
        if (!consignments.length){
          if (soLogMeta) soLogMeta.textContent = 'No consignments found for this sales order.';
          soLogBody.innerHTML = '<div class="so-log-empty">No consignments are expected for this sales order.</div>';
          return;
        }
        const scannedSet = scanned[normalizedSO] || new Set();
        const scannedCodes = Array.from(scannedSet);
        const pendingCodes = consignments.filter(code => !scannedSet.has(code));
        if (soLogMeta){
          soLogMeta.textContent = `${scannedCodes.length}/${consignments.length} consignments scanned`;
        }
        const scannedHTML = scannedCodes.length
          ? scannedCodes.map(code => `<span class="log-code log-code--scanned">${escapeHTML(code)}</span>`).join('')
          : '<span class="so-log-empty-note">None yet.</span>';
        const pendingHTML = pendingCodes.length
          ? pendingCodes.map(code => `<span class="log-code log-code--pending">${escapeHTML(code)}</span>`).join('')
          : '<span class="so-log-empty-note">All consignments have been scanned.</span>';
        soLogBody.innerHTML = `
          <div class="so-log-section">
            <div class="so-log-section-title">Scanned (${scannedCodes.length})</div>
            <div class="so-log-code-list">${scannedHTML}</div>
          </div>
          <div class="so-log-section">
            <div class="so-log-section-title">Pending (${pendingCodes.length})</div>
            <div class="so-log-code-list">${pendingHTML}</div>
          </div>
        `;
      }

      function resetRouteDisplay(message){
        if (!routeDisplayWrap) return;
        const statusText = message || 'Awaiting scan.';
        if (routeSoEl) routeSoEl.textContent = '-';
        if (routeRunEl) routeRunEl.textContent = '-';
        if (routeDropEl) routeDropEl.textContent = '-';
        if (routeStatusEl){
          routeStatusEl.textContent = statusText;
          routeStatusEl.classList.remove('route-display-status--success','route-display-status--error');
          routeStatusEl.classList.add('route-display-status--info');
        }
      }

      function updateRouteDisplay({ so, run, drop, statusMessage } = {}){
        if (!routeDisplayWrap) return;
        if (routeSoEl) routeSoEl.textContent = so ? String(so) : '-';
        if (routeRunEl) routeRunEl.textContent = run && run !== '-' ? String(run) : '-';
        if (routeDropEl) routeDropEl.textContent = drop && drop !== '-' ? String(drop) : '-';
        if (routeStatusEl){
          routeStatusEl.classList.remove('route-display-status--info','route-display-status--success','route-display-status--error');
          if (statusMessage){
            routeStatusEl.textContent = statusMessage;
            routeStatusEl.classList.add('route-display-status--error');
          }else{
            routeStatusEl.textContent = 'Route details updated.';
            routeStatusEl.classList.add('route-display-status--success');
          }
        }
      }

      function recalcManifest(){
        const manifest = computeManifestData(tableData);
        generated = manifest.generated;
        rowLookup = manifest.rowLookup;
        rebuildRunSOMap();
        renderRunStatus();
      }

      function applyScheduleFilter(so){
        if (!hasScheduleUI) return;
        filteredSO = filtersDisabled ? null : so;
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
          localStorage.setItem(KEYS.notes, JSON.stringify(notes));
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
          const notesRaw = localStorage.getItem(KEYS.notes);
          notes = notesRaw ? (JSON.parse(notesRaw) || {}) : {};

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
              tableData = sanitizeTableData(tableData);
              pruneNotes();
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
        tableData=[]; generated={}; scanned={}; rowLookup={}; loadedFiles=[]; notes={};
        runFilter = 'all';
        if (hasScheduleUI) scheduleEntries=[];
        filteredSO = null;
        lastScanInfo = null;
        if (autoScanTimer){ clearTimeout(autoScanTimer); autoScanTimer=null; }
        if (hasRunsheetUI && tableWrap) tableWrap.innerHTML='<div class="table-scroll"></div>';
        if (hasScheduleUI && scheduleWrap) scheduleWrap.innerHTML='<div class="table-scroll"></div>';
        scanEl.disabled = true;
        updateFilterUI();
        updateSummaryDisplay();
        if (isFinalModule){
          resetSoLogDisplay();
          resetRouteDisplay();
        }
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
          localStorage.removeItem(KEYS.notes);
        }
        updateFileMeta();
        const shouldFetch = clear && SUPABASE_ENABLED;
        if (hasScheduleUI){
          refreshSchedule({ fetchRemote: shouldFetch, preserveLocal: !clear });
        } else {
          loadInitialData({ fetchRemote: shouldFetch, preserveLocal: !clear }).catch(err => console.error(err));
        }
        updateScanAvailability();
        if (isGlueline){
          gluelineLogEntries = [];
          updateGluelineLog();
        }
        if (runFilterEl){
          runFilterEl.value = 'all';
          runFilterEl.disabled = true;
        }
      }

      function renderTable(){
        if (!hasRunsheetUI || !tableWrap) return;
        pruneNotes();
        const normalizedFilter = runFilter.trim().toUpperCase();
        if (isFinalModule && !isGlueline){
          const headers = ['Run / Drop','Zone','Sales Order','Name','FP','Type','Notes'];
          let html = '<div class="table-scroll"><table><thead><tr>';
          headers.forEach(h=> html += `<th>${escapeHTML(h)}</th>`);
          html += '</tr></thead><tbody>';
          tableData.slice(1).forEach((row, idx)=>{
            if (runFilter !== 'all'){
              const runValue = String(row?.[0] ?? '').trim().toUpperCase();
              if (runValue !== normalizedFilter) return;
            }
            const cells = manifestRowToCells(row);
            const runDropText = `${(cells[0] && cells[0] !== '-') ? cells[0] : '-'} / ${(cells[1] && cells[1] !== '-') ? cells[1] : '-'}`;
            const zoneVal = cells[2] ?? '-';
            const soVal = cells[5] ?? '-';
            const nameVal = cells[6] ?? '-';
            const fpVal = cells[3] ?? '-';
            const typeVal = cells[4] ?? '-';
            html += `<tr id="${prefix}-row-${idx}" data-row-index="${idx}">`;
            html += `<td>${escapeHTML(runDropText)}</td>`;
            html += `<td>${escapeHTML(zoneVal || '-')}</td>`;
            html += `<td>${escapeHTML(soVal || '-')}</td>`;
            html += `<td>${escapeHTML(nameVal || '-')}</td>`;
            html += `<td>${escapeHTML(fpVal || '-')}</td>`;
            html += `<td>${escapeHTML(typeVal || '-')}</td>`;
            html += `<td class="notes-cell">${buildNotesCellContent(idx, hasRunsheetUI)}</td>`;
            html += '</tr>';
          });
          html += '</tbody></table></div>';
          tableWrap.innerHTML = html;
          try{ Object.keys(rowLookup || {}).forEach(updateRowHighlight); }catch{}
          updateRunFilterOptions();
          return;
        }
        const headers = MANIFEST_HEADERS;
        let html = '<div class="table-scroll"><table><thead><tr>';
        headers.forEach(h=> html += `<th>${escapeHTML(h)}</th>`);
        html += '<th>Notes</th></tr></thead><tbody>';
        tableData.slice(1).forEach((row, idx)=>{
          if (runFilter !== 'all'){
            const runValue = String(row?.[0] ?? '').trim().toUpperCase();
            if (runValue !== normalizedFilter) return;
          }
          html += `<tr id="${prefix}-row-${idx}" data-row-index="${idx}">`;
          const cells = manifestRowToCells(row);
          cells.forEach(cell=> html += `<td>${escapeHTML(cell)}</td>`);
          html += `<td class="notes-cell">${buildNotesCellContent(idx, hasRunsheetUI)}</td>`;
          html += '</tr>';
        });
        html += '</tbody></table></div>';
        tableWrap.innerHTML = html;
        try{ Object.keys(rowLookup || {}).forEach(updateRowHighlight); }catch{}
        updateRunFilterOptions();
      }

      if (hasRunsheetUI && tableWrap && !tableWrap.dataset.notesBound){
        tableWrap.addEventListener('click', handleTableClick);
        tableWrap.dataset.notesBound = 'true';
      }

      function renderScheduleTable(){
        if (!hasScheduleUI || !scheduleWrap) return;
        pruneNotes();
        if (!hasRunsheetUI) updateRunFilterOptions();
        const condensed = isFinalModule && !isGlueline;
        const headers = condensed ? ['Run / Drop','Zone','Sales Order','Name','FP','Type','Notes'] : MANIFEST_HEADERS;
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
        headers.forEach(h=> html += `<th>${escapeHTML(h)}</th>`);
        if (!condensed) html += '<th>Notes</th>';
        html += '</tr></thead><tbody>';
        const normalizedFilter = runFilter.trim().toUpperCase();
        entries.forEach(entry=>{
          const idxs = rowLookup[entry.so] || [];
          const route = firstRunDrop(entry.so);
          const runText = route.run && route.run !== '-' ? route.run : '-';
          const dropText = route.drop && route.drop !== '-' ? route.drop : '-';
          if (idxs.length){
            idxs.forEach(idx=>{
              const row = tableData[idx+1] || [];
              if (runFilter !== 'all' && String(row?.[0] ?? '').trim().toUpperCase() !== normalizedFilter) return;
              if (condensed){
                const cells = manifestRowToCells(row);
                const runDropText = `${(cells[0] && cells[0] !== '-') ? cells[0] : '-'} / ${(cells[1] && cells[1] !== '-') ? cells[1] : '-'}`;
                const zoneVal = cells[2] ?? '-';
                const soVal = cells[5] ?? '-';
                const nameVal = cells[6] ?? '-';
                const fpVal = cells[3] ?? '-';
                const typeVal = cells[4] ?? '-';
                html += `<tr><td>${escapeHTML(runDropText)}</td><td>${escapeHTML(zoneVal || '-')}</td><td>${escapeHTML(soVal || '-')}</td><td>${escapeHTML(nameVal || '-')}</td><td>${escapeHTML(fpVal || '-')}</td><td>${escapeHTML(typeVal || '-')}</td><td class="notes-cell">${buildNotesCellContent(idx, false)}</td></tr>`;
              }else{
                const cells = manifestRowToCells(row);
                const noteCell = buildNotesCellContent(idx, false);
                html += '<tr>' + cells.map(cell=>`<td>${escapeHTML(cell)}</td>`).join('') + `<td class="notes-cell">${noteCell}</td></tr>`;
              }
            });
          }else{
            if (runFilter !== 'all' && String(runText ?? '').trim().toUpperCase() !== normalizedFilter) return;
            if (condensed){
              const runDropText = `${runText || '-'} / ${dropText || '-'}`;
              const soVal = entry.so || '-';
              const zoneVal = '-';
              const nameVal = '-';
              const fpVal = '-';
              const typeVal = '-';
              html += `<tr><td>${escapeHTML(runDropText)}</td><td>${escapeHTML(zoneVal)}</td><td>${escapeHTML(soVal)}</td><td>${escapeHTML(nameVal)}</td><td>${escapeHTML(fpVal)}</td><td>${escapeHTML(typeVal)}</td><td class="notes-cell"><span class="note-placeholder">-</span></td></tr>`;
            }else{
              const cells = new Array(headers.length).fill('-');
              cells[0] = runText;
              cells[1] = dropText;
              cells[4] = entry.createdFrom || '-';
              cells[5] = entry.so || '-';
              html += '<tr>' + cells.map(cell=>`<td>${escapeHTML(cell)}</td>`).join('') + '<td class="notes-cell"><span class="note-placeholder">-</span></td></tr>';
            }
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
          const notesRaw = localStorage.getItem(KEYS.notes);
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
          notes = notesRaw ? (JSON.parse(notesRaw) || {}) : {};
        }catch{
          // ignore malformed localStorage values
        }
        tableData = sanitizeTableData(tableData);
        pruneNotes();
      }

      async function loadInitialData({ fetchRemote = SUPABASE_ENABLED, isInitial = false, preserveLocal = true } = {}){
        // Use depot_id when present; fallback to user id
        const depotId = currentUser?.depot_id || currentUser?.id || 'unknown';
        tableData = [];
        generated = {};
        rowLookup = {};
        loadedFiles = [];
        scheduleEntries = [];
        filteredSO = null;
        lastScanInfo = null;
        scanned = {};
        notes = {};

        // fallback local data first
        loadFromLocalStorage();
        if (!hasRunsheetUI){
          try{
            const finalKey = suffix => `drm_final_${suffix}`;
            const finalNotesRaw = localStorage.getItem(finalKey('notes_v1'));
            if (finalNotesRaw) notes = JSON.parse(finalNotesRaw) || notes;
          }catch{}
        }

        const hasLocalManifest = preserveLocal && (
          tableData.length > 1 ||
          Object.values(scanned || {}).some(set => set && typeof set.size === 'number' && set.size > 0)
        );

        if (fetchRemote && SUPABASE_ENABLED){
          try{
            const remote = await fetchLatestDepotManifest(depotId);
            if (remote?.payload && !hasLocalManifest){
              hydrateStateFromPayload(remote.payload);
            } else if (!remote && !tableData.length && !isInitial){
              showToast('No shared manifest available yet.', 'info');
            }
          }catch(err){
            console.error('Supabase fetch error', err);
            if (!isInitial){
              showToast('Unable to fetch latest data from server. Using local copy.', 'error');
            }
          }
        }

        tableData = sanitizeTableData(tableData);
        pruneNotes();

        if (tableData.length){
          const manifest = computeManifestData(tableData);
          generated = manifest.generated;
          rowLookup = manifest.rowLookup;
        }else{
          generated = {};
          rowLookup = {};
        }

        if (isGlueline && gluelineLogEntries.length === 0){
          rebuildGluelineLogFromStoredScans();
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
        if (hasRunsheetUI){
          Object.keys(scanned).forEach(updateRowHighlight);
        }
        renderScheduleTable();
        updateScanAvailability();
        updateSummaryDisplay();
        if (isFinalModule){
          resetSoLogDisplay();
          resetRouteDisplay();
        }
      }

      function refreshSchedule(options = {}){
        if (!hasScheduleUI) return;
        const { fetchRemote = false, isInitial = false, preserveLocal = true } = options || {};
        loadInitialData({ fetchRemote, isInitial, preserveLocal }).catch(err => console.error(err));
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

      function resetScanInput(){
        if (autoScanTimer){
          clearTimeout(autoScanTimer);
          autoScanTimer = null;
        }
        if (scanEl) scanEl.value = '';
      }

      function getRowKey(idx){
        return String(idx);
      }

      function pruneNotes(){
        const maxIdx = Math.max(0, tableData.length - 1);
        Object.keys(notes).forEach(key=>{
          const idx = Number(key);
          if (!Number.isFinite(idx) || idx < 0 || idx >= maxIdx){
            delete notes[key];
          }
        });
      }

      function updateRunFilterOptions(){
        if (!runFilterEl) return;
        const runsMap = new Map();
        tableData.slice(1).forEach(row=>{
          const raw = row?.[0];
          if (raw == null) return;
          const display = String(raw).trim();
          if (!display) return;
          const upper = display.toUpperCase();
          if (!runsMap.has(upper)) runsMap.set(upper, display);
        });

        const previous = runFilter;
        runFilterEl.innerHTML = '';
        const allOption = document.createElement('option');
        allOption.value = 'all';
        allOption.textContent = 'All runs';
        runFilterEl.appendChild(allOption);

        if (runsMap.size){
          const sorted = Array.from(runsMap.values()).sort((a,b)=> a.localeCompare(b, undefined, { numeric:true, sensitivity:'base' }));
          sorted.forEach(runValue=>{
            const option = document.createElement('option');
            option.value = runValue;
            option.textContent = `Run ${runValue}`;
            runFilterEl.appendChild(option);
          });
          const upperPrev = (previous || '').trim().toUpperCase();
          if (upperPrev !== 'ALL' && !runsMap.has(upperPrev)){
            runFilter = 'all';
          }
          runFilterEl.disabled = false;
        } else {
          runFilter = 'all';
          runFilterEl.disabled = true;
        }

        runFilterEl.value = runFilter;
      }

      function buildNotesCellContent(idx, includeActions){
        const key = getRowKey(idx);
        const noteText = notes[key] || '';
        const hasNote = noteText.trim() !== '';
        const display = hasNote
          ? `<span class="note-text">${escapeHTML(noteText)}</span>`
          : '<span class="note-placeholder">No note</span>';
        let html = `<div class="note-display">${display}</div>`;
      if (includeActions){
        const btnLabel = hasNote ? 'Edit Note' : 'Add Note';
        html += '<div class="note-actions">';
        html += `<button type="button" class="note-btn" data-row="${idx}">${btnLabel}</button>`;
        html += `<button type="button" class="manual-btn" data-row="${idx}">Manual Mark</button>`;
        if (isFinalModule && !isGlueline){
          html += `<button type="button" class="details-btn" data-row="${idx}">Details</button>`;
        }
        html += '</div>';
      }
      return html;
    }

      function buildDetailContent(idx){
        const row = tableData[idx + 1];
        if (!row){
          return '<div class="detail-empty">No additional details available.</div>';
        }
        const so = normSO(row[COL_SO]);
        const expectedCodes = generated[so] || [];
        const scannedSet = scanned[so] || new Set();
        const scannedCodes = Array.from(scannedSet);
        scannedCodes.sort((a, b)=> String(a).localeCompare(String(b), undefined, { numeric:true, sensitivity:'base' }));
        const pendingCodes = expectedCodes.filter(code => !scannedSet.has(code));
        pendingCodes.sort((a, b)=> String(a).localeCompare(String(b), undefined, { numeric:true, sensitivity:'base' }));
        const cells = manifestRowToCells(row);
        const phoneValue = row[9] ?? ''; // original column likely phone
        const details = [
          { label: 'Sales Order', value: cells[5] },
          { label: 'Name', value: cells[6] },
          { label: 'Address', value: cells[7] },
          { label: 'Suburb', value: cells[8] },
          { label: 'Postcode', value: cells[9] },
          { label: 'Phone', value: phoneValue },
          { label: 'FP', value: cells[3] },
          { label: 'Type', value: cells[4] },
          { label: 'CH', value: cells[10] },
          { label: 'FL', value: cells[11] },
          { label: 'Weight', value: cells[12] },
          { label: 'Date', value: cells[13] }
        ];
        const items = details.map(detail=>{
          const text = String(detail.value ?? '').trim();
          const display = text ? escapeHTML(text) : '-';
          return `<div class="detail-item"><span class="detail-label">${escapeHTML(detail.label)}</span><span class="detail-value">${display}</span></div>`;
        }).join('');
        const scannedList = scannedCodes.length
          ? scannedCodes.map(code => `<span class="detail-pill detail-pill--scanned">${escapeHTML(code)}</span>`).join('')
          : '<span class="detail-pill detail-pill--empty">No barcodes scanned yet.</span>';
        let pendingList;
        if (!expectedCodes.length){
          pendingList = '<span class="detail-pill detail-pill--empty">No consignments expected.</span>';
        }else if (!pendingCodes.length){
          pendingList = '<span class="detail-pill detail-pill--empty">All consignments scanned.</span>';
        }else{
          pendingList = pendingCodes.map(code => `<span class="detail-pill detail-pill--pending">${escapeHTML(code)}</span>`).join('');
        }
        const scannedSummary = expectedCodes.length
          ? `<span class="detail-summary">${scannedCodes.length}/${expectedCodes.length} scanned</span>`
          : '';
        const pendingSummary = expectedCodes.length && pendingCodes.length
          ? `<span class="detail-summary detail-summary--pending">${pendingCodes.length} remaining</span>`
          : '';
        return `
          <div class="detail-grid">${items}</div>
          <div class="detail-section">
            <div class="detail-section-header">
              <span class="detail-section-title">Scanned Barcodes</span>
              ${scannedSummary}
            </div>
            <div class="detail-list">${scannedList}</div>
          </div>
          <div class="detail-section">
            <div class="detail-section-header">
              <span class="detail-section-title">Pending Barcodes</span>
              ${pendingSummary}
            </div>
            <div class="detail-list">${pendingList}</div>
          </div>
        `;
      }

      function toggleDetailsRow(idx){
        if (!hasRunsheetUI) return;
        const rowEl = document.getElementById(`${prefix}-row-${idx}`);
        if (!rowEl) return;
        const existing = rowEl.nextElementSibling;
        if (existing && existing.classList.contains('details-row')){
          existing.remove();
          rowEl.classList.remove('details-open');
          return;
        }
        const openRows = tableWrap ? tableWrap.querySelectorAll('.details-row') : null;
        if (openRows){
          openRows.forEach(row=>{
            const prev = row.previousElementSibling;
            if (prev) prev.classList.remove('details-open');
            row.remove();
          });
        }
        const detailRow = document.createElement('tr');
        detailRow.className = 'details-row';
        const colSpan = rowEl.children.length || 1;
        detailRow.innerHTML = `<td colspan="${colSpan}">${buildDetailContent(idx)}</td>`;
        rowEl.parentNode.insertBefore(detailRow, rowEl.nextSibling);
        rowEl.classList.add('details-open');
      }

     function updateRowNoteCell(idx){
       const tr = document.getElementById(`${prefix}-row-${idx}`);
       if (!tr) return;
       const cell = tr.querySelector('.notes-cell');
       if (!cell) return;
        cell.innerHTML = buildNotesCellContent(idx, hasRunsheetUI);
      }

      function editNoteForRow(idx){
        const key = getRowKey(idx);
        const existing = notes[key] || '';
        const input = prompt('Enter note for this consignment:', existing);
        if (input === null) return;
        const value = input.trim();
        if (value){
          notes[key] = value;
        }else{
          delete notes[key];
        }
        updateRowNoteCell(idx);
        renderScheduleTable();
        save();
      }

      function manualMarkRow(idx){
        if (!hasRunsheetUI) return;
        const row = tableData[idx + 1];
        if (!row){
          toast('Unable to locate this consignment row.', 'error');
          return;
        }
        const so = normSO(row[COL_SO]);
        if (!so){
          toast('This row does not contain a sales order.', 'error');
          return;
        }
        const expected = generated[so];
        if (!expected?.length){
          toast('No consignments are pending for this sales order.', 'error');
          return;
        }
        if (!scanned[so]) scanned[so] = new Set();
        const nextCode = expected.find(code => !scanned[so].has(code));
        if (!nextCode){
          toast('All consignments already marked for this sales order.', 'info');
          return;
        }
        scanned[so].add(nextCode);
        const scannedCount = scanned[so].size;
        const total = expected.length;
        const { run, drop } = firstRunDrop(so);
        setStatus({ so, run, drop, scannedCount, total });
        updateRowHighlight(so);
        lastScanInfo = { so, run, drop };
        updateSummaryDisplay();
        recordGluelineScan({ code: nextCode, so, run, drop });
        if (remoteScansEnabled){
          syncGluelineScanRemote({ code: nextCode, so, run, drop });
        }
        save();
        focusScan();
        toast('Consignment marked manually.', 'success');
      }

      function handleTableClick(event){
        const noteBtn = event.target.closest('.note-btn');
        if (noteBtn){
          const idx = Number(noteBtn.dataset.row);
          if (Number.isFinite(idx)) editNoteForRow(idx);
          return;
        }
        const manualBtn = event.target.closest('.manual-btn');
        if (manualBtn){
          const idx = Number(manualBtn.dataset.row);
          if (Number.isFinite(idx)) manualMarkRow(idx);
          return;
        }
        const detailsBtn = event.target.closest('.details-btn');
        if (detailsBtn){
          const idx = Number(detailsBtn.dataset.row);
          if (Number.isFinite(idx)) toggleDetailsRow(idx);
          return;
        }
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
          tableData = sanitizeTableData(tableData);
          const previousRows = Math.max(0, tableData.length - 1);
          const files = Array.from(fileList);
          const results = await Promise.all(files.map(f => readWorkbookFile(f).then(rows => ({ name:f.name, rows }))));
          if (!results.length || !results[0].rows?.length){
            toast('The selected workbook appears to be empty.', 'error');
            return;
          }

          let base = tableData.length ? tableData[0] : results[0].rows[0] || [];
          let merged = [ base ];
          let newFilesMeta = [];

          if (tableData.length > 1) {
            merged = merged.concat(tableData.slice(1));
          }

          results.forEach(r => {
            const cleanedBody = ((r.rows || []).slice(1) || []).filter(rowHasMeaningfulData);
            if (cleanedBody.length) {
              merged = merged.concat(cleanedBody);
              newFilesMeta.push({ name: r.name, rows: cleanedBody.length });
            }
          });

          merged = sanitizeTableData(merged);
          tableData = merged;
          pruneNotes();
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
          const totalRows = Math.max(0, tableData.length - 1);
          const addedRows = Math.max(0, totalRows - previousRows);
          toast(`Merged ${newFilesMeta.length} file(s), ${addedRows.toLocaleString()} new row(s).`, 'success');
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
        if(!s){
          resetScanInput();
          return false;
        }
        if (s.length < MIN_BARCODE_LENGTH){
          toast(`Barcode must be ${MIN_BARCODE_LENGTH} characters.`, 'error');
          shakeInput();
          resetScanInput();
          return false;
        }
        const code = s.toUpperCase();
        const so = code.slice(0,-3);
        const known = generated[so];
        if(!known || !known.includes(code)){
          toast('Sales Order not found or barcode invalid.','error');
          setStatus({
            so: so || code,
            run: '-',
            drop: '-',
            scannedCount: 0,
            total: 0,
            statusMessage: 'Consignment not found'
          });
          lastScanInfo = null;
          updateSummaryDisplay();
          shakeInput();
          resetScanInput();
          return false;
        }
        if(!scanned[so]) scanned[so]=new Set();
        const preventDuplicates = hasRunsheetUI;
        if (preventDuplicates && scanned[so].has(code)){
          const scannedCount = scanned[so].size;
          const total = known.length;
          const {run, drop} = firstRunDrop(so);
          setStatus({ so, run, drop, scannedCount, total });
          if (hasScheduleUI && !hasRunsheetUI) applyScheduleFilter(so);
          lastScanInfo = { so, run, drop };
          updateSummaryDisplay();
          toast('This barcode has already been scanned.','info');
          resetScanInput();
          focusScan();
          renderRunStatus();
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
        recordGluelineScan({ code, so, run, drop });
        if (remoteScansEnabled){
          syncGluelineScanRemote({ code, so, run, drop });
        }
        if (autoScanTimer){ clearTimeout(autoScanTimer); autoScanTimer=null; }
        save();
        focusScan();
        const statusText = hasRunsheetUI
          ? `Marked 1 / ${total} for ${so}`
          : `Run ${run || '-'} / Drop ${drop || '-'} for ${so}`;
        toast(statusText,'success');
        resetScanInput();
        renderRunStatus();
        return true;
      }

      scanEl.addEventListener('keydown', (e)=>{
        if(e.key==='Enter'){
          handleScan(e.target.value);
        }
      });

      // Auto-enter: if a complete, valid barcode is present, submit immediately.
      scanEl.addEventListener('input', ()=>{
        if (scanEl.disabled) return;
        const valueRaw = scanEl.value ? scanEl.value.trim() : '';
        if (!valueRaw){
          if (autoScanTimer){ clearTimeout(autoScanTimer); autoScanTimer=null; }
          return;
        }
        const value = valueRaw.toUpperCase();
        if (value.length < MIN_BARCODE_LENGTH){
          // Too short to be a full barcode; wait for more input.
          return;
        }

        // Determine if this is a full, known barcode. If so, handle immediately.
        let isKnownFull = false;
        try{
          const so = value.slice(0, -3);
          const known = generated[so];
          if (Array.isArray(known) && known.includes(value)){
            isKnownFull = true;
          }
        }catch(_){}

        if (autoScanTimer){
          clearTimeout(autoScanTimer);
          autoScanTimer = null;
        }

        if (isKnownFull){
          // Immediate submit when a valid, full barcode is detected.
          handleScan(value);
          return;
        }
        if (AUTO_ENTER_ON_LENGTH && value.length === MIN_BARCODE_LENGTH){
          // Auto-enter purely on reaching the expected length.
          handleScan(value);
          return;
        }

        // Fallback: debounce briefly and then attempt to handle whatever is present.
        autoScanTimer = setTimeout(()=>{
          autoScanTimer = null;
          handleScan(value);
        }, AUTOSCAN_DELAY);
      });

      if (canUpload && fileEl) fileEl.addEventListener('change', (e)=>{ handleFiles(e.target.files); });
      if (canUpload && scheduleFileEl) scheduleFileEl.addEventListener('change', (e)=>{ handleSchedule(e.target.files); });
      if (!filtersDisabled && filterClearEl){
        filterClearEl.addEventListener('click', ()=>{ clearScheduleFilter(); });
        filterClearEl.addEventListener('keydown', (event)=>{
          if (event.key === 'Enter' || event.key === ' '){
            event.preventDefault();
            clearScheduleFilter();
          }
        });
      }

      clearEl.addEventListener('click', async ()=>{
        const promptMsg = 'This will remove ALL runsheets, scans, and notes for this depot. Continue?';
        if(!confirm(promptMsg)) return;

        // Attempt remote cleanup first when possible
        let remoteOk = true;
        if (SUPABASE_ENABLED) {
          try{
            const depotId = currentDepotId;
            // Delete scan logs for this depot
            const { error: scanDelErr } = await supabase
              .from('glueline_scans')
              .delete()
              .eq('depot_id', depotId);
            if (scanDelErr) {
              console.error('Failed clearing remote scans for depot', depotId, scanDelErr);
              remoteOk = false;
            }
            // Delete final manifests for this depot
            const { error: manDelErr } = await supabase
              .from('depot_manifests')
              .delete()
              .eq('depot_id', depotId)
              .eq('kind', 'final');
            if (manDelErr) {
              console.error('Failed clearing remote manifests for depot', depotId, manDelErr);
              remoteOk = false;
            }
          }catch(err){
            console.error('Remote clear failed', err);
            remoteOk = false;
          }
        }

        // Always clear local state
        reset(true);
        scanEl.value='';
        if (hasRunsheetUI && fileEl) fileEl.value='';
        if (hasScheduleUI && scheduleFileEl) scheduleFileEl.value='';
        toast(remoteOk ? 'Depot cleared.' : 'Local cleared. Remote clear may have failed.', remoteOk ? 'success' : 'error');
      });

      if (hasScheduleUI && !hasRunsheetUI && typeof window !== 'undefined'){
        window.addEventListener('drm:runsheet-updated', event=>{
          const prefixDetail = event?.detail?.prefix;
          const shouldFetch = SUPABASE_ENABLED && prefixDetail === 'admin';
          refreshSchedule({ fetchRemote: shouldFetch });
        });
      }

      exportEl.addEventListener('click', async ()=>{
        if(!tableData.length){
          toast('Nothing to report yet.', 'error');
          return;
        }
        if (tableData.length <= 1){
          toast('No manifest entries yet.', 'info');
          return;
        }
        const esc = v => {
          const s = (v??'')==='' ? '-' : String(v);
          return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
        };

        const headers = [...MANIFEST_HEADERS, 'Notes', 'Status'];
        const statusBySo = {};
        Object.keys(generated).forEach(so=>{
          const expected = generated[so]?.length ?? 0;
          const counted = scanned[so]?.size ?? 0;
          statusBySo[so] = expected > 0 && counted >= expected ? 'Complete' : 'Not Complete';
        });
        const dataRows = tableData.slice(1).map((row, idx)=>{
          const cells = manifestRowToCells(row);
          const so = normSO(row[COL_SO]);
          const note = notes[getRowKey(idx)] || '';
          cells.push(note || '-');
          cells.push(statusBySo[so] || 'Not Complete');
          return cells;
        });

        let csv = headers.join(',') + '\n';
        dataRows.forEach(cells => { csv += cells.slice(0, headers.length).map(esc).join(',') + '\n'; });

        const report = {
          id: generateClientUuid(),
          depotId: currentDepotId || 'unknown',
          depotName: currentUser?.name || 'Unknown Depot',
          kind: 'final',
          created: new Date().toISOString(),
          rows: dataRows.length,
          filename: `final_${currentDepotId || 'unknown'}_${Date.now()}.csv`,
          csv: encodeCSV(csv)
        };
        try{
          await addReport(report);
          toast('Report sent to admin.', 'success');
        }catch(err){
          console.error('Failed to send report', err);
          const message = err?.message || err?.error_description || 'Failed to send report.';
          toast(message, 'error');
        }
      });

      loadInitialData({ fetchRemote: SUPABASE_ENABLED, isInitial: true })
        .then(()=>{
          if (remoteScansEnabled){
            ensureGluelineRealtime().catch(err => console.error(err));
          }
        })
        .catch(err => console.error(err));

      return { focus: () => { if(!scanEl.disabled) scanEl.focus(); } };
    }

    function AdminModule(){
      const uploadEl = document.getElementById('admin_upload');
      const metaEl   = document.getElementById('admin_meta');
      const pushFinalEl = document.getElementById('admin_push_final');
      const pushAllEl   = document.getElementById('admin_push_all');
      const clearCacheEl = document.getElementById('admin_clear_cache');
      const previewWrap = document.getElementById('admin_preview');
      const targetsWrap = document.getElementById('admin_targets');
      const reportsMeta = document.getElementById('admin_reports_meta');
      const reportsTable= document.getElementById('admin_reports_table');
      const usersMeta   = document.getElementById('admin_users_meta');
      const usersTable  = document.getElementById('admin_users_table');
      const supaStatus  = document.getElementById('admin_supabase_status');
      const monitorSelect = document.getElementById('admin_monitor_select');
      const manifestMeta = document.getElementById('admin_manifest_meta');
      const monitorLogWrap = document.getElementById('admin_monitor_log');
      let adminScanChannel = null;
      let adminLogEntries = [];
      // Only require the core admin controls; reports section is optional
      if (!uploadEl || !metaEl || !pushFinalEl || !pushAllEl || !previewWrap || !targetsWrap){
        return { focus: () => {} };
      }

      let tableData = [];
      let fileName = '';
      let reportsCache = [];
      let reportsLoading = false;
      const toast = showToast;

      // This page is for admin tasks only; hide controls not needed here
      if (clearCacheEl){
        clearCacheEl.style.display = 'none';
      }

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

      function clearLocalCache(){
        const keysToRemove = [];
        for (let i = 0; i < localStorage.length; i++){
          const key = localStorage.key(i);
          if (key && key.startsWith('drm_')){
            keysToRemove.push(key);
          }
        }
        keysToRemove.forEach(key => localStorage.removeItem(key));
        tableData = [];
        fileName = '';
        reportsCache = [];
        renderPreview();
        updateMeta();
        renderTargets();
        renderReports().catch(err => console.error(err));
      // Admin monitor block removed due to corruption\n      toast('Local cache cleared.', 'success');
      }

      async function fetchDepotsList(){
        if (!SUPABASE_ENABLED) return [];
        try{
          const { data, error } = await supabase
            .from('depots')
            .select('id, name, role')
            .order('name');
          if (error) return [];
          return (data || []).map(r => ({ id: r.id, name: r.name || r.id }));
        }catch{ return []; }
      }

      async function renderTargets(){
        if (!targetsWrap) return;
        targetsWrap.innerHTML = '';
        const wrap = document.createElement('div');
        wrap.className = 'depot-checkbox-list';
        wrap.textContent = 'Loading depots...';
        targetsWrap.appendChild(wrap);
        try{
          let depots = await fetchDepotsList();
          if (!depots.length){
            // fallback to static list if none in DB
            depots = [
              { id:'glueline', name:'Glueline' },
              { id:'albury', name:'Albury' },
              { id:'sydney', name:'Sydney' },
              { id:'brisbane', name:'Brisbane' },
              { id:'melbourne', name:'Melbourne' },
              { id:'perth', name:'Perth' }
            ];
          }
          wrap.innerHTML = '';
          depots.forEach(d => {
            const label = document.createElement('label');
            label.style.display = 'inline-flex';
            label.style.alignItems = 'center';
            label.style.gap = '6px';
            label.style.marginRight = '10px';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.className = 'depot-checkbox';
            cb.value = d.id;
            label.appendChild(cb);
            label.appendChild(document.createTextNode(d.name || d.id));
            wrap.appendChild(label);
          });
        }catch(err){
          console.error('Failed to render depots list', err);
          wrap.textContent = 'Unable to load depots.';
        }
      }

      function selectedDepotIds(){
        const boxes = targetsWrap ? targetsWrap.querySelectorAll('.depot-checkbox:checked') : null;
        if (!boxes || !boxes.length) return [];
        return Array.from(boxes).map(b => b.value);
      }

      async function renderReports(){
        reportsLoading = true;
        reportsMeta.textContent = 'Loading reportsâ€¦';
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
            const kindLabel = 'Final';
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

      async function fetchPendingUsers(){
        if (!SUPABASE_ENABLED) return [];
        try{
          const { data, error } = await supabase
            .from('profiles')
            .select('user_id, username, full_name, depot_id, role, approved')
            .eq('approved', false)
            .order('full_name');
          if (error) return [];
          return data || [];
        }catch(err){
          console.error('Failed to fetch pending users', err);
          return [];
        }
      }

      async function approveUser(userId){
        if (!SUPABASE_ENABLED || !userId) return false;
        try{
          const { error } = await supabase
            .from('profiles')
            .update({ approved: true })
            .eq('user_id', userId);
          if (error){ throw error; }
          return true;
        }catch(err){
          console.error('Approve user failed', err);
          return false;
        }
      }

      async function renderPendingUsers(){
        if (!usersMeta || !usersTable){ return; }
        usersMeta.textContent = 'Loading pending usersâ€¦';
        usersTable.innerHTML = '<div class="table-scroll"></div>';
        try{
          const pending = await fetchPendingUsers();
          if (!pending.length){
            usersMeta.textContent = 'No pending users.';
            usersTable.innerHTML = '<div class="table-scroll"></div>';
            return;
          }
          usersMeta.textContent = `${pending.length} pending user(s).`;
          let html = '<div class="table-scroll"><table><thead><tr>'+
                     '<th>Username</th><th>Full name</th><th>Depot</th><th>Actions</th>'+
                     '</tr></thead><tbody>';
          pending.forEach(u => {
            html += `<tr data-user-id="${u.user_id}">`+
                    `<td>${escapeHTML(u.username || '')}</td>`+
                    `<td>${escapeHTML(u.full_name || '')}</td>`+
                    `<td>${escapeHTML(u.depot_id || '')}</td>`+
                    `<td><button type="button" class="approve-user" data-user="${u.user_id}">Approve</button></td>`+
                    `</tr>`;
          });
          html += '</tbody></table></div>';
          usersTable.innerHTML = html;
        }catch(err){
          console.error('Render pending users failed', err);
          usersMeta.textContent = 'Unable to load pending users.';
          usersTable.innerHTML = '<div class="table-scroll"><div style="padding:1rem;text-align:center;">Error loading users.</div></div>';
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
          tableData = sanitizeTableData(rows);
          fileName = file.name;
          renderPreview();
          updateMeta();
          const rowCount = Math.max(0, tableData.length - 1);
          showToast(`Loaded admin workbook (${rowCount} rows).`, 'success');
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

      async function pushFinal(targetsOverride=null){
        if (!ensureData()) return;
        const targets = Array.isArray(targetsOverride) ? targetsOverride : selectedDepotIds();
        if (!targets.length){
          showToast('Select a depot first.', 'error');
          return;
        }
        const meta = currentFilesMeta();
        const manifest = computeManifestData(tableData);
        pushFinalEl.disabled = true;
        pushAllEl.disabled = true;
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
          pushAllEl.disabled = false;
        }
      }

      pushFinalEl.addEventListener('click', pushFinal);
      pushAllEl.addEventListener('click', async ()=>{
        try{
          const list = await fetchDepotsList();
          let allDepots = (list || []).map(d => d.id);
          if (!allDepots.length){
            showToast(SUPABASE_ENABLED ? 'No depots configured.' : 'No depots (Supabase not configured).', 'error');
            return;
          }
          pushFinal(allDepots);
        }catch(err){
          console.error('Unable to determine depots for push-all', err);
          showToast('Failed to load depots list.', 'error');
        }
      });
      // Clear Local Cache button is not needed on Admin page
      reportsTable.addEventListener('click', event=>{
        handleReportAction(event).catch(err => console.error(err));
      });
      if (usersTable){
        usersTable.addEventListener('click', async (event)=>{
          const btn = event.target.closest('button.approve-user');
          if (!btn) return;
          const id = btn.dataset.user;
          if (!id) return;
          btn.disabled = true;
          const ok = await approveUser(id);
          if (ok){
            showToast(`Approved ${id}.`, 'success');
            renderPendingUsers().catch(err => console.error(err));
          } else {
            showToast('Failed to approve user.', 'error');
          }
          btn.disabled = false;
        });
      }
      window.addEventListener('drm:reports-updated', ()=>{
        renderReports().catch(err => console.error(err));
      });

      renderTargets();
      renderReports().catch(err => console.error(err));
      renderPendingUsers().catch(err => console.error(err));

      async function checkSupabaseConnectivity(){
        if (!supaStatus) return;
        if (!SUPABASE_ENABLED){
          supaStatus.textContent = 'Supabase: Not configured';
          return;
        }
        const started = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
        try{
          let ok = false; let msg = '';
          let res = await supabase.from('glueline_scans').select('id', { head:true, count:'exact' }).limit(1);
          if (res && !res.error){ ok = true; }
          else {
            res = await supabase.from('depot_manifests').select('id', { head:true, count:'exact' }).limit(1);
            if (res && !res.error){ ok = true; }
            else { msg = res?.error?.message || 'Unknown error'; }
          }
          const ended = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
          const ms = Math.max(0, Math.round((ended - started)));
          if (ok){
            supaStatus.textContent = `Supabase: Connected (${ms} ms)`;
          } else {
            supaStatus.textContent = `Supabase: Error â€” ${msg}`;
          }
        }catch(err){
          const ended = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
          const ms = Math.max(0, Math.round((ended - started)));
          supaStatus.textContent = `Supabase: Error (${ms} ms)`;
          console.error('Supabase connectivity check failed', err);
        }
      }

      checkSupabaseConnectivity().catch(err => console.error(err));

      return {
        focus: () => uploadEl.focus()
      };
    }

    function startApp(user){
      if (appStarted) return;
    appStarted = true;
    currentUser = user;

    if (typeof document !== 'undefined'){
      document.body.classList.toggle('glueline-mode', user.id === 'glueline');
    }

      const headerTitle = document.querySelector('header h1');
      if (headerTitle){
        headerTitle.textContent = user.id === 'glueline' ? 'Glueline Marking' : 'Delivery Run Manager';
      }
      const topBar = document.querySelector('.top-bar');
      if (topBar && user.id !== 'glueline'){
        const gluelineClear = topBar.querySelector('.glueline-clear-btn');
        if (gluelineClear){
          gluelineClear.remove();
        }
      }

      if (logoutBtn){
        logoutBtn.style.display = 'inline-flex';
        logoutBtn.textContent = `Logout (${user.name})`;
        if (!logoutBtn.dataset.bound){
          logoutBtn.addEventListener('click', async ()=>{ try{ await supabase?.auth?.signOut?.(); }catch{}; location.reload(); });
          logoutBtn.dataset.bound = 'true';
        }
      }
      const finalTabEl = document.getElementById('tab-final');
      const finalPanelEl = document.getElementById('panel-final');
      const adminTabBtn = document.getElementById('tab-admin');
      const adminPanelEl = document.getElementById('panel-admin');
      // Safety: ensure overlay is gone and panels are not force-hidden
      try{
        document.body.classList.remove('auth-locked');
        const ov = document.getElementById('auth_overlay');
        if (ov){ ov.classList.remove('show'); ov.setAttribute('aria-hidden','true'); }
        if (finalPanelEl) finalPanelEl.style.display = finalPanelEl.style.display || '';
        if (adminPanelEl) adminPanelEl.style.display = adminPanelEl.style.display || '';
      }catch{}
      const isAdmin = user.role === 'admin';
      let finalModule = null;
      let adminModule = null;

      // Admin users: Admin tab only (no Final Marking)
      // Depot users: Final Marking only (no Admin tab)
      if (isAdmin){
        // Hide Final Marking tab/panel for admins
        if (finalTabEl) finalTabEl.style.display = 'none';
        if (finalPanelEl) finalPanelEl.style.display = 'none';
        // Show and initialize Admin
        if (adminTabBtn) adminTabBtn.style.display = '';
        adminModule = AdminModule();
      } else {
        // Initialize Final Marking for depot users
        finalModule = MarkingModule('final');
        if (finalTabEl) finalTabEl.style.display = '';
        // Hide Admin tab for non-admins
        if (adminTabBtn) adminTabBtn.style.display = 'none';
        adminPanelEl?.classList.remove('active');
      }

      let activeTab = isAdmin ? 'admin' : 'final';
      // Ensure the chosen panel is visibly toggled correctly on first load
      try{
        if (activeTab === 'admin'){
          adminTabBtn?.setAttribute('aria-selected','true');
          adminPanelEl?.classList.add('active');
          if (adminPanelEl) adminPanelEl.style.display = '';
          if (finalPanelEl){ finalPanelEl.classList.remove('active'); finalPanelEl.style.display = 'none'; }
        }else{
          finalTabEl?.setAttribute('aria-selected','true');
          finalPanelEl?.classList.add('active');
          if (finalPanelEl) finalPanelEl.style.display = '';
          if (adminPanelEl){ adminPanelEl.classList.remove('active'); adminPanelEl.style.display = 'none'; }
        }
      }catch{}

      function activate(which){
        activeTab = which;
        const tabs = {};
        const panels = {};
        if (finalModule){
          tabs.final = finalTabEl;
          panels.final = finalPanelEl;
        }
        if (adminModule){
          tabs.admin = adminTabBtn;
          panels.admin = adminPanelEl;
        }
        Object.entries(tabs).forEach(([,tab])=> tab?.setAttribute('aria-selected','false'));
        Object.entries(panels).forEach(([,panel])=> panel?.classList.remove('active'));
        if (tabs[which]) tabs[which].setAttribute('aria-selected','true');
                if (panels.final) panels.final.style.display = (which === 'final') ? '' : 'none';
        if (panels.admin) panels.admin.style.display = (which === 'admin') ? '' : 'none';
        // Hide top-bar Final Report button when on Admin page
        const exportTopBtn = document.getElementById('final_export_top');
        if (exportTopBtn){ exportTopBtn.style.display = (which === 'admin') ? 'none' : 'inline-flex'; }
        if (which === 'final' && finalModule) finalModule.focus();
        if (which === 'admin' && adminModule) adminModule.focus();
      }

      if (finalTabEl && finalModule){
        finalTabEl.addEventListener('click', ()=>activate('final'));
      }
      if (adminModule && adminTabBtn){
        adminTabBtn.addEventListener('click', ()=>activate('admin'));
      }

      activate(activeTab);
      // Fallback: explicitly show the chosen panel even if activation short-circuited
      if (activeTab === 'admin'){
        if (adminPanelEl){ adminPanelEl.classList.add('active'); adminPanelEl.style.display = ''; }
        if (finalPanelEl){ finalPanelEl.classList.remove('active'); finalPanelEl.style.display = 'none'; }
      } else {
        if (finalPanelEl){ finalPanelEl.classList.add('active'); finalPanelEl.style.display = ''; }
        if (adminPanelEl){ adminPanelEl.classList.remove('active'); adminPanelEl.style.display = 'none'; }
      }

      window.addEventListener('focus', ()=>{
        if (activeTab === 'final' && finalModule) finalModule.focus();
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






















