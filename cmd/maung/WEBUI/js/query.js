async function runQuery() {
  // 1. Ambil elemen
  const inputEl = document.getElementById("queryInput");
  if (!inputEl) {
      console.error("Error: Element #queryInput tidak ditemukan.");
      return;
  }
  const q = inputEl.value;

  const out = document.getElementById("queryOutput");
  
  // Tampilkan Loading State yang lebih menarik
  out.innerHTML = `
      <div style="text-align:center; padding: 20px; color: var(--text-muted);">
          <span style="font-size: 20px;">⏳</span> <br> Sedang memproses query...
      </div>`;

  // 2. Request ke Server
  const res = await API.post("/query", { query: q });

  // 3. Handle Error
  if (!res.success) {
    out.innerHTML = `
      <div class="alert alert-error" style="display:flex; align-items:center; gap:10px;">
          <span>❌</span> 
          <div><strong>Error Eksekusi:</strong><br>${res.error}</div>
      </div>`;
    return;
  }

  const data = res.data;

  // 4. Handle Empty Data (Query sukses tapi tidak ada hasil, misal INSERT/UPDATE)
  if (!data || !data.Rows || data.Rows.length === 0) {
    const msg = res.Message || "Query berhasil dieksekusi.";
    out.innerHTML = `
      <div class="alert alert-success" style="display:flex; align-items:center; gap:10px;">
          <span>✅</span> 
          <div>${msg} <br> <span style="font-size:0.8em; opacity:0.8;">(Tidak ada data untuk ditampilkan)</span></div>
      </div>`;
    return;
  }

  // 5. Render Tabel Cantik
  // Kita bungkus dengan 'result-wrapper' untuk styling border radius & shadow
  let html = `<div class="result-wrapper">`;
  html += `<div class="table-scroll"><table class="maung-table">`;
  
  // -- Header --
  html += "<thead><tr>";
  data.Columns.forEach(c => {
      html += `<th>${c}</th>`;
  });
  html += "</tr></thead>";

  // -- Body --
  html += "<tbody>";
  data.Rows.forEach(r => {
    html += "<tr>";
    r.forEach(v => {
        // Cek null atau kosong biar rapi
        const displayVal = (v === null || v === undefined) ? '<span style="color:#ccc; font-style:italic;">NULL</span>' : v;
        html += `<td>${displayVal}</td>`;
    });
    html += "</tr>";
  });
  html += "</tbody></table></div>";
  
  // -- Footer Meta Info --
  html += `<div class="result-footer">
              <span>Status: <strong>Sukses</strong></span>
              <span>Total: <strong>${data.Rows.length}</strong> baris</span>
           </div>`;
           
  html += `</div>`; // Tutup result-wrapper

  out.innerHTML = html;
}