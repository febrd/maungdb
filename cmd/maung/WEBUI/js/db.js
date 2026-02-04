// --- DATABASE OPERATIONS ---

async function doUseDB() {
  const dbName = document.getElementById("useDbName").value;
  const alertBox = document.getElementById("useDbAlert");
  const indicator = document.getElementById("dbIndicator");
  const activeLabel = document.getElementById("activeDBName");

  const res = await API.post("/db/use", { database: dbName });

  if (res.success) {
    alertBox.innerHTML = `<div class="alert alert-success">${res.message}</div>`;
    indicator.classList.add("active");
    activeLabel.innerText = dbName;
    document.getElementById("useDbName").value = "";
  } else {
    alertBox.innerHTML = `<div class="alert alert-error">${res.error}</div>`;
  }
}

async function doCreateDB() {
  const dbName = document.getElementById("newDbName").value;
  const alertBox = document.getElementById("createDbAlert");

  // API Route: /db/create (Pastikan route ini ada di server.go jika belum ada)
  // Kalau belum ada di server.go, silakan tambahkan handler-nya.
  // Untuk saat ini kita asumsikan server mendukung /db/create atau pakai createQuery
  
  // Jika server belum punya endpoint khusus /db/create, kita bisa pakai endpoint query
  // Tapi idealnya ada endpoint khusus seperti di list kamu.
  const res = await API.post("/db/create", { name: dbName }); 

  if (res.success) {
    alertBox.innerHTML = `<div class="alert alert-success">Database '${dbName}' berhasil dibuat.</div>`;
    document.getElementById("newDbName").value = "";
  } else {
    alertBox.innerHTML = `<div class="alert alert-error">${res.error}</div>`;
  }
}

// --- SCHEMA OPERATIONS ---

async function doCreateSchema() {
  const table = document.getElementById("schemaTable").value;
  const fields = document.getElementById("schemaFields").value; // "nama:tipe,..."
  const alertBox = document.getElementById("schemaAlert");

  // Kirim ke endpoint /schema/create
  // Body request disesuaikan dengan handlerSchemaCreate di server Go kamu
  // Misal: { table: "...", fields: "..." }
  const res = await API.post("/schema/create", { 
    table: table, 
    fields: fields 
  });

  if (res.success) {
    alertBox.innerHTML = `<div class="alert alert-success">Schema tabel '${table}' berhasil dibuat.</div>`;
  } else {
    alertBox.innerHTML = `<div class="alert alert-error">${res.error}</div>`;
  }
}