async function login() {
  const username = document.getElementById("username").value;
  const password = document.getElementById("password").value;
  const alertBox = document.getElementById("loginAlert");

  try {
    const res = await API.post("/auth/login", { username, password });

    if (!res.success) {
      alertBox.style.display = "block";
      alertBox.innerText = res.error || "Gagal Login";
      return;
    }

    // Simpan token atau state jika perlu, lalu redirect
    window.location.href = "/static/app.html";
  } catch (err) {
    alertBox.style.display = "block";
    alertBox.innerText = "Kesalahan koneksi server.";
  }
}

async function logout() {
  await API.post("/auth/logout", {});
  window.location.href = "/";
}

async function whoami() {
  const res = await API.get("/auth/whoami");
  if (!res.success) {
    window.location.href = "/";
    return null;
  }
  return res.message; // Mengembalikan pesan "Login sukses salaku..."
}