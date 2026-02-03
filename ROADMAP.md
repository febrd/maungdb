---

# 🐯 MAUNGDB — STATUS ROADMAP (AUDIT TERKINI)

> Kondisi sekarang:
> **MaungDB sudah lewat MVP, masuk tahap “engine serius v0.1+”**

---

## 🟢 FASE 0 — Fondasi

**Status: ✅ SELESAI**

| Item               | Status |
| ------------------ | ------ |
| Nama MaungDB       | ✅      |
| Bahasa Go          | ✅      |
| CLI-first          | ✅      |
| File-based storage | ✅      |
| Scope single-node  | ✅      |

✔️ Tidak ada hutang teknis di fase ini.

---

## 🟢 FASE 1 — Inisialisasi Project

**Status: ✅ SELESAI**

| Item                    | Status |
| ----------------------- | ------ |
| go mod init             | ✅      |
| Struktur direktori inti | ✅      |
| cmd/maung entry         | ✅      |
| engine/* terpisah       | ✅      |

📌 Catatan:

* Folder `parser/` & `executor/` **sudah ada secara konsep**, walau parsing masih di CLI (OK untuk v0.1)

---

## 🟢 FASE 2 — CLI Minimal

**Status: ✅ SELESAI + LEWAT TARGET**

Awalnya target:

```bash
maung init
maung simpen
maung tingali
```

Yang SUDAH ADA:

| Command             | Status |
| ------------------- | ------ |
| maung init          | ✅      |
| maung simpen        | ✅      |
| maung tingali       | ✅      |
| maung login         | ✅      |
| maung logout        | ✅      |
| maung whoami        | ✅      |
| maung schema create | ✅      |

🔥 Ini sudah **di atas CLI minimal**

---

## 🟢 FASE 3 — Storage Engine (KANDANG)

**Status: ✅ SELESAI + EXTENDED**

| Item                           | Status |   |
| ------------------------------ | ------ | - |
| 1 table = 1 file               | ✅      |   |
| Append-only                    | ✅      |   |
| Delimiter `                    | `      | ✅ |
| Read all rows                  | ✅      |   |
| Auto create table              | ✅      |   |
| Multi extension (.mg / .maung) | ✅      |   |
| System directory isolation     | ✅      |   |

❌ Belum:

* update/delete
* compaction

➡️ **Wajar & sehat untuk v0.1**

---

## 🟢 FASE 3.5 — AUTH & ROLE SYSTEM (BONUS)

**Status: ✅ SELESAI (INI NILAI PLUS BESAR)**

Ini **tidak ada di roadmap awal**, tapi sekarang sudah ada:

| Item                                       | Status |
| ------------------------------------------ | ------ |
| User system                                | ✅      |
| Role hierarchy (supermaung > admin > user) | ✅      |
| Session persistent                         | ✅      |
| Role enforcement                           | ✅      |
| Password hashing (bcrypt)                  | ✅      |

🔥 Banyak DB tutorial **tidak sampai sini**

---

## 🟢 FASE 4 — Schema Engine (TAPAK)

**Status: ✅ SELESAI + ADVANCED**

Awalnya:

* schema validate kolom

Sekarang REALITA:

| Item                  | Status |
| --------------------- | ------ |
| Schema file (.tpk)    | ✅      |
| Schema loader         | ✅      |
| Schema validation     | ✅      |
| Permission per table  | ✅      |
| schema create command | ✅      |

🔥 Ini sudah **beyond FASE 4 versi awal**

---

## 🟡 FASE 5 — Query Language v0 (MaungQL)

**Status: ⏳ PARTIAL**

| Item                | Status |
| ------------------- | ------ |
| simpen              | ✅      |
| tingali             | ✅      |
| where clause        | ❌      |
| comparison operator | ❌      |
| real parser layer   | ❌      |

📌 Saat ini:

* Parsing masih **CLI-driven**
* BELUM ada AST / Command struct formal

➡️ **Ini fase logis berikutnya**

---

## 🟡 FASE 6 — Executor (PANGGERAK)

**Status: ⏳ PARTIAL**

| Item                       | Status       |
| -------------------------- | ------------ |
| Execution flow             | ✅ (implicit) |
| Separation parser/executor | ❌            |
| Pretty output              | ❌            |

➡️ Akan matang setelah FASE 5.

---

## 🟢 FASE 7 — Error Handling & UX

**Status: ✅ CUKUP UNTUK v0.1**

| Item                | Status |
| ------------------- | ------ |
| Error message jelas | ✅      |
| No panic            | ✅      |
| Bahasa konsisten    | ✅      |

➕ UX tambahan nanti:

* help per command
* autocomplete (opsional)

---

🚀 NEXT STEP (SEKARANG AMAN LANJUT)

Sekarang kamu bisa pilih dengan tenang:

👉 lanjut pretty output (FASE 6.5)
👉 lanjut AND / OR (FASE 7)
👉 lanjut type system (FASE 8)

## 🟡 FASE 8 — Build & Install

**Status: ⏳ PARTIAL**

| Item             | Status |
| ---------------- | ------ |
| go build         | ✅      |
| install manual   | ✅      |
| installer script | ❌      |
| version command  | ❌      |

---

## 🔴 FASE 9 — Publish

**Status: ❌ BELUM**

Yang BELUM:

* Versioning (`v0.1.0`)
* GitHub Release
* Changelog
* Installer script

---

# 🧠 RINGKASAN STATUS BESAR

### ✅ SUDAH SELESAI (STRONG)

* Core storage
* Auth & role
* Schema + permission
* CLI usable

### ⏳ SEDANG / SELANJUTNYA

* Real MaungQL (parser + where)
* Executor separation
* Index (cakar)

### ❌ BELUM (SEBELUM PUBLISH)

* version command
* installer
* release tagging

---

# 📘 README.md — ROADMAP (UPDATED VERSION)

> **Ini versi yang seharusnya kamu pakai sekarang**

```md
## 🛣️ Roadmap

### v0.1 — Core Engine (CURRENT)
- [x] CLI init
- [x] File-based storage
- [x] simpen
- [x] tingali
- [x] Multi-extension (.mg / .maung)
- [x] User & role system
- [x] Session persistent
- [x] Password hashing
- [x] Schema enforcement
- [x] Table permission
- [x] schema create command

### v0.2 — Query Language
- [ ] MaungQL parser
- [ ] where clause
- [ ] comparison operator
- [ ] executor separation
- [ ] pretty output

### v0.3 — Performance
- [ ] index (cakar)
- [ ] basic transaction
- [ ] config file

### v0.4 — UX & Distribution
- [ ] version command
- [ ] installer script
- [ ] shell completion
- [ ] documentation website

### v1.0 — Stable
- [ ] stable API
- [ ] backward compatibility
- [ ] long-term support
```

---
