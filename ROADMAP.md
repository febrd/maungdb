Mantap. Keputusan **pakai Go** itu *tepat secara engineering* 👍
Sekarang kita bikin ini **rapi, terstruktur, dan bisa kamu publish tanpa malu**.

Di bawah ini aku kasih:

1. **Tahapan kerja yang jelas (end-to-end)**
2. **Checklist teknis per fase**
3. **README.md (TODO-based) siap taruh di repo**

---

# 🐯 MAUNGDB — ROADMAP TEKNIS (GO)

Target akhir:

> **MaungDB bisa di-install, dipakai via CLI, dan dipublish (GitHub Release)**

---

## 🟢 FASE 0 — Fondasi (WAJIB, jangan dilewati)

### Yang harus kamu putuskan

* Nama: **MaungDB**
* Bahasa: **Go**
* Interface: **CLI**
* Storage: **File-based**
* Scope v0.1: **single-node, single-user**

📌 Prinsip:

> *Make it work → make it usable → make it nice*

---

## 🟢 FASE 1 — Inisialisasi Project

### 1. Init module

```bash
mkdir maungdb
cd maungdb
go mod init github.com/username/maungdb
```

### 2. Struktur direktori (FINAL v0.x)

```txt
maungdb/
├── cmd/
│   └── maung/
│       └── main.go        # CLI entry
├── engine/
│   ├── parser/            # panyaur
│   ├── executor/          # panggerak
│   ├── storage/           # kandang
│   └── schema/            # tapak
├── internal/
│   └── config/
├── examples/
├── docs/
├── README.md
└── go.mod
```

⚠️ Jangan nambah folder dulu selain ini.

---

## 🟢 FASE 2 — CLI Minimal (HARUS JALAN CEPAT)

Target:

```bash
maung version
maung init
```

### CLI command v0.1

* `maung init`
* `maung simpen <table> <data>`
* `maung tingali <table>`

Gunakan:

* `os.Args` (cukup, jangan overkill)
* atau `cobra` (kalau mau lebih rapi)

---

## 🟢 FASE 3 — Storage Engine (KANDANG)

### Konsep

* 1 table = 1 file
* Append-only
* Delimiter `|`

📄 Contoh file:

```
data/pamake.mg
```

Isi:

```
1|Febrian|21
2|Andi|25
```

### Yang harus dibuat

* `CreateTable()`
* `AppendRow()`
* `ReadAllRows()`

❌ Belum ada index
❌ Belum ada delete/update

---

## 🟢 FASE 4 — Schema Engine (TAPAK)

📄 `pamake.tpk`

```json
{
  "table": "pamake",
  "primary": "id",
  "fields": ["id", "ngaran", "umur"]
}
```

### Fungsi schema

* Validasi jumlah kolom
* Mapping posisi kolom

---

## 🟢 FASE 5 — Query Language v0 (MaungQL)

### Grammar MINIMAL

```txt
simpen pamake 1|Febrian|21
tingali pamake
tingali pamake lamun umur > 20
```

### Parsing

* `strings.Fields`
* `strings.Split`
* struct `Command`

Contoh:

```go
type Command struct {
  Action string
  Table  string
  Values []string
  Where  *Condition
}
```

---

## 🟢 FASE 6 — Executor (PANGGERAK)

Flow:

```
CLI
 → Parser
 → Command
 → Executor
 → Storage
 → Output
```

Output harus **human-readable**:

```
ID | Ngaran | Umur
------------------
1  | Febrian | 21
```

---

## 🟢 FASE 7 — Error Handling & UX

WAJIB:

* Error jelas
* Bahasa ramah
* Jangan panic()

Contoh:

```
❌ Table pamake teu kapanggih
```

---

## 🟢 FASE 8 — Build & Install

### Build binary

```bash
go build -o maung ./cmd/maung
```

### Install local

```bash
sudo mv maung /usr/local/bin
```

Test:

```bash
maung init
maung simpen pamake 1|Febrian|21
maung tingali pamake
```

---

## 🟢 FASE 9 — Publish

### Yang HARUS ADA sebelum publish

* README rapi
* Example usage
* Binary release
* Version tag

---

# 📘 README.md — TODO VERSION (SIAP PAKAI)

> **Ini langsung bisa kamu copas ke repo**

````md
# 🐯 MaungDB
**Maung Database Bandung**

MaungDB adalah database engine ringan berbasis file,
dengan query language sederhana bernuansa Sunda.

> Kuat, gancang, jeung jujur.

---

## ✨ Goals
- Lightweight
- Human-readable storage
- CLI-first
- Easy to install
- Local-first

---

## 📦 Installation (WIP)

```bash
# build
go build -o maung ./cmd/maung

# install
sudo mv maung /usr/local/bin
````

---

## 🚀 Quick Start (Planned)

```bash
maung init
maung simpen pamake 1|Febrian|21
maung tingali pamake
```

---

## 🗂️ Project Structure

```txt
cmd/maung        # CLI entry
engine/parser   # Query parser (Panyaur)
engine/executor # Query executor (Panggerak)
engine/storage  # File storage (Kandang)
engine/schema   # Schema engine (Tapak)
```

---

## 🛣️ Roadmap

### v0.1 — Core

* [ ] CLI init
* [ ] File-based storage
* [ ] simpen
* [ ] tingali
* [ ] Schema validation

### v0.2 — Query

* [ ] where clause
* [ ] comparison operator
* [ ] better error handling

### v0.3 — Performance

* [ ] index (cakar)
* [ ] config file
* [ ] basic transaction

### v1.0

* [ ] stable release
* [ ] documentation
* [ ] installer script

---

## 📜 License

MIT

````

---

## 🧭 PATOKAN BERHASIL

Kalau ini jalan:
```bash
maung simpen pamake 1|Febrian|21
maung tingali pamake
````

Dan orang lain bisa install → **MaungDB SUDAH JADI**

---
