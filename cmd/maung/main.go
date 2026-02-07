package main

import (
    "fmt"
    "os"
    "strings"
    
    // --- IMPORT BARU ---
    "github.com/joho/godotenv"
    "github.com/febrd/maungdb/internal/config"
    "github.com/febrd/maungdb/engine/auth"
    "github.com/febrd/maungdb/engine/executor"
    "github.com/febrd/maungdb/engine/parser"
    "github.com/febrd/maungdb/engine/schema"
    "github.com/febrd/maungdb/engine/storage"
    "github.com/febrd/maungdb/engine/transaction" // <-- Import Transaction
)

func main() {
    _ = godotenv.Load()
    walPath := "maung_data/wal.log"
    _ = storage.Init() 
    transaction.InitManager(walPath)

    if len(os.Args) < 2 {
        help()
        return
    }

    if strings.Contains(os.Args[1], " ") {
        runQueryFromString(os.Args[1])
        return
    }

    switch os.Args[1] {

    case "init":
        initDB()

    case "cli":
        startShell()

    case "login":
        login()

    case "logout":
        logout()

    case "whoami":
        whoami()

    case "createdb":
        require("supermaung")
        createDB()

    case "use", "angge", "anggo":
        useDB()

    case "schema":
        require("admin")
        schemaCmd()

    case "simpen", "tingali":
        require("user")
        runQuery()

    case "createuser":
        require("supermaung")
        createUserCmd()

    case "setdb":
        require("supermaung")
        setDbCmd()

    case "passwd":
        require("supermaung")
        passwdCmd()

    case "listuser":
        require("supermaung")
        listUserCmd()

    case "version", "-v", "--version":
        fmt.Printf("🐯 MaungDB %s\n", config.VERSION)
        return

    case "server":
        port := "7070"
        enableGUI := true
        serverArgs := os.Args[2:]
        for _, arg := range serverArgs {
            if arg == "--no-gui" {
                enableGUI = false
            } else {
                port = arg
            }
        }

        startServer(port, enableGUI)

    default:
        help()
    }
}

func require(role string) {
    if err := auth.RequireRole(role); err != nil {
        fmt.Println("❌", err)
        os.Exit(1)
    }
}

func createUserCmd() {
    if len(os.Args) < 5 {
        fmt.Println("❌ format: createuser <name> <pass> <role>")
        return
    }

    if err := auth.CreateUser(os.Args[2], os.Args[3], os.Args[4]); err != nil {
        fmt.Println("❌", err)
        return
    }

    fmt.Println("✅ user dijieun:", os.Args[2])
}

func setDbCmd() {
    if len(os.Args) < 4 {
        fmt.Println("❌ format: setdb <user> <db1,db2>")
        return
    }

    dbs := strings.Split(os.Args[3], ",")
    if err := auth.SetUserDatabases(os.Args[2], dbs); err != nil {
        fmt.Println("❌", err)
        return
    }

    fmt.Println("✅ database di-assign ka user:", os.Args[2])
}

func passwdCmd() {
    if len(os.Args) < 4 {
        fmt.Println("❌ format: passwd <user> <newpass>")
        return
    }

    if err := auth.ChangePassword(os.Args[2], os.Args[3]); err != nil {
        fmt.Println("❌", err)
        return
    }

    fmt.Println("✅ password diganti pikeun user:", os.Args[2])
}

func listUserCmd() {
    users, err := auth.ListUsers()
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    for _, u := range users {
        fmt.Println(u)
    }
}

func createDB() {
    if len(os.Args) < 3 {
        fmt.Println("❌ format: maung createdb <database>")
        return
    }

    if err := storage.CreateDatabase(os.Args[2]); err != nil {
        fmt.Println("❌", err)
        return
    }

    fmt.Println("✅ database dijieun:", os.Args[2])
}

func useDB() {
    if len(os.Args) < 3 {
        fmt.Println("❌ format: maung use <database>")
        return
    }

    if err := auth.SetDatabase(os.Args[2]); err != nil {
        fmt.Println("❌", err)
        return
    }

    fmt.Println("✅ make database:", os.Args[2])
}

func schemaCmd() {
    if len(os.Args) < 5 || os.Args[2] != "create" {
        fmt.Println("❌ format: maung schema create <table> <field1,field2> --read=a,b --write=c,d")
        return
    }

    user, err := auth.CurrentUser()
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    if user.Database == "" {
        fmt.Println("❌ can use database heula")
        return
    }

    table := os.Args[3]
    fields := strings.Split(os.Args[4], ",")

    perms := map[string][]string{
        "read":  {"user", "admin", "supermaung"},
        "write": {"admin", "supermaung"},
    }

    for _, arg := range os.Args {
        if strings.HasPrefix(arg, "--read=") {
            perms["read"] = strings.Split(strings.TrimPrefix(arg, "--read="), ",")
        }
        if strings.HasPrefix(arg, "--write=") {
            perms["write"] = strings.Split(strings.TrimPrefix(arg, "--write="), ",")
        }
    }

    if err := schema.Create(user.Database, table, fields, perms); err != nil {
        fmt.Println("❌", err)
        return
    }

    fmt.Println("✅ schema dijieun pikeun table:", table)
}

func runQuery() {
    user, err := auth.CurrentUser()
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    if user.Database == "" {
        fmt.Println("❌ can use database heula")
        return
    }

    query := strings.Join(os.Args[1:], " ")
    cmd, err := parser.Parse(query)
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    result, err := executor.Execute(cmd)
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    printResult(result)
}

func runQueryFromString(query string) {
    user, err := auth.CurrentUser()
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    if user.Database == "" {
        fmt.Println("❌ can use database heula")
        return
    }

    cmd, err := parser.Parse(query)
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    result, err := executor.Execute(cmd)
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    printResult(result)
}

func printResult(result *executor.ExecutionResult) {
    if result.Message != "" {
        fmt.Println(result.Message)
        return
    }

    if len(result.Columns) == 0 {
        return
    }

    widths := make([]int, len(result.Columns))
    for i, col := range result.Columns {
        widths[i] = len(col)
    }
    for _, row := range result.Rows {
        for i, val := range row {
            if len(val) > widths[i] {
                widths[i] = len(val)
            }
        }
    }

    printSeparator := func() {
        fmt.Print("+")
        for _, w := range widths {
            fmt.Print(strings.Repeat("-", w+2) + "+")
        }
        fmt.Println()
    }

    printSeparator()
    fmt.Print("|")
    for i, col := range result.Columns {
        fmt.Printf(" %-*s |", widths[i], col)
    }
    fmt.Println()
    printSeparator()

    for _, row := range result.Rows {
        fmt.Print("|")
        for i, val := range row {
            fmt.Printf(" %-*s |", widths[i], val)
        }
        fmt.Println()
    }
    printSeparator()
}

func login() {
    if len(os.Args) < 4 {
        fmt.Println("❌ format: maung login <user> <pass>")
        return
    }

    if err := auth.Login(os.Args[2], os.Args[3]); err != nil {
        fmt.Println("❌", err)
        return
    }

    user, _ := auth.CurrentUser()
    fmt.Printf("✅ login salaku %s (%s)\n", user.Username, user.Role)
}

func logout() {
    if err := auth.Logout(); err != nil {
        fmt.Println("❌ can logout:", err)
        return
    }
    fmt.Println("✅ logout hasil")
}

func whoami() {
    user, err := auth.CurrentUser()
    if err != nil {
        fmt.Println("❌", err)
        return
    }

    db := user.Database
    if db == "" {
        db = "-"
    }
    
    fmt.Printf("👤 %s (%s) | db: %s\n", user.Username, user.Role, db)
}

func initDB() {
    if err := storage.Init(); err != nil {
        fmt.Println("❌ gagal init:", err)
        return
    }
    fmt.Println("MaungDB siap Di angge")
    fmt.Println("Default user: maung / maung (supermaung)")
}


func help() {
	fmt.Println("\n🐯  MAUNG DB v2.2.8 (Enterprise Edition) - CHEAT SHEET LENGKAP  🐯")
	fmt.Println("==================================================================")
	fmt.Println("Catetan: Tanda '/' hartosna 'ATAWA' (Sinonim/Alias)")

	fmt.Println("\n🛠️  PARÉNTAH SISTEM (System & Discovery)")
	fmt.Println("  maung init                       : Inisialisasi folder data")
	fmt.Println("  maung server [port]              : Ngahurungkeun server API/Kalau port Kosong default: 7070")
	fmt.Println("  maung login <u,p>                : Masuk (Login)")
	fmt.Println("  maung logout                     : Keluar")
	fmt.Println("  maung whoami                     : Cek user aktif")
	fmt.Println("  TINGALI / SELECT ...             : Perintah Query Dasar")
	fmt.Println("  ...  PANGKAL / DATABASES  : Ningali daptar database")
	fmt.Println("  maung use <name>                 : Milih database aktip")

	fmt.Println("\n🏗️  DEFINISI STRUKTUR (DDL)")
	fmt.Println("  DAMEL / BIKIN / NYIEUN / SCHEMA  : Keyword nyieun objek")
	fmt.Println("  ... <tbl> <cols>                 : Nyieun Tabel")
	fmt.Println("  ... KACA / VIEW <nm> TINA...     : Nyieun View (Tabel Virtual)")
	fmt.Println("  ... JARAMBAH / TRIGGER <nm>...   : Nyieun Trigger")
	fmt.Println("      Format Waktu: WAKTU / WHEN <event> PADA / ON <table>")
	fmt.Println("      Format Aksi : LAKUKAN / DO <query>")

	fmt.Println("\n🚀  OPTIMASI & PENCARIAN (Performance)")
	fmt.Println("  TANDAIN / TANDAAN / TAWISAN      : Indexing Hash (Cepat)")
	fmt.Println("      Format: ... <tbl> DINA / ON <col>")
	fmt.Println("  DAMEL INDEKS_TEKS                : Indexing Teks (Inverted)")
	fmt.Println("  KOREHAN <tbl> DINA <c> MILARI... : Full Text Search")
	fmt.Println("  JELASKEUN <query>                : Analisa Query (Explain)")

	fmt.Println("\n📝  MANIPULASI DATA (CRUD)")
	fmt.Println("  SIMPEN / TENDEUN / INSERT        : Nambah data")
	fmt.Println("  OMEAN / ROBIH / UPDATE           : Update data")
	fmt.Println("      Format: ... JADI / JANTEN / SET <c>=<v>")
	fmt.Println("  MICEUN / PICEUN / DELETE         : Hapus data")
	fmt.Println("      Format: ... TI / FROM <tbl>")
	
	fmt.Println("\n🔐  TRANSAKSI (ACID)")
	fmt.Println("  MIMITIAN / BEGIN                 : Mulai transaksi")
	fmt.Println("  JADIKEUN / COMMIT                : Simpan permanen")
	fmt.Println("  BATALKEUN / ROLLBACK             : Batalkan perubahan")

	fmt.Println("\n👀  ANALISA DATA (SELECT)")
	fmt.Println("  TINGALI / TENJO / SELECT         : Muka data")
	fmt.Println("  ... TI / FROM <tbl>              : Sumber tabel")
	fmt.Println("  ... KUMPULKEUN / GROUP           : Grouping")
	fmt.Println("      ... DUMASAR / BY <col>")
	fmt.Println("  ... MUN / HAVING [SYARATNA]      : Filter hasil group")

	fmt.Println("\n🔗  RELASI TABEL (JOIN)")
	fmt.Println("  ... GABUNG / HIJIKEUN / JOIN     : Inner Join")
	fmt.Println("  ... KENCA / LEFT ...             : Left Join")
	fmt.Println("  ... KATUHU / RIGHT ...           : Right Join")
	fmt.Println("  ... PINUH / FULL ...             : Full Join")
	fmt.Println("  ... DINA / ON <kondisi>          : Syarat Join")

	fmt.Println("\n🛡️  REPLIKASI (Enterprise)")
	fmt.Println("  JADI INDUNG                      : Set Master (Read/Write)")
	fmt.Println("  JADI ANAK NGINTIL <ip>           : Set Slave (Read Only)")

	fmt.Println("\n🔍  FILTER & LOGIKA & URUTAN")
	fmt.Println("  DIMANA / WHERE <k>=<v>           : Kondisi")
	fmt.Println("  ... SARENG / AND                 : Logika DAN")
	fmt.Println("  ... ATAWA / OR                   : Logika ATAU")
	fmt.Println("  RUNTUYKEUN / ORDER               : Urutkeun data")
	fmt.Println("      ... NAEK / ASC / TI_HANDAP   : Urutan A-Z")
	fmt.Println("      ... TURUN / DESC / TI_LUHUR  : Urutan Z-A")
	fmt.Println("  SAKADAR / LIMIT <n>              : Batesan jumlah")
	fmt.Println("  LIWATAN / OFFSET <n>             : Loncatan awal")

	fmt.Println("==================================================================")
}