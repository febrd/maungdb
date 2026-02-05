package main

import (
	"fmt"
	"os"
	"strings"
	"github.com/joho/godotenv"


	"github.com/febrd/maungdb/internal/config"
	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/executor"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
)

func main() {
	_ = godotenv.Load()
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

	case "use":
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
        if len(os.Args) > 2 {
            port = os.Args[2]
        }
        startServer(port)

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
	fmt.Println("\n🐯  MAUNG DB v2.2 (Enterprise) - CHEAT SHEET  🐯")
	fmt.Println("================================================")

	fmt.Println("\n🛠️  PARÉNTAH SISTEM (System Commands)")
	fmt.Println("  maung init                       : Inisialisasi folder data")
	fmt.Println("  maung server [port]              : Ngahurungkeun server API/Web")
	fmt.Println("  maung login <user> <pass>        : Masuk (Otentikasi)")
	fmt.Println("  maung logout                     : Keluar")
	fmt.Println("  maung whoami                     : Cek status user & database")
	fmt.Println("  maung createuser <u,p,role>      : Ngadamel user anyar")
	fmt.Println("  maung setdb <user> <db1,db2>     : Mere akses database")
	fmt.Println("  maung createdb <name>            : Ngadamel database")
	fmt.Println("  maung use <name>                 : Milih database")

	fmt.Println("\n🏗️  DEFINISI TABEL (DDL) & CONSTRAINT")
	fmt.Println("  DAMEL <tbl> <cols>               : Ngadamel tabel anyar")
	fmt.Println("    Format Kolom: nama:TIPE:CONSTRAINT")
	fmt.Println("    :PK                            : Primary Key (Unik + Not Null)")
	fmt.Println("    :UNIQUE                        : Data teu kenging kembar")
	fmt.Println("    :NOT NULL                      : Data wajib diisi")
	fmt.Println("    :FK(tabel.col)                 : Foreign Key (Relasi)")
	fmt.Println("  Conto: DAMEL siswa nis:INT:PK, nama:STRING:NOT NULL, kelas:INT:FK(kelas.id)")

	fmt.Println("\n📝  MANIPULASI DATA (CRUD)")
	fmt.Println("  SIMPEN <tbl> val1|val2           : Nambahkeun data (Delimiter |)")
	fmt.Println("  OMEAN <tbl> JADI c=v DIMANA...   : Update data")
	fmt.Println("  MICEUN TI <tbl> DIMANA...        : Hapus data")

	fmt.Println("\n👀  PANGGIL DATA (SELECT)")
	fmt.Println("  TINGALI <tbl>                    : Ningali sadaya data")
	fmt.Println("  TINGALI <c1,c2> TI <tbl>         : Ningali kolom spesifik")

	fmt.Println("\n🔗  RELASI TABEL (JOIN)")
	fmt.Println("  ... GABUNG <t2> DINA t1.a=t2.b   : Inner Join (Data nu cocok hungkul)")
	fmt.Println("  ... KENCA GABUNG <t2> DINA...    : Left Join (Sadaya data kiri)")
	fmt.Println("  ... KATUHU GABUNG <t2> DINA...   : Right Join (Sadaya data kanan)")

	fmt.Println("\n🔍  FILTER & LOGIKA")
	fmt.Println("  DIMANA <kolom> = <nilai>         : Kondisi (Where)")
	fmt.Println("  ... SARENG ...                   : Logika AND")
	fmt.Println("  ... ATAWA ...                    : Logika OR")
	fmt.Println("  ... JIGA 'teks'                  : Pencarian (Like)")

	fmt.Println("\n⚡  PENGATUR DATA")
	fmt.Println("  RUNTUYKEUN <col> [TI_LUHUR/TI_HANDAP/NAEK/TURUN]      : Order By (Naek/Turun)")
	fmt.Println("  SAKADAR <n>                      : Limit (Batesan baris)")
	fmt.Println("  LIWATAN <n>                      : Offset (Loncatan baris)")

	fmt.Println("\n🧮  ARITMATIKA & AGREGASI")
	fmt.Println("  JUMLAH()                         : Ngitung total baris (Count)")
	fmt.Println("  RATA(col)                        : Rata-rata (Avg)")
	fmt.Println("  TOTAL(col)                       : Penjumlahan (Sum)")
	fmt.Println("  PANGGEDENA(col)                  : Nilai Maksimum (Max)")
	fmt.Println("  PANGLEUTIKNA(col)                : Nilai Minimum (Min)")

	fmt.Println("\n💎  TIPE DATA (Data Types)")
	fmt.Println("  INT, FLOAT, BOOL                 : Angka & Logika")
	fmt.Println("  STRING, TEXT, CHAR(n)            : Teks & Karakter")
	fmt.Println("  DATE                             : Tanggal (YYYY-MM-DD)")
	fmt.Println("  ENUM(a,b,c)                      : Pilihan Terbatas")
	fmt.Println("================================================")
}