package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/executor"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
)

func main() {
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

	default:
		help()
	}
}

//
// =======================
// ACCESS CONTROL
// =======================
//

func require(role string) {
	if err := auth.RequireRole(role); err != nil {
		fmt.Println("❌", err)
		os.Exit(1)
	}
}

//
// =======================
// DATABASE COMMANDS
// =======================
//

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

//
// =======================
// SCHEMA COMMAND
// =======================
//

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

//
// =======================
// QUERY (FASE 5 & 6)
// =======================
//

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

	// Gabungkan input jadi satu query MaungQL
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

	for _, row := range result {
		fmt.Println(row)
	}
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

	rows, err := executor.Execute(cmd)
	if err != nil {
		fmt.Println("❌", err)
		return
	}

	for _, r := range rows {
		fmt.Println(r)
	}
}


//
// =======================
// AUTH COMMANDS
// =======================
//

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

//
// =======================
// INIT & HELP
// =======================
//

func initDB() {
	if err := storage.Init(); err != nil {
		fmt.Println("❌ gagal init:", err)
		return
	}
	fmt.Println("✅ MaungDB siap dipaké")
	fmt.Println("👤 default user: maung / maung (supermaung)")
}

func help() {
	fmt.Println("🐯 MaungDB")
	fmt.Println("Paréntah:")
	fmt.Println("  maung init")
	fmt.Println("  maung createuser <name> <pass> <role>")
	fmt.Println("  maung login <user> <pass>")
	fmt.Println("  maung setdb <user> <db1,db2>")
	fmt.Println("  maung passwd <user> <newpass>")
	fmt.Println("  maung listuser")
	fmt.Println("  maung logout")
	fmt.Println("  maung whoami")
	fmt.Println("  maung createdb <database>")
	fmt.Println("  maung use <database>")
	fmt.Println("  maung schema create <table> <fields>")
	fmt.Println("  maung simpen <table> <data>")
	fmt.Println("  maung tingali <table> [dimana <field> <op> <value>]")
}
