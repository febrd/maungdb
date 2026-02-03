package main

import (
	"fmt"
	"os"
	"strings"
	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/storage"
	"github.com/febrd/maungdb/engine/schema"


)

func main() {
	if len(os.Args) < 2 {
		help()
		return
	}

	switch os.Args[1] {

	case "init":
		require("supermaung")
		initDB()

	case "login":
		login()

	case "logout":
		logout()
	
	case "whoami":
		whoami()
	
	case "simpen":
		require("user")
		simpen()

	case "tingali":
		require("user")
		tingali()

	case "schema":
		require("admin")
		schemaCmd()
	
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

func schemaCmd() {
	if len(os.Args) < 4 || os.Args[2] != "create" {
		fmt.Println("❌ format: maung schema create <table> <field1,field2> --read=a,b --write=c,d")
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

	if err := schema.Create(table, fields, perms); err != nil {
		fmt.Println("❌", err)
		return
	}

	fmt.Println("✅ schema dijieun pikeun table:", table)
}


func help() {
	fmt.Println("🐯 MaungDB")
	fmt.Println("Paréntah:")
	fmt.Println("  maung login <user> <pass>")
	fmt.Println("  maung init")
	fmt.Println("  maung simpen <table> <data>")
	fmt.Println("  maung tingali <table>")
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

	user, err := auth.CurrentUser()
	if err != nil {
		fmt.Println("❌", err)
		return
	}
	fmt.Printf("✅ login salaku %s (%s)\n", user.Username, user.Role)
}

func initDB() {
	if err := storage.Init(); err != nil {
		fmt.Println("❌ gagal init:", err)
		return
	}
	fmt.Println("✅ MaungDB siap dipaké")
	fmt.Println("👤 default user: maung / maung (supermaung)")
}

func simpen() {
	if len(os.Args) < 4 {
		fmt.Println("❌ format: maung simpen <table> <data>")
		return
	}

	user, err := auth.CurrentUser()
	if err != nil {
		fmt.Println("❌", err)
		return
	}

	s, err := schema.Load(os.Args[2])
	if err != nil {
		fmt.Println("❌", err)
		return
	}

	if !s.Can(user.Role, "write") {
		fmt.Println("❌ teu boga hak nulis ka table ieu")
		return
	}

	if err := s.ValidateRow(os.Args[3]); err != nil {
		fmt.Println("❌", err)
		return
	}

	if err := storage.Append(os.Args[2], os.Args[3]); err != nil {
		fmt.Println("❌", err)
		return
	}

	fmt.Println("✅ data disimpen")
}

func tingali() {
	if len(os.Args) < 3 {
		fmt.Println("❌ format: maung tingali <table>")
		return
	}

	user, err := auth.CurrentUser()
	if err != nil {
		fmt.Println("❌", err)
		return
	}

	s, err := schema.Load(os.Args[2])
	if err != nil {
		fmt.Println("❌", err)
		return
	}

	if !s.Can(user.Role, "read") {
		fmt.Println("❌ teu boga hak maca table ieu")
		return
	}

	rows, err := storage.ReadAll(os.Args[2])
	if err != nil {
		fmt.Println("❌", err)
		return
	}

	for _, r := range rows {
		fmt.Println(r)
	}
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
	fmt.Printf("👤 %s (%s)\n", user.Username, user.Role)
}
