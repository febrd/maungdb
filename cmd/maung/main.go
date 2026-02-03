package main

import (
	"fmt"
	"os"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/storage"
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

	case "simpen":
		require("user")
		simpen()

	case "tingali":
		require("user")
		tingali()

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

// =======================
// COMMANDS
// =======================

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

	rows, err := storage.ReadAll(os.Args[2])
	if err != nil {
		fmt.Println("❌", err)
		return
	}

	for _, r := range rows {
		fmt.Println(r)
	}
}
