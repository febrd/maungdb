package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/executor"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
)

func startShell() {
	fmt.Println("🐯 MaungDB Shell")
	fmt.Println("ketik `exit` pikeun kaluar")

	reader := bufio.NewReader(os.Stdin)

	for {
		user, _ := auth.CurrentUser()
		prompt := "maung> "
		if user != nil && user.Database != "" {
			prompt = fmt.Sprintf("maung[%s]> ", user.Database)
		}

		fmt.Print(prompt)

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println()
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// =========================
		// SHELL BUILT-IN COMMANDS
		// =========================

		args := strings.Fields(line)
		switch args[0] {

		case "exit", "quit":
			return

		case "help":
			help()
			continue

		case "createuser":
			if len(args) < 4 {
				fmt.Println("❌ format: createuser <name> <pass> <role>")
				continue
			}
			if err := auth.CreateUser(args[1], args[2], args[3]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ user dijieun:", args[1])
			continue

		case "setdb":
			if len(args) < 3 {
				fmt.Println("❌ format: setdb <user> <db1,db2>")
				continue
			}
			dbs := strings.Split(args[2], ",")
			if err := auth.SetUserDatabases(args[1], dbs); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ database di-assign ka user:", args[1])
			continue

		case "passwd":
			if len(args) < 3 {
				fmt.Println("❌ format: passwd <user> <newpass>")
				continue
			}
			if err := auth.ChangePassword(args[1], args[2]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ password diganti pikeun user:", args[1])
			continue

		case "listuser":
			require("supermaung")
			users, err := auth.ListUsers()
			if err != nil {
				fmt.Println("❌", err)
				continue
			}
			for _, u := range users {
				fmt.Println(u)
			}
			continue
	
		case "login":
			if len(args) < 3 {
				fmt.Println("❌ format: login <user> <pass>")
				continue
			}
			if err := auth.Login(args[1], args[2]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ login hasil")
			continue

		case "logout":
			if err := auth.Logout(); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ logout hasil")
			continue

		case "whoami":
			whoami()
			continue

		case "createdb":
			if len(args) < 2 {
				fmt.Println("❌ format: createdb <database>")
				continue
			}
			if err := storage.CreateDatabase(args[1]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ database dijieun:", args[1])
			continue

		case "use":
			if len(args) < 2 {
				fmt.Println("❌ format: use <database>")
				continue
			}
			if err := auth.SetDatabase(args[1]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ make database:", args[1])
			continue

		case "schema":
			// schema create <table> <fields>
			if len(args) < 4 || args[1] != "create" {
				fmt.Println("❌ format: schema create <table> <field1,field2>")
				continue
			}

			user, err := auth.CurrentUser()
			if err != nil || user.Database == "" {
				fmt.Println("❌ can use database heula")
				continue
			}

			table := args[2]
			fields := strings.Split(args[3], ",")

			perms := map[string][]string{
				"read":  {"user", "admin", "supermaung"},
				"write": {"admin", "supermaung"},
			}

			if err := schema.Create(user.Database, table, fields, perms); err != nil {
				fmt.Println("❌", err)
				continue
			}

			fmt.Println("✅ schema dijieun pikeun table:", table)
			continue
		}

		// =========================
		// QUERY (MaungQL)
		// =========================

		cmd, err := parser.Parse(line)
		if err != nil {
			fmt.Println("❌", err)
			continue
		}

		result, err := executor.Execute(cmd)
		if err != nil {
			fmt.Println("❌", err)
			continue
		}

		for _, r := range result {
			fmt.Println(r)
		}
	}
}
