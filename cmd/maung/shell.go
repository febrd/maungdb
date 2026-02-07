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
	fmt.Println("=== MaungDB Shell====")
	fmt.Println("ketik `help` pikeun bantosan")

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

		args := strings.Fields(line)
		cmdName := args[0]

		switch cmdName {

		case "exit", "quit":
			return

		case "help":
			help()
			continue

		case "init":
			if err := storage.Init(); err != nil {
				fmt.Println("❌ gagal init:", err)
				continue
			}
			fmt.Println("✅ MaungDB siap Di angge")
			fmt.Println("Default user: maung / maung (supermaung)")
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
			u, _ := auth.CurrentUser()
			fmt.Printf("✅ login salaku %s (%s)\n", u.Username, u.Role)
			continue

		case "logout":
			if err := auth.Logout(); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ Logoutna Bersih")
			continue

		case "whoami":
			whoami()
			continue


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
			continue

		case "createuser", "CREATEUSER":
			if err := auth.RequireRole("supermaung"); err != nil {
				fmt.Println("❌", err)
				continue
			}
			if len(args) < 4 {
				fmt.Println("❌ format: createuser <name> <pass> <role>")
				continue
			}
			if err := auth.CreateUser(args[1], args[2], args[3]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ User dijieun:", args[1])
			continue

		case "setdb":
			if err := auth.RequireRole("supermaung"); err != nil {
				fmt.Println("❌", err)
				continue
			}
			if len(args) < 3 {
				fmt.Println("❌ format: setdb <user> <db1,db2>")
				continue
			}
			dbs := strings.Split(args[2], ",")
			if err := auth.SetUserDatabases(args[1], dbs); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ Databasena di assignkeun ka user:", args[1])
			continue

		case "passwd":
			if err := auth.RequireRole("supermaung"); err != nil {
				fmt.Println("❌", err)
				continue
			}
			if len(args) < 3 {
				fmt.Println("❌ format: passwd <user> <newpass>")
				continue
			}
			if err := auth.ChangePassword(args[1], args[2]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ Password diganti pikeun user:", args[1])
			continue

		case "listuser":
			if err := auth.RequireRole("supermaung"); err != nil {
				fmt.Println("❌", err)
				continue
			}
			users, err := auth.ListUsers()
			if err != nil {
				fmt.Println("❌", err)
				continue
			}
			for _, u := range users {
				fmt.Println(u)
			}
			continue

		case "createdb", "CREATEDB":
			if err := auth.RequireRole("supermaung"); err != nil {
				fmt.Println("❌", err)
				continue
			}
			if len(args) < 2 {
				fmt.Println("❌ format: createdb <database>")
				continue
			}
			if err := storage.CreateDatabase(args[1]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ Databasena dijieun:", args[1])
			continue

		case "use", "angge", "anggo":
			if len(args) < 2 {
				fmt.Println("❌ format: use <database>")
				continue
			}
			if err := auth.SetDatabase(args[1]); err != nil {
				fmt.Println("❌", err)
				continue
			}
			fmt.Println("✅ Ngangge database:", args[1])
			continue

		case "schema":
			// Access Control: admin
			if err := auth.RequireRole("admin"); err != nil {
				fmt.Println("❌", err)
				continue
			}
			if len(args) < 4 || args[1] != "create" {
				fmt.Println("❌ format: schema create <table> <col:type,col:type> --read=..")
				fmt.Println("   tipe: INT, STRING")
				continue
			}

			user, err := auth.CurrentUser()
			if err != nil || user.Database == "" {
				fmt.Println("❌ can use database heula")
				continue
			}

			table := args[2]
			fieldsRaw := strings.Split(args[3], ",")
			var columns []schema.Column
			for _, f := range fieldsRaw {
				parts := strings.Split(f, ":")
				if len(parts) >= 2 {
					col := schema.Column{
						Name: parts[0],
						Type: strings.ToUpper(parts[1]),
					}
					columns = append(columns, col)
				}
			}

			perms := map[string][]string{
				"read":  {"user", "admin", "supermaung"},
				"write": {"admin", "supermaung"},
			}

			for _, arg := range args {
				if strings.HasPrefix(arg, "--read=") {
					perms["read"] = strings.Split(strings.TrimPrefix(arg, "--read="), ",")
				}
				if strings.HasPrefix(arg, "--write=") {
					perms["write"] = strings.Split(strings.TrimPrefix(arg, "--write="), ",")
				}
			}

			if err := schema.CreateComplex(user.Database, table, columns, perms); err != nil {
				fmt.Println("❌", err)
				continue
			}
			
			storage.InitTableFile(user.Database, table)

			fmt.Println("✅ Schema dijieun pikeun table:", table)
			continue

		case "simpen", "tingali", "damel", "omean", "miceun":
			if err := auth.RequireRole("user"); err != nil {
				fmt.Println("❌", err)
				continue
			}
			processQuery(line)
			continue
		}

		processQuery(line)
	}
}

func processQuery(line string) {
	cmd, err := parser.Parse(line)
	if err != nil {
		fmt.Println("❌ Syntax Error:", err)
		return
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		fmt.Println("❌ Execution Error:", err)
		return
	}

	renderTable(result)
}

func renderTable(result *executor.ExecutionResult) {
	if result.Message != "" {
		fmt.Println(result.Message)
		if len(result.Columns) == 0 {
			return
		}
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
			if i < len(widths) {
				if len(val) > widths[i] {
					widths[i] = len(val)
				}
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
			if i < len(widths) {
				fmt.Printf(" %-*s |", widths[i], val)
			}
		}
		fmt.Println()
	}
	printSeparator()
	
	
}