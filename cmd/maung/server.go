package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"os"
	"io"
	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/executor"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
)

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type UseRequest struct {
	Database string `json:"database"`
}

type CreateDBRequest struct {
	Name string `json:"name"`
}

type SchemaRequest struct {
	Table  string   `json:"table"`
	Fields []string `json:"fields"`
	Read   []string `json:"read,omitempty"`
	Write  []string `json:"write,omitempty"`
}

type QueryRequest struct {
	Query string `json:"query"`
}

type APIResponse struct {
	Success bool                      `json:"success"`
	Message string                    `json:"message,omitempty"`
	Data    *executor.ExecutionResult `json:"data,omitempty"`
	Error   string                    `json:"error,omitempty"`
}

type CreateSchemaRequest struct {
	Table  string   `json:"table"`
	Fields []string `json:"fields"` 
}

func startServer(port string, enableGUI bool) {
    if err := storage.Init(); err != nil {
        panic(err)
    }

    http.HandleFunc("/auth/login", handleLogin)
    http.HandleFunc("/auth/logout", handleLogout)
    http.HandleFunc("/auth/whoami", handleWhoami)
    http.HandleFunc("/db/create", handleCreateDB)
    http.HandleFunc("/db/use", handleUse)
    http.HandleFunc("/schema/create", handleSchemaCreate)
    http.HandleFunc("/query", handleQuery)
    http.HandleFunc("/db/export", handleExport)
    http.HandleFunc("/db/import", handleImport)
    http.HandleFunc("/ai", handleAIChat)

    if enableGUI {
        serveWebUI() 
    }

    fmt.Println("🐯 MaungDB Server running")
    
    if enableGUI {
        fmt.Println("🌐 Web UI  : http://localhost:" + port)
    } else {
        fmt.Println("🌐 Web UI  : DISABLED (--no-gui)")
    }
    
    fmt.Println("🔌 API     : http://localhost:" + port + "/query")

    if err := http.ListenAndServe(":"+port, nil); err != nil {
        fmt.Println("❌ Server error:", err)
    }
}

func setupHeader(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
}

func sendError(w http.ResponseWriter, msg string) {
	_ = json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Error:   msg,
	})
}

func sendSuccess(w http.ResponseWriter, msg string, data *executor.ExecutionResult) {
	_ = json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Message: msg,
		Data:    data,
	})
}


func handleLogin(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendError(w, "Method kudu POST")
		return
	}

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, "JSON Error")
		return
	}

	if err := auth.Login(req.Username, req.Password); err != nil {
		sendError(w, "Gagal Login: "+err.Error())
		return
	}

	user, _ := auth.CurrentUser()
	sendSuccess(
		w,
		fmt.Sprintf("✅ Login sukses salaku %s (%s)", user.Username, user.Role),
		nil,
	)
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendError(w, "Method kudu POST")
		return
	}

	if err := auth.Logout(); err != nil {
		sendError(w, err.Error())
		return
	}

	sendSuccess(w, "✅ Logout hasil", nil)
}

func handleWhoami(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)

	user, err := auth.CurrentUser()
	if err != nil {
		sendError(w, err.Error())
		return
	}

	sendSuccess(
		w,
		"OK",
		&executor.ExecutionResult{
			Message: fmt.Sprintf(
				"%s (%s) | db: %s",
				user.Username,
				user.Role,
				user.Database,
			),
		},
	)
}


func handleCreateDB(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendError(w, "Method kudu POST")
		return
	}

	if err := auth.RequireRole("supermaung"); err != nil {
		sendError(w, err.Error())
		return
	}

	var req CreateDBRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, "JSON Error")
		return
	}

	if err := storage.CreateDatabase(req.Name); err != nil {
		sendError(w, err.Error())
		return
	}

	sendSuccess(w, "✅ Database dijieun", nil)
}

func handleUse(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendError(w, "Method kudu POST")
		return
	}

	if _, err := auth.CurrentUser(); err != nil {
		sendError(w, "❌ Anjeun kedah login heula")
		return
	}

	var req UseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, "JSON Error")
		return
	}

	if err := auth.SetDatabase(req.Database); err != nil {
		sendError(w, err.Error())
		return
	}

	sendSuccess(w, "✅ Ayeuna ngangge database: "+req.Database, nil)
}

func handleSchemaCreate(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendError(w, "Method kudu POST")
		return
	}

	if err := auth.RequireRole("admin"); err != nil {
		sendError(w, "Akses ditolak: "+err.Error())
		return
	}

	user, _ := auth.CurrentUser()
	if user.Database == "" {
		sendError(w, "Pilih database heula (use)")
		return
	}

	var req CreateSchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, "Format JSON Salah: "+err.Error())
		return
	}

	if req.Table == "" || len(req.Fields) == 0 {
		sendError(w, "Table sareng Fields teu kenging kosong")
		return
	}

	rawFieldsString := strings.Join(req.Fields, ",")
	columns := executor.ParseColumnDefinitions(rawFieldsString)

	if len(columns) == 0 {
		sendError(w, "Gagal parsing definisi kolom")
		return
	}

	for i := range columns {
		if columns[i].ForeignKey != "" {
			columns[i].ForeignKey = strings.ToLower(columns[i].ForeignKey)
		}
	}

	perms := map[string][]string{
		"read":  {"user", "admin", "supermaung"},
		"write": {"admin", "supermaung"},
	}

	if err := schema.CreateComplex(user.Database, req.Table, columns, perms); err != nil {
		sendError(w, "Gagal nyieun schema: "+err.Error())
		return
	}

	if err := storage.InitTableFile(user.Database, req.Table); err != nil {
		fmt.Println("Warning: Gagal init storage file", err)
	}
	
	sendSuccess(w, fmt.Sprintf("✅ Schema tabel '%s' parantos didamel!", req.Table), nil)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendError(w, "Method kudu POST")
		return
	}

	user, err := auth.CurrentUser()
	if err != nil {
		sendError(w, "❌ Anjeun kedah login heula")
		return
	}

	if user.Database == "" {
		sendError(w, "❌ Database can dipilih (POST /db/use)")
		return
	}

	if err := auth.RequireRole("user"); err != nil {
		sendError(w, err.Error())
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, "JSON Error")
		return
	}

	cmd, err := parser.Parse(req.Query)
	if err != nil {
		sendError(w, "Syntax Error: "+err.Error())
		return
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		sendError(w, "Execution Error: "+err.Error())
		return
	}

	sendSuccess(w, "Query Berhasil", result)
}

func handleExport(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Access-Control-Allow-Origin", "*")
    if r.Method == "OPTIONS" { return }
    table := r.URL.Query().Get("table")
    if table == "" {
        http.Error(w, "Parameter 'table' wajib diisi", http.StatusBadRequest)
        return
    }

    filePath, err := storage.ExportCSV(table)
    if err != nil {
        http.Error(w, "Gagal export: "+err.Error(), http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Disposition", "attachment; filename="+table+".csv")
    w.Header().Set("Content-Type", "text/csv")
    http.ServeFile(w, r, filePath)

}

func handleImport(w http.ResponseWriter, r *http.Request) {
    setupHeader(w)
    if r.Method != "POST" { return }

    if err := r.ParseMultipartForm(10 << 20); err != nil {
        sendError(w, "File terlalu besar")
        return
    }

    file, _, err := r.FormFile("csv_file")
    if err != nil {
        sendError(w, "Gagal maca file")
        return
    }
    defer file.Close()

    tableName := r.FormValue("table")
    if tableName == "" {
        sendError(w, "Ngaran tabel kosong")
        return
    }

    tempFile, err := os.CreateTemp("", "upload-*.csv")
    if err != nil {
        sendError(w, "Gagal nyieun temp file")
        return
    }
    defer os.Remove(tempFile.Name())
    if _, err := io.Copy(tempFile, file); err != nil {
        sendError(w, "Gagal nyalin file")
        return
    }

    count, err := storage.ImportCSV(tableName, tempFile.Name())
    if err != nil {
        sendError(w, "Gagal import: "+err.Error())
        return
    }

    sendSuccess(w, fmt.Sprintf("✅ Suksés import %d baris data ka tabel '%s'", count, tableName), nil)
}