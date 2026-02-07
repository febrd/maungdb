package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/executor"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
	"github.com/febrd/maungdb/internal/config"
)

type ColumnInfo struct {
    Name        string `json:"name"`
    Type        string `json:"type"`
    IsPrimary   bool   `json:"is_primary"`
    IsUnique    bool   `json:"is_unique"`
    IsNotNull   bool   `json:"is_not_null"`
    ForeignKey  string `json:"foreign_key,omitempty"` 
}

type TableInfo struct {
    Name    string       `json:"name"`
    Columns []ColumnInfo `json:"columns"`
    RowCount int         `json:"row_count"`
}

type SchemaInfoResponse struct {
    Database string      `json:"database"`
    Tables   []TableInfo `json:"tables"`
}

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
	Data    interface{}               `json:"data,omitempty"`
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
	http.HandleFunc("/db/export", handleExport)
	http.HandleFunc("/db/import", handleImport)
	
	http.HandleFunc("/schema/create", handleSchemaCreate)
	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/ai", handleAIChat)
	http.HandleFunc("/health-check", handleHealthCheck)

	http.HandleFunc("/schema/info", handleSchemaInfo)

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
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept")
	w.Header().Set("Content-Type", "application/json")
}

func sendError(w http.ResponseWriter, msg string) {
	_ = json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Error:   msg,
	})
}

func sendSuccess(w http.ResponseWriter, msg string, data interface{}) {
	_ = json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Message: msg,
		Data:    data,
	})
}


func handleLogin(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

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
	
	responseData := map[string]string{
		"username": user.Username,
		"role":     user.Role,
		"database": user.Database,
	}

	sendSuccess(
		w,
		fmt.Sprintf("✅ Login sukses salaku %s (%s)", user.Username, user.Role),
		responseData,
	)
}

func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		sendError(w, "Method kudu GET")
		return
	}

	status := "ok"

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   status,
		"service":  "maungdb",
		"version":  config.VERSION,
		"time":     fmt.Sprintf("%d", time.Now().Unix()),
	})
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

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
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

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
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

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
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

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
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

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

    if r.Method == "OPTIONS" {
        w.WriteHeader(http.StatusOK)
        return
    }

    if r.Method != http.MethodPost {
        sendError(w, "Method kudu POST")
        return
    }

    user, err := auth.CurrentUser()
    if err != nil {
        sendError(w, "❌ Anjeun kedah login heula")
        return
    }

    var req QueryRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        sendError(w, "JSON Error: Format request teu valid")
        return
    }

    cmd, err := parser.Parse(req.Query)
    if err != nil {
        sendError(w, "Syntax Error: "+err.Error())
        return
    }

    isSystemCmd := (cmd.Type == "SHOW_DB" || cmd.Type == "JADI_INDUNG" || cmd.Type == "JADI_ANAK")

    if !isSystemCmd {
        if user.Database == "" {
            sendError(w, "❌ Database can dipilih. Gunakeun menu 'Switch Database' heula.")
            return
        }
    }

    switch cmd.Type {
    case parser.CmdCreate, parser.CmdCreateView, parser.CmdCreateTrigger, parser.CmdIndex, "CREATE_FTS":
        if user.Role != "admin" && user.Role != "supermaung" {
            sendError(w, "⛔ Akses Ditolak: Ngan Admin/Supermaung nu tiasa ngarobah struktur/schema.")
            return
        }
    
    case "JADI_INDUNG", "JADI_ANAK":
        if user.Role != "supermaung" {
            sendError(w, "⛔ Akses Ditolak: Konfigurasi Server khusus Supermaung.")
            return
        }
    }

    if err := auth.RequireRole("user"); err != nil {
        sendError(w, err.Error())
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
	setupHeader(w) 

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

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
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		return
	}

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

func handleSchemaInfo(w http.ResponseWriter, r *http.Request) {
    setupHeader(w)
    
    if r.Method != "GET" {
        sendError(w, "Method kudu GET")
        return
    }

    user, err := auth.CurrentUser()
    if err != nil {
        sendError(w, "❌ Anjeun kedah login heula")
        return
    }

    if user.Database == "" {
        sendError(w, "❌ Database can dipilih. Gunakeun menu 'Use Database' heula.")
        return
    }

    tableNames, err := storage.ListTables(user.Database)
    if err != nil {
        sendError(w, "Gagal maca tabel: "+err.Error())
        return
    }

    var tablesInfo []TableInfo

    for _, tblName := range tableNames {
        s, err := schema.Load(user.Database, tblName)
        if err != nil {
            fmt.Println("Warning: Gagal load schema tabel", tblName, err)
            continue
        }

        var colsInfo []ColumnInfo
        for _, col := range s.Columns {
            colsInfo = append(colsInfo, ColumnInfo{
                Name:       col.Name,
                Type:       string(col.Type),
                IsPrimary:  col.Primary,
                IsUnique:   col.Unique,
                IsNotNull:  col.NotNull,
                ForeignKey: col.ForeignKey,
            })
        }
        
        rows, _ := storage.ReadAll(tblName)
        
        tablesInfo = append(tablesInfo, TableInfo{
            Name:     tblName,
            Columns:  colsInfo,
            RowCount: len(rows),
        })
    }

    respData := SchemaInfoResponse{
        Database: user.Database,
        Tables:   tablesInfo,
    }


    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(APIResponse{
        Success: true,
        Message: "Schema info loaded",
        Data:    respData,
    })
}