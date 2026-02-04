package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/executor"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
)

// ===========================
// Request & Response Structs
// ===========================

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
	Fields []string `json:"fields"` // PENTING: Tipe datana []string (Array)
}
// ===========================
// Server Entry Point
// ===========================

func startServer(port string) {
	if err := storage.Init(); err != nil {
		panic(err)
	}

	// ---- API ROUTES ----
	http.HandleFunc("/auth/login", handleLogin)
	http.HandleFunc("/auth/logout", handleLogout)
	http.HandleFunc("/auth/whoami", handleWhoami)

	http.HandleFunc("/db/create", handleCreateDB)
	http.HandleFunc("/db/use", handleUse)

	http.HandleFunc("/schema/create", handleSchemaCreate)
	http.HandleFunc("/query", handleQuery)

	// ---- EMBEDDED WEB UI ----
	serveWebUI()

	fmt.Println("🐯 MaungDB Server running")
	fmt.Println("🌐 Web UI  : http://localhost:" + port)
	fmt.Println("🔌 API     : http://localhost:" + port + "/query")

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Println("❌ Server error:", err)
	}
}

// ===========================
// Helpers
// ===========================

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

// ===========================
// AUTH HANDLERS
// ===========================

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

// ===========================
// DATABASE HANDLERS
// ===========================

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

// ===========================
// SCHEMA HANDLER
// ===========================



// 2. Tambahkeun Handler na
func handleSchemaCreate(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendError(w, "Method kudu POST")
		return
	}

	// Cek Login & Role
	if err := auth.RequireRole("admin"); err != nil {
		sendError(w, "Akses ditolak: "+err.Error())
		return
	}

	user, _ := auth.CurrentUser()
	if user.Database == "" {
		sendError(w, "Pilih database heula (use)")
		return
	}

	// Decode JSON Body
	var req CreateSchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, "Format JSON Salah: "+err.Error())
		return
	}

	// Validasi input
	if req.Table == "" || len(req.Fields) == 0 {
		sendError(w, "Table sareng Fields teu kenging kosong")
		return
	}

	// Panggil Logic Schema Create (Engine)
	// Catatan: Permissions default diset
	perms := map[string][]string{
		"read":  {"user", "admin", "supermaung"},
		"write": {"admin", "supermaung"},
	}

	// Panggil schema.Create (fungsi anu di engine)
	// Pastikan import "github.com/febrd/maungdb/engine/schema" tos aya
	if err := schema.Create(user.Database, req.Table, req.Fields, perms); err != nil {
		sendError(w, "Gagal nyieun schema: "+err.Error())
		return
	}

	sendSuccess(w, fmt.Sprintf("✅ Schema tabel '%s' parantos didamel!", req.Table), nil)
}

// ===========================
// QUERY HANDLER
// ===========================

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
