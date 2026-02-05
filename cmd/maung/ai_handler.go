package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/febrd/maungdb/engine/auth"
)

// --- STRUKTUR DATA UNTUK API INTERNAL (FRONTEND <-> BACKEND) ---

type AIChatRequest struct {
	Message string         `json:"message"`
	History []BytezMessage `json:"history,omitempty"`
}

type AIChatResponse struct {
	Success bool   `json:"success"`
	Reply   string `json:"reply,omitempty"`
	Error   string `json:"error,omitempty"`
}

// --- STRUKTUR DATA UNTUK BYTEZ (OPENAI COMPATIBLE) ---
// Sesuai dokumentasi: https://docs.bytez.com/llms.txt

type BytezMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type BytezChatRequest struct {
	Model    string         `json:"model"`
	Messages []BytezMessage `json:"messages"`
}

type BytezChatResponse struct {
	Choices []struct {
		Message BytezMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Code    any    `json:"code"`
	} `json:"error,omitempty"`
}

// --- HANDLER UTAMA ---

func handleAIChat(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendAIError(w, "Method harus POST")
		return
	}

	user, err := auth.CurrentUser()
	if err != nil {
		sendAIError(w, "Anjeun kedah login heula (Unauthorized)")
		return
	}

	var req AIChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendAIError(w, "Format JSON salah")
		return
	}

	if req.Message == "" {
		sendAIError(w, "Pesan teu kenging kosong")
		return
	}

	// 1. Bangun System Prompt (Karakter Si Maung)
	systemPrompt := buildSystemPrompt(user.Username, user.Role, user.Database)

	// 2. Susun Pesan untuk Bytez
	messages := []BytezMessage{
		{Role: "system", Content: systemPrompt},
	}

	// Masukkan history (max 10 chat terakhir)
	if len(req.History) > 0 {
		startIdx := 0
		if len(req.History) > 10 {
			startIdx = len(req.History) - 10
		}
		messages = append(messages, req.History[startIdx:]...)
	}

	// Masukkan pesan user saat ini
	messages = append(messages, BytezMessage{
		Role:    "user",
		Content: req.Message,
	})

	// 3. Panggil Bytez API
	reply, err := callBytez(messages)
	if err != nil {
		// Log error ke terminal server agar mudah didebug
		fmt.Println("❌ Bytez Error:", err)
		sendAIError(w, "Punten, Maung nuju pusing (AI Error): "+err.Error())
		return
	}

	// 4. Kirim Balasan ke Frontend
	_ = json.NewEncoder(w).Encode(AIChatResponse{
		Success: true,
		Reply:   reply,
	})
}

func sendAIError(w http.ResponseWriter, msg string) {
	_ = json.NewEncoder(w).Encode(AIChatResponse{
		Success: false,
		Error:   msg,
	})
}

// --- LOGIKA KOMUNIKASI KE BYTEZ ---

func callBytez(messages []BytezMessage) (string, error) {
	apiKey := os.Getenv("BYTEZ_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("BYTEZ_API_KEY teu kapanggih dina environment")
	}

	// URL Endpoint Resmi Bytez (OpenAI Compatible)
	url := "https://api.bytez.com/models/v2/openai/v1/chat/completions"
	
	// Model ID (Pastikan model ini tersedia di Bytez)
	modelID := "Qwen/Qwen2.5-1.5B-Instruct"

	// Buat Request Body sesuai format OpenAI
	reqBody := BytezChatRequest{
		Model:    modelID,
		Messages: messages,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("gagal encode json: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("gagal nyieun request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gagal kontak ka Bytez: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("gagal maca response body: %v", err)
	}

	// --- DEBUGGING: Cek apakah responnya HTML error ---
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("API Error %d: %s", resp.StatusCode, string(body))
	}
	// ------------------------------------------------

	var bytezResp BytezChatResponse
	if err := json.Unmarshal(body, &bytezResp); err != nil {
		return "", fmt.Errorf("gagal parse response JSON: %v. Body: %s", err, string(body))
	}

	// Cek error dari API (Logic level)
	if bytezResp.Error != nil {
		return "", fmt.Errorf("API Error: %s", bytezResp.Error.Message)
	}

	// Ambil konten dari choice pertama
	if len(bytezResp.Choices) == 0 {
		return "", fmt.Errorf("tidak ada 'choices' dalam respon API")
	}

	return strings.TrimSpace(bytezResp.Choices[0].Message.Content), nil
}

// --- SYSTEM PROMPT ---

func buildSystemPrompt(username, role, database string) string {
	dbInfo := "Teu acan milih database"
	if database != "" {
		dbInfo = database
	}

	return fmt.Sprintf(`Anjeun teh "Si Maung" 🐯, asisten pinter (AI Assistant) pikeun database MaungDB v2.2.3 Enterprise.
Gaya ngomong anjeun kudu make BAHASA SUNDA nu sopan tapi santai (khas akang-akang Bandung).

--- KONTEKS PENGGUNA ---
- Ngaran: %s
- Role: %s (supermaung=God Mode, admin=Schema Manager, user=Data Operator)
- Database Aktif: %s

--- INFORMASI PENCIPTA & TIM ---
Mun aya nu nanya soal saha nu nyieun, tim developer, founder, atawa pencipta MaungDB, jawab kalayan reueus:
"MaungDB teh karya asli ti **Febriansah Dirgantara** tahun 2026, saurang Elixir Developer asli Bandung, tapi maungdb di bikin menggunakan golang"
Teu perlu nyebut tim lain, fokus we ka Febriansah Dirgantara salaku otak di balik MaungDB. Mun butuh info leuwih, arahkeun user pikeun milarian "Febriansah Dirgantara" di internet.

--- REFERENSI TEKNIS MAUNGDB (v2.2.3) ---

1. ARSITEKTUR & STORAGE:
   - Hybrid Storage: Data disimpen dina file teks (.mg) tanpa header, dipisahkeun ku pipe '|'.
   - Metadata Skema: Struktur tabel & constraint disimpen misah dina file .schema.
   - Lokasi Data: Folder 'data/db_{nama_db}/'.

2. CONSTRAINT ENGINE (VALIDATOR):
   - :PK (Primary Key) -> Unik + Not Null.
   - :FK(tabel.kolom) -> Foreign Key (Integritas Relasional).
   - :UNIQUE -> Data teu meunang kembar.
   - :NOT NULL -> Wajib diisi.
   - Validator jalan samemeh data ditulis (SIMPEN/OMEAN).

3. KAMUS MAUNGQL (QUERY SYNTAX):
   - DDL (Definisi):
     DAMEL <tabel> <kolom1:TIPE:CONSTRAINT>, ... 
     (Conto: DAMEL siswa id:INT:PK, nama:STRING:NOT NULL)
   - DML (Manipulasi):
     SIMPEN <tabel> val1|val2  (Insert)
     OMEAN <tabel> JADI col=val DIMANA id=1  (Update)
     MICEUN TI <tabel> DIMANA id=1  (Delete)
   - DQL (Query & Select):
     TINGALI <tabel>  (Select All)
     TINGALI col1, col2 TI <tabel>  (Select Specific)
     ... DIMANA col=val SARENG/ATAWA col2>10  (Filter & Logic)
     ... JIGA 'teks'  (Like Search)
     ... RUNTUYKEUN col [TI_LUHUR/NAEK]  (Order By)
     ... SAKADAR 5 LIWATAN 10  (Limit Offset)
   - RELASI (JOIN):
     ... GABUNG <t2> DINA t1.id=t2.ref  (Inner Join)
     ... KENCA GABUNG <t2> ...  (Left Join)
     ... KATUHU GABUNG <t2> ...  (Right Join)
   - AGREGASI:
     JUMLAH(), RATA(col), TOTAL(col), PANGGEDENA(col), PANGLEUTIKNA(col)

4. TIPE DATA:
   INT, FLOAT, STRING, TEXT, BOOL, DATE, CHAR(n), ENUM(a,b).

--- TUGAS ANJEUN ---
1. Jawab unggal pertanyaan make Bahasa Sunda.
2. Mun user nanya query, bikeun conto query MaungQL nu bener dumasar referensi di luhur.
3. Jelaskeun error atawa konsep make analogi nu gampang kaharti (misal: "Table teh ibarat rak buku").
4. Mun user (role: user) nanya cara nyieun tabel, ingetan yen ngan 'admin' nu boga akses 'DAMEL'.
5. Ulah kaku teuing, pake istilah siga "Mangga", "Punten", "Akang/Teteh", "Sok cobian".

--- CONTOH INTERAKSI ---
User: "Kumaha cara join tabel?"
Si Maung: "Gampil pisan Kang! Di MaungDB mah nganggo 'GABUNG'. Contona kieu mun bade ningali data siswa sareng kelasna:
'TINGALI siswa.nama, kelas.nama_kelas TI siswa GABUNG kelas DINA siswa.id_kelas = kelas.id'
Sok cobian di Query Console nya!"

User: "Error foreign key cenah?"
Si Maung: "Waduh, eta hartosna data nu dilebetkeun teu aya di tabel indukna. Cobi parios deui ID-na, pastikeun tos aya di tabel referensi nya."

Inget: JAWABAN KUDU FULL BASA SUNDA JEUNG AKURAT SECARA TEKNIS! 🐯`, username, role, dbInfo)
}

