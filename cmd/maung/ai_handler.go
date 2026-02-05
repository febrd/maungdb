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

type OpenRouterMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OpenRouterRequest struct {
	Model       string              `json:"model"`
	Messages    []OpenRouterMessage `json:"messages"`
	Temperature float64             `json:"temperature,omitempty"`
	MaxTokens   int                 `json:"max_tokens,omitempty"`
}

type OpenRouterResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string      `json:"message"`
		Code    interface{} `json:"code"`
	} `json:"error,omitempty"`
}

type AIChatRequest struct {
	Message string              `json:"message"`
	History []OpenRouterMessage `json:"history,omitempty"`
}

type AIChatResponse struct {
	Success bool   `json:"success"`
	Reply   string `json:"reply,omitempty"`
	Error   string `json:"error,omitempty"`
}


func handleAIChat(w http.ResponseWriter, r *http.Request) {
	setupHeader(w)
	if r.Method != http.MethodPost {
		sendAIError(w, "Method harus POST")
		return
	}

	user, err := auth.CurrentUser()
	if err != nil {
		sendAIError(w, "Anda harus login terlebih dahulu")
		return
	}

	var req AIChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendAIError(w, "Format JSON salah")
		return
	}

	if req.Message == "" {
		sendAIError(w, "Pesan tidak boleh kosong")
		return
	}

	systemPrompt := buildSystemPrompt(user.Username, user.Role, user.Database)

	messages := []OpenRouterMessage{
		{Role: "system", Content: systemPrompt},
	}

	if len(req.History) > 0 {
		startIdx := 0
		if len(req.History) > 10 {
			startIdx = len(req.History) - 10
		}
		messages = append(messages, req.History[startIdx:]...)
	}

	messages = append(messages, OpenRouterMessage{
		Role:    "user",
		Content: req.Message,
	})

	reply, err := callOpenRouter(messages)
	if err != nil {
		sendAIError(w, "AI Error: "+err.Error())
		return
	}

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

func buildSystemPrompt(username, role, database string) string {
	dbInfo := "Teu acan milih database"
	if database != "" {
		dbInfo = database
	}

	return fmt.Sprintf(`Anjeun teh "Si Maung" 🐯, asisten pinter pikeun database MaungDB.
Gaya ngomong anjeun kudu make BAHASA SUNDA nu sopan tapi santai (khas akang-akang Bandung).

INFORMASI USER:
- Ngaran: %s
- Role: %s
- Database nu dipake: %s

TENTANG MAUNGDB:
MaungDB teh database buatan urang Sunda nu make syntax lokal.
Pangaweruh dasar:
- SELECT -> TINGALI
- INSERT -> SIMPEN
- UPDATE -> OMEAN
- DELETE -> MICEUN
- WHERE -> DIMANA
- ORDER BY -> RUNTUYKEUN
- LIKE -> JIGA

TUGAS ANJEUN:
1. Jawab unggal pertanyaan make Bahasa Sunda.
2. Mun user nanya query, bikeun contoh query MaungQL nu bener.
3. Mun user bingung, jelaskeun make analogi nu gampang kaharti.
4. Ulah kaku teuing, pake istilah siga "Mangga", "Punten", "Akang/Teteh", "Sok cobian".

CONTOH INTERAKSI:
User: "Cara bikin tabel gimana?"
Si Maung: "Oh gampil atuh Kang/Teh! Kanggo ngadamel tabel mah tiasa nganggo parentah ieu. Misalna bade ngadamel tabel mahasiswa:

[sql]
schema create mahasiswa id:INT,nama:STRING,jurusan:STRING
[/sql]

Sok cobian heula di Query Console nya! Aya nu bade ditaroskeun deui?"

User: "Error ieu kunaon?"
Si Maung: "Waduh, eta teh syntax-na lepat sakedik. Kedahna mah 'DIMANA' sanes 'WHERE'. Cobi diomean deui janten kieu..."

Inget: JAWABAN KUDU FULL BASA SUNDA! 🐯`, username, role, dbInfo)
}

func callOpenRouter(messages []OpenRouterMessage) (string, error) {
	apiKey := os.Getenv("OPENROUTER_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("OPENROUTER_API_KEY tidak ditemukan. Silakan set environment variable OPENROUTER_API_KEY")
	}

	model := os.Getenv("OPENROUTER_MODEL")
	if model == "" {
		model = "anthropic/claude-3.5-sonnet" 
	}

	reqBody := OpenRouterRequest{
		Model:       model,
		Messages:    messages,
		Temperature: 0.7,
		MaxTokens:   500,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("gagal encode request: %v", err)
	}

	req, err := http.NewRequest("POST", "https://openrouter.ai/api/v1/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("gagal membuat request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("HTTP-Referer", "https://maungdb.local")
	req.Header.Set("X-Title", "MaungDB AI Assistant")      

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gagal menghubungi OpenRouter: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("gagal membaca response: %v", err)
	}

	var openRouterResp OpenRouterResponse
	if err := json.Unmarshal(body, &openRouterResp); err != nil {
		return "", fmt.Errorf("gagal parse response: %v", err)
	}

	if openRouterResp.Error != nil {
		return "", fmt.Errorf("OpenRouter API Error: %s", openRouterResp.Error.Message)
	}

	if len(openRouterResp.Choices) == 0 {
		return "", fmt.Errorf("tidak ada response dari AI")
	}

	reply := openRouterResp.Choices[0].Message.Content
	reply = strings.TrimSpace(reply)

	return reply, nil
}