package auth

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/febrd/maungdb/internal/config"
	"golang.org/x/crypto/bcrypt"
)

type User struct {
	Username string
	Role     string
}


func Login(username, password string) error {
	userFile := filepath.Join(
		config.DataDir,
		config.SystemDir,
		"users.maung",
	)

	file, err := os.Open(userFile)
	if err != nil {
		return errors.New("system user file teu kapanggih")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			continue
		}

		if parts[0] == username && bcrypt.CompareHashAndPassword([]byte(parts[1]), []byte(password)) == nil {
				 return writeSession(username, parts[2])
		}
	}

	return errors.New("login gagal")
}

func Logout() error {
	sessionPath := filepath.Join(
		config.DataDir,
		config.SystemDir,
		config.SessionFile,
	)
	return os.Remove(sessionPath)
}


// =======================
// SESSION
// =======================

func writeSession(username, role string) error {
	sessionPath := filepath.Join(
		config.DataDir,
		config.SystemDir,
		config.SessionFile,
	)

	file, err := os.Create(sessionPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(username + "|" + role)
	return err
}

func CurrentUser() (*User, error) {
	sessionPath := filepath.Join(
		config.DataDir,
		config.SystemDir,
		config.SessionFile,
	)

	file, err := os.Open(sessionPath)
	if err != nil {
		return nil, errors.New("can login heula")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) == 2 {
			return &User{
				Username: parts[0],
				Role:     parts[1],
			}, nil
		}
	}

	return nil, errors.New("session teu valid")
}

// =======================
// ROLE CHECK
// =======================

func RequireRole(minRole string) error {
	user, err := CurrentUser()
	if err != nil {
		return err
	}

	userLevel := config.Roles[user.Role]
	requiredLevel := config.Roles[minRole]

	if userLevel > requiredLevel {
		return errors.New("hak aksés teu cukup")
	}

	return nil
}
