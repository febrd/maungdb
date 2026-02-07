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
	Username  string
	Role      string
	Databases []string
	Database  string 
}

func userFilePath() string {
	return filepath.Join(
		config.DataDir,
		config.SystemDir,
		"users.maung",
	)
}

func sessionFilePath() string {
	return filepath.Join(
		config.DataDir,
		config.SystemDir,
		config.SessionFile,
	)
}

func Login(username, password string) error {
	file, err := os.Open(userFilePath())
	if err != nil {
		return errors.New("system user file teu kapanggih")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		user, hash, err := parseUser(line)
		if err != nil {
			continue
		}

		if user.Username == username &&
			bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil {

			user.Database = "" 
			return writeSession(user)
		}
	}

	return errors.New("Username/Password Salah")
}

func Logout() error {
	return os.Remove(sessionFilePath())
}

func writeSession(u *User) error {
	dbs := strings.Join(u.Databases, ",")

	line := strings.Join([]string{
		u.Username,
		u.Role,
		dbs,
		u.Database,
	}, "|")

	return os.WriteFile(sessionFilePath(), []byte(line), 0644)
}

func CurrentUser() (*User, error) {
	data, err := os.ReadFile(sessionFilePath())
	if err != nil {
		return nil, errors.New("can login heula")
	}

	parts := strings.Split(strings.TrimSpace(string(data)), "|")
	if len(parts) < 4 {
		return nil, errors.New("session teu valid")
	}

	dbs := []string{}
	if parts[2] != "" && parts[2] != "*" {
		dbs = strings.Split(parts[2], ",")
	}

	return &User{
		Username:  parts[0],
		Role:      parts[1],
		Databases: dbs,
		Database:  parts[3],
	}, nil
}

func SetDatabase(db string) error {
	u, err := CurrentUser()
	if err != nil {
		return err
	}

	if u.Role != "supermaung" {
		allowed := false
		for _, d := range u.Databases {
			if d == db {
				allowed = true
				break
			}
		}
		if !allowed {
			return errors.New("teu boga aksés ka database ieu")
		}
	}

	u.Database = db
	return writeSession(u)
}

func RequireRole(minRole string) error {
	u, err := CurrentUser()
	if err != nil {
		return err
	}

	if config.Roles[u.Role] > config.Roles[minRole] {
		return errors.New("hak aksés teu cukup")
	}
	return nil
}

func RequireDatabase() error {
	u, err := CurrentUser()
	if err != nil {
		return err
	}

	if u.Database == "" {
		return errors.New("can make / use database heula")
	}
	return nil
}

func parseUser(line string) (*User, string, error) {
	parts := strings.Split(line, "|")
	if len(parts) < 4 {
		return nil, "", errors.New("format user teu valid")
	}

	dbs := []string{}
	if parts[3] != "" && parts[3] != "*" {
		dbs = strings.Split(parts[3], ",")
	}

	return &User{
		Username:  parts[0],
		Role:      parts[2],
		Databases: dbs,
	}, parts[1], nil
}

func CreateUser(name, pass, role string) error {
	hash, _ := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)

	line := strings.Join([]string{
		name,
		string(hash),
		role,
		"",
	}, "|") + "\n"

	return appendToUserFile(line)
}

func SetUserDatabases(username string, dbs []string) error {
	return updateUser(username, func(u *User) {
		u.Databases = dbs
	})
}

func ChangePassword(username, newpass string) error {
	hash, _ := bcrypt.GenerateFromPassword([]byte(newpass), bcrypt.DefaultCost)
	return updateUserRaw(username, func(parts []string) {
		parts[1] = string(hash)
	})
}

func ListUsers() ([]string, error) {
	return readAllUsers()
}

func appendToUserFile(line string) error {
	f, err := os.OpenFile(userFilePath(), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(line)
	return err
}

func readAllUsers() ([]string, error) {
	file, err := os.Open(userFilePath())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var users []string
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		users = append(users, sc.Text())
	}
	return users, nil
}

func updateUser(username string, fn func(*User)) error {
	return updateUserRaw(username, func(parts []string) {
		u, _, err := parseUser(strings.Join(parts, "|"))
		if err != nil {
			return
		}
		fn(u)
		parts[3] = strings.Join(u.Databases, ",")
	})
}

func updateUserRaw(username string, fn func([]string)) error {
	file, err := os.Open(userFilePath())
	if err != nil {
		return err
	}
	defer file.Close()

	var lines []string
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), "|")
		if parts[0] == username {
			fn(parts)
			lines = append(lines, strings.Join(parts, "|"))
		} else {
			lines = append(lines, sc.Text())
		}
	}

	return os.WriteFile(
		userFilePath(),
		[]byte(strings.Join(lines, "\n")+"\n"),
		0644,
	)
}
