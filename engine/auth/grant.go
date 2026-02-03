package auth

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/febrd/maungdb/internal/config"
)

func CanAccessDB(username, db string) bool {
	path := filepath.Join(config.DataDir, config.SystemDir, config.GrantsFile)
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	sc := bufio.NewScanner(file)
	for sc.Scan() {
		p := strings.Split(sc.Text(), "|")
		if len(p) != 3 {
			continue
		}
		if p[0] == username && (p[2] == db || p[2] == "*") {
			return true
		}
	}
	return false
}

func Grant(username, role, db string) error {
	path := filepath.Join(config.DataDir, config.SystemDir, config.GrantsFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(username + "|" + role + "|" + db + "\n")
	return err
}

func RequireDBAccess() error {
	u, err := CurrentUser()
	if err != nil {
		return err
	}
	if u.Role == "supermaung" {
		return nil
	}
	if u.Database == "" {
		return errors.New("can use database heula")
	}
	if !CanAccessDB(u.Username, u.Database) {
		return errors.New("teu boga aksés ka database ieu")
	}
	return nil
}
