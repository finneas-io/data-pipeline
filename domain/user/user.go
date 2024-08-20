package user

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

type User struct {
	Id       uuid.UUID
	Username string
	Password string
}

type Session struct {
	Token     string
	User      *User
	ExpiresAt time.Time
}

func GenerateToken(now time.Time) (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	// we use the time and a random string to avoid collision
	str := now.Format(time.RFC3339) + string(b)
	sum := md5.Sum([]byte(str))
	result := hex.EncodeToString(sum[:])
	return result, nil
}

func EncryptSHA256(str string) (string, error) {
	h := sha256.New()
	_, err := h.Write([]byte(str))
	if err != nil {
		return "", err
	}
	b := h.Sum(nil)
	return hex.EncodeToString(b), nil
}
