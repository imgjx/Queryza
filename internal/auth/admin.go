package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AdminAuth 管理后台的登录态：HMAC 签名 token。
type AdminAuth struct {
	user   string
	pass   string
	secret []byte
	ttl    time.Duration
}

func NewAdminAuth(user, pass string, ttlHours int) *AdminAuth {
	if user == "" {
		user = "admin"
	}
	if ttlHours <= 0 {
		ttlHours = 24
	}
	secret := make([]byte, 32)
	rand.Read(secret)
	return &AdminAuth{user: user, pass: pass, secret: secret, ttl: time.Duration(ttlHours) * time.Hour}
}

func (a *AdminAuth) Login(username, password string) (string, bool) {
	if subtle.ConstantTimeCompare([]byte(username), []byte(a.user)) != 1 {
		return "", false
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(a.pass)) != 1 {
		return "", false
	}
	exp := time.Now().Add(a.ttl).Unix()
	payload := fmt.Sprintf("%s|%d", a.user, exp)
	return payload + "." + a.sign(payload), true
}

func (a *AdminAuth) Verify(token string) bool {
	i := strings.LastIndex(token, ".")
	if i < 0 {
		return false
	}
	payload, sig := token[:i], token[i+1:]
	if !hmac.Equal([]byte(sig), []byte(a.sign(payload))) {
		return false
	}
	parts := strings.Split(payload, "|")
	if len(parts) != 2 {
		return false
	}
	exp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return false
	}
	return time.Now().Unix() < exp
}

func (a *AdminAuth) sign(payload string) string {
	m := hmac.New(sha256.New, a.secret)
	m.Write([]byte(payload))
	return hex.EncodeToString(m.Sum(nil))
}

func BasicAuthHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}
