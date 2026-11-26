package cluster

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 节点内部通信使用 HMAC-SHA256 签名：
//
//	X-QZ-Node: 节点 ID
//	X-QZ-Ts:   Unix 秒
//	X-QZ-Sign: HMAC(secret, nodeID\nts\nMETHOD\npath\nbody)
//
// 每个节点一把独立密钥，主节点在管理后台创建节点时生成，节点首次 enroll 时领取。
const (
	HdrNode = "X-QZ-Node"
	HdrTs   = "X-QZ-Ts"
	HdrSign = "X-QZ-Sign"

	signSkew = 300 * time.Second
)

var (
	ErrNoAuth     = errors.New("缺少节点签名")
	ErrBadAuth    = errors.New("节点签名校验失败")
	ErrExpired    = errors.New("签名时间戳超出允许范围")
	ErrNodeDenied = errors.New("节点未注册或被禁用")
)

// RandToken 生成 n 字节随机数的十六进制串（2n 个字符）。
func RandToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func signPayload(secret, nodeID, ts, method, path string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%s\n%s\n%s\n%s\n", nodeID, ts, method, path)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// SignRequest 给请求写入鉴权头。
func SignRequest(req *http.Request, secret, nodeID string, body []byte) {
	if secret == "" {
		return
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set(HdrNode, nodeID)
	req.Header.Set(HdrTs, ts)
	req.Header.Set(HdrSign, signPayload(secret, nodeID, ts, req.Method, req.URL.Path, body))
}

// VerifyRequest 校验请求签名，返回节点 ID。
// secretFor 由调用方提供：按节点 ID 查密钥，并判断节点是否可用。
func VerifyRequest(r *http.Request, body []byte, secretFor func(nodeID string) (string, error)) (string, error) {
	nodeID := strings.TrimSpace(r.Header.Get(HdrNode))
	tsStr := strings.TrimSpace(r.Header.Get(HdrTs))
	sign := strings.TrimSpace(r.Header.Get(HdrSign))
	if nodeID == "" || tsStr == "" || sign == "" {
		return "", ErrNoAuth
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", ErrBadAuth
	}
	if d := time.Since(time.Unix(ts, 0)); d > signSkew || d < -signSkew {
		return "", ErrExpired
	}
	secret, err := secretFor(nodeID)
	if err != nil {
		return "", err
	}
	if secret == "" {
		return "", ErrBadAuth
	}
	want := signPayload(secret, nodeID, tsStr, r.Method, r.URL.Path, body)
	if !hmac.Equal([]byte(want), []byte(sign)) {
		return "", ErrBadAuth
	}
	return nodeID, nil
}
