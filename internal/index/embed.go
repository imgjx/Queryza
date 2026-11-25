package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"queryza/internal/config"
)

// Embedder 调用 OpenAI 兼容的 embeddings 接口。
type Embedder struct {
	cfg    config.EmbeddingConfig
	client *http.Client
}

func NewEmbedder(cfg config.EmbeddingConfig) *Embedder {
	t := cfg.Timeout
	if t <= 0 {
		t = 20
	}
	return &Embedder{cfg: cfg, client: &http.Client{Timeout: time.Duration(t) * time.Second}}
}

func (e *Embedder) Enabled() bool { return e != nil && e.cfg.Enabled && e.cfg.BaseURL != "" }

func (e *Embedder) Dim() int {
	if e.cfg.Dim > 0 {
		return e.cfg.Dim
	}
	return 1536
}

func (e *Embedder) Config() config.EmbeddingConfig { return e.cfg }

type embedReq struct {
	Input string `json:"input"`
	Model string `json:"model"`
}

type embedResp struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if !e.Enabled() {
		return nil, fmt.Errorf("embedding 未启用")
	}
	text = strings.TrimSpace(text)
	if len([]rune(text)) > 4000 {
		text = string([]rune(text)[:4000])
	}
	body, _ := json.Marshal(embedReq{Input: text, Model: e.cfg.Model})
	url := strings.TrimRight(e.cfg.BaseURL, "/") + "/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIKey)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("embedding 接口返回 %d: %s", resp.StatusCode, truncate(string(b), 200))
	}
	var er embedResp
	if err := json.Unmarshal(b, &er); err != nil {
		return nil, err
	}
	if er.Error != nil {
		return nil, fmt.Errorf("embedding 错误: %s", er.Error.Message)
	}
	if len(er.Data) == 0 || len(er.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("embedding 返回为空")
	}
	return er.Data[0].Embedding, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
