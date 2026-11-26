package crawler

import (
	"strings"

	"golang.org/x/net/html"
)

// SiteRule 一条正文清洗规则。
//
// 匹配顺序：域名后缀 → 页面特征串（CMS 指纹）→ 通用规则。
// 命中规则后按 Content 选择器取正文，按 Strip / StripKW 剪枝，
// 长度不足 MinLen 时回落通用抽取，保证不会「越洗越空」。
type SiteRule struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Priority int      `json:"priority"`
	Domains  []string `json:"domains"`  // 域名后缀，如 cnblogs.com
	Gens     []string `json:"gens"`     // 页面特征串（小写，同时满足才算命中）
	Content  []string `json:"content"`  // 正文容器选择器，按顺序尝试
	Strip    []string `json:"strip"`    // 需要整块剔除的选择器
	StripKW  []string `json:"strip_kw"` // id/class 关键词，命中即剔除
	KeepKW   []string `json:"keep_kw"`  // id/class 关键词，命中则保留（优先级高于 StripKW）
	Title    []string `json:"title"`    // 标题选择器，优先于 <title>
	MinLen   int      `json:"min_len"`
}

// ---------- 选择器 ----------

type attrKV struct{ k, v string }

type selector struct {
	tag     string
	id      string
	classes []string
	attrs   []attrKV
	raw     string
}

// parseSelector 支持 `div#id.cls[attr=val]`、`#id`、`.cls`、`[attr=val]`。
func parseSelector(s string) selector {
	out := selector{raw: s}
	s = strings.TrimSpace(s)
	if s == "" {
		return out
	}
	i := 0
	for i < len(s) && s[i] != '#' && s[i] != '.' && s[i] != '[' {
		i++
	}
	out.tag = strings.ToLower(s[:i])
	for i < len(s) {
		switch s[i] {
		case '#':
			i++
			st := i
			for i < len(s) && s[i] != '#' && s[i] != '.' && s[i] != '[' {
				i++
			}
			out.id = s[st:i]
		case '.':
			i++
			st := i
			for i < len(s) && s[i] != '#' && s[i] != '.' && s[i] != '[' {
				i++
			}
			out.classes = append(out.classes, s[st:i])
		case '[':
			end := strings.IndexByte(s[i:], ']')
			if end < 0 {
				return out
			}
			body := s[i+1 : i+end]
			i += end + 1
			k, v, _ := strings.Cut(body, "=")
			out.attrs = append(out.attrs, attrKV{
				k: strings.ToLower(strings.TrimSpace(k)),
				v: strings.Trim(strings.TrimSpace(v), `"'`),
			})
		default:
			i++
		}
	}
	return out
}

func (s selector) match(n *html.Node) bool {
	if n.Type != html.ElementNode {
		return false
	}
	if s.tag != "" && s.tag != "*" && !strings.EqualFold(n.Data, s.tag) {
		return false
	}
	if s.id != "" && attrVal(n, "id") != s.id {
		return false
	}
	if len(s.classes) > 0 {
		cls := " " + strings.ToLower(attrVal(n, "class")) + " "
		for _, c := range s.classes {
			if !strings.Contains(cls, " "+strings.ToLower(c)+" ") {
				return false
			}
		}
	}
	for _, a := range s.attrs {
		v := attrVal(n, a.k)
		if v == "" || !strings.EqualFold(v, a.v) {
			return false
		}
	}
	return true
}

func attrVal(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}

func queryAll(root *html.Node, s selector) []*html.Node {
	var out []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if s.match(n) {
			out = append(out, n)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return out
}

// ---------- 规则索引 ----------

var (
	ruleByDomain map[string][]*SiteRule
	ruleByGen    []*SiteRule
	genericRule  *SiteRule
	allRules     []*SiteRule
)

func init() {
	ruleByDomain = make(map[string][]*SiteRule, 256)
	allRules = builtinRules
	for _, r := range builtinRules {
		if r.ID == "generic" {
			genericRule = r
		}
		for _, d := range r.Domains {
			d = strings.ToLower(strings.TrimPrefix(d, "."))
			ruleByDomain[d] = append(ruleByDomain[d], r)
		}
		if len(r.Gens) > 0 {
			ruleByGen = append(ruleByGen, r)
		}
	}
	if genericRule == nil {
		genericRule = &SiteRule{ID: "generic", Name: "通用规则"}
	}
}

// PickRule 按域名 → CMS 指纹 → 通用规则 的顺序选规则。
// host 不区分大小写，可带端口；lowerHTML 为页面 HTML 的小写副本。
func PickRule(host, lowerHTML string) *SiteRule {
	host = strings.ToLower(host)
	if i := strings.LastIndexByte(host, ':'); i > 0 && !strings.Contains(host[i:], "]") {
		host = host[:i]
	}
	host = strings.TrimPrefix(host, "www.")

	if r := matchDomain(host); r != nil {
		return r
	}
	if r := matchGen(lowerHTML); r != nil {
		return r
	}
	return genericRule
}

func matchDomain(host string) *SiteRule {
	if host == "" {
		return nil
	}
	// 从最具体的域名逐级向上找，例如 a.b.cnblogs.com → b.cnblogs.com → cnblogs.com
	parts := strings.Split(host, ".")
	for i := 0; i < len(parts) && i < 4; i++ {
		cand := strings.Join(parts[i:], ".")
		if rs, ok := ruleByDomain[cand]; ok {
			return bestOf(rs)
		}
	}
	return nil
}

// matchGen 页面指纹匹配：规则内特征串全部出现才算命中，命中越多越优先。
func matchGen(lowerHTML string) *SiteRule {
	if lowerHTML == "" {
		return nil
	}
	var (
		best   *SiteRule
		bestN  = 0
		bestPr = -1
	)
	for _, r := range ruleByGen {
		hit := 0
		for _, g := range r.Gens {
			if !strings.Contains(lowerHTML, g) {
				hit = 0
				break
			}
			hit++
		}
		if hit == 0 {
			continue
		}
		if hit > bestN || (hit == bestN && r.Priority > bestPr) {
			best, bestN, bestPr = r, hit, r.Priority
		}
	}
	return best
}

func bestOf(rs []*SiteRule) *SiteRule {
	var best *SiteRule
	bestLen := -1
	for _, r := range rs {
		l := 0
		for _, d := range r.Domains {
			if len(d) > l {
				l = len(d)
			}
		}
		if best == nil || r.Priority > best.Priority || (r.Priority == best.Priority && l > bestLen) {
			best, bestLen = r, l
		}
	}
	return best
}

// RuleList 返回内置规则摘要，供管理后台展示。
func RuleList() []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(allRules))
	for _, r := range allRules {
		out = append(out, map[string]interface{}{
			"id": r.ID, "name": r.Name, "priority": r.Priority,
			"domains": len(r.Domains), "gens": len(r.Gens),
			"content": r.Content, "strip": r.Strip,
			"kind": ruleKind(r),
		})
	}
	return out
}

func ruleKind(r *SiteRule) string {
	switch {
	case len(r.Domains) > 0 && len(r.Gens) > 0:
		return "domain+gen"
	case len(r.Domains) > 0:
		return "domain"
	case len(r.Gens) > 0:
		return "generator"
	default:
		return "generic"
	}
}
