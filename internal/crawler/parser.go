package crawler

import (
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

type Page struct {
	Title string
	Text  string
	Links []string
	Lang  string
	// LinkDensity 链接文字占全页文字比例，用于识别导航页/列表页
	LinkDensity float64
	// Confident 是否成功定位到正文容器（false 表示退化为整页文本）
	Confident bool
	// Kind article=文章页，listing=列表/聚合页，unknown=无法判断
	Kind string
	// Rule 命中的清洗规则 ID，便于排查
	Rule string
}

// 结构性丢弃：这些标签永远不是正文
var dropTags = map[string]bool{
	"script": true, "style": true, "noscript": true, "svg": true,
	"iframe": true, "template": true, "canvas": true, "video": true,
	"audio": true, "form": true, "button": true, "select": true,
	"textarea": true, "input": true, "option": true, "menu": true,
	"dialog": true, "head": true,
}

// 块级标签：渲染时换行，保证段落结构
var blockTags = map[string]bool{
	"p": true, "div": true, "li": true, "tr": true, "br": true, "td": true, "dd": true,
	"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true,
	"pre": true, "blockquote": true, "section": true, "article": true, "main": true,
	"ul": true, "ol": true, "table": true, "figure": true, "figcaption": true,
}

func Parse(rawURL string, body []byte) *Page {
	p := &Page{}
	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return p
	}

	rule := PickRule(hostOf(rawURL), lowerHTML(body))
	ex := newExtractor(rule)
	ex.prepare(doc)
	p.Rule = rule.ID

	p.Lang = attrOf(doc, "html", "lang")
	p.Links = collectLinks(doc, rawURL)
	p.Title = ex.title(doc)
	p.Text, p.Confident = ex.content(doc)
	if len(p.Text) > 200000 {
		p.Text = p.Text[:200000]
	}
	p.LinkDensity = nodeLinkDensity(doc, nil)
	p.Kind = classify(doc, p)

	if p.Title == "" {
		p.Title = firstLine(p.Text, 60)
	}
	return p
}

// lowerHTML 返回用于特征匹配的小写 HTML（超大页面只取首尾，够用且省内存）。
func lowerHTML(body []byte) string {
	const head, tail = 1 << 20, 512 << 10
	s := body
	if len(s) > head+tail {
		s = append(append([]byte{}, s[:head]...), s[len(s)-tail:]...)
	}
	return strings.ToLower(string(s))
}

// ---------- 抽取器 ----------

type extractor struct {
	rule     *SiteRule
	strip    []selector
	stripped map[*html.Node]bool
	textLen  map[*html.Node]int
	total    int
	fullDone bool
	full     string
}

func newExtractor(rule *SiteRule) *extractor {
	e := &extractor{rule: rule}
	for _, s := range rule.Strip {
		if sel := parseSelector(s); sel.raw != "" {
			e.strip = append(e.strip, sel)
		}
	}
	return e
}

func (e *extractor) minLen() int {
	if e.rule.MinLen > 0 {
		return e.rule.MinLen
	}
	return 120
}

// prepare 预处理：一次后序遍历统计各节点文字量，再一次前序遍历标记需剔除的块。
// 这样渲染时只查表，避免每层重复统计。
func (e *extractor) prepare(doc *html.Node) {
	e.textLen = make(map[*html.Node]int, 1024)

	var measure func(*html.Node) int
	measure = func(n *html.Node) int {
		if n.Type == html.ElementNode && dropTags[strings.ToLower(n.Data)] {
			return 0
		}
		total := 0
		if n.Type == html.TextNode {
			total = len([]rune(strings.TrimSpace(n.Data)))
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			total += measure(c)
		}
		if n.Type == html.ElementNode {
			e.textLen[n] = total
		}
		return total
	}
	e.total = measure(doc)

	e.stripped = make(map[*html.Node]bool, 256)
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type != html.ElementNode {
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				walk(c)
			}
			return
		}
		tag := strings.ToLower(n.Data)
		// body/html 永远保留：页面级 class 常带 no-navbar 之类的装饰词
		if tag == "body" || tag == "html" {
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				walk(c)
			}
			return
		}
		if dropTags[tag] || tag == "nav" || tag == "footer" || tag == "aside" || tag == "header" {
			e.stripped[n] = true
			return
		}
		for _, s := range e.strip {
			if s.match(n) {
				e.stripped[n] = true
				return
			}
		}
		if e.hitStripKW(n) {
			e.stripped[n] = true
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
}

func (e *extractor) hitStripKW(n *html.Node) bool {
	if len(e.rule.StripKW) == 0 {
		return false
	}
	s := attrString(n)
	if s == "" {
		return false
	}
	hit := false
	for _, k := range e.rule.StripKW {
		if strings.Contains(s, k) {
			hit = true
			break
		}
	}
	if !hit {
		return false
	}
	// 大字块且带正文特征词的容器豁免，「post-content-footer」这类小块仍会剔除
	if e.textLen[n] > 500 {
		for _, k := range e.rule.KeepKW {
			if strings.Contains(s, k) {
				return false
			}
		}
	}
	return true
}

func (e *extractor) skip(n *html.Node) bool { return e.stripped[n] }

func (e *extractor) render(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(x *html.Node) {
		if x.Type == html.ElementNode {
			if e.stripped[x] {
				return
			}
			if blockTags[strings.ToLower(x.Data)] {
				b.WriteByte('\n')
			}
		}
		if x.Type == html.TextNode {
			if t := strings.TrimSpace(x.Data); t != "" {
				b.WriteString(t)
				b.WriteByte(' ')
			}
		}
		for c := x.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return b.String()
}

func (e *extractor) text(n *html.Node) string { return normalizeSpace(e.render(n)) }

// ---------- 标题 ----------

func (e *extractor) title(doc *html.Node) string {
	for _, s := range e.rule.Title {
		sel := parseSelector(s)
		if sel.raw == "" {
			continue
		}
		for _, n := range queryAll(doc, sel) {
			if e.stripped[n] {
				continue
			}
			if t := e.text(n); t != "" && len([]rune(t)) <= 200 {
				return cleanTitle(t)
			}
		}
	}
	return cleanTitle(extractTitle(doc))
}

func extractTitle(doc *html.Node) string {
	var title, og, h1 string

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch strings.ToLower(n.Data) {
			case "title":
				if title == "" {
					title = normalizeSpace(textOf(n))
				}
			case "meta":
				if og == "" {
					var prop, content string
					for _, a := range n.Attr {
						switch strings.ToLower(a.Key) {
						case "property", "name":
							prop = strings.ToLower(a.Val)
						case "content":
							content = a.Val
						}
					}
					if (prop == "og:title" || prop == "twitter:title") && content != "" {
						og = normalizeSpace(content)
					}
				}
			case "h1":
				if h1 == "" {
					h1 = normalizeSpace(textOf(n))
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)

	for _, cand := range []string{og, title, h1} {
		if cand != "" && len([]rune(cand)) <= 200 {
			return cand
		}
	}
	return h1
}

// 标题尾巴上的站点名，命中则去掉
var titleSuffixes = []string{
	" - 博客园", "_博客园", "-CSDN博客", "_CSDN博客", " - CSDN博客", "- 知乎", " - 知乎",
	" - 简书", "_简书", " - 掘金", " - 思否", " - SegmentFault", " | 菜鸟教程",
	" - 阮一峰的网络日志", " - 少数派", " - 36氪", " - 澎湃新闻", " - 虎扑",
	" | Medium", " - DEV Community", " - Stack Overflow", " - 维基百科，自由的百科全书",
}

func cleanTitle(s string) string {
	s = normalizeSpace(s)
	for _, suf := range titleSuffixes {
		if strings.HasSuffix(s, suf) && len([]rune(s)) > len([]rune(suf))+4 {
			s = strings.TrimSpace(strings.TrimSuffix(s, suf))
			break
		}
	}
	return s
}

// ---------- 正文 ----------

func (e *extractor) content(doc *html.Node) (string, bool) {
	generic := e.rule.ID == "generic"
	for _, s := range e.rule.Content {
		sel := parseSelector(s)
		if sel.raw == "" {
			continue
		}
		if t, ok := e.bestIn(doc, sel, generic); ok {
			return t, true
		}
	}
	// 规则选择器都没命中：段落密度打分兜底
	return e.density(doc)
}

// bestIn 在候选节点中取「文本量 × (1-链接密度)」最高者。
func (e *extractor) bestIn(doc *html.Node, sel selector, needCoverage bool) (string, bool) {
	nodes := queryAll(doc, sel)
	if len(nodes) == 0 {
		return "", false
	}
	minLen := e.minLen()

	var best *html.Node
	bestScore := 0.0
	bestText := ""

	for _, n := range nodes {
		if e.stripped[n] {
			continue
		}
		raw := e.render(n)
		txt := cleanText(raw)
		l := len([]rune(txt))
		if l < minLen {
			continue
		}
		ld := nodeLinkDensity(n, e.skip)
		if ld > 0.45 {
			continue
		}
		score := float64(l) * (1 - ld)
		if score > bestScore {
			best, bestScore, bestText = n, score, txt
		}
	}
	if best == nil {
		return "", false
	}
	// 通用规则额外要求候选占全页一定比例，避免把列表页的某张卡片当正文
	if needCoverage {
		if !e.fullDone {
			e.full = cleanText(e.render(doc))
			e.fullDone = true
		}
		fl := len([]rune(e.full))
		l := len([]rune(bestText))
		if fl > 0 && float64(l)/float64(fl) < 0.18 && l < 1200 {
			return "", false
		}
	}
	_ = best
	return bestText, true
}

// density 段落密度打分：长且带标点的段落给祖先容器加分。
func (e *extractor) density(doc *html.Node) (string, bool) {
	scores := map[*html.Node]float64{}

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			if e.stripped[n] {
				return
			}
			switch strings.ToLower(n.Data) {
			case "p", "pre", "blockquote", "dd", "li", "td":
				text := textOfNode(n, e.skip)
				l := len([]rune(text))
				if l >= 24 {
					w := 1.0 + float64(countPunct(text))/12.0
					if l > 300 {
						w *= 1.2
					}
					cur := n.Parent
					for d := 0; cur != nil && d < 3; d++ {
						if cur.Type == html.ElementNode && !e.stripped[cur] {
							scores[cur] += w / float64(d+1)
						}
						cur = cur.Parent
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)

	var best *html.Node
	bestScore := 0.0
	for n, s := range scores {
		s *= classFactor(n)
		if s > bestScore {
			best, bestScore = n, s
		}
	}
	if best == nil {
		return cleanText(e.render(doc)), false
	}

	text := e.render(best)
	// 兄弟合并：正文常被拆成多个并列 div
	if best.Parent != nil {
		for sib := best.Parent.FirstChild; sib != nil; sib = sib.NextSibling {
			if sib == best || sib.Type != html.ElementNode || e.stripped[sib] {
				continue
			}
			if ld := nodeLinkDensity(sib, e.skip); ld > 0.35 {
				continue
			}
			if st := e.render(sib); len([]rune(st)) >= 120 {
				text += "\n" + st
			}
		}
	}
	clean := cleanText(text)
	if len([]rune(clean)) >= 180 {
		return clean, true
	}
	return cleanText(e.render(doc)), false
}

// scoreHints 打分阶段的正文特征词，比规则里的 KeepKW 宽松（不影响剔除判断）
var scoreHints = []string{
	"article", "post", "content", "entry", "main", "body", "markdown",
	"detail", "story", "essay", "rich_media", "blogpost", "text",
}

func classFactor(n *html.Node) float64 {
	s := attrString(n)
	if s == "" {
		return 1
	}
	for _, h := range scoreHints {
		if strings.Contains(s, h) {
			return 1.8
		}
	}
	return 1
}

func attrString(n *html.Node) string {
	var sb strings.Builder
	for _, a := range n.Attr {
		switch strings.ToLower(a.Key) {
		case "id", "class", "role", "itemprop":
			sb.WriteString(strings.ToLower(a.Val))
			sb.WriteByte(' ')
		}
	}
	return sb.String()
}

func countPunct(s string) int {
	n := 0
	for _, r := range s {
		switch r {
		case ',', '.', '!', '?', ';', ':', '，', '。', '！', '？', '；', '：', '、':
			n++
		}
	}
	return n
}

// ---------- 渲染与统计 ----------

func textOfNode(n *html.Node, skip func(*html.Node) bool) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(x *html.Node) {
		if x.Type == html.ElementNode {
			if skip != nil && skip(x) {
				return
			}
			if blockTags[strings.ToLower(x.Data)] {
				b.WriteByte('\n')
			}
		}
		if x.Type == html.TextNode {
			if t := strings.TrimSpace(x.Data); t != "" {
				b.WriteString(t)
				b.WriteByte(' ')
			}
		}
		for c := x.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return normalizeSpace(b.String())
}

func textOf(n *html.Node) string { return textOfNode(n, nil) }

func nodeLinkDensity(n *html.Node, skip func(*html.Node) bool) float64 {
	var total, linkText float64
	var walk func(*html.Node, bool)
	walk = func(x *html.Node, inLink bool) {
		if x.Type == html.ElementNode {
			tag := strings.ToLower(x.Data)
			if dropTags[tag] {
				return
			}
			if skip != nil && skip(x) {
				return
			}
			if tag == "a" {
				inLink = true
			}
		}
		if x.Type == html.TextNode {
			l := float64(len([]rune(strings.TrimSpace(x.Data))))
			total += l
			if inLink {
				linkText += l
			}
		}
		for c := x.FirstChild; c != nil; c = c.NextSibling {
			walk(c, inLink)
		}
	}
	walk(n, false)
	if total == 0 {
		return 0
	}
	return linkText / total
}

// ---------- 文本清洗 ----------

var (
	reZeroWidth  = regexp.MustCompile(`[\x{200b}-\x{200f}\x{feff}\x{00ad}]`)
	reMultiSpace = regexp.MustCompile(`[ \t\x{00a0}\x{3000}]+`)
)

var boilerplateLines = []string{
	"ICP备", "ICP证", "公网安备", "版权所有", "免责声明", "免责条款",
	"扫码关注", "扫描二维码", "点击关注", "微信公众号", "微信扫一扫",
	"Copyright ©", "All Rights Reserved", "Powered by",
	"上一篇", "下一篇", "相关阅读", "相关推荐", "热门推荐", "猜你喜欢",
	"本文转载", "转载请注明", "未经授权", "举报", "责任编辑",
}

func cleanText(s string) string {
	s = reZeroWidth.ReplaceAllString(s, "")
	lines := strings.Split(s, "\n")
	seen := make(map[string]bool, len(lines))
	out := make([]string, 0, len(lines))

	for _, raw := range lines {
		line := normalizeSpace(reMultiSpace.ReplaceAllString(raw, " "))
		if line == "" {
			continue
		}
		r := []rune(line)
		if len(r) < 2 || !hasContentRune(line) {
			continue
		}
		if isBoilerplateLine(line) {
			continue
		}
		// 长行去重：导航/模板文字常整段重复
		if len(r) >= 16 {
			if seen[line] {
				continue
			}
			seen[line] = true
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func hasContentRune(s string) bool {
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r > 0x4e00 {
			return true
		}
	}
	return false
}

func isBoilerplateLine(s string) bool {
	if len([]rune(s)) > 80 {
		return false
	}
	for _, b := range boilerplateLines {
		if strings.Contains(s, b) {
			return true
		}
	}
	return false
}

// ---------- 页面类型 ----------

func classify(doc *html.Node, p *Page) string {
	textLen := len([]rune(p.Text))
	if textLen < 200 {
		return "listing"
	}
	h1 := findH1(doc)
	title := normalizeSpace(p.Title)

	if h1 != "" && textLen > 400 {
		if similar(h1, title) || len([]rune(h1)) > 8 {
			return "article"
		}
	}
	if p.Confident && textLen > 600 {
		return "article"
	}
	// 列表页特征：大量日期、正文偏少
	if countDates(p.Text) >= 3 && textLen < 3000 {
		return "listing"
	}
	if !p.Confident {
		return "listing"
	}
	return "unknown"
}

func findH1(doc *html.Node) string {
	var h1 string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if h1 != "" {
			return
		}
		if n.Type == html.ElementNode && strings.EqualFold(n.Data, "h1") {
			h1 = normalizeSpace(textOf(n))
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return h1
}

func similar(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	ar, br := []rune(a), []rune(b)
	short := ar
	if len(br) < len(ar) {
		short = br
	}
	hit := 0
	for _, r := range short {
		if strings.ContainsRune(a, r) && strings.ContainsRune(b, r) {
			hit++
		}
	}
	return float64(hit)/float64(len(short)) > 0.5
}

var reDate = regexp.MustCompile(`20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}`)

func countDates(s string) int { return len(reDate.FindAllString(s, -1)) }

// ---------- 链接 ----------

func collectLinks(doc *html.Node, rawURL string) []string {
	var links []string
	seen := map[string]bool{}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			tag := strings.ToLower(n.Data)
			if dropTags[tag] && tag != "head" {
				return
			}
			if tag == "a" {
				for _, a := range n.Attr {
					if strings.EqualFold(a.Key, "href") {
						if abs := resolve(rawURL, a.Val); abs != "" && !seen[abs] {
							seen[abs] = true
							links = append(links, abs)
						}
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	if len(links) > 800 {
		links = links[:800]
	}
	return links
}

func attrOf(doc *html.Node, tag, key string) string {
	var val string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if val != "" {
			return
		}
		if n.Type == html.ElementNode && strings.EqualFold(n.Data, tag) {
			for _, a := range n.Attr {
				if strings.EqualFold(a.Key, key) {
					val = a.Val
					return
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return val
}

func resolve(base, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") ||
		strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") ||
		strings.HasPrefix(href, "tel:") || strings.HasPrefix(href, "data:") {
		return ""
	}
	b, err := url.Parse(base)
	if err != nil {
		return ""
	}
	h, err := url.Parse(href)
	if err != nil {
		return ""
	}
	abs := b.ResolveReference(h)
	if abs.Scheme != "http" && abs.Scheme != "https" {
		return ""
	}
	abs.Fragment = ""
	q := abs.Query()
	for _, k := range []string{"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "spm", "from"} {
		q.Del(k)
	}
	abs.RawQuery = q.Encode()
	return abs.String()
}

func normalizeSpace(s string) string {
	var b strings.Builder
	space := false
	for _, r := range s {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' || r == '\u00a0' || r == '\u3000' {
			if !space {
				b.WriteByte(' ')
				space = true
			}
			continue
		}
		space = false
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func firstLine(s string, n int) string {
	rs := []rune(s)
	if len(rs) <= n {
		return s
	}
	return string(rs[:n])
}
