package crawler

import (
	"net/url"
	"strings"

	"golang.org/x/net/html"
)

type Page struct {
	Title string
	Text  string
	Links []string
	Lang  string
}

var skipTags = map[string]bool{
	"script": true, "style": true, "noscript": true, "head": true,
	"svg": true, "iframe": true, "template": true, "meta": true, "link": true,
}

func Parse(rawURL string, body []byte) *Page {
	p := &Page{}
	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return p
	}

	var textBuf strings.Builder
	var links []string
	seen := map[string]bool{}

	var walk func(*html.Node, bool)
	walk = func(n *html.Node, inTitle bool) {
		if n.Type == html.ElementNode {
			tag := strings.ToLower(n.Data)
			if tag == "html" {
				for _, a := range n.Attr {
					if a.Key == "lang" {
						p.Lang = a.Val
					}
				}
			}
			if tag == "title" {
				if n.FirstChild != nil {
					p.Title = strings.TrimSpace(n.FirstChild.Data)
				}
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
			if skipTags[tag] {
				return
			}
		}
		if n.Type == html.TextNode {
			t := strings.TrimSpace(n.Data)
			if t != "" {
				textBuf.WriteString(t)
				textBuf.WriteByte(' ')
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, inTitle)
		}
	}
	walk(doc, false)

	p.Text = normalizeSpace(textBuf.String())
	if len(p.Text) > 200000 {
		p.Text = p.Text[:200000]
	}
	if len(links) > 800 {
		links = links[:800]
	}
	p.Links = links
	if p.Title == "" {
		p.Title = firstLine(p.Text, 60)
	}
	return p
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
	// 去掉常见追踪参数
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
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' || r == '\u00a0' {
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
	if len(s) <= n {
		return s
	}
	rs := []rune(s)
	if len(rs) <= n {
		return s
	}
	return string(rs[:n])
}
