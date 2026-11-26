package crawler

import "testing"

func TestPickRuleByDomain(t *testing.T) {
	cases := []struct {
		host string
		want string
	}{
		{"www.cnblogs.com", "cnblogs"},
		{"cnblogs.com", "cnblogs"},
		{"blog.csdn.net", "csdn"},
		{"juejin.cn", "juejin"},
		{"www.zhihu.com", "zhihu"},
		{"zhuanlan.zhihu.com", "zhihu"},
		{"news.ycombinator.com", "hn"},
		{"example.org", "generic"},
		{"unknown.invalid", "generic"},
	}
	for _, c := range cases {
		if got := PickRule(c.host, "").ID; got != c.want {
			t.Errorf("PickRule(%q) = %q, want %q", c.host, got, c.want)
		}
	}
}

func TestPickRuleByGenerator(t *testing.T) {
	html := `<html><head><meta name="generator" content="WordPress 6.4" />
<link rel="stylesheet" href="/wp-content/themes/x/style.css" /></head>
<body><div class="entry-content">hi</div></body></html>`
	if got := PickRule("blog.example.com", lowerHTML([]byte(html))).ID; got != "wordpress" {
		t.Errorf("wordpress 指纹未命中，得到 %q", got)
	}

	html2 := `<html><body><div id="postlist"><div class="t_f">内容</div></div>
<div id="frs">Powered By Discuz! X3.4</div></body></html>`
	if got := PickRule("bbs.example.com", lowerHTML([]byte(html2))).ID; got != "discuz" {
		t.Errorf("discuz 指纹未命中，得到 %q", got)
	}
}

func TestDomainBeatsGenerator(t *testing.T) {
	html := `<div id="frs">Powered By Discuz!</div>`
	if got := PickRule("www.csdn.net", lowerHTML([]byte(html))).ID; got != "csdn" {
		t.Errorf("域名规则应优先，得到 %q", got)
	}
}

func TestSelectorParse(t *testing.T) {
	s := parseSelector("div.post-body#main[data-x=y]")
	if s.tag != "div" || s.id != "main" || len(s.classes) != 1 || s.classes[0] != "post-body" {
		t.Fatalf("解析结果异常: %+v", s)
	}
	if len(s.attrs) != 1 || s.attrs[0].k != "data-x" || s.attrs[0].v != "y" {
		t.Fatalf("属性解析异常: %+v", s.attrs)
	}
}

func TestGenericRuleStripsNav(t *testing.T) {
	html := `<html><body>
<nav>首页 产品 关于</nav>
<article><h1>标题</h1><p>` + longText(80) + `</p></article>
<footer>版权所有</footer>
</body></html>`
	p := Parse("https://example.org/a", []byte(html))
	if p.Rule != "generic" {
		t.Fatalf("应命中通用规则，得到 %q", p.Rule)
	}
	if contains(p.Text, "首页 产品") {
		t.Errorf("导航未被剔除: %q", firstLine(p.Text, 80))
	}
	if !contains(p.Text, "正文段落") {
		t.Errorf("正文丢失: %q", firstLine(p.Text, 80))
	}
}

func TestSiteRuleOverridesGeneric(t *testing.T) {
	body := `<html><body>
<div id="navigator">博客园 首页 新随笔</div>
<div id="cnblogs_post_body"><p>` + longText(300) + `</p></div>
<div class="comment">评论内容不该进来</div>
</body></html>`
	p := Parse("https://www.cnblogs.com/x/p/1", []byte(body))
	if p.Rule != "cnblogs" {
		t.Fatalf("应命中 cnblogs 规则，得到 %q", p.Rule)
	}
	if contains(p.Text, "博客园 首页") {
		t.Errorf("导航未剔除")
	}
	if contains(p.Text, "评论内容") {
		t.Errorf("评论未剔除")
	}
}

func longText(n int) string {
	base := "这是一段用于测试的正文段落，内容需要足够长。"
	out := ""
	for len([]rune(out)) < n {
		out += base
	}
	return out
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
