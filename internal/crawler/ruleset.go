package crawler

// 内置规则库。
//
// 约定：
//   - Domains 用不带 www 的后缀，匹配时逐级向上找（a.b.example.com → b.example.com → example.com）
//   - Gens 为页面 HTML 小写副本中的特征串，全部命中才算匹配（用来认 CMS）
//   - Content 按顺序尝试，取第一个长度达标的容器
//   - StripKW 是 id/class 关键词，命中整块剔除；KeepKW 命中则豁免
//   - 规则命中但正文过短时会回落通用规则，不会把页面洗成空
var builtinRules = []*SiteRule{

	// ==================== 国内技术社区 / 博客 ====================
	{ID: "cnblogs", Name: "博客园", Priority: 100, Domains: []string{"cnblogs.com"},
		Content: []string{"#cnblogs_post_body", ".postBody", "#post_body"},
		Strip:   []string{"#navigator", "#blog_post_info_block", ".postNav", "#comments", "#sidebar", ".commentform", "#cnblogs_c1", ".clear"},
		Title:   []string{"#cb_post_title_url", ".postTitle a", "h1.postTitle"}, MinLen: 150},

	{ID: "csdn", Name: "CSDN", Priority: 100, Domains: []string{"csdn.net", "csdnimg.cn"},
		Content: []string{"#content_views", ".article_content", ".htmledit_views"},
		Strip:   []string{"#treeSkill", ".hide-article-box", ".recommend-box", ".toolbox", ".article-info-box", ".blog-tags-box", "#blogExtensionBox"},
		StripKW: []string{"comment", "recommend", "toolbar", "reward", "copyright-box"},
		Title:   []string{"h1.title-article", ".article-title-box h1", "h1"}, MinLen: 150},

	{ID: "juejin", Name: "稀土掘金", Priority: 100, Domains: []string{"juejin.cn"},
		Content: []string{".markdown-body", ".article-content", "article"},
		Strip:   []string{".article-suspended-panel", ".recommend-box", ".comment-box", ".follow-box"},
		Title:   []string{"h1.article-title", ".article-title"}, MinLen: 150},

	{ID: "oschina", Name: "开源中国", Priority: 100, Domains: []string{"oschina.net"},
		Content: []string{".article-detail", ".article-box", "#articleContent", ".content"},
		Strip:   []string{".ad-wrap", ".osc_git_box", ".article-footer", ".comment-list"},
		Title:   []string{".article-box__header h1", "h1"}, MinLen: 150},

	{ID: "jianshu", Name: "简书", Priority: 100, Domains: []string{"jianshu.com"},
		Content: []string{"article", ".show-content", "._2rhmJa"},
		Strip:   []string{".follow-detail", ".note-bottom", ".comment-list", "._3kba3h"},
		Title:   []string{"h1._1RuRku", "h1"}, MinLen: 150},

	{ID: "segmentfault", Name: "思否", Priority: 100, Domains: []string{"segmentfault.com"},
		Content: []string{".article-content", ".fmt", "article"},
		Strip:   []string{".article__cmt", ".widget", ".side", ".ad"},
		Title:   []string{".article__title", "h1"}, MinLen: 150},

	{ID: "51cto", Name: "51CTO", Priority: 100, Domains: []string{"51cto.com", "blog.51cto.com"},
		Content: []string{".article_content", "#content", ".article-content"},
		Strip:   []string{".recommend", ".comment", ".share-box"},
		Title:   []string{"h1", ".article-title"}, MinLen: 150},

	{ID: "infoq", Name: "InfoQ 中文", Priority: 100, Domains: []string{"infoq.cn"},
		Content: []string{".article-preview", ".content", "article"},
		Strip:   []string{".comment", ".recommend"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "ruanyifeng", Name: "阮一峰的网络日志", Priority: 100, Domains: []string{"ruanyifeng.com"},
		Content: []string{"#main-content", ".entry-content", ".post"},
		Strip:   []string{"#sidebar", ".comment", ".share"}, Title: []string{"h1", ".post-title"}, MinLen: 120},

	{ID: "liaoxuefeng", Name: "廖雪峰的官方网站", Priority: 100, Domains: []string{"liaoxuefeng.com"},
		Content: []string{"#content", ".x-wiki-content", "#main"},
		Strip:   []string{"#footer", ".x-sidebar", "#top"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "runoob", Name: "菜鸟教程", Priority: 100, Domains: []string{"runoob.com"},
		Content: []string{"#content", ".article-body", ".article-intro"},
		Strip:   []string{"#footer", ".sidebar", ".ad", "#respond"},
		Title:   []string{"h1"}, MinLen: 100},

	{ID: "w3school-cn", Name: "W3School", Priority: 100, Domains: []string{"w3school.com.cn"},
		Content: []string{"#main", "#content", ".article-body"},
		Strip:   []string{"#nav", "#footer", ".sidebar"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "v2ex", Name: "V2EX", Priority: 100, Domains: []string{"v2ex.com"},
		Content: []string{".topic_content", "#Main .box", ".cell"},
		Strip:   []string{".topic_buttons", ".reply_content", ".cell_ops", ".inner a"},
		Title:   []string{"h1"}, MinLen: 60},

	{ID: "zhihu", Name: "知乎", Priority: 100, Domains: []string{"zhihu.com"},
		Content: []string{".Post-RichText", ".RichContent-inner", ".RichText", ".ArticleItem"},
		Strip:   []string{".ContentItem-actions", ".Post-Sub", ".Recommendations", ".CommentArea", ".Reward", ".Pc-word"},
		Title:   []string{".QuestionHeader-title", ".Post-Title", "h1"}, MinLen: 100},

	{ID: "douban", Name: "豆瓣", Priority: 100, Domains: []string{"douban.com"},
		Content: []string{"#link-report", ".note-content", ".review-content", ".article"},
		Strip:   []string{"#comments", ".comment", ".side", ".aside", ".sns-bar"},
		Title:   []string{"h1", ".note-title"}, MinLen: 100},

	{ID: "tieba", Name: "百度贴吧", Priority: 100, Domains: []string{"tieba.baidu.com"},
		Content: []string{".d_post_content", ".post_content", ".article"},
		Strip:   []string{".post_bubble", ".d_post_content_abstract", ".core_reply", ".j_lzl_container"},
		Title:   []string{".core_title_txt", "h1"}, MinLen: 40},

	{ID: "baidu-baike", Name: "百度百科", Priority: 100, Domains: []string{"baike.baidu.com"},
		Content: []string{".J-lemma-content", ".main-content", ".lemmaWgt-lemmaTitle"},
		Strip:   []string{".lemma-catalog", ".side-box", ".reference-list", ".album-list"},
		Title:   []string{".lemmaWgt-lemmaTitle-title h1", "h1"}, MinLen: 100},

	{ID: "weibo", Name: "微博", Priority: 100, Domains: []string{"weibo.com", "weibo.cn"},
		Content: []string{".weibo-text", ".article", ".WB_text"},
		Strip:   []string{".WB_feed_handle", ".toolbar", ".comment"}, Title: []string{"h1"}, MinLen: 20},

	{ID: "sspai", Name: "少数派", Priority: 100, Domains: []string{"sspai.com"},
		Content: []string{".content", ".article", "article"},
		Strip:   []string{".article-actions", ".comment", ".related"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "cnbeta", Name: "cnBeta", Priority: 100, Domains: []string{"cnbeta.com", "cnbeta.com.tw"},
		Content: []string{".article-content", ".cnbeta-article", "#news_content"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1", ".title"}, MinLen: 100},

	{ID: "ithome", Name: "IT之家", Priority: 100, Domains: []string{"ithome.com"},
		Content: []string{".post_content", "#paragraph", ".news-content"},
		Strip:   []string{".comment", ".related-post", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "36kr", Name: "36氪", Priority: 100, Domains: []string{"36kr.com"},
		Content: []string{".article-content", ".common-width", "article"},
		Strip:   []string{".comment", ".recommend", ".aside"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "ifanr", Name: "爱范儿", Priority: 100, Domains: []string{"ifanr.com"},
		Content: []string{".article-content", ".entry-content", "article"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "tmtpost", Name: "钛媒体", Priority: 100, Domains: []string{"tmtpost.com"},
		Content: []string{".article-content", ".post-content", "article"},
		Strip:   []string{".comment", ".recommend"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "leiphone", Name: "雷锋网", Priority: 100, Domains: []string{"leiphone.com"},
		Content: []string{".article-content", ".content", "article"},
		Strip:   []string{".comment", ".related"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "geekpark", Name: "极客公园", Priority: 100, Domains: []string{"geekpark.net"},
		Content: []string{".article-content", ".content", "article"},
		Strip:   []string{".comment", ".related"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "qbitai", Name: "量子位", Priority: 100, Domains: []string{"qbitai.com"},
		Content: []string{".article-content", ".entry-content", "article"},
		Strip:   []string{".comment", ".related"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "jiqizhixin", Name: "机器之心", Priority: 100, Domains: []string{"jiqizhixin.com"},
		Content: []string{".article-content", ".post-content", "article"},
		Strip:   []string{".comment", ".related"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "thepaper", Name: "澎湃新闻", Priority: 100, Domains: []string{"thepaper.cn"},
		Content: []string{".news_txt", ".index_centent", ".article"},
		Strip:   []string{".comment", ".related", ".share"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "guokr", Name: "果壳", Priority: 100, Domains: []string{"guokr.com"},
		Content: []string{".article-content", ".content", "article"},
		Strip:   []string{".comment", ".related"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "163news", Name: "网易", Priority: 90, Domains: []string{"163.com"},
		Content: []string{".post_body", "#content", ".article-content", "article"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "sina", Name: "新浪", Priority: 90, Domains: []string{"sina.com.cn", "sina.cn"},
		Content: []string{"#artibody", ".article", "#article_content"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "sohu", Name: "搜狐", Priority: 90, Domains: []string{"sohu.com"},
		Content: []string{"article", "#mp-editor", ".article-content"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "ifeng", Name: "凤凰网", Priority: 90, Domains: []string{"ifeng.com"},
		Content: []string{".main_content", "#main_content", ".article-content"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "qq-news", Name: "腾讯网", Priority: 90, Domains: []string{"qq.com", "new.qq.com"},
		Content: []string{"#ArticleContent", ".content-article", ".article-content"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "xinhua", Name: "新华网", Priority: 90, Domains: []string{"xinhuanet.com", "news.cn"},
		Content: []string{"#detail", ".main-aticle", ".article"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "people", Name: "人民网", Priority: 90, Domains: []string{"people.com.cn"},
		Content: []string{".rm_txt_con", "#rwb_zw", ".article"},
		Strip:   []string{".comment", ".related", ".ad"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "bilibili", Name: "哔哩哔哩专栏", Priority: 100, Domains: []string{"bilibili.com"},
		Content: []string{".article-content", "#read-article-holder", ".opus-module-content"},
		Strip:   []string{".article-actions", ".comment", ".recommend"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "aliyun-dev", Name: "阿里云开发者社区", Priority: 100, Domains: []string{"developer.aliyun.com", "aliyun.com"},
		Content: []string{".article-content", ".markdown-body", "#article-content"},
		Strip:   []string{".comment", ".recommend", ".article-footer"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "tencent-cloud", Name: "腾讯云开发者社区", Priority: 100, Domains: []string{"cloud.tencent.com"},
		Content: []string{".rno-markdown", ".article-content", ".markdown-body", ".J-articleContent"},
		Strip:   []string{".comment", ".recommend", ".ad"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "huawei-cloud", Name: "华为云社区", Priority: 100, Domains: []string{"huaweicloud.com"},
		Content: []string{".article-content", ".markdown-body", ".content"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "imooc", Name: "慕课网", Priority: 100, Domains: []string{"imooc.com"},
		Content: []string{".article-content", ".course-content", ".content"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "ld246", Name: "链滴", Priority: 100, Domains: []string{"ld246.com"},
		Content: []string{".vditor-reset", ".article-content", "#article"}, Title: []string{"h1"}, MinLen: 80},

	{ID: "dgtle", Name: "数字尾巴", Priority: 100, Domains: []string{"dgtle.com"},
		Content: []string{".article-content", ".content", "article"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "smzdm", Name: "什么值得买", Priority: 100, Domains: []string{"smzdm.com"},
		Content: []string{".article-content", "#article", ".item-name"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "qidian", Name: "起点中文网", Priority: 100, Domains: []string{"qidian.com"},
		Content: []string{".read-content", "#content", ".j_readContent"}, Title: []string{"h1"}, MinLen: 300},

	{ID: "jjwxc", Name: "晋江文学城", Priority: 100, Domains: []string{"jjwxc.net"},
		Content: []string{"#noveltext", ".noveltext"}, Title: []string{"h2"}, MinLen: 300},

	{ID: "nga", Name: "NGA", Priority: 100, Domains: []string{"nga.cn", "ngacn.cc"},
		Content: []string{".postcontent", "#postcontent0", ".topic_content"}, Title: []string{"h1", "#postsubject0"}, MinLen: 60},

	{ID: "hupu", Name: "虎扑", Priority: 100, Domains: []string{"hupu.com"},
		Content: []string{".thread-content-detail", ".article-content", ".content"}, Title: []string{"h1"}, MinLen: 80},

	{ID: "tiexue", Name: "铁血网", Priority: 100, Domains: []string{"tiexue.net"},
		Content: []string{".article-content", "#content", ".text"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "zhihu-daily", Name: "知乎日报", Priority: 100, Domains: []string{"daily.zhihu.com"},
		Content: []string{".article", ".content", "article"}, Title: []string{"h1"}, MinLen: 100},

	// ==================== 政府 / 教育 ====================
	{ID: "gov-cn", Name: "政府网站", Priority: 50, Domains: []string{"gov.cn"},
		Content: []string{"#zoom", ".article-content", ".content", "#content", ".TRS_Editor"},
		Strip:   []string{".footer", ".nav", ".crumb", ".share"},
		StripKW: []string{"footer", "nav", "crumb", "share", "related"},
		Title:   []string{"h1", ".title"}, MinLen: 150},

	{ID: "edu-cn", Name: "教育网", Priority: 50, Domains: []string{"edu.cn"},
		Content: []string{".article-content", "#content", ".v_news_content", ".content", "article"},
		Strip:   []string{".footer", ".nav", ".share"}, Title: []string{"h1"}, MinLen: 120},

	// ==================== 国外站点 ====================
	{ID: "medium", Name: "Medium", Priority: 100, Domains: []string{"medium.com"},
		Content: []string{"article", ".section-content", ".postArticle-content"},
		Strip:   []string{".postActions", ".js-postShareWidget", ".postMetaInline"}, Title: []string{"h1"}, MinLen: 200},

	{ID: "devto", Name: "DEV Community", Priority: 100, Domains: []string{"dev.to"},
		Content: []string{"#article-body", ".crayons-article__body", "article"},
		Strip:   []string{".crayons-article__actions", "#comments"}, Title: []string{"h1"}, MinLen: 200},

	{ID: "stackoverflow", Name: "Stack Overflow", Priority: 100, Domains: []string{"stackoverflow.com", "stackexchange.com", "superuser.com", "serverfault.com", "askubuntu.com"},
		Content: []string{".js-post-body", "#mainbar .post-text", ".post-text"},
		Strip:   []string{".js-post-menu", ".post-signature", ".comments", "#sidebar", ".js-vote-buttons"},
		Title:   []string{"h1 a", "h1"}, MinLen: 120},

	{ID: "github", Name: "GitHub", Priority: 100, Domains: []string{"github.com", "gist.github.com"},
		Content: []string{"article.markdown-body", ".markdown-body", "#readme"},
		Strip:   []string{".file-navigation", ".repository-content .Box-header"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "gitlab", Name: "GitLab", Priority: 100, Domains: []string{"gitlab.com"},
		Content: []string{".md", ".file-content", "#content-body"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "reddit", Name: "Reddit", Priority: 100, Domains: []string{"reddit.com"},
		Content: []string{"[slot=text-body]", ".md", "shreddit-post"},
		Strip:   []string{"#comment-tree", ".commentarea"}, Title: []string{"h1"}, MinLen: 60},

	{ID: "hn", Name: "Hacker News", Priority: 100, Domains: []string{"news.ycombinator.com"},
		Content: []string{"#hnmain", ".comment-tree"},
		Strip:   []string{".navs", ".yclinks"}, Title: []string{"title"}, MinLen: 40},

	{ID: "wikipedia", Name: "维基百科", Priority: 100, Domains: []string{"wikipedia.org", "wikimedia.org", "wikiwand.com"},
		Content: []string{"#mw-content-text", ".mw-parser-output", "#content"},
		Strip:   []string{".mw-editsection", "#toc", ".navbox", ".reflist", ".catlinks", ".infobox"},
		Title:   []string{"#firstHeading", "h1"}, MinLen: 200},

	{ID: "mdn", Name: "MDN", Priority: 100, Domains: []string{"developer.mozilla.org"},
		Content: []string{"article.main-page-content", "#content", "main"},
		Strip:   []string{".sidebar", ".page-footer", ".prev-next"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "freecodecamp", Name: "freeCodeCamp", Priority: 100, Domains: []string{"freecodecamp.org"},
		Content: []string{".article-content", ".post-full-content", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "geeksforgeeks", Name: "GeeksforGeeks", Priority: 100, Domains: []string{"geeksforgeeks.org"},
		Content: []string{".text", ".article-content", "article"},
		Strip:   []string{".article_bottom", ".sideBar", ".comment"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "tutorialspoint", Name: "TutorialsPoint", Priority: 100, Domains: []string{"tutorialspoint.com"},
		Content: []string{".col-md-7", "#main", ".content"}, Strip: []string{".sidebar", ".footer"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "w3schools", Name: "W3Schools", Priority: 100, Domains: []string{"w3schools.com"},
		Content: []string{"#main", "#content", ".w3-panel"}, Strip: []string{"#sidenav", ".footer"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "css-tricks", Name: "CSS-Tricks", Priority: 100, Domains: []string{"css-tricks.com"},
		Content: []string{".article-content", ".entry-content", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "smashing", Name: "Smashing Magazine", Priority: 100, Domains: []string{"smashingmagazine.com"},
		Content: []string{".article__body", ".entry-content", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "substack", Name: "Substack", Priority: 100, Domains: []string{"substack.com"},
		Content: []string{".body", ".post-content", "article"}, Strip: []string{".subscription-widget", ".post-footer"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "hashnode", Name: "Hashnode", Priority: 100, Domains: []string{"hashnode.dev", "hashnode.com"},
		Content: []string{".prose", "#article-body", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "baeldung", Name: "Baeldung", Priority: 100, Domains: []string{"baeldung.com"},
		Content: []string{".article-inner", ".entry-content", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "realpython", Name: "Real Python", Priority: 100, Domains: []string{"realpython.com"},
		Content: []string{".article-body", ".prose", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "python-docs", Name: "Python 文档", Priority: 100, Domains: []string{"docs.python.org", "python.org"},
		Content: []string{"article", ".body", "#content", "main"}, Strip: []string{".sphinxsidebar", ".related"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "godev", Name: "Go 官方文档", Priority: 100, Domains: []string{"go.dev", "golang.org"},
		Content: []string{".Article", "#content", "main", "article"}, Strip: []string{".Site-nav", ".Footer"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "pkg-go-dev", Name: "pkg.go.dev", Priority: 100, Domains: []string{"pkg.go.dev"},
		Content: []string{"#main-content", ".Documentation-content", "main"}, Title: []string{"h1"}, MinLen: 80},

	{ID: "rust-lang", Name: "Rust 文档", Priority: 100, Domains: []string{"rust-lang.org", "docs.rs"},
		Content: []string{"main", "#content", ".docblock", "article"}, Strip: []string{".sidebar", ".nav"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "nodejs", Name: "Node.js 文档", Priority: 100, Domains: []string{"nodejs.org"},
		Content: []string{"#content", "article", "main"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "kubernetes", Name: "Kubernetes 文档", Priority: 100, Domains: []string{"kubernetes.io"},
		Content: []string{"#content", "main", "article"}, Strip: []string{".td-sidebar", ".pageinfo"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "docker", Name: "Docker 文档", Priority: 100, Domains: []string{"docs.docker.com", "docker.com"},
		Content: []string{"#content", "article", "main"}, Strip: []string{".sidebar"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "mslearn", Name: "Microsoft Learn", Priority: 100, Domains: []string{"learn.microsoft.com", "docs.microsoft.com"},
		Content: []string{"#main", "main", "article"}, Strip: []string{".sidebar", ".metadata"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "aws-docs", Name: "AWS 文档", Priority: 100, Domains: []string{"aws.amazon.com", "docs.aws.amazon.com"},
		Content: []string{"#main-content", "main", "article"}, Strip: []string{"#awsdocs-sidebar"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "gcp-docs", Name: "Google Cloud 文档", Priority: 100, Domains: []string{"cloud.google.com"},
		Content: []string{"article", "main", ".devsite-article-body"}, Strip: []string{".devsite-nav"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "digitalocean", Name: "DigitalOcean", Priority: 100, Domains: []string{"digitalocean.com"},
		Content: []string{".article-content", "article", "main"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "postgres", Name: "PostgreSQL 文档", Priority: 100, Domains: []string{"postgresql.org"},
		Content: []string{"#docContent", ".content", "main"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "mysql-docs", Name: "MySQL 文档", Priority: 100, Domains: []string{"dev.mysql.com", "mysql.com"},
		Content: []string{"#content", "main", "article"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "redis-docs", Name: "Redis 文档", Priority: 100, Domains: []string{"redis.io"},
		Content: []string{"main", "article", ".content"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "elastic", Name: "Elastic 文档", Priority: 100, Domains: []string{"elastic.co"},
		Content: []string{"article", ".content", "main"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "nginx", Name: "nginx 文档", Priority: 100, Domains: []string{"nginx.org"},
		Content: []string{"#content", ".content", "main"}, Title: []string{"h1"}, MinLen: 80},

	{ID: "php-net", Name: "PHP 手册", Priority: 100, Domains: []string{"php.net"},
		Content: []string{"#content", ".refentry", "div.clearfix"}, Strip: []string{".manualnavbar", ".changelog"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "bbc", Name: "BBC", Priority: 100, Domains: []string{"bbc.com", "bbc.co.uk"},
		Content: []string{"article", "[data-component=text-block]", "main"},
		Strip:   []string{"[data-component=related-content]", "footer"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "guardian", Name: "The Guardian", Priority: 100, Domains: []string{"theguardian.com"},
		Content: []string{"[data-gu-name=body]", ".article-body", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "nytimes", Name: "纽约时报", Priority: 100, Domains: []string{"nytimes.com"},
		Content: []string{"section[name=articleBody]", "article", "main"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "reuters", Name: "路透社", Priority: 100, Domains: []string{"reuters.com"},
		Content: []string{"[data-testid=paragraph-0]", "article", "main"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "arstechnica", Name: "Ars Technica", Priority: 100, Domains: []string{"arstechnica.com"},
		Content: []string{".article-content", "article", "main"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "techcrunch", Name: "TechCrunch", Priority: 100, Domains: []string{"techcrunch.com"},
		Content: []string{".article-content", ".entry-content", "article"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "theverge", Name: "The Verge", Priority: 100, Domains: []string{"theverge.com"},
		Content: []string{".duet--article--article-body-component", "article", "main"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "wired", Name: "WIRED", Priority: 100, Domains: []string{"wired.com"},
		Content: []string{"[data-testid=BodyWrapper]", "article", "main"}, Title: []string{"h1"}, MinLen: 150},

	{ID: "quora", Name: "Quora", Priority: 100, Domains: []string{"quora.com"},
		Content: []string{".q-box.qu-userSelect--text", ".q-text", "article"}, Title: []string{"h1"}, MinLen: 80},

	{ID: "javatpoint", Name: "JavaTpoint", Priority: 100, Domains: []string{"javatpoint.com"},
		Content: []string{".col-sm-8", "#main", "article"}, Strip: []string{".leftmenu", ".footer"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "programiz", Name: "Programiz", Priority: 100, Domains: []string{"programiz.com"},
		Content: []string{"article", "main", ".content"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "w3resource", Name: "w3resource", Priority: 100, Domains: []string{"w3resource.com"},
		Content: []string{"#main", "article", ".content"}, Title: []string{"h1"}, MinLen: 120},

	// ==================== CMS / 论坛引擎指纹 ====================
	{ID: "wordpress", Name: "WordPress", Priority: 60, Gens: []string{"wp-content"},
		Content: []string{".entry-content", ".post-content", ".article-content", "article", "#content"},
		Strip:   []string{".sharedaddy", ".jp-relatedposts", ".post-navigation", ".comments-area", "#comments", ".widget-area", ".sidebar"},
		StripKW: []string{"comment", "sidebar", "widget", "related", "share", "breadcrumb"},
		Title:   []string{".entry-title", "h1"}, MinLen: 150},

	{ID: "discuz", Name: "Discuz 论坛", Priority: 60, Gens: []string{"powered by discuz"},
		Content: []string{"#postlist .t_f", ".t_f", "#thread_subject", ".postmessage", "#article_content"},
		Strip:   []string{"#postlist .sign", ".quote", ".attach_nopermoken", ".pstatus", ".pct", "#comment"},
		StripKW: []string{"sign", "quote", "attach", "ad", "footer"},
		Title:   []string{"#thread_subject", "h1"}, MinLen: 30},

	{ID: "phpwind", Name: "phpwind 论坛", Priority: 60, Gens: []string{"powered by phpwind"},
		Content: []string{".read_content", "#read_tpc", ".tpc_content"},
		Strip:   []string{".sign", ".quote", ".ad"}, Title: []string{"h1"}, MinLen: 30},

	{ID: "xiuno", Name: "Xiuno 论坛", Priority: 60, Gens: []string{"xiuno"},
		Content: []string{".message", ".post_message", "#postlist"}, Title: []string{"h1"}, MinLen: 30},

	{ID: "flarum", Name: "Flarum 论坛", Priority: 60, Gens: []string{"flarum"},
		Content: []string{".Post-body", ".post-body", ".PostStream-item"},
		Strip:   []string{".Post-actions", ".Post-footer"}, Title: []string{"h1"}, MinLen: 30},

	{ID: "discourse", Name: "Discourse 论坛", Priority: 60, Gens: []string{"discourse"},
		Content: []string{"#main-outlet .cooked", ".cooked", ".topic-body"},
		Strip:   []string{".post-menu-area", ".topic-map", ".actions"}, Title: []string{"h1"}, MinLen: 30},

	{ID: "nodebb", Name: "NodeBB 论坛", Priority: 60, Gens: []string{"nodebb"},
		Content: []string{".post-content", "[component=post/content]", ".content"},
		Strip:   []string{".post-footer", ".votes"}, Title: []string{"h1"}, MinLen: 30},

	{ID: "typecho", Name: "Typecho", Priority: 55, Gens: []string{"typecho"},
		Content: []string{".post-content", ".entry-content", "article", "#content"},
		Strip:   []string{".comment", "#comments", ".sidebar"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "zblog", Name: "Z-Blog", Priority: 55, Gens: []string{"zblogphp"},
		Content: []string{".post-body", ".article-content", "#article", ".content"},
		Strip:   []string{"#comments", ".sidebar", ".related"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "emlog", Name: "EMLOG", Priority: 55, Gens: []string{"emlog"},
		Content: []string{"#article", ".log-content", ".content"},
		Strip:   []string{"#comments", ".sidebar", ".related"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "halo", Name: "Halo", Priority: 55, Gens: []string{"halo"},
		Content: []string{".post-content", ".markdown-body", "article", "#content"},
		Strip:   []string{".post-footer", ".comment", ".sidebar"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "hexo", Name: "Hexo", Priority: 50, Gens: []string{"hexo"},
		Content: []string{".post-content", ".article-entry", "article", "#content"},
		Strip:   []string{".post-nav", "#comments", ".sidebar"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "mediawiki", Name: "MediaWiki", Priority: 55, Gens: []string{"mediawiki"},
		Content: []string{"#mw-content-text", ".mw-parser-output"},
		Strip:   []string{"#toc", ".mw-editsection", ".navbox", ".catlinks"}, Title: []string{"#firstHeading", "h1"}, MinLen: 150},

	{ID: "drupal", Name: "Drupal", Priority: 55, Gens: []string{"drupal"},
		Content: []string{".node__content", ".field--name-body", "article", "main"},
		Strip:   []string{".comment", ".region-sidebar", ".breadcrumb"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "joomla", Name: "Joomla", Priority: 55, Gens: []string{"joomla"},
		Content: []string{".com-content-article__body", ".item-page", "article", "main"},
		Strip:   []string{".breadcrumb", ".sidebar", ".comments"}, Title: []string{"h1"}, MinLen: 120},

	{ID: "dedecms", Name: "织梦 DedeCMS", Priority: 55, Gens: []string{"dedecms"},
		Content: []string{"#content", ".content", ".article-content"},
		Strip:   []string{".footer", ".nav", ".share"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "phpcms", Name: "PHPCMS", Priority: 55, Gens: []string{"phpcms"},
		Content: []string{".content", "#content", ".article-content"},
		Strip:   []string{".footer", ".nav"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "empirecms", Name: "帝国 CMS", Priority: 55, Gens: []string{"empirecms"},
		Content: []string{"#content", ".article_content", ".content"},
		Strip:   []string{".footer", ".nav"}, Title: []string{"h1"}, MinLen: 100},

	{ID: "thinkphp", Name: "ThinkPHP 站点", Priority: 40, Gens: []string{"thinkphp"},
		Content: []string{".article-content", "#content", ".content", "article"},
		Title:   []string{"h1"}, MinLen: 120},

	// ==================== 通用规则（兜底） ====================
	{
		ID: "generic", Name: "通用规则", Priority: 0,
		Content: []string{
			"[itemprop=articleBody]",
			".markdown-body", ".rich_media_content", ".article-content", ".post-content",
			".entry-content", ".article-body", ".post-body", ".content-body", ".article__body",
			"#article_content", "#articleContent", "#post-content", "#content",
			"article", "main", "[role=main]", ".article", ".post", ".content",
		},
		StripKW: []string{
			"nav", "navbar", "menu", "footer", "sidebar", "aside", "widget",
			"breadcrumb", "pagination", "pager", "copyright", "advert", "ad-",
			"banner", "qrcode", "toolbar", "subscribe", "share", "sharebox",
			"comment", "recommend", "related", "hot-list", "hotlist", "ranklist",
			"login", "signin", "signup", "search-box", "searchbar", "popup",
			"cookie", "newsletter", "social", "tag-list", "meta-info", "post-nav",
		},
		// 只保留强正文特征：这些词出现在 id/class 里基本可确定是正文容器
		KeepKW: []string{
			"article-content", "article-body", "article_content", "article_body",
			"post-content", "post-body", "post_content", "post_body",
			"entry-content", "entry-body", "content-body", "content_body",
			"markdown-body", "markdown_body", "rich_media", "articlebody",
		},
		MinLen: 200,
	},
}
