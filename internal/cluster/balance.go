package cluster

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"sort"
	"strings"
	"time"

	"queryza/internal/store"
)

// ---------- 域名分片 ----------
//
// 一个域名文档量太大时（例如论坛、聚合站），单节点会先成为瓶颈。
// 这里把域名按 URL 哈希拆成 N 个分片，每个分片独立选主副本节点，
// 抓取按哈希落到对应分片，检索仍按节点 fan-out 并由 URL 去重合并。

// ShardsOf 返回域名的分片数（默认 1）。整体缓存 30 秒，避免每次判定都查库。
func (m *Master) ShardsOf(domain string) int {
	m.shardMu.RLock()
	if m.shards != nil && time.Since(m.shardAt) < 30*time.Second {
		n := m.shards[domain]
		m.shardMu.RUnlock()
		if n <= 0 {
			return 1
		}
		return n
	}
	m.shardMu.RUnlock()

	m.shardMu.Lock()
	defer m.shardMu.Unlock()
	// 双重检查
	if m.shards != nil && time.Since(m.shardAt) < 30*time.Second {
		if n := m.shards[domain]; n > 0 {
			return n
		}
		return 1
	}
	next := map[string]int{}
	rows, err := m.db.Query(`SELECT domain,shards FROM domain_shards`)
	if err == nil {
		for rows.Next() {
			var d string
			var n int
			if rows.Scan(&d, &n) == nil && n > 0 {
				next[d] = n
			}
		}
		rows.Close()
	}
	m.shards, m.shardAt = next, time.Now()
	if n := next[domain]; n > 0 {
		return n
	}
	return 1
}

// ShardOf 计算某个 URL 落在该域名的第几个分片。
func (m *Master) ShardOf(domain, urlHash string) int {
	n := m.ShardsOf(domain)
	if n <= 1 {
		return 0
	}
	if urlHash == "" {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(urlHash))
	return int(h.Sum64() % uint64(n))
}

// SetShards 调整某域名的分片数，并为新增分片建立主副本。
// 注意：已入库的文档不会迁移，只有新抓取的文档按新分片落位；
// 调大分片后建议对该域名重新入队抓取一轮。
func (m *Master) SetShards(domain string, n int) error {
	if domain == "" {
		return nil
	}
	if n < 1 {
		n = 1
	}
	if n > 32 {
		n = 32
	}
	if _, err := m.db.Exec(`INSERT INTO domain_shards(domain,shards) VALUES(?,?)
		ON DUPLICATE KEY UPDATE shards=VALUES(shards)`, domain, n); err != nil {
		return err
	}
	m.shardMu.Lock()
	m.shards = nil
	m.shardMu.Unlock()

	nodes := m.OnlineNodes()
	if len(nodes) == 0 {
		return nil
	}
	// 多加的分片按权重补主副本
	for s := 0; s < n; s++ {
		if m.lookupPrimary(domain, s) != "" {
			continue
		}
		node := m.pickNode(nodes, m.loadMap(), s)
		if node == "" {
			break
		}
		m.insertReplicaGroup(domain, s, node, nodes)
	}
	m.invalidateDomainList()
	return nil
}

// EnsureAutoShards 对文档量过大的域名自动加分片。
// perNode 表示单节点期望承载的文档数，超过即拆分。
func (m *Master) EnsureAutoShards(perNode int) int {
	if perNode <= 0 {
		perNode = 20000
	}
	rows, err := m.db.Query(`SELECT domain,COUNT(*) c FROM documents GROUP BY domain HAVING c > ? LIMIT 200`, perNode)
	if err != nil {
		return 0
	}
	type item struct {
		domain string
		docs   int
	}
	var list []item
	for rows.Next() {
		var it item
		if rows.Scan(&it.domain, &it.docs) == nil {
			list = append(list, it)
		}
	}
	rows.Close()

	changed := 0
	for _, it := range list {
		want := (it.docs + perNode - 1) / perNode
		if want > 8 {
			want = 8
		}
		if want <= m.ShardsOf(it.domain) {
			continue
		}
		if err := m.SetShards(it.domain, want); err == nil {
			log.Printf("域名 %s 有 %d 篇文档，分片数调整为 %d", it.domain, it.docs, want)
			changed++
		}
	}
	return changed
}

// ---------- 分配 ----------

// AssignDomain 兼容旧调用：等价于 shard 0 的主副本。
func (m *Master) AssignDomain(domain string) string {
	return m.AssignRoute(domain, 0)
}

// AssignRoute 返回 (domain, shard) 的主副本节点，不存在则按「负载/权重」最小优先新建副本组。
func (m *Master) AssignRoute(domain string, shard int) string {
	if domain == "" {
		return ""
	}
	if shard < 0 {
		shard = 0
	}
	key := routeKey(domain, shard)

	m.asgMu.RLock()
	e := m.asgCache[key]
	m.asgMu.RUnlock()
	if e != nil && time.Since(e.at) < e.ttl {
		return e.primary
	}

	m.assignMu.Lock()
	m.asgMu.RLock()
	e = m.asgCache[key]
	m.asgMu.RUnlock()
	if e != nil && time.Since(e.at) < e.ttl {
		m.assignMu.Unlock()
		return e.primary
	}
	primary, created := m.lookupOrAssignRoute(domain, shard)
	m.assignMu.Unlock()

	ttl := 120 * time.Second
	if primary == "" {
		ttl = 5 * time.Second // 暂无在线节点，短暂负缓存，等节点上线
	}
	m.asgMu.Lock()
	if len(m.asgCache) > 200000 {
		now := time.Now()
		for k, v := range m.asgCache {
			if now.Sub(v.at) >= v.ttl {
				delete(m.asgCache, k)
			}
		}
	}
	m.asgCache[key] = &asgEntry{primary: primary, at: time.Now(), ttl: ttl}
	m.asgMu.Unlock()

	if created {
		// 只让「节点 → 域名列表」缓存过期。
		// 原来调用 invalidateDomains 会连 asgCache 一起清空，
		// 而抓取时每发现一个新域名就会触发一次，导致已有域名的归属缓存
		// 被反复清掉、每个页面都要回查数据库。新域名不影响老域名的归属。
		m.invalidateDomainList()
	}
	return primary
}

func routeKey(domain string, shard int) string {
	if shard == 0 {
		return domain
	}
	return domain + "\x00" + itoaShard(shard)
}

func itoaShard(n int) string {
	if n == 0 {
		return "0"
	}
	var b [4]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func (m *Master) lookupPrimary(domain string, shard int) string {
	var primary string
	rows, err := m.db.Query(`SELECT node_id FROM domain_replicas
		WHERE domain=? AND shard=? AND role='primary' LIMIT 1`, domain, shard)
	if err != nil {
		return ""
	}
	for rows.Next() {
		rows.Scan(&primary)
	}
	rows.Close()
	return primary
}

// lookupOrAssignRoute 查库或新建副本组；调用方需持有 assignMu。
func (m *Master) lookupOrAssignRoute(domain string, shard int) (string, bool) {
	if primary := m.lookupPrimary(domain, shard); primary != "" {
		return primary, false
	}

	nodes := m.OnlineNodes()
	if len(nodes) == 0 {
		return "", false
	}
	node := m.pickNode(nodes, m.loadMap(), shard)
	if node == "" {
		return "", false
	}
	m.insertReplicaGroup(domain, shard, node, nodes)
	return node, true
}

// insertReplicaGroup 写入主副本与副本（主副本已由调用方选定）。
func (m *Master) insertReplicaGroup(domain string, shard int, primary string, nodes []store.Node) {
	m.db.Exec(`INSERT IGNORE INTO domain_replicas(domain,shard,node_id,role,weight)
		VALUES(?,?,?,'primary',1)`, domain, shard, primary)

	factor := m.cfg.Cluster.ReplicaFactor
	if factor <= 1 {
		return
	}
	if factor > len(nodes) {
		factor = len(nodes)
	}
	// 副本尽量放到负载率最低的其他节点
	rest := make([]store.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.NodeID != primary {
			rest = append(rest, n)
		}
	}
	sort.SliceStable(rest, func(i, j int) bool {
		return nodeLoadRate(rest[i], m.loadMap()) < nodeLoadRate(rest[j], m.loadMap())
	})
	for i := 0; i < factor-1 && i < len(rest); i++ {
		m.db.Exec(`INSERT IGNORE INTO domain_replicas(domain,shard,node_id,role,weight)
			VALUES(?,?,?,'replica',1)`, domain, shard, rest[i].NodeID)
	}
}

// loadMap 统计每个节点当前承载的主副本分片数。
func (m *Master) loadMap() map[string]int {
	out := map[string]int{}
	rows, err := m.db.Query(`SELECT node_id, COUNT(*) FROM domain_replicas WHERE role='primary' GROUP BY node_id`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var c int
		if rows.Scan(&id, &c) == nil {
			out[id] = c
		}
	}
	return out
}

func nodeLoadRate(n store.Node, load map[string]int) float64 {
	w := n.Weight
	if w <= 0 {
		w = 1
	}
	return float64(load[n.NodeID]) / w
}

// pickNode 选择「负载/权重」最小的节点；同分时按 node_id 稳定排序，seed 用于错开分片。
func (m *Master) pickNode(nodes []store.Node, load map[string]int, seed int) string {
	if len(nodes) == 0 {
		return ""
	}
	best := ""
	bestRate := 1e18
	for _, n := range nodes {
		rate := nodeLoadRate(n, load)
		if rate < bestRate-1e-9 {
			best, bestRate = n.NodeID, rate
			continue
		}
		if rate < bestRate+1e-9 && best != "" && n.NodeID < best {
			best = n.NodeID
		}
	}
	// 同一轮多个分片按 seed 轮转，避免所有分片挤到同一个节点
	if len(nodes) > 1 && seed > 0 {
		idx := seed % len(nodes)
		if cand := nodes[idx].NodeID; cand != "" && nodeLoadRate(nodes[idx], load) <= bestRate+1e-9 {
			best = cand
		}
	}
	return best
}

// OwnDomain 判断本节点是否为该域名（shard 0）的主副本。
func (m *Master) OwnDomain(domain string) bool {
	primary := m.AssignDomain(domain)
	if primary == "" {
		return true
	}
	return primary == m.cfg.Cluster.NodeID
}

// OwnRoute 判断本节点是否为指定分片的主副本。
func (m *Master) OwnRoute(domain string, shard int) bool {
	primary := m.AssignRoute(domain, shard)
	if primary == "" {
		return true
	}
	return primary == m.cfg.Cluster.NodeID
}

// ---------- 再均衡 ----------

// Rebalance 按节点权重把分片从超载节点搬到低载节点。
// 权重即「该节点能承载多少内容」，权重高的节点分到更多域名分片。
func (m *Master) Rebalance() int {
	nodes := m.OnlineNodes()
	if len(nodes) < 2 {
		return 0
	}
	type route struct {
		domain string
		shard  int
		node   string
	}
	rows, err := m.db.Query(`SELECT domain,shard,node_id FROM domain_replicas WHERE role='primary'`)
	if err != nil {
		return 0
	}
	var list []route
	total := 0
	for rows.Next() {
		var r route
		if rows.Scan(&r.domain, &r.shard, &r.node) == nil {
			list = append(list, r)
			total++
		}
	}
	rows.Close()
	if total < len(nodes) {
		return 0
	}

	online := map[string]bool{}
	var sumWeight float64
	for _, n := range nodes {
		online[n.NodeID] = true
		w := n.Weight
		if w <= 0 {
			w = 1
		}
		sumWeight += w
	}
	if sumWeight <= 0 {
		return 0
	}

	load := m.loadMap()
	for _, n := range nodes {
		if _, ok := load[n.NodeID]; !ok {
			load[n.NodeID] = 0
		}
	}

	// 目标：每个节点承载 total*weight/sumWeight 个分片
	target := map[string]float64{}
	for _, n := range nodes {
		w := n.Weight
		if w <= 0 {
			w = 1
		}
		target[n.NodeID] = float64(total) * w / sumWeight
	}

	moved := 0
	maxMove := len(list)
	for moved < maxMove {
		hi, lo := "", ""
		hiOver, loUnder := 0.0, 0.0
		for id, c := range load {
			if !online[id] {
				continue
			}
			over := float64(c) - target[id]
			if over > hiOver {
				hi, hiOver = id, over
			}
			if under := target[id] - float64(c); under > loUnder {
				lo, loUnder = id, under
			}
		}
		// 差异不足半个分片就停，避免来回搬
		if hi == "" || lo == "" || hiOver < 0.5 || loUnder < 0.5 {
			break
		}
		// 从超载节点挑一个分片搬过去（优先域名分片数多的，分摊更均匀）
		movedOne := false
		for i := range list {
			r := list[i]
			if r.node != hi {
				continue
			}
			if _, err := m.db.Exec(`UPDATE domain_replicas SET role='replica'
				WHERE domain=? AND shard=? AND node_id=?`, r.domain, r.shard, hi); err != nil {
				continue
			}
			m.db.Exec(`INSERT INTO domain_replicas(domain,shard,node_id,role,weight)
				VALUES(?,?,?,'primary',1)
				ON DUPLICATE KEY UPDATE role='primary'`, r.domain, r.shard, lo)
			list[i].node = lo
			load[hi]--
			load[lo]++
			moved++
			movedOne = true
			break
		}
		if !movedOne {
			break
		}
	}

	// 副本数不足的补齐（按权重挑低载节点）
	if factor := m.cfg.Cluster.ReplicaFactor; factor > 1 {
		rows, err := m.db.Query(`SELECT domain,shard,node_id FROM domain_replicas`)
		if err == nil {
			have := map[string]map[string]bool{}
			for rows.Next() {
				var d, n string
				var s int
				if rows.Scan(&d, &s, &n) == nil {
					k := routeKey(d, s)
					if have[k] == nil {
						have[k] = map[string]bool{}
					}
					have[k][n] = true
				}
			}
			rows.Close()
			for k, set := range have {
				if len(set) >= factor {
					continue
				}
				d, s := splitRouteKey(k)
				for _, n := range nodes {
					if len(set) >= factor {
						break
					}
					if set[n.NodeID] {
						continue
					}
					m.db.Exec(`INSERT IGNORE INTO domain_replicas(domain,shard,node_id,role,weight)
						VALUES(?,?,?,'replica',1)`, d, s, n.NodeID)
					set[n.NodeID] = true
				}
			}
		}
	}

	if moved > 0 {
		m.invalidateDomains()
	}
	return moved
}

// MoveRoute 手动把某个 (域名, 分片) 的主副本改到指定节点。
func (m *Master) MoveRoute(domain string, shard int, nodeID string) error {
	m.mu.RLock()
	node := m.nodes[nodeID]
	m.mu.RUnlock()
	if node == nil {
		return errors.New("节点不存在")
	}
	if _, err := m.db.Exec(`UPDATE domain_replicas SET role='replica'
		WHERE domain=? AND shard=? AND role='primary'`, domain, shard); err != nil {
		return err
	}
	if _, err := m.db.Exec(`INSERT INTO domain_replicas(domain,shard,node_id,role,weight)
		VALUES(?,?,?,'primary',1) ON DUPLICATE KEY UPDATE role='primary'`, domain, shard, nodeID); err != nil {
		return err
	}
	m.invalidateDomains()
	return nil
}

// ShardPolicy 分片策略展示项。
type ShardPolicy struct {
	Domain string `json:"domain"`
	Shards int    `json:"shards"`
	Docs   int64  `json:"docs"`
}

// ShardPolicies 返回文档量靠前的域名的分片情况，供后台查看。
func (m *Master) ShardPolicies(ctx context.Context) []ShardPolicy {
	rows, err := m.db.QueryContext(ctx, `SELECT d.domain, COUNT(*) c,
		COALESCE((SELECT shards FROM domain_shards s WHERE s.domain=d.domain),1) sh
		FROM documents d GROUP BY d.domain ORDER BY c DESC LIMIT 200`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := make([]ShardPolicy, 0, 32)
	for rows.Next() {
		var p ShardPolicy
		if rows.Scan(&p.Domain, &p.Docs, &p.Shards) == nil {
			out = append(out, p)
		}
	}
	return out
}

func splitRouteKey(k string) (string, int) {
	if i := strings.IndexByte(k, 0); i >= 0 {
		d := k[:i]
		s := 0
		for _, c := range k[i+1:] {
			s = s*10 + int(c-'0')
		}
		return d, s
	}
	return k, 0
}
