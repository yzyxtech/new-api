package codex

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"
)

// codexToolNameLimit 是 Codex 上游对工具名 / call_id 的 UTF-8 字节上限。
const codexToolNameLimit = 64

// codexToolNameMapping 维护 bridge 请求作用域内的工具名 / call_id 收缩与还原映射。
// 仅存活于单次请求 session 内存,request session 写入、response session 只读,
// 不进入 gin context、无持久化、无跨请求 / 跨 retry 状态。
type codexToolNameMapping struct {
	nameByShort   map[string]string // short tool name   -> original tool name
	callIDByShort map[string]string // shortened call_id -> original tool_use_id
}

// newCodexToolNameMapping 基于请求内全量工具名集合构建映射。
// 唯一化在全集上按序进行(含 ≤64 字节原名,与已产出 short 名碰撞的原名同样被加 _N 后缀改写)。
func newCodexToolNameMapping(names []string) *codexToolNameMapping {
	m := &codexToolNameMapping{
		nameByShort:   make(map[string]string),
		callIDByShort: make(map[string]string),
	}
	used := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n == "" {
			continue
		}
		uniq := uniqueShortName(baseCandidate(n), used)
		used[uniq] = struct{}{}
		if uniq != n {
			m.nameByShort[uniq] = n
		}
	}
	return m
}

// buildShortNameMap 仅当调用方需要后置重建映射时使用;通常应直接使用 newCodexToolNameMapping。
func (m *codexToolNameMapping) buildShortNameMap(names []string) {
	used := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n == "" {
			continue
		}
		uniq := uniqueShortName(baseCandidate(n), used)
		used[uniq] = struct{}{}
		if uniq != n {
			m.nameByShort[uniq] = n
		}
	}
}

// shrinkName 返回给定原始工具名对应的上行 short 名(未在预构建集合中也按单一规则收缩并登记)。
func (m *codexToolNameMapping) shrinkName(orig string) string {
	for short, original := range m.nameByShort {
		if original == orig {
			return short
		}
	}
	short := baseCandidate(orig)
	if short != orig {
		m.nameByShort[short] = orig
	}
	return short
}

// restoreName 命中映射还原原值,未命中原样透传(INV-4)。
func (m *codexToolNameMapping) restoreName(short string) string {
	if orig, ok := m.nameByShort[short]; ok {
		return orig
	}
	return short
}

// shrinkCallID 对超 64 字节的 call_id 做 sha256 后缀截断收缩并登记还原映射;≤64 原样。
func (m *codexToolNameMapping) shrinkCallID(id string) string {
	if id == "" {
		return id
	}
	if len(id) <= codexToolNameLimit {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	suffix := "_" + hex.EncodeToString(sum[:8])
	if len(suffix) >= codexToolNameLimit {
		short := truncateBytesRune(suffix, codexToolNameLimit)
		if short != id {
			m.callIDByShort[short] = id
		}
		return short
	}
	prefixLen := codexToolNameLimit - len(suffix)
	short := truncateBytesRune(id, prefixLen) + suffix
	if short != id {
		m.callIDByShort[short] = id
	}
	return short
}

// restoreCallID 命中映射还原原值,未命中原样透传(INV-4)。
func (m *codexToolNameMapping) restoreCallID(short string) string {
	if orig, ok := m.callIDByShort[short]; ok {
		return orig
	}
	return short
}

// baseCandidate 计算单个名字的基线收缩候选:
// 含 mcp__ 前缀时保留前缀 + 最后一个 "__" 之后的尾段,否则直接截断至 64 字节;
// 所有截断均按 rune 边界修正,保证产出合法 UTF-8 且 ≤64 字节。
func baseCandidate(name string) string {
	if len(name) <= codexToolNameLimit {
		return name
	}
	if strings.HasPrefix(name, "mcp__") {
		if idx := strings.LastIndex(name, "__"); idx > 0 {
			cand := "mcp__" + name[idx+2:]
			if len(cand) > codexToolNameLimit {
				return truncateBytesRune(cand, codexToolNameLimit)
			}
			return cand
		}
	}
	return truncateBytesRune(name, codexToolNameLimit)
}

// uniqueShortName 在 used 集合上做请求内唯一化。碰撞时先为 _N 后缀预留长度(截到
// limit-len(suffix)),再加后缀,避免"64+后缀"突破上限;再截断同样按 rune 边界修正。
func uniqueShortName(cand string, used map[string]struct{}) string {
	if _, ok := used[cand]; !ok {
		return cand
	}
	base := cand
	for i := 1; ; i++ {
		suffix := "_" + strconv.Itoa(i)
		allowed := codexToolNameLimit - len(suffix)
		if allowed < 0 {
			allowed = 0
		}
		tmp := truncateBytesRune(base, allowed) + suffix
		if _, ok := used[tmp]; !ok {
			return tmp
		}
	}
}

// truncateBytesRune 把字符串按 rune 边界截断到不超过 limit 字节:截断后若前缀末尾落在
// 多字节字符中间(含残留单个 lead 字节),则逐字节从尾部丢弃直到构成合法 UTF-8 前缀。
func truncateBytesRune(s string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(s) <= limit {
		return s
	}
	b := []byte(s)[:limit]
	for len(b) > 0 && !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b)
}
