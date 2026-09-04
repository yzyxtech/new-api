package codex

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestCodexToolNameMapping_ShrinkOver64:>64 字节 name → 收缩且 ≤64 字节合法 UTF-8。
func TestCodexToolNameMapping_ShrinkOver64(t *testing.T) {
	longName := strings.Repeat("a", 100)
	m := newCodexToolNameMapping([]string{longName})

	short := m.shrinkName(longName)
	if len(short) > codexToolNameLimit {
		t.Fatalf("short name %d bytes exceeds %d", len(short), codexToolNameLimit)
	}
	if short == longName {
		t.Fatalf("expected shortening for %d-byte name", len(longName))
	}
	if !utf8.ValidString(short) {
		t.Fatalf("short name %q is not valid UTF-8", short)
	}
	if got := m.restoreName(short); got != longName {
		t.Fatalf("restore = %q, want %q", got, longName)
	}
}

// TestCodexToolNameMapping_McpPrefix:含 mcp__ 前缀 → 保留尾段。
func TestCodexToolNameMapping_McpPrefix(t *testing.T) {
	mcpName := "mcp__" + strings.Repeat("b", 100)
	m := newCodexToolNameMapping([]string{mcpName})

	short := m.shrinkName(mcpName)
	if !strings.HasPrefix(short, "mcp__") {
		t.Fatalf("mcp__ prefix lost: %q", short)
	}
	if len(short) > codexToolNameLimit {
		t.Fatalf("short name %d bytes exceeds %d", len(short), codexToolNameLimit)
	}
	if !utf8.ValidString(short) {
		t.Fatalf("short name %q is not valid UTF-8", short)
	}
	if got := m.restoreName(short); got != mcpName {
		t.Fatalf("restore = %q, want %q", got, mcpName)
	}
}

// TestCodexToolNameMapping_CallIDShrink:>64 字节 call_id → sha256 后缀截断。
func TestCodexToolNameMapping_CallIDShrink(t *testing.T) {
	callID := strings.Repeat("c", 100)
	m := newCodexToolNameMapping(nil)

	short := m.shrinkCallID(callID)
	if len(short) > codexToolNameLimit {
		t.Fatalf("short call_id %d bytes exceeds %d", len(short), codexToolNameLimit)
	}
	if short == callID {
		t.Fatalf("expected shortening for %d-byte call_id", len(callID))
	}
	if !utf8.ValidString(short) {
		t.Fatalf("short call_id %q is not valid UTF-8", short)
	}

	sum := sha256.Sum256([]byte(callID))
	suffix := "_" + hex.EncodeToString(sum[:8])
	if !strings.HasSuffix(short, suffix) {
		t.Fatalf("short call_id %q missing sha256 suffix %q", short, suffix)
	}
	if got := m.restoreCallID(short); got != callID {
		t.Fatalf("restore call_id = %q, want %q", got, callID)
	}
}

// TestCodexToolNameMapping_UniqueCollision:恰 64 字节原名与截断 short 碰撞 → 后缀余量预留。
func TestCodexToolNameMapping_UniqueCollision(t *testing.T) {
	nameA := strings.Repeat("a", 64) // 恰 64 字节,候选即自身
	nameB := strings.Repeat("a", 70) // 截断候选同为 64 字节 "a",与 nameA 碰撞
	m := newCodexToolNameMapping([]string{nameA, nameB})

	shortA := m.shrinkName(nameA)
	shortB := m.shrinkName(nameB)

	if shortA != nameA {
		t.Fatalf("first 64-byte name short = %q, want original %q", shortA, nameA)
	}
	if shortA == shortB {
		t.Fatalf("collision not resolved: both = %q", shortA)
	}
	// 后缀余量预留:先截到 64-len("_1")=62 再加后缀,总长恰 64
	wantShortB := strings.Repeat("a", 62) + "_1"
	if shortB != wantShortB {
		t.Fatalf("shortB = %q, want %q", shortB, wantShortB)
	}
	if len(shortB) > codexToolNameLimit {
		t.Fatalf("shortB %d bytes exceeds %d", len(shortB), codexToolNameLimit)
	}
	if got := m.restoreName(shortB); got != nameB {
		t.Fatalf("restore nameB = %q, want %q", got, nameB)
	}
}

// TestCodexToolNameMapping_MultibyteBoundary:多字节 UTF-8 名/tool_use_id 恰在截断边界。
func TestCodexToolNameMapping_MultibyteBoundary(t *testing.T) {
	mbName := strings.Repeat("中", 30) // 90 字节,t截断点在 64,落在多字节字符中间
	mName := newCodexToolNameMapping([]string{mbName})
	shortName := mName.shrinkName(mbName)
	if !utf8.ValidString(shortName) {
		t.Fatalf("short name %q is not valid UTF-8", shortName)
	}
	if len(shortName) > codexToolNameLimit {
		t.Fatalf("short name %d bytes exceeds %d", len(shortName), codexToolNameLimit)
	}
	// 64 / 3 = 21 个"中" = 63 字节(退回最近完整字符边界)
	if shortName != strings.Repeat("中", 21) {
		t.Fatalf("short name = %q (len %d), want 21 CJK runes", shortName, len(shortName))
	}
	if got := mName.restoreName(shortName); got != mbName {
		t.Fatalf("restore name = %q, want %q", got, mbName)
	}

	mbCall := strings.Repeat("中", 30) // 90 字节
	mCall := newCodexToolNameMapping(nil)
	shortCall := mCall.shrinkCallID(mbCall)
	if !utf8.ValidString(shortCall) {
		t.Fatalf("short call_id %q is not valid UTF-8", shortCall)
	}
	if len(shortCall) > codexToolNameLimit {
		t.Fatalf("short call_id %d bytes exceeds %d", len(shortCall), codexToolNameLimit)
	}
	// 前缀 64-17=47,截到 45 字节(15 个"中"),再加 17 字节后缀 = 62
	if len(shortCall) != 62 {
		t.Fatalf("short call_id len = %d, want 62", len(shortCall))
	}
	if got := mCall.restoreCallID(shortCall); got != mbCall {
		t.Fatalf("restore call_id = %q, want %q", got, mbCall)
	}
}

// TestCodexToolNameMapping_RestoreAndPassthrough:命中映射还原原值,未命中原样透传。
func TestCodexToolNameMapping_RestoreAndPassthrough(t *testing.T) {
	orig := strings.Repeat("a", 70)
	m := newCodexToolNameMapping([]string{orig})

	short := m.shrinkName(orig)
	if got := m.restoreName(short); got != orig {
		t.Fatalf("hit restore = %q, want %q", got, orig)
	}
	if got := m.restoreName("unknown_tool_name"); got != "unknown_tool_name" {
		t.Fatalf("miss passthrough = %q, want %q", got, "unknown_tool_name")
	}

	callID := strings.Repeat("c", 100)
	shortCall := m.shrinkCallID(callID)
	if got := m.restoreCallID(shortCall); got != callID {
		t.Fatalf("hit restore call_id = %q, want %q", got, callID)
	}
	if got := m.restoreCallID("unknown_call_id"); got != "unknown_call_id" {
		t.Fatalf("miss passthrough call_id = %q, want %q", got, "unknown_call_id")
	}

	// ≤64 原值:收缩不产生变化,还原仍一致
	shortLe := m.shrinkName("tool_short")
	if shortLe != "tool_short" {
		t.Fatalf("≤64 name altered: %q", shortLe)
	}
	callLe := m.shrinkCallID("call_short")
	if callLe != "call_short" {
		t.Fatalf("≤64 call_id altered: %q", callLe)
	}
}
