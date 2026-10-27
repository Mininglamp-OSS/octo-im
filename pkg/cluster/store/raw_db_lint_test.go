package store

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// T25c: slot-owned tables are read only through fenced accessors. This test
// scans every raw handle of the business database:
//
//	store.Store.wdb and its export Store.DB()   (pkg/cluster/store, callers)
//	cluster.Server.db                           (pkg/cluster/cluster)
//	channel storage.db and channel opts.DB      (pkg/cluster/channel)
//	icluster.IStore.WKDB()                      (anywhere; no caller allowed)
//
// A read of a slot-owned table through one of them must sit lexically inside
// a fenced read (readOwned, readConversations, icluster.FencedRead or
// icluster.FencedReadSlots). Every other read must be on the allow-lists
// below, each with its reason. An unclassified read fails, so a new raw read
// has to be classified before it can merge.

// slotOwnedReads are reads of tables written only through slot Raft.
var slotOwnedReads = setOf(
	// channel slot
	"ExistSubscriber", "GetSubscribers", "GetSubscriberCount", "GetChannel", "ExistChannel",
	"GetDenylist", "ExistDenylist", "GetAllowlist", "ExistAllowlist", "HasAllowlist",
	"GetMessageEventByEventID", "ListMessageEvents", "GetMessageEventStates", "GetMessageEventStatesBatch", "GetMessageEventState",
	"GetChannelClusterConfig", "GetChannelClusterConfigs", "GetChannelClusterConfigCountWithSlotId",
	"GetChannelClusterConfigWithSlotId", "GetChannelClusterConfigVersion", "SearchChannelClusterConfig",
	"SearchChannels",
	// uid slot (conversations: uid slot and, for non-person rows, channel slot)
	"GetUser", "ExistUser", "SearchUser", "GetDevice", "GetDevices", "GetDeviceCount", "SearchDevice",
	"GetConversations", "GetConversationsByType", "GetLastConversations", "GetConversation",
	"ExistConversation", "GetLastConversationIds", "SearchConversation",
	// slot 0
	"GetSystemUids", "GetTester", "GetTesters",
	"GetPluginUsers", "GetHighestPriorityPluginByUid", "ExistPluginByUid", "SearchPluginUsers",
)

// unfencedReads are reads that are not of slot-owned rows, or explicitly
// non-authoritative views. The value restricts where they may appear ("" means
// any raw-handle site).
var unfencedReads = map[string][]string{
	// Channel Raft state: messages, applied index, term start, hard state.
	"GetMessage": {""}, "LoadPrevRangeMsgs": {""}, "LoadNextRangeMsgs": {""}, "LoadNextRangeMsgsForSize": {""},
	"LoadMsg": {""}, "LoadLastMsgsWithEnd": {""}, "LoadLastMsgs": {""}, "GetChannelLastMessageSeq": {""},
	"SearchMessages": {""}, "GetLastMsg": {""}, "LoadMsgByClientMsgNo": {""}, "LoadMsgBySenderClientMsgNo": {""},
	"LoadMsgsBatch": {""}, "GetUserLastMsgSeq": {""}, "GetUserLastMsgSeqBatch": {""},
	"GetChannelAppliedIndex": {""}, "RaftHardState": {""}, "LeaderTermStartIndex": {""},
	"LeaderLastTerm": {""}, "LeaderLastTermGreaterEqThan": {""},
	// Conversation local users are derived from channel Raft state.
	"GetChannelConversationLocalUsers": {""},
	// Node-local notify queue.
	"GetMessagesOfNotifyQueue": {""},
	// Database shape and process-local helpers, not table reads.
	"GetShardNum": {""}, "GetChannelShardIndex": {""}, "GetCacheManager": {""}, "GetPerformanceMonitor": {""},
	// Approximate node-local counters (admin).
	"GetTotalMessageCount": {"pkg/cluster/cluster/api_"}, "GetTotalUserCount": {"pkg/cluster/cluster/api_"},
	"GetTotalDeviceCount": {"pkg/cluster/cluster/api_"}, "GetTotalSessionCount": {"pkg/cluster/cluster/api_"},
	"GetTotalChannelCount": {"pkg/cluster/cluster/api_"}, "GetTotalConversationCount": {"pkg/cluster/cluster/api_"},
	"GetTotalChannelClusterConfigCount": {"pkg/cluster/cluster/api_"},
	// Node-local plugin table (written without Raft).
	"GetPlugins": {"internal/plugin/"}, "GetPlugin": {"internal/plugin/"}, "GetPluginConfig": {"internal/plugin/"},
}

// unfencedSlotOwnedViews are slot-owned reads allowed without the fence at
// exactly these sites: non-authoritative admin scans across all slots, and
// the reconciler's key discovery, which reloads every key through the fenced
// loader before acting on it. They make no completeness claim.
var unfencedSlotOwnedViews = map[string]string{
	"SearchChannels":             "pkg/cluster/cluster/api_channel.go",
	"SearchConversation":         "pkg/cluster/cluster/api_conversation.go",
	"SearchUser":                 "pkg/cluster/cluster/api_user.go",
	"SearchDevice":               "pkg/cluster/cluster/api_user.go",
	"SearchChannelClusterConfig": "pkg/cluster/cluster/api_node.go",
	"GetChannelClusterConfigs":   "pkg/cluster/cluster/channel_config_reconcile.go",
}

// storeDBMethods may be used through the exported Store.DB(), by the plugin
// server only: the node-local plugin table.
var storeDBMethods = setOf("GetPlugins", "GetPlugin", "GetPluginConfig", "AddOrUpdatePlugin", "UpdatePluginConfig", "DeletePlugin")

var fenceCalls = setOf("readOwned", "readConversations", "FencedRead", "FencedReadSlots")

var readPrefixes = []string{"Get", "Exist", "Has", "Search", "List", "Load", "Is"}

func setOf(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

func isRead(method string) bool {
	if _, ok := unfencedReads[method]; ok {
		return true
	}
	for _, p := range readPrefixes {
		if strings.HasPrefix(method, p) {
			return true
		}
	}
	return false
}

// rawHandle reports which raw handle expr is, or "".
func rawHandle(rel string, expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.CallExpr:
		sel, ok := x.Fun.(*ast.SelectorExpr)
		if !ok || len(x.Args) != 0 {
			return ""
		}
		switch sel.Sel.Name {
		case "WKDB":
			return "WKDB()"
		case "DB":
			if strings.HasSuffix(strings.ToLower(exprString(sel.X)), "store") {
				return "Store.DB()"
			}
		}
	case *ast.SelectorExpr:
		switch {
		case x.Sel.Name == "wdb" && strings.HasPrefix(rel, "pkg/cluster/store/"):
			return "store.wdb"
		case x.Sel.Name == "db" && (strings.HasPrefix(rel, "pkg/cluster/cluster/") || strings.HasPrefix(rel, "pkg/cluster/channel/")):
			return "db"
		case x.Sel.Name == "DB" && strings.HasPrefix(rel, "pkg/cluster/channel/") && strings.HasSuffix(exprString(x.X), "opts"):
			return "channel opts.DB"
		}
	}
	return ""
}

func exprString(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprString(x.X) + "." + x.Sel.Name
	case *ast.CallExpr:
		return exprString(x.Fun) + "()"
	}
	return ""
}

func calleeName(c *ast.CallExpr) string {
	switch f := c.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	case *ast.IndexExpr: // generic instantiation
		return calleeName(&ast.CallExpr{Fun: f.X})
	case *ast.IndexListExpr:
		return calleeName(&ast.CallExpr{Fun: f.X})
	}
	return ""
}

type stackVisitor struct {
	rel    string
	fset   *token.FileSet
	stack  []ast.Node
	issues *[]string
}

func (v *stackVisitor) Visit(n ast.Node) ast.Visitor {
	if n == nil {
		v.stack = v.stack[:len(v.stack)-1]
		return nil
	}
	v.stack = append(v.stack, n)
	sel, ok := n.(*ast.SelectorExpr)
	if !ok {
		return v
	}
	if call, ok := sel.X.(*ast.CallExpr); ok && rawHandle(v.rel, call) == "WKDB()" {
		v.report(sel, "WKDB() has no permitted caller")
		return v
	}
	handle := rawHandle(v.rel, sel.X)
	if handle == "" {
		return v
	}
	method := sel.Sel.Name
	if handle == "Store.DB()" {
		if !storeDBMethods[method] || !strings.HasPrefix(v.rel, "internal/plugin/") {
			v.report(sel, fmt.Sprintf("Store.DB().%s: only the plugin server may use Store.DB(), for the node-local plugin table", method))
		}
		return v
	}
	if !isRead(method) {
		return v // writes go through slot or channel Raft apply
	}
	if slotOwnedReads[method] {
		if v.fenced() {
			return v
		}
		if file, ok := unfencedSlotOwnedViews[method]; ok && file == v.rel {
			return v
		}
		v.report(sel, fmt.Sprintf("%s.%s reads a slot-owned table without the read fence", handle, method))
		return v
	}
	if places, ok := unfencedReads[method]; ok {
		for _, p := range places {
			if p == "" || strings.HasPrefix(v.rel, p) {
				return v
			}
		}
		v.report(sel, fmt.Sprintf("%s.%s is not allowed in %s", handle, method, v.rel))
		return v
	}
	v.report(sel, fmt.Sprintf("%s.%s is an unclassified read; classify it in raw_db_lint_test.go", handle, method))
	return v
}

func (v *stackVisitor) fenced() bool {
	for i := len(v.stack) - 1; i >= 0; i-- {
		if c, ok := v.stack[i].(*ast.CallExpr); ok && fenceCalls[calleeName(c)] {
			return true
		}
	}
	return false
}

func (v *stackVisitor) report(n ast.Node, msg string) {
	*v.issues = append(*v.issues, fmt.Sprintf("%s: %s", v.fset.Position(n.Pos()), msg))
}

func lintSource(t *testing.T, rel string, src any) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, rel, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", rel, err)
	}
	var issues []string
	ast.Walk(&stackVisitor{rel: rel, fset: fset, issues: &issues}, f)
	return issues
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func TestRawDBReadsAreFenced(t *testing.T) {
	root := repoRoot(t)
	var issues []string
	scanned := 0
	for _, dir := range []string{"internal", "pkg", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if d.IsDir() {
				if rel == "pkg/wkdb" || strings.HasPrefix(rel, "pkg/wkdb/") || d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
				return nil
			}
			scanned++
			issues = append(issues, lintSource(t, rel, readFile(t, path))...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if scanned < 100 {
		t.Fatalf("scanned only %d files from %s", scanned, root)
	}
	sort.Strings(issues)
	for _, issue := range issues {
		t.Error(issue)
	}
}

// The lint itself: a new unfenced read of a slot-owned table, a new raw read
// that nobody classified, a WKDB() caller and a non-plugin Store.DB() use all
// fail; fenced reads and allow-listed views pass.
func TestRawDBLintCatchesNewReads(t *testing.T) {
	cases := []struct {
		rel, src string
		want     int
	}{
		{"pkg/cluster/cluster/new.go", `package cluster
func (s *Server) f() { s.db.GetSubscribers("c", 2) }`, 1},
		{"pkg/cluster/cluster/new.go", `package cluster
func (s *Server) f() { r.s.db.GetChannelClusterConfig("c", 2) }`, 1},
		{"pkg/cluster/cluster/new.go", `package cluster
func (s *Server) f() { x := s.db.GetChannelClusterConfigs; _ = x }`, 1},
		{"pkg/cluster/cluster/new.go", `package cluster
func (s *Server) f() { s.db.GetBrandNewThing() }`, 1},
		{"pkg/cluster/store/new.go", `package store
func (s *Store) f() { s.wdb.GetUser("u") }`, 1},
		{"internal/api/new.go", `package api
func f() { service.Store.DB().GetUser("u") }`, 1},
		{"internal/api/new.go", `package api
func f() { service.Store.DB().GetPlugins() }`, 1},
		{"internal/x/new.go", `package x
func f(s IStore) { s.WKDB().GetUser("u") }`, 1},
		{"pkg/cluster/channel/new.go", `package channel
func (s *storage) f() { s.db.GetChannel("c", 2) }`, 1},
		{"pkg/cluster/channel/new.go", `package channel
func (s *Server) f() { s.opts.DB.GetSubscribers("c", 2) }`, 1},
		// Passing cases.
		{"pkg/cluster/store/new.go", `package store
func (s *Store) f() { readOwned(s, 1, func() (int, error) { return s.wdb.GetUser("u") }) }`, 0},
		{"pkg/cluster/cluster/new.go", `package cluster
func (s *Server) f() { icluster.FencedRead(s.slotServer, 1, func() (int, error) { return s.db.GetSubscribers("c", 2) }) }`, 0},
		{"pkg/cluster/cluster/new.go", `package cluster
func (s *Server) f() { s.db.GetChannelLastMessageSeq("c", 2); s.db.AddSubscribers("c", 2, nil) }`, 0},
		{"pkg/cluster/cluster/api_user.go", `package cluster
func (s *Server) f() { s.db.SearchUser(req); s.db.GetTotalUserCount() }`, 0},
		{"internal/plugin/api.go", `package plugin
func f() { service.Store.DB().GetPlugins() }`, 0},
	}
	for i, tc := range cases {
		got := lintSource(t, tc.rel, tc.src)
		if len(got) != tc.want {
			t.Errorf("case %d (%s): issues = %v, want %d", i, tc.rel, got, tc.want)
		}
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
