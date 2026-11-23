package beads

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	beads "github.com/steveyegge/beads"

	"github.com/k3s-io/kine/pkg/server"
)

// --- fake-store extensions for the transaction seam ---------------------------

// fakeTx is the beads.Transaction the fake store hands to RunInTransaction; only
// UpdateIssue is implemented (the rollback writes nothing else).
type fakeTx struct {
	beads.Transaction
	s *fakeStore
}

func (t *fakeTx) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	return t.s.UpdateIssue(ctx, id, updates, actor)
}

func (s *fakeStore) RunInTransaction(ctx context.Context, commitMsg string, fn func(tx beads.Transaction) error) error {
	s.mu.Lock()
	s.txnCommits = append(s.txnCommits, commitMsg)
	s.mu.Unlock()
	return fn(&fakeTx{s: s})
}

func (s *fakeStore) GetAllConfig(ctx context.Context) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.config))
	for k, v := range s.config {
		out[k] = v
	}
	return out, nil
}

// snapshot captures the fake store's issue descriptions, standing in for a Dolt
// commit; diffFrom then plays dolt_diff(base, HEAD, 'issues') against it.
type storeSnapshot map[string]*beads.Issue

func (s *fakeStore) snapshot() storeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := storeSnapshot{}
	for id, iss := range s.byID {
		cp := *iss
		snap[id] = &cp
	}
	return snap
}

func (s *fakeStore) diffFrom(snap storeSnapshot) []diffRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows []diffRow
	ref := func(i *beads.Issue) string {
		if i == nil || i.ExternalRef == nil {
			return ""
		}
		return *i.ExternalRef
	}
	for id, cur := range s.byID {
		base, ok := snap[id]
		if !ok {
			rows = append(rows, diffRow{diffType: "added", toID: id, toRef: ref(cur)})
			continue
		}
		if base.Description != cur.Description {
			rows = append(rows, diffRow{diffType: "modified", fromID: id, toID: id, fromRef: ref(base), toRef: ref(cur), fromDesc: base.Description})
		}
	}
	for id, base := range snap {
		if _, ok := s.byID[id]; !ok {
			rows = append(rows, diffRow{diffType: "removed", fromID: id, fromRef: ref(base), fromDesc: base.Description})
		}
	}
	return rows
}

const testBaseCommit = "abcdefghijklmnopqrstuvwxyz012345"

// newTxnTestLog wires a test log with a fake brancher whose diff is served from
// a store snapshot taken at begin time (the "commit").
func newTxnTestLog(t *testing.T) (*beadsLog, *fakeStore, *fakeBrancher) {
	t.Helper()
	store := newFakeStore()
	b := newTestLog(store)
	b.curBranch = "main"
	var snap storeSnapshot
	fb := &fakeBrancher{
		defaultBranch: "main",
		branches:      []string{"main"},
		commits:       map[string]string{"main": testBaseCommit},
	}
	fb.diff = func(fromCommit, branch string) ([]diffRow, error) {
		if fromCommit != testBaseCommit {
			return nil, fmt.Errorf("unexpected base commit %q", fromCommit)
		}
		if branch != "main" {
			return nil, fmt.Errorf("unexpected branch %q", branch)
		}
		return store.diffFrom(snap), nil
	}
	b.ctl = fb
	// BranchCommit is read at begin; snapshot the store at that moment so the
	// "commit" and the diff base agree.
	fb.onCommit = func() { snap = store.snapshot() }
	return b, store, fb
}

// objJSON renders a minimal k8s object (JSON-stored, like a CR) with uid,
// ownerReferences and annotations, for ownership-cone tests.
func objJSON(uid string, owners []string, ann map[string]string, payload string) []byte {
	type ref struct {
		UID string `json:"uid"`
	}
	refs := make([]ref, 0, len(owners))
	for _, o := range owners {
		refs = append(refs, ref{UID: o})
	}
	m := map[string]interface{}{
		"metadata": map[string]interface{}{"uid": uid, "ownerReferences": refs, "annotations": ann},
		"spec":     payload,
	}
	raw, _ := json.Marshal(m)
	return raw
}

func mustAppend(t *testing.T, b *beadsLog, ev *server.Event) int64 {
	t.Helper()
	rev, err := b.Append(context.Background(), ev)
	if err != nil {
		t.Fatalf("append %s: %v", ev.KV.Key, err)
	}
	if rev == 0 {
		t.Fatalf("append %s: CAS miss", ev.KV.Key)
	}
	return rev
}

func updateEvent(b *beadsLog, key string, value []byte) *server.Event {
	b.mu.RLock()
	cur := b.index[key]
	b.mu.RUnlock()
	return &server.Event{
		KV:     &server.KeyValue{Key: key, Value: value},
		PrevKV: &server.KeyValue{Key: key, ModRevision: cur.r.ModRevision},
	}
}

func deleteEvent(b *beadsLog, key string) *server.Event {
	b.mu.RLock()
	cur := b.index[key]
	b.mu.RUnlock()
	return &server.Event{
		Delete: true,
		KV:     &server.KeyValue{Key: key},
		PrevKV: &server.KeyValue{Key: key, ModRevision: cur.r.ModRevision},
	}
}

func liveKeys(b *beadsLog) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []string
	for k, e := range b.index {
		if !e.r.Deleted {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// --- scope key mapping ---------------------------------------------------------

func TestMapScopeKeyShapes(t *testing.T) {
	cases := map[string]string{
		"configmaps/rewind-smoke/cm":                                   "/registry/configmaps/rewind-smoke/cm",
		"apps/deployments/rewind-smoke/web":                            "/registry/deployments/rewind-smoke/web",
		"apps/replicasets/ns/web-abc":                                  "/registry/replicasets/ns/web-abc",
		"cluster.x-k8s.io/machinedeployments/default/mirage-fork-md-0": "/registry/cluster.x-k8s.io/machinedeployments/default/mirage-fork-md-0",
		"infrastructure.cluster.x-k8s.io/miragemachines/default/m":     "/registry/infrastructure.cluster.x-k8s.io/miragemachines/default/m",
		"nodes/mirage-fork-md-0-x":                                     "/registry/minions/mirage-fork-md-0-x",
		"namespaces/rewind-smoke":                                      "/registry/namespaces/rewind-smoke",
		"services/ns/svc":                                              "/registry/services/specs/ns/svc",
		"rbac.authorization.k8s.io/roles/ns/r":                         "/registry/roles/ns/r",
		"rbac.authorization.k8s.io/clusterroles/cr":                    "/registry/clusterroles/cr",
		"apiextensions.k8s.io/customresourcedefinitions/x.y.z":         "/registry/apiextensions.k8s.io/customresourcedefinitions/x.y.z",
		"storage.k8s.io/storageclasses/local-path":                     "/registry/storageclasses/local-path",
		"batch/jobs/ns/j":                                              "/registry/jobs/ns/j",
		"/registry/already/mapped":                                     "/registry/already/mapped",
	}
	for in, want := range cases {
		got, err := mapScopeKey(in)
		if err != nil {
			t.Errorf("mapScopeKey(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("mapScopeKey(%q) = %q, want %q", in, got, want)
		}
	}
	for _, bad := range []string{"", "pods", "a//b", "a/b/c/d/e"} {
		if _, err := mapScopeKey(bad); err == nil {
			t.Errorf("mapScopeKey(%q) accepted, want error", bad)
		}
	}
}

func TestKeyNamespace(t *testing.T) {
	cases := map[string]string{
		"/registry/pods/ns1/p":                                       "ns1",
		"/registry/minions/n":                                        "",
		"/registry/cluster.x-k8s.io/machines/default/m":              "default",
		"/registry/cluster.x-k8s.io/clusterclasses/cc":               "",
		"/registry/services/specs/ns2/s":                             "ns2",
		"/registry/services/endpoints/ns3/s":                         "ns3",
		"/registry/leases/kube-node-lease/n":                         "kube-node-lease",
		"/registry/apiextensions.k8s.io/customresourcedefinitions/x": "",
		"/kine/other": "",
	}
	for in, want := range cases {
		if got := keyNamespace(in); got != want {
			t.Errorf("keyNamespace(%q) = %q, want %q", in, got, want)
		}
	}
}

// --- overlap detection ---------------------------------------------------------

func TestScopesOverlap(t *testing.T) {
	mk := func(keys, prefixes, namespaces []string) *txnRecord {
		r := &txnRecord{Scope: txnScope{Keys: keys, Prefixes: prefixes, Namespaces: namespaces}}
		for _, k := range keys {
			m, err := mapScopeKey(k)
			if err != nil {
				t.Fatal(err)
			}
			r.EtcdKeys = append(r.EtcdKeys, m)
		}
		return r
	}
	nsA := mk(nil, nil, []string{"a"})
	nsB := mk(nil, nil, []string{"b"})
	keyInA := mk([]string{"configmaps/a/x"}, nil, nil)
	keyInB := mk([]string{"configmaps/b/x"}, nil, nil)
	sameKey := mk([]string{"configmaps/b/x"}, nil, nil)
	prefA := mk(nil, []string{"/registry/configmaps/a/"}, nil)
	prefAll := mk(nil, []string{"/registry/configmaps/"}, nil)
	clusterKey := mk([]string{"nodes/n1"}, nil, nil)

	cases := []struct {
		name string
		x, y *txnRecord
		want bool
	}{
		{"disjoint namespaces", nsA, nsB, false},
		{"same namespace", nsA, mk(nil, nil, []string{"a"}), true},
		{"key inside other's namespace", keyInA, nsA, true},
		{"key outside other's namespace", keyInA, nsB, false},
		{"same key", keyInB, sameKey, true},
		{"different keys", keyInA, keyInB, false},
		{"prefix contains prefix", prefA, prefAll, true},
		{"prefix covers key", prefAll, keyInB, true},
		{"prefix lands in namespace", prefA, nsA, true},
		{"prefix outside namespace", prefA, nsB, false},
		{"cluster-scoped key vs namespace", clusterKey, nsA, false},
	}
	for _, c := range cases {
		if got := scopesOverlap(c.x, c.y); got != c.want {
			t.Errorf("%s: scopesOverlap = %v, want %v", c.name, got, c.want)
		}
		if got := scopesOverlap(c.y, c.x); got != c.want {
			t.Errorf("%s (reversed): scopesOverlap = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestBeginRefusesOverlap(t *testing.T) {
	b, _, fb := newTxnTestLog(t)
	ctx := context.Background()
	_ = fb
	if _, err := b.txnBegin(ctx, txnBeginRequest{ID: "t1", Scope: txnScope{Namespaces: []string{"app"}}}); err != nil {
		t.Fatalf("begin t1: %v", err)
	}
	_, err := b.txnBegin(ctx, txnBeginRequest{ID: "t2", Scope: txnScope{Keys: []string{"configmaps/app/cm"}}})
	he, ok := err.(*httpError)
	if !ok || he.code != http.StatusConflict {
		t.Fatalf("begin t2 overlapping: err=%v, want 409", err)
	}
	if _, err := b.txnBegin(ctx, txnBeginRequest{ID: "t3", Scope: txnScope{Namespaces: []string{"other"}}}); err != nil {
		t.Fatalf("begin t3 disjoint: %v", err)
	}
}

// --- rollback: the three diff cases ----------------------------------------------

func TestRollbackRevertsCreateModifyDelete(t *testing.T) {
	b, store, _ := newTxnTestLog(t)
	ctx := context.Background()
	ns := "rewind-smoke"
	keep := "/registry/configmaps/" + ns + "/keep"      // modified in the window
	gone := "/registry/configmaps/" + ns + "/gone"      // deleted in the window
	outside := "/registry/configmaps/other/untouched"   // out of scope, modified
	newKey := "/registry/configmaps/" + ns + "/created" // created in the window

	mustAppend(t, b, createEvent(keep, objJSON("u-keep", nil, nil, "v1")))
	mustAppend(t, b, createEvent(gone, objJSON("u-gone", nil, nil, "v1")))
	mustAppend(t, b, createEvent(outside, objJSON("u-out", nil, nil, "v1")))

	tx, err := b.txnBegin(ctx, txnBeginRequest{ID: "smoke/1", Scope: txnScope{Namespaces: []string{ns}}})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if tx.BaseRev != 3 || tx.BaseCommit != testBaseCommit || tx.State != txnOpen {
		t.Fatalf("begin record = %+v", tx)
	}

	mustAppend(t, b, updateEvent(b, keep, objJSON("u-keep", nil, nil, "v2")))
	mustAppend(t, b, deleteEvent(b, gone))
	mustAppend(t, b, createEvent(newKey, objJSON("u-new", nil, nil, "v1")))
	mustAppend(t, b, updateEvent(b, outside, objJSON("u-out", nil, nil, "v2")))
	b.mu.RLock()
	before := b.rev
	b.mu.RUnlock()

	res, err := b.txnRollback(ctx, "smoke/1")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if res.State != txnRolledBack || res.Result == nil {
		t.Fatalf("rollback record = %+v", res)
	}
	if res.Result.Events != 3 {
		t.Errorf("events = %d, want 3 (delete created, recreate deleted, revert modified)", res.Result.Events)
	}
	wantReverted := []string{newKey, gone, keep} // deletes, then creates, then updates
	if strings.Join(res.Result.Reverted, ",") != strings.Join(wantReverted, ",") {
		t.Errorf("reverted = %v, want %v", res.Result.Reverted, wantReverted)
	}

	// Events: fresh consecutive revisions, delete first.
	b.mu.RLock()
	evs := b.events[len(b.events)-3:]
	endRev := b.rev
	b.mu.RUnlock()
	if endRev != before+3 || res.Result.EndRev != endRev {
		t.Errorf("rev = %d (endRev %d), want %d", endRev, res.Result.EndRev, before+3)
	}
	for i, ev := range evs {
		if ev.KV.ModRevision != before+int64(i+1) {
			t.Errorf("event[%d] rev = %d, want %d", i, ev.KV.ModRevision, before+int64(i+1))
		}
	}
	if !evs[0].Delete || evs[0].KV.Key != newKey || evs[0].PrevKV == nil {
		t.Errorf("event[0] should delete %s with PrevKV: %+v", newKey, evs[0])
	}
	if !evs[1].Create || evs[1].KV.Key != gone || string(evs[1].KV.Value) != string(objJSON("u-gone", nil, nil, "v1")) {
		t.Errorf("event[1] should recreate %s with the base value: %+v", gone, evs[1])
	}
	if evs[1].KV.CreateRevision != evs[1].KV.ModRevision {
		t.Errorf("recreate must carry a fresh CreateRevision: %+v", evs[1].KV)
	}
	if evs[2].Create || evs[2].Delete || evs[2].KV.Key != keep || string(evs[2].KV.Value) != string(objJSON("u-keep", nil, nil, "v1")) {
		t.Errorf("event[2] should restore %s to the base value: %+v", keep, evs[2])
	}
	if evs[2].PrevKV == nil || string(evs[2].PrevKV.Value) != string(objJSON("u-keep", nil, nil, "v2")) {
		t.Errorf("event[2] PrevKV should carry the current value: %+v", evs[2].PrevKV)
	}
	if evs[2].KV.CreateRevision != 1 {
		t.Errorf("update must keep CreateRevision 1, got %d", evs[2].KV.CreateRevision)
	}

	// Index: in-scope keys back to base, out-of-scope untouched.
	got := liveKeys(b)
	want := []string{gone, keep, outside}
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("live keys = %v, want %v", got, want)
	}
	b.mu.RLock()
	outEnt := b.index[outside]
	b.mu.RUnlock()
	if string(outEnt.r.Value) != string(objJSON("u-out", nil, nil, "v2")) || outEnt.r.ModRevision != 7 {
		t.Errorf("out-of-scope key was touched: %+v", outEnt)
	}

	// Durable: the fake store holds the reverted records, written in one
	// transaction.
	for _, k := range []string{keep, gone, newKey} {
		iss := store.byRef[k]
		r, _ := decodeRec(iss.Description)
		b.mu.RLock()
		ent := b.index[k]
		b.mu.RUnlock()
		if r.ModRevision != ent.r.ModRevision || r.Deleted != ent.r.Deleted || string(r.Value) != string(ent.r.Value) {
			t.Errorf("durable record for %s = %+v, index %+v", k, r, ent.r)
		}
	}
	if len(store.txnCommits) != 1 {
		t.Errorf("rollback used %d store transactions, want 1", len(store.txnCommits))
	}
	// Ledger closed.
	if raw := store.config[txnConfigPrefix+"smoke/1"]; !strings.Contains(raw, `"state":"rolledBack"`) {
		t.Errorf("ledger entry not rolledBack: %s", raw)
	}
}

// --- cone expansion -------------------------------------------------------------

func TestRollbackExpandsOwnershipCone(t *testing.T) {
	b, _, _ := newTxnTestLog(t)
	ctx := context.Background()
	deploy := "/registry/deployments/app/web"
	rs := "/registry/replicasets/app/web-1"
	pod := "/registry/pods/app/web-1-x"
	cm := "/registry/configmaps/elsewhere/unrelated"
	node := "/registry/minions/web-node"
	lease := "/registry/leases/kube-node-lease/web-node"
	machine := "/registry/cluster.x-k8s.io/machines/app/web-node"

	// Scope is the Deployment key only (no namespace), with expandOwners: the
	// ReplicaSet and the Pod are reached through ownerReferences; the Node
	// through the CAPI annotation to a Machine owned by the Deployment (stand-in
	// for a MachineSet); the node lease through its name.
	tx, err := b.txnBegin(ctx, txnBeginRequest{ID: "cone", Scope: txnScope{Keys: []string{"apps/deployments/app/web"}, ExpandOwners: true}})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if tx.EtcdKeys[0] != deploy {
		t.Fatalf("mapped key = %v", tx.EtcdKeys)
	}
	mustAppend(t, b, createEvent(deploy, objJSON("u-dep", nil, nil, "d")))
	mustAppend(t, b, createEvent(rs, objJSON("u-rs", []string{"u-dep"}, nil, "r")))
	mustAppend(t, b, createEvent(pod, objJSON("u-pod", []string{"u-rs"}, nil, "p")))
	mustAppend(t, b, createEvent(cm, objJSON("u-cm", nil, nil, "c")))
	mustAppend(t, b, createEvent(machine, objJSON("u-m", []string{"u-dep"}, nil, "m")))
	mustAppend(t, b, createEvent(node, objJSON("u-node", nil, map[string]string{"cluster.x-k8s.io/machine": "web-node", "cluster.x-k8s.io/cluster-namespace": "app"}, "n")))
	mustAppend(t, b, createEvent(lease, objJSON("u-lease", nil, nil, "l")))

	res, err := b.txnRollback(ctx, "cone")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	want := []string{machine, deploy, lease, node, pod, rs}
	sort.Strings(want)
	gotRev := append([]string(nil), res.Result.Reverted...)
	sort.Strings(gotRev)
	if strings.Join(gotRev, ",") != strings.Join(want, ",") {
		t.Errorf("reverted = %v, want %v", gotRev, want)
	}
	if got := liveKeys(b); len(got) != 1 || got[0] != cm {
		t.Errorf("live keys after rollback = %v, want only %s", got, cm)
	}
}

func TestRollbackWithoutExpandOwnersLeavesChildren(t *testing.T) {
	b, _, _ := newTxnTestLog(t)
	ctx := context.Background()
	deploy := "/registry/deployments/app/web"
	rs := "/registry/replicasets/app/web-1"
	if _, err := b.txnBegin(ctx, txnBeginRequest{ID: "flat", Scope: txnScope{Keys: []string{"apps/deployments/app/web"}}}); err != nil {
		t.Fatalf("begin: %v", err)
	}
	mustAppend(t, b, createEvent(deploy, objJSON("u-dep", nil, nil, "d")))
	mustAppend(t, b, createEvent(rs, objJSON("u-rs", []string{"u-dep"}, nil, "r")))
	res, err := b.txnRollback(ctx, "flat")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if len(res.Result.Reverted) != 1 || res.Result.Reverted[0] != deploy {
		t.Errorf("reverted = %v, want only the deployment", res.Result.Reverted)
	}
}

// --- ledger ---------------------------------------------------------------------

func TestLedgerIdempotencyAndClosedIDs(t *testing.T) {
	b, store, _ := newTxnTestLog(t)
	ctx := context.Background()
	mustAppend(t, b, createEvent("/registry/configmaps/ns/a", []byte("a")))

	first, err := b.txnBegin(ctx, txnBeginRequest{ID: "idem", Scope: txnScope{Namespaces: []string{"ns"}}})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	mustAppend(t, b, createEvent("/registry/configmaps/ns/b", []byte("b")))
	again, err := b.txnBegin(ctx, txnBeginRequest{ID: "idem", Scope: txnScope{Namespaces: []string{"ns"}}})
	if err != nil {
		t.Fatalf("second begin: %v", err)
	}
	if again.BaseRev != first.BaseRev || again.StartedAt != first.StartedAt {
		t.Errorf("begin not idempotent: %+v vs %+v", first, again)
	}
	if raw := store.config[txnConfigPrefix+"idem"]; !strings.Contains(raw, `"state":"open"`) || !strings.Contains(raw, `"baseRev":1`) {
		t.Errorf("ledger entry = %s", raw)
	}

	if _, err := b.txnCommit(ctx, "idem"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for name, f := range map[string]func() error{
		"commit again": func() error { _, err := b.txnCommit(ctx, "idem"); return err },
		"rollback":     func() error { _, err := b.txnRollback(ctx, "idem"); return err },
		"begin closed": func() error {
			_, err := b.txnBegin(ctx, txnBeginRequest{ID: "idem", Scope: txnScope{Namespaces: []string{"ns"}}})
			return err
		},
	} {
		err := f()
		he, ok := err.(*httpError)
		if !ok || he.code != http.StatusConflict {
			t.Errorf("%s on a committed id: err=%v, want 409", name, err)
		}
	}
	if _, err := b.txnGet("nope"); err == nil || err.(*httpError).code != http.StatusNotFound {
		t.Errorf("get unknown: err=%v, want 404", err)
	}
	// After the commit, the same scope can be begun under a new id, and a
	// restart reloads the ledger from config.
	if _, err := b.txnBegin(ctx, txnBeginRequest{ID: "idem-2", Scope: txnScope{Namespaces: []string{"ns"}}}); err != nil {
		t.Fatalf("begin after commit: %v", err)
	}
	b2 := newTestLog(store)
	b2.loadTxnLedger(ctx)
	if got := b2.txnList(); len(got) != 2 {
		t.Errorf("reloaded ledger has %d entries, want 2: %v", len(got), got)
	}
	if t2, err := b2.txnGet("idem-2"); err != nil || t2.State != txnOpen {
		t.Errorf("reloaded idem-2 = %+v, %v", t2, err)
	}
}

// --- bearer auth ----------------------------------------------------------------

func TestBearerAuthOnControlMux(t *testing.T) {
	b, _, _ := newTxnTestLog(t)
	srv := httptest.NewServer(bearerAuth("s3cret", b.controlMux()))
	defer srv.Close()

	get := func(auth string) int {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/txn", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := get(""); code != http.StatusUnauthorized {
		t.Errorf("no token: %d, want 401", code)
	}
	if code := get("Bearer wrong"); code != http.StatusUnauthorized {
		t.Errorf("wrong token: %d, want 401", code)
	}
	if code := get("Bearer s3cret"); code != http.StatusOK {
		t.Errorf("right token: %d, want 200", code)
	}
	// The unauthenticated mux (unix socket) serves without a token.
	plain := httptest.NewServer(b.controlMux())
	defer plain.Close()
	resp, err := http.Get(plain.URL + "/txn")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("unix-style mux: %d, want 200", resp.StatusCode)
	}
}

func TestControlMuxTxnRoutes(t *testing.T) {
	b, _, _ := newTxnTestLog(t)
	srv := httptest.NewServer(b.controlMux())
	defer srv.Close()
	post := func(path, body string) (int, map[string]interface{}) {
		resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	code, m := post("/txn/begin", `{"id":"argo/argocd/app/abc","scope":{"namespaces":["x"],"keys":["apps/deployments/x/web"],"expandOwners":true}}`)
	if code != 200 || m["state"] != "open" || m["baseCommit"] != testBaseCommit {
		t.Fatalf("begin: %d %v", code, m)
	}
	code, m = post("/txn/begin", `{"id":"other","scope":{"namespaces":["x"]}}`)
	if code != 409 {
		t.Errorf("overlapping begin: %d %v, want 409", code, m)
	}
	code, m = post("/txn/begin", `{"id":"empty","scope":{}}`)
	if code != 400 {
		t.Errorf("empty scope: %d %v, want 400", code, m)
	}
	resp, _ := http.Get(srv.URL + "/txn/argo/argocd/app/abc")
	var got map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if got["state"] != "open" || got["scope"] == nil {
		t.Errorf("GET /txn/{id} = %v", got)
	}
	code, m = post("/txn/argo/argocd/app/abc/rollback", "")
	if code != 200 || m["state"] != "rolledBack" || m["events"] != float64(0) {
		t.Errorf("rollback: %d %v", code, m)
	}
	code, m = post("/txn/argo/argocd/app/abc/commit", "")
	if code != 409 {
		t.Errorf("commit after rollback: %d %v, want 409", code, m)
	}
	resp, _ = http.Get(srv.URL + "/txn/missing")
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("GET missing: %d, want 404", resp.StatusCode)
	}
}
