package beads

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	beads "github.com/steveyegge/beads"

	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/server"
)

// fakeStore is a minimal in-memory beads.Storage for unit tests. It embeds the
// (nil) beads.Storage interface so it satisfies the full type; only the methods
// the driver's write path exercises are implemented. Any other method panics,
// which keeps the fake honest — a test that needs more must add it explicitly.
type fakeStore struct {
	beads.Storage

	mu       sync.Mutex
	byRef    map[string]*beads.Issue
	byID     map[string]*beads.Issue
	config   map[string]string
	deps     int
	idSeq    int64
	failNext atomic.Bool // when set, the next CreateIssue/UpdateIssue returns an error

	txnCommits []string // commit messages passed to RunInTransaction
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		byRef:  map[string]*beads.Issue{},
		byID:   map[string]*beads.Issue{},
		config: map[string]string{},
	}
}

func (s *fakeStore) CreateIssue(ctx context.Context, iss *beads.Issue, actor string) error {
	if s.failNext.Swap(false) {
		return fmt.Errorf("fakeStore: injected create failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idSeq++
	iss.ID = fmt.Sprintf("k8s-%d", s.idSeq)
	cp := *iss
	s.byID[iss.ID] = &cp
	if iss.ExternalRef != nil {
		s.byRef[*iss.ExternalRef] = &cp
	}
	return nil
}

func (s *fakeStore) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	if s.failNext.Swap(false) {
		return fmt.Errorf("fakeStore: injected update failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	iss, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("fakeStore: no issue %s", id)
	}
	if d, ok := updates["description"].(string); ok {
		iss.Description = d
	}
	return nil
}

func (s *fakeStore) GetIssueByExternalRef(ctx context.Context, externalRef string) (*beads.Issue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byRef[externalRef], nil
}

func (s *fakeStore) AddDependency(ctx context.Context, dep *beads.Dependency, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deps++
	return nil
}

func (s *fakeStore) SetConfig(ctx context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config[key] = value
	return nil
}

func (s *fakeStore) GetConfig(ctx context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config[key], nil
}

// newTestLog builds a beadsLog wired to a fake store, bypassing rebuildFromBeads
// so tests start from an empty, deterministic state.
func newTestLog(store beads.Storage) *beadsLog {
	return &beadsLog{
		store:        store,
		ctx:          context.Background(),
		notify:       make(chan int64, 1024),
		pollInterval: defaultPollInterval,
		index:        map[string]entry{},
		types:        map[string]bool{},
		uidToKey:     map[string]string{},
		pending:      map[string][]string{},
	}
}

func createEvent(key string, value []byte) *server.Event {
	return &server.Event{
		Create: true,
		KV:     &server.KeyValue{Key: key, Value: value},
		PrevKV: &server.KeyValue{},
	}
}

// --- Bug 2: type width -------------------------------------------------------

func TestMappedType(t *testing.T) {
	// Short kinds (<= 32 chars) pass through unchanged.
	for _, k := range []string{"", "pods", "configmaps", "deployments", "cluster.x-k8s.io/machines"} {
		if got := mappedType(k); got != k {
			t.Errorf("mappedType(%q) = %q, want unchanged", k, got)
		}
	}
	// A kind of exactly 32 chars is still unchanged (boundary).
	exact := "abcdefghijklmnopqrstuvwxyzABCDEF" // 32
	if len(exact) != 32 {
		t.Fatalf("exact test string is %d chars, want 32", len(exact))
	}
	if got := mappedType(exact); got != exact {
		t.Errorf("mappedType(32-char) = %q, want unchanged", got)
	}

	// The two real 33-char kinds that failed on the rig map to distinct,
	// 32-char, deterministic values.
	a := "validatingadmissionpolicybindings" // 33
	b := "managedresourceactivationpolicies" // 33
	if len(a) != 33 || len(b) != 33 {
		t.Fatalf("test kinds are %d/%d chars, want 33/33", len(a), len(b))
	}
	ma, mb := mappedType(a), mappedType(b)
	if len(ma) != 32 || len(mb) != 32 {
		t.Errorf("mapped lengths = %d/%d, want 32/32 (%q,%q)", len(ma), len(mb), ma, mb)
	}
	if ma == mb {
		t.Errorf("distinct kinds mapped to same value %q", ma)
	}
	if ma != mappedType(a) || mb != mappedType(b) {
		t.Errorf("mappedType is not deterministic")
	}
	// Shape: first 24 chars of the kind, then '-', then 7 hex chars.
	if ma[:24] != a[:24] || ma[24] != '-' {
		t.Errorf("mapped value %q does not have the <24>-<7hex> shape", ma)
	}
}

// TestLongKindStoredWithMappedTypeAndLabel checks that a 33-char kind is written
// with the squeezed issue type and the full-kind label — not silently dropped.
func TestLongKindStoredWithMappedTypeAndLabel(t *testing.T) {
	store := newFakeStore()
	b := newTestLog(store)
	kind := "validatingadmissionpolicybindings"
	key := "/registry/" + kind + "/some-binding"

	rev, err := b.Append(context.Background(), createEvent(key, []byte("payload")))
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if rev != 1 {
		t.Fatalf("rev = %d, want 1", rev)
	}
	iss := store.byRef[key]
	if iss == nil {
		t.Fatalf("no bead written for %q", key)
	}
	if got := string(iss.IssueType); got != mappedType(kind) {
		t.Errorf("issue type = %q, want mapped %q", got, mappedType(kind))
	}
	if len(string(iss.IssueType)) > maxTypeLen {
		t.Errorf("issue type %q exceeds VARCHAR(%d)", iss.IssueType, maxTypeLen)
	}
	foundLabel := false
	for _, l := range iss.Labels {
		if l == kindLabel(kind) {
			foundLabel = true
		}
	}
	if !foundLabel {
		t.Errorf("bead labels %v missing full-kind label %q", iss.Labels, kindLabel(kind))
	}
}

// --- Bug 1.1: After returns only the tail -----------------------------------

func TestAfterReturnsTail(t *testing.T) {
	store := newFakeStore()
	b := newTestLog(store)
	const n = 200
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("/registry/configmaps/ns/cm-%03d", i)
		if _, err := b.Append(context.Background(), createEvent(key, []byte("v"))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// After(rev=150) must return exactly revisions 151..200.
	batch, err := b.After(context.Background(), "", "", 150, 0)
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	if len(batch.Events) != 50 {
		t.Fatalf("After(150) returned %d events, want 50", len(batch.Events))
	}
	if first := batch.Events[0].KV.ModRevision; first != 151 {
		t.Errorf("first returned revision = %d, want 151", first)
	}
	if last := batch.Events[len(batch.Events)-1].KV.ModRevision; last != 200 {
		t.Errorf("last returned revision = %d, want 200", last)
	}
	// After a revision below eventsBase (0) returns the whole retained log.
	all, _ := b.After(context.Background(), "", "", 0, 0)
	if len(all.Events) != n {
		t.Errorf("After(0) returned %d, want %d", len(all.Events), n)
	}
	// Key filter still applies to the tail.
	one := fmt.Sprintf("/registry/configmaps/ns/cm-%03d", 175)
	kb, _ := b.After(context.Background(), one, "", 0, 0)
	if len(kb.Events) != 1 || kb.Events[0].KV.Key != one {
		t.Errorf("After with key filter returned %d events, want 1 for %q", len(kb.Events), one)
	}
}

// --- Bug 1.2: concurrent appends are consecutive and ordered ----------------

func TestConcurrentAppendsConsecutive(t *testing.T) {
	store := newFakeStore()
	b := newTestLog(store)
	const goroutines = 8
	const perG = 100
	var wg sync.WaitGroup
	revs := make([][]int64, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			revs[g] = make([]int64, 0, perG)
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("/registry/configmaps/ns/g%d-%03d", g, i)
				rev, err := b.Append(context.Background(), createEvent(key, []byte("v")))
				if err != nil {
					t.Errorf("append g%d i%d: %v", g, i, err)
					return
				}
				revs[g] = append(revs[g], rev)
			}
		}(g)
	}
	wg.Wait()

	total := goroutines * perG
	// Every revision 1..total must be assigned exactly once across all writers.
	seen := make(map[int64]bool, total)
	for g := 0; g < goroutines; g++ {
		for _, r := range revs[g] {
			if r < 1 || r > int64(total) {
				t.Fatalf("revision %d out of range 1..%d", r, total)
			}
			if seen[r] {
				t.Fatalf("revision %d assigned twice", r)
			}
			seen[r] = true
		}
	}
	if len(seen) != total {
		t.Fatalf("got %d distinct revisions, want %d (gaps present)", len(seen), total)
	}
	// The events log is strictly increasing and gap-free (poller invariant).
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.events) != total {
		t.Fatalf("events len = %d, want %d", len(b.events), total)
	}
	for i, ev := range b.events {
		if ev.KV.ModRevision != int64(i+1) {
			t.Fatalf("events[%d].ModRevision = %d, want %d (not consecutive)", i, ev.KV.ModRevision, i+1)
		}
	}
	if b.rev != int64(total) {
		t.Errorf("final rev = %d, want %d", b.rev, total)
	}
}

// --- Bug 1.2 invariant: a CAS failure returns (0, nil) and does not advance --

func TestCASFailureDoesNotAdvanceRevision(t *testing.T) {
	store := newFakeStore()
	b := newTestLog(store)
	key := "/registry/configmaps/ns/dup"

	rev, err := b.Append(context.Background(), createEvent(key, []byte("v1")))
	if err != nil || rev != 1 {
		t.Fatalf("first create: rev=%d err=%v, want rev=1 nil", rev, err)
	}
	// A second create of the same live key is a CAS miss: (0, nil), no advance.
	rev, err = b.Append(context.Background(), createEvent(key, []byte("v2")))
	if rev != 0 || err != nil {
		t.Fatalf("CAS-miss create: rev=%d err=%v, want 0 nil", rev, err)
	}
	b.mu.RLock()
	gotRev := b.rev
	gotEvents := len(b.events)
	b.mu.RUnlock()
	if gotRev != 1 {
		t.Errorf("rev advanced to %d on CAS miss, want 1", gotRev)
	}
	if gotEvents != 1 {
		t.Errorf("events len = %d after CAS miss, want 1", gotEvents)
	}
}

// --- Bug 2 loud-failure: a write error propagates through logstructured ------

func TestWriteErrorPropagatesThroughLogStructured(t *testing.T) {
	store := newFakeStore()
	b := newTestLog(store)
	ls := logstructured.New(b)

	store.failNext.Store(true)
	rev, err := ls.Create(context.Background(), "/registry/configmaps/ns/boom", []byte("v"), 0)
	if err == nil {
		t.Fatalf("Create returned nil error on a failed write; the error was swallowed")
	}
	if rev != 0 {
		t.Errorf("Create returned rev=%d on a failed write, want 0", rev)
	}
	// The failed write must not have advanced the revision or left a phantom event.
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.rev != 0 {
		t.Errorf("rev = %d after failed write, want 0", b.rev)
	}
	if len(b.events) != 0 {
		t.Errorf("events len = %d after failed write, want 0", len(b.events))
	}
}
