package beads

import (
	"context"
	"sort"
	"testing"
)

// mkEntry builds an index entry for tests.
func mkEntry(id string, value []byte, cr, mr int64, deleted bool) entry {
	return entry{id: id, r: rec{Value: value, CreateRevision: cr, ModRevision: mr, Deleted: deleted}}
}

// --- diffToEvents: the three cases --------------------------------------------

func TestDiffToEventsForkOnlyKeyDeletes(t *testing.T) {
	// A key present on the current (fork) branch but absent on the target becomes
	// a delete, carrying the live value as PrevKV.
	live := map[string]entry{
		"/registry/configmaps/ns/on-fork": mkEntry("k8s-1", []byte("v"), 5, 5, false),
	}
	candidate := map[string]entry{}

	events, idx, end := diffToEvents(live, candidate, 100)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	if !ev.Delete {
		t.Errorf("event is not a delete: %+v", ev)
	}
	if ev.KV.ModRevision != 101 || end != 101 {
		t.Errorf("delete rev = %d (end %d), want 101", ev.KV.ModRevision, end)
	}
	if ev.PrevKV == nil || string(ev.PrevKV.Value) != "v" {
		t.Errorf("delete PrevKV missing live value: %+v", ev.PrevKV)
	}
	tomb, ok := idx["/registry/configmaps/ns/on-fork"]
	if !ok || !tomb.r.Deleted || tomb.r.ModRevision != 101 {
		t.Errorf("index tombstone wrong: %+v (ok=%v)", tomb, ok)
	}
}

func TestDiffToEventsMainOnlyKeyCreates(t *testing.T) {
	// A key present on the target (main) but not on the current branch becomes a
	// create carrying main's value.
	live := map[string]entry{}
	candidate := map[string]entry{
		"/registry/configmaps/ns/restore": mkEntry("k8s-9", []byte("mainval"), 3, 7, false),
	}

	events, idx, end := diffToEvents(live, candidate, 200)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	if !ev.Create {
		t.Errorf("event is not a create: %+v", ev)
	}
	if string(ev.KV.Value) != "mainval" {
		t.Errorf("create value = %q, want mainval", ev.KV.Value)
	}
	if ev.KV.ModRevision != 201 || ev.KV.CreateRevision != 201 || end != 201 {
		t.Errorf("create revs = cr %d mr %d (end %d), want 201/201/201", ev.KV.CreateRevision, ev.KV.ModRevision, end)
	}
	if ev.PrevKV != nil {
		t.Errorf("create must not carry PrevKV: %+v", ev.PrevKV)
	}
	got := idx["/registry/configmaps/ns/restore"]
	if got.id != "k8s-9" || got.r.ModRevision != 201 {
		t.Errorf("index entry stamped wrong: %+v", got)
	}
}

func TestDiffToEventsDifferingValueUpdatesWithPrevKV(t *testing.T) {
	// A key on both branches with different values becomes an update carrying the
	// target's value as KV and the current value as PrevKV.
	live := map[string]entry{
		"/registry/configmaps/ns/base": mkEntry("k8s-2", []byte("forkval"), 4, 50, false),
	}
	candidate := map[string]entry{
		"/registry/configmaps/ns/base": mkEntry("k8s-2", []byte("mainval"), 4, 10, false),
	}

	events, idx, end := diffToEvents(live, candidate, 300)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.Create || ev.Delete {
		t.Errorf("event should be a plain update: %+v", ev)
	}
	if string(ev.KV.Value) != "mainval" {
		t.Errorf("update KV value = %q, want mainval", ev.KV.Value)
	}
	if ev.PrevKV == nil || string(ev.PrevKV.Value) != "forkval" {
		t.Errorf("update PrevKV = %+v, want forkval", ev.PrevKV)
	}
	if ev.PrevKV.ModRevision != 50 {
		t.Errorf("update PrevKV.ModRevision = %d, want 50 (live)", ev.PrevKV.ModRevision)
	}
	if ev.KV.ModRevision != 301 || end != 301 {
		t.Errorf("update rev = %d (end %d), want 301", ev.KV.ModRevision, end)
	}
	if got := idx["/registry/configmaps/ns/base"]; got.r.ModRevision != 301 || string(got.r.Value) != "mainval" {
		t.Errorf("index entry wrong after update: %+v", got)
	}
}

// --- diffToEvents: ordering and strictly consecutive revisions ----------------

func TestDiffToEventsConsecutiveRevisionsDeletesBeforeCreates(t *testing.T) {
	live := map[string]entry{
		"/registry/configmaps/ns/del-a": mkEntry("k8s-1", []byte("a"), 1, 10, false),
		"/registry/configmaps/ns/del-b": mkEntry("k8s-2", []byte("b"), 1, 11, false),
		"/registry/configmaps/ns/upd-c": mkEntry("k8s-3", []byte("c-fork"), 1, 12, false),
		"/registry/configmaps/ns/same":  mkEntry("k8s-4", []byte("s"), 1, 13, false),
	}
	candidate := map[string]entry{
		"/registry/configmaps/ns/new-x": mkEntry("k8s-5", []byte("x"), 2, 20, false),
		"/registry/configmaps/ns/new-y": mkEntry("k8s-6", []byte("y"), 2, 21, false),
		"/registry/configmaps/ns/upd-c": mkEntry("k8s-3", []byte("c-main"), 1, 9, false),
		"/registry/configmaps/ns/same":  mkEntry("k8s-4", []byte("s"), 1, 13, false),
	}

	events, _, end := diffToEvents(live, candidate, 1000)
	// 2 deletes + 2 creates + 1 update = 5 events, revs 1001..1005 consecutive.
	if len(events) != 5 {
		t.Fatalf("got %d events, want 5", len(events))
	}
	if end != 1005 {
		t.Errorf("end rev = %d, want 1005", end)
	}
	for i, ev := range events {
		want := int64(1001 + i)
		if ev.KV.ModRevision != want {
			t.Errorf("event[%d] rev = %d, want %d (not consecutive)", i, ev.KV.ModRevision, want)
		}
	}
	// First two must be deletes, then creates, then the update last.
	if !events[0].Delete || !events[1].Delete {
		t.Errorf("first two events must be deletes: %v %v", events[0].Delete, events[1].Delete)
	}
	if !events[2].Create || !events[3].Create {
		t.Errorf("events 2,3 must be creates: %v %v", events[2].Create, events[3].Create)
	}
	if events[4].Create || events[4].Delete {
		t.Errorf("last event must be a plain update: %+v", events[4])
	}
	// The "same" key must not produce an event.
	for _, ev := range events {
		if ev.KV.Key == "/registry/configmaps/ns/same" {
			t.Errorf("unchanged key produced an event: %+v", ev)
		}
	}
}

// --- status reporting with a fake brancher ------------------------------------

type fakeBrancher struct {
	defaultBranch string
	branches      []string
	commits       map[string]string
	created       []string
	forced        [][2]string
	dropped       []string
	diff          func(fromCommit, branch string) ([]diffRow, error)
	onCommit      func() // called on BranchCommit (tests snapshot the store here)
}

func (f *fakeBrancher) CreateBranch(ctx context.Context, name, startPoint string) error {
	f.created = append(f.created, name)
	f.branches = append(f.branches, name)
	if f.commits == nil {
		f.commits = map[string]string{}
	}
	f.commits[name] = f.commits[startPoint]
	return nil
}
func (f *fakeBrancher) ForceBranch(ctx context.Context, name, startPoint string) error {
	f.forced = append(f.forced, [2]string{name, startPoint})
	f.commits[name] = f.commits[startPoint]
	return nil
}
func (f *fakeBrancher) DropBranch(ctx context.Context, name string) error {
	f.dropped = append(f.dropped, name)
	out := f.branches[:0]
	for _, b := range f.branches {
		if b != name {
			out = append(out, b)
		}
	}
	f.branches = out
	return nil
}
func (f *fakeBrancher) SetDefaultBranch(ctx context.Context, branch string) error {
	f.defaultBranch = branch
	return nil
}
func (f *fakeBrancher) GetDefaultBranch(ctx context.Context) (string, error) {
	return f.defaultBranch, nil
}
func (f *fakeBrancher) ListBranches(ctx context.Context) ([]string, error) {
	out := append([]string(nil), f.branches...)
	sort.Strings(out)
	return out, nil
}
func (f *fakeBrancher) BranchCommit(ctx context.Context, name string) (string, error) {
	if f.onCommit != nil {
		f.onCommit()
	}
	return f.commits[name], nil
}
func (f *fakeBrancher) DiffSince(ctx context.Context, fromCommit, branch string) ([]diffRow, error) {
	if f.diff != nil {
		return f.diff(fromCommit, branch)
	}
	return nil, nil
}
func (f *fakeBrancher) Close() error { return nil }

func TestStatusReporting(t *testing.T) {
	b := newTestLog(newFakeStore())
	b.ctl = &fakeBrancher{
		defaultBranch: "try-1",
		branches:      []string{"main", "try-1"},
		commits:       map[string]string{"main": "abc", "try-1": "abc"},
	}
	b.curBranch = "try-1"
	b.forkBase = 42
	b.forkCommit = "deadbeefcafe0000"
	b.rev = 57

	st, err := b.status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Branch != "try-1" {
		t.Errorf("branch = %q, want try-1", st.Branch)
	}
	if st.ForkBase != 42 {
		t.Errorf("fork_base = %d, want 42", st.ForkBase)
	}
	if st.Rev != 57 || st.HWM != 57 {
		t.Errorf("rev/hwm = %d/%d, want 57/57", st.Rev, st.HWM)
	}
	if len(st.Branches) != 2 || st.Branches[0] != "main" || st.Branches[1] != "try-1" {
		t.Errorf("branches = %v, want [main try-1]", st.Branches)
	}
}
