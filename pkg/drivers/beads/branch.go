package beads

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/k3s-io/kine/pkg/server"
)

// Branch control turns kine's beads driver into the forkable control plane the
// k3s-on-beads-fork spec describes: fork the Dolt-backed store at the current
// revision, let the running control plane reconcile on the fork, then either
// promote the fork (it becomes main) or switch back to main and emit the inverse
// of the fork's changes as fresh revisions. No process restarts: kine flips the
// Dolt default branch, reopens its beads store onto the new branch, and — for a
// switch — replays a diff as synthetic watch events so no resourceVersion ever
// goes backwards.
//
// Everything here is gated behind the control socket (--branch-control-socket /
// KINE_BRANCH_CONTROL_SOCKET) and requires SERVER mode (a shared dolt
// sql-server), since the branch flips go through a kine-owned MySQL control
// connection to that server.

// hwmConfigKey is the store-config key holding the revision high-water mark. It
// is persisted on every fork/switch/promote and on shutdown so a restart
// recovers rev = max(max(mr), hwm): after a rollback kine's counter is ahead of
// the highest mr durably stored on the target branch (the synthetic revisions
// live only in memory), and recovering from max(mr) alone would re-hand numbers
// it has already used.
const hwmConfigKey = "kine.rev_hwm"

// dbNameRE guards the database name that is interpolated into the
// `@@GLOBAL.<db>_default_branch` variable name (a system variable name cannot be
// a bound parameter). Dolt database names are identifiers, so this is a tight
// allowlist, not general escaping.
var dbNameRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// branchNameRE guards branch names that are interpolated into a `USE db/branch`
// statement (USE cannot take a bound parameter). DOLT_BRANCH arguments are bound,
// so this is belt-and-braces there; it is load-bearing for the USE path.
var branchNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// brancher is the narrow Dolt branch-control surface the driver needs. It is an
// interface so unit tests inject a fake without a live sql-server; the live
// implementation is doltControl over a database/sql MySQL connection.
type brancher interface {
	// CreateBranch makes name at the commit of startPoint (a branch name). Passing
	// the start point explicitly makes the result independent of whichever branch
	// the pooled control connection happens to sit on.
	CreateBranch(ctx context.Context, name, startPoint string) error
	// ForceBranch force-moves name to startPoint's commit (promote's fast-forward).
	ForceBranch(ctx context.Context, name, startPoint string) error
	// DropBranch deletes name. Refused by Dolt for a checked-out branch.
	DropBranch(ctx context.Context, name string) error
	// SetDefaultBranch points @@GLOBAL.<db>_default_branch at branch: every NEW
	// connection then lands on it; existing sessions keep theirs.
	SetDefaultBranch(ctx context.Context, branch string) error
	GetDefaultBranch(ctx context.Context) (string, error)
	ListBranches(ctx context.Context) ([]string, error)
	// BranchCommit returns the commit hash at the head of name.
	BranchCommit(ctx context.Context, name string) (string, error)
	// DiffSince returns the issues rows that differ between fromCommit and the
	// head of branch (dolt_diff(from, branch, 'issues')), with the record as of
	// fromCommit. Used by the scoped-transaction rollback.
	DiffSince(ctx context.Context, fromCommit, branch string) ([]diffRow, error)
	Close() error
}

// doltControl is the live brancher: a single-connection MySQL pool to the shared
// dolt sql-server. MaxOpenConns is 1 so a USE/SET on the control session is not
// silently lost to a different pooled connection.
type doltControl struct {
	db     *sql.DB
	dbName string
}

func newDoltControl(serverAddr, dbName string) (*doltControl, error) {
	if !dbNameRE.MatchString(dbName) {
		return nil, fmt.Errorf("beads branch control: unsafe dolt database name %q", dbName)
	}
	dsn := fmt.Sprintf("root:@tcp(%s)/%s?multiStatements=false&interpolateParams=true", serverAddr, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("beads branch control: open dolt control connection: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("beads branch control: ping dolt sql-server %s/%s: %w", serverAddr, dbName, err)
	}
	return &doltControl{db: db, dbName: dbName}, nil
}

func (c *doltControl) CreateBranch(ctx context.Context, name, startPoint string) error {
	_, err := c.db.ExecContext(ctx, "CALL DOLT_BRANCH(?, ?)", name, startPoint)
	return err
}

// ForceBranch force-moves name to startPoint's commit. Dolt refuses to force-move
// the branch the SESSION has checked out, and the single pooled control
// connection stays pinned to its connect-time branch (main) regardless of the
// default-branch flips, so it is explicitly positioned onto startPoint first.
func (c *doltControl) ForceBranch(ctx context.Context, name, startPoint string) error {
	if !branchNameRE.MatchString(startPoint) {
		return fmt.Errorf("beads branch control: unsafe branch name %q", startPoint)
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s/%s`", c.dbName, startPoint)); err != nil {
		return fmt.Errorf("position control session on %q: %w", startPoint, err)
	}
	_, err = conn.ExecContext(ctx, "CALL DOLT_BRANCH('-f', ?, ?)", name, startPoint)
	return err
}

// DropBranch force-deletes name (-D). drop discards a fork that may be unmerged
// (a rolled-back fork never merges into main), so -d would refuse it. The session
// is positioned on main first, since Dolt refuses to delete the checked-out
// branch; the driver separately refuses to drop the live branch.
func (c *doltControl) DropBranch(ctx context.Context, name string) error {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s/main`", c.dbName)); err != nil {
		return fmt.Errorf("position control session on main: %w", err)
	}
	_, err = conn.ExecContext(ctx, "CALL DOLT_BRANCH('-D', ?)", name)
	return err
}

func (c *doltControl) SetDefaultBranch(ctx context.Context, branch string) error {
	// The variable name embeds the (validated) database name and cannot be bound;
	// the value is bound.
	stmt := fmt.Sprintf("SET @@GLOBAL.%s_default_branch = ?", c.dbName)
	_, err := c.db.ExecContext(ctx, stmt, branch)
	return err
}

func (c *doltControl) GetDefaultBranch(ctx context.Context) (string, error) {
	stmt := fmt.Sprintf("SELECT @@GLOBAL.%s_default_branch", c.dbName)
	var b sql.NullString
	if err := c.db.QueryRowContext(ctx, stmt).Scan(&b); err != nil {
		return "", err
	}
	return b.String, nil
}

func (c *doltControl) ListBranches(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, "SELECT name FROM dolt_branches ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (c *doltControl) BranchCommit(ctx context.Context, name string) (string, error) {
	var h sql.NullString
	err := c.db.QueryRowContext(ctx, "SELECT hash FROM dolt_branches WHERE name = ?", name).Scan(&h)
	if err != nil {
		return "", err
	}
	return h.String, nil
}

// DiffSince runs the two-point diff of the issues table between fromCommit and
// the branch head. Only the k8s-relevant columns are fetched: the diff type,
// the bead id and external_ref (the etcd key) on each side, and the base-side
// description (the kine record as of fromCommit, i.e. the AS OF value). The
// current side is taken from the live index by the caller. Both refs are
// validated before interpolation (dolt_diff takes literals).
func (c *doltControl) DiffSince(ctx context.Context, fromCommit, branch string) ([]diffRow, error) {
	if !commitHashRE.MatchString(fromCommit) {
		return nil, fmt.Errorf("beads branch control: unsafe commit hash %q", fromCommit)
	}
	if !branchNameRE.MatchString(branch) {
		return nil, fmt.Errorf("beads branch control: unsafe branch name %q", branch)
	}
	q := fmt.Sprintf(`SELECT diff_type, COALESCE(from_id,''), COALESCE(to_id,''), COALESCE(from_external_ref,''), COALESCE(to_external_ref,''), COALESCE(from_description,'') FROM dolt_diff('%s', '%s', 'issues')`, fromCommit, branch)
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []diffRow
	for rows.Next() {
		var r diffRow
		if err := rows.Scan(&r.diffType, &r.fromID, &r.toID, &r.fromRef, &r.toRef, &r.fromDesc); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (c *doltControl) Close() error { return c.db.Close() }

// --- driver-side branch state and operations --------------------------------

// branchStatus is the GET /branch/status payload.
type branchStatus struct {
	Branch     string   `json:"branch"`
	ForkBase   int64    `json:"fork_base"`
	ForkCommit string   `json:"fork_commit"`
	Rev        int64    `json:"rev"`
	HWM        int64    `json:"hwm"`
	Branches   []string `json:"branches"`
}

// opResult is the response for fork/switch/promote/drop.
type opResult struct {
	Op       string `json:"op"`
	Branch   string `json:"branch"`
	Rev      int64  `json:"rev"`
	FreezeMs int64  `json:"freeze_ms"`
	Events   int    `json:"events"`
	Note     string `json:"note,omitempty"`
}

// persistHWM records the current revision as the high-water mark in store config.
// Best-effort: a failure is logged, not fatal, since the mark only matters across
// a restart.
func (b *beadsLog) persistHWM(ctx context.Context, rev int64) {
	if err := b.store.SetConfig(ctx, hwmConfigKey, strconv.FormatInt(rev, 10)); err != nil {
		logrus.Warnf("Kine beads driver: persist rev high-water mark: %v", err)
	}
}

// readHWM loads the persisted high-water mark (0 if unset/absent).
func (b *beadsLog) readHWM(ctx context.Context) int64 {
	raw, err := b.store.GetConfig(ctx, hwmConfigKey)
	if err != nil || raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// reopenStore closes the current beads handle and reopens it through the same
// openOrInit path, so the new connection pool lands on whatever branch Dolt's
// default-branch variable now points at. Must be called with writeMu held (no
// writer runs concurrently, so b.store is otherwise untouched); mu is taken only
// to swap the handle so an in-flight reader never observes a closed store.
func (b *beadsLog) reopenStore(ctx context.Context) error {
	old := b.store
	ns, err := openOrInit(ctx, b.dir, b.serverAddr)
	if err != nil {
		return fmt.Errorf("reopen beads store: %w", err)
	}
	b.mu.Lock()
	b.store = ns
	b.mu.Unlock()
	if err := old.Close(); err != nil {
		logrus.Warnf("Kine beads driver: close previous beads store after reopen: %v", err)
	}
	return nil
}

// reseedTypes re-writes the types.custom config row onto the current (just
// reopened) branch from kine's in-memory type registry. beads leaves the config
// and custom_types tables UNCOMMITTED in the working set (only issue writes
// auto-commit), so a Dolt branch taken at HEAD inherits the committed issues but
// NOT the type registry — and beads then rejects already-known kinds (e.g.
// "events") on the fork. Writing types.custom is branch-local (it never touches
// main) and sufficient: beads' type validation falls back to the types.custom
// config row when the custom_types table is empty, which it is on a fresh branch.
// Must be called with writeMu held (b.types is writeMu-guarded write-only state).
func (b *beadsLog) reseedTypes(ctx context.Context) {
	if len(b.types) == 0 {
		return
	}
	list := make([]string, 0, len(b.types))
	for t := range b.types {
		list = append(list, t)
	}
	sort.Strings(list)
	if err := b.store.SetConfig(ctx, "types.custom", strings.Join(list, ",")); err != nil {
		logrus.Warnf("Kine beads driver: reseed types.custom on branch %q: %v", b.curBranch, err)
	}
}

// fork branches the store at the current revision, flips the default branch to
// the fork, and reopens. Content is identical, so the in-memory index, event log
// and revision counter are kept verbatim and the apiserver sees nothing.
func (b *beadsLog) fork(ctx context.Context, name string) (*opResult, error) {
	if b.ctl == nil {
		return nil, fmt.Errorf("branch control is disabled")
	}
	if name == "" {
		return nil, fmt.Errorf("fork: name required")
	}
	if !branchNameRE.MatchString(name) {
		return nil, fmt.Errorf("fork: unsafe branch name %q", name)
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	base := b.curBranch
	baseCommit, err := b.ctl.BranchCommit(ctx, base)
	if err != nil {
		return nil, fmt.Errorf("fork: read base commit of %q: %w", base, err)
	}
	if err := b.ctl.CreateBranch(ctx, name, base); err != nil {
		return nil, fmt.Errorf("fork: create branch %q: %w", name, err)
	}
	forkCommit, err := b.ctl.BranchCommit(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("fork: read fork commit of %q: %w", name, err)
	}
	if err := b.ctl.SetDefaultBranch(ctx, name); err != nil {
		return nil, fmt.Errorf("fork: set default branch %q: %w", name, err)
	}
	if err := b.reopenStore(ctx); err != nil {
		return nil, err
	}
	// The new branch inherits committed issues but not the uncommitted type
	// registry; re-seed it so writes of already-known kinds are accepted.
	b.reseedTypes(ctx)

	b.mu.Lock()
	b.curBranch = name
	b.forkBase = b.rev
	b.forkCommit = forkCommit
	b.parentBranch = base
	b.parentCommit = baseCommit
	rev := b.rev
	b.mu.Unlock()

	b.persistHWM(ctx, rev)
	logrus.Infof("Kine beads driver: forked %q -> %q at rev %d (commit %s)", base, name, rev, shortHash(forkCommit))
	return &opResult{Op: "fork", Branch: name, Rev: rev}, nil
}

// switchBranch is the general case (rollback, or jumping between branches): flip
// the default branch, reopen, rebuild a candidate index from the target branch,
// diff it against the live index and append the differences as synthetic events
// with fresh consecutive revisions, then swap the index and notify the poller.
// The whole thing runs under writeMu (a privileged writer) so the freeze is a
// bounded burst to the apiserver, not a datastore reset.
func (b *beadsLog) switchBranch(ctx context.Context, target string) (*opResult, error) {
	if b.ctl == nil {
		return nil, fmt.Errorf("branch control is disabled")
	}
	if target == "" {
		return nil, fmt.Errorf("switch: target required")
	}
	if !branchNameRE.MatchString(target) {
		return nil, fmt.Errorf("switch: unsafe branch name %q", target)
	}
	start := time.Now()
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	from := b.curBranch
	if err := b.ctl.SetDefaultBranch(ctx, target); err != nil {
		return nil, fmt.Errorf("switch: set default branch %q: %w", target, err)
	}
	if err := b.reopenStore(ctx); err != nil {
		return nil, err
	}
	b.reseedTypes(ctx)

	// Rebuild a candidate index from the target branch (fresh map; does not touch
	// the live index).
	candidate, _, err := b.loadIndex(ctx, b.store)
	if err != nil {
		return nil, fmt.Errorf("switch: rebuild index from %q: %w", target, err)
	}

	b.mu.Lock()
	live := b.index
	startRev := b.rev
	b.mu.Unlock()

	events, newIndex, endRev := diffToEvents(live, candidate, startRev)

	b.mu.Lock()
	b.rev = endRev
	b.index = newIndex
	b.events = append(b.events, events...)
	b.curBranch = target
	// Leaving a fork clears the fork marker; the target is now the live branch.
	if target == b.parentBranch || len(events) == 0 {
		b.forkBase = 0
		b.forkCommit = ""
	}
	b.mu.Unlock()

	// Rebuild the UID->key map in memory so future ownership wiring is correct on
	// the new branch. The parent-child EDGES already exist in Dolt on the target
	// branch (they were written when those objects were created there), so no
	// AddDependency calls happen here — the freeze stays a pure in-memory swap.
	b.rebuildUIDMap(newIndex)

	b.notifyRevision(endRev)
	b.persistHWM(ctx, endRev)

	freeze := time.Since(start).Milliseconds()
	logrus.Infof("Kine beads driver: switched %q -> %q, %d synthetic events, rev %d, freeze %dms",
		from, target, len(events), endRev, freeze)
	return &opResult{Op: "switch", Branch: target, Rev: endRev, FreezeMs: freeze, Events: len(events)}, nil
}

// promote makes the current fork the new main by fast-forwarding main to the
// fork's commit, then flips the default branch back to main and reopens. Because
// main was frozen while the fork was live, this is a fast-forward and the content
// is identical, so no events are emitted. It refuses if main moved since the fork
// (it cannot here — kine is main's only writer — but the commit hash is checked).
func (b *beadsLog) promote(ctx context.Context) (*opResult, error) {
	if b.ctl == nil {
		return nil, fmt.Errorf("branch control is disabled")
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	fork := b.curBranch
	parent := b.parentBranch
	if parent == "" || b.forkBase == 0 {
		return nil, fmt.Errorf("promote: not currently on a fork")
	}
	// Guard: the parent (main) must not have moved since the fork.
	nowParent, err := b.ctl.BranchCommit(ctx, parent)
	if err != nil {
		return nil, fmt.Errorf("promote: read %q commit: %w", parent, err)
	}
	if nowParent != b.parentCommit {
		return nil, fmt.Errorf("promote: %q moved since fork (%s != %s); refusing fast-forward",
			parent, shortHash(nowParent), shortHash(b.parentCommit))
	}

	// Fast-forward parent to the fork's commit. The control session sits on the
	// fork (the default branch), not on parent, so force-moving parent is allowed.
	if err := b.ctl.ForceBranch(ctx, parent, fork); err != nil {
		return nil, fmt.Errorf("promote: fast-forward %q to %q: %w", parent, fork, err)
	}
	if err := b.ctl.SetDefaultBranch(ctx, parent); err != nil {
		return nil, fmt.Errorf("promote: set default branch %q: %w", parent, err)
	}
	if err := b.reopenStore(ctx); err != nil {
		return nil, err
	}
	b.reseedTypes(ctx)

	b.mu.Lock()
	b.curBranch = parent
	b.forkBase = 0
	b.forkCommit = ""
	b.parentBranch = ""
	b.parentCommit = ""
	rev := b.rev
	b.mu.Unlock()

	b.persistHWM(ctx, rev)
	logrus.Infof("Kine beads driver: promoted %q -> %q (fast-forward), now on %q at rev %d", fork, parent, parent, rev)
	return &opResult{Op: "promote", Branch: parent, Rev: rev, Note: "promoted " + fork + " into " + parent}, nil
}

// drop deletes a branch. Refused for the current branch.
func (b *beadsLog) drop(ctx context.Context, name string) (*opResult, error) {
	if b.ctl == nil {
		return nil, fmt.Errorf("branch control is disabled")
	}
	if name == "" {
		return nil, fmt.Errorf("drop: name required")
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if name == b.curBranch {
		return nil, fmt.Errorf("drop: refusing to drop the current branch %q", name)
	}
	if err := b.ctl.DropBranch(ctx, name); err != nil {
		return nil, fmt.Errorf("drop: delete branch %q: %w", name, err)
	}
	logrus.Infof("Kine beads driver: dropped branch %q", name)
	return &opResult{Op: "drop", Branch: name}, nil
}

// status reports the current branch, fork base, revision high-water mark and the
// list of branches.
func (b *beadsLog) status(ctx context.Context) (*branchStatus, error) {
	b.mu.RLock()
	st := &branchStatus{
		Branch:     b.curBranch,
		ForkBase:   b.forkBase,
		ForkCommit: b.forkCommit,
		Rev:        b.rev,
		HWM:        b.rev,
	}
	b.mu.RUnlock()
	if b.ctl != nil {
		if branches, err := b.ctl.ListBranches(ctx); err == nil {
			st.Branches = branches
		} else {
			logrus.Warnf("Kine beads driver: list branches for status: %v", err)
		}
	}
	return st, nil
}

// diffToEvents computes the synthetic event burst that turns the live index into
// the candidate (target-branch) index, and the resulting stamped index. Keys on
// the current branch but not the target become deletes; keys on the target but
// not the current become creates (carrying the target's value and uid); keys on
// both with a different value become updates (target value as KV, current value
// as PrevKV). Deletes are emitted before creates so a name deleted on the fork
// and re-created on the target cannot collide; updates come last. Revisions are
// strictly consecutive from startRev+1. Pure function: unit tested.
func diffToEvents(live, candidate map[string]entry, startRev int64) (events []*server.Event, newIndex map[string]entry, endRev int64) {
	newIndex = make(map[string]entry, len(candidate))
	for k, e := range candidate {
		newIndex[k] = e
	}
	isLive := func(e entry, ok bool) bool { return ok && !e.r.Deleted }

	var deletes, creates, updates []string
	for k, lv := range live {
		if lv.r.Deleted {
			continue
		}
		cv, ok := candidate[k]
		if !isLive(cv, ok) {
			deletes = append(deletes, k)
		} else if !bytes.Equal(lv.r.Value, cv.r.Value) {
			updates = append(updates, k)
		}
	}
	for k, cv := range candidate {
		if cv.r.Deleted {
			continue
		}
		lv, ok := live[k]
		if !isLive(lv, ok) {
			creates = append(creates, k)
		}
	}
	sort.Strings(deletes)
	sort.Strings(creates)
	sort.Strings(updates)

	rev := startRev
	for _, k := range deletes {
		rev++
		lv := live[k]
		// Tombstone keeps the live bead id (best available). A key deleted here
		// does not exist on the target branch, so recreating the SAME name before
		// the next restart is a known limitation (the stale id would be used for an
		// update); the experiment never does this.
		newIndex[k] = entry{id: lv.id, r: rec{CreateRevision: lv.r.CreateRevision, ModRevision: rev, Deleted: true}}
		events = append(events, &server.Event{
			Delete: true,
			KV:     &server.KeyValue{Key: k, CreateRevision: lv.r.CreateRevision, ModRevision: rev},
			PrevKV: &server.KeyValue{Key: k, Value: lv.r.Value, CreateRevision: lv.r.CreateRevision, ModRevision: lv.r.ModRevision, Lease: lv.r.Lease},
		})
	}
	for _, k := range creates {
		rev++
		cv := candidate[k]
		r := cv.r
		r.CreateRevision = rev
		r.ModRevision = rev
		r.Deleted = false
		newIndex[k] = entry{id: cv.id, r: r}
		events = append(events, &server.Event{
			Create: true,
			KV:     &server.KeyValue{Key: k, Value: cv.r.Value, CreateRevision: rev, ModRevision: rev, Lease: cv.r.Lease},
		})
	}
	for _, k := range updates {
		rev++
		cv := candidate[k]
		lv := live[k]
		r := cv.r
		r.ModRevision = rev
		newIndex[k] = entry{id: cv.id, r: r}
		events = append(events, &server.Event{
			KV:     &server.KeyValue{Key: k, Value: cv.r.Value, CreateRevision: r.CreateRevision, ModRevision: rev, Lease: cv.r.Lease},
			PrevKV: &server.KeyValue{Key: k, Value: lv.r.Value, CreateRevision: lv.r.CreateRevision, ModRevision: lv.r.ModRevision, Lease: lv.r.Lease},
		})
	}
	return events, newIndex, rev
}

// rebuildUIDMap resets the in-memory UID->key map (and clears the pending map)
// from an index, with no Dolt writes. Called under writeMu after a switch.
func (b *beadsLog) rebuildUIDMap(index map[string]entry) {
	uidToKey := make(map[string]string, len(index))
	for key, ent := range index {
		if ent.r.Deleted {
			continue
		}
		if uid, _, ok := parseK8sMeta(ent.r.Value); ok && uid != "" {
			uidToKey[uid] = key
		}
	}
	b.uidToKey = uidToKey
	b.pending = make(map[string][]string)
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

// --- HTTP control server (unix socket + bearer-guarded TCP) ------------------

// controlMux builds the control API mux shared by every listener: the branch
// verbs (POST /branch/{fork,switch,promote,drop}, GET /branch/status) and the
// scoped-transaction verbs (POST /txn/begin, GET /txn, GET /txn/{id},
// POST /txn/{id}/commit, POST /txn/{id}/rollback). JSON in and out; errors are
// {"error": "..."} with 400/404/409/405.
func (b *beadsLog) controlMux() *http.ServeMux {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, code int, v interface{}) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	writeErr := func(w http.ResponseWriter, err error) {
		code := http.StatusBadRequest
		if he, ok := err.(*httpError); ok {
			code = he.code
		}
		if code == http.StatusBadRequest {
			logrus.Warnf("Kine beads driver: control API error: %v", err)
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
	}
	requirePost := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
			return false
		}
		return true
	}

	mux.HandleFunc("/branch/fork", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) {
			return
		}
		res, err := b.fork(r.Context(), r.URL.Query().Get("name"))
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("/branch/switch", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) {
			return
		}
		res, err := b.switchBranch(r.Context(), r.URL.Query().Get("target"))
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("/branch/promote", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) {
			return
		}
		res, err := b.promote(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("/branch/drop", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) {
			return
		}
		res, err := b.drop(r.Context(), r.URL.Query().Get("name"))
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("/branch/status", func(w http.ResponseWriter, r *http.Request) {
		res, err := b.status(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})

	// Scoped transactions.
	mux.HandleFunc("/txn/begin", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) {
			return
		}
		var req txnBeginRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil {
			writeErr(w, errBadRequest("decode begin request: %v", err))
			return
		}
		t, err := b.txnBegin(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"id": t.ID, "state": t.State, "baseRev": t.BaseRev, "baseCommit": t.BaseCommit, "startedAt": t.StartedAt,
		})
	})
	mux.HandleFunc("/txn", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET required"})
			return
		}
		writeJSON(w, http.StatusOK, b.txnList())
	})
	mux.HandleFunc("/txn/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/txn/")
		switch {
		case strings.HasSuffix(rest, "/commit"):
			if !requirePost(w, r) {
				return
			}
			t, err := b.txnCommit(r.Context(), strings.TrimSuffix(rest, "/commit"))
			if err != nil {
				writeErr(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{"id": t.ID, "state": t.State, "baseRev": t.BaseRev, "finishedAt": t.FinishedAt})
		case strings.HasSuffix(rest, "/rollback"):
			if !requirePost(w, r) {
				return
			}
			t, err := b.txnRollback(r.Context(), strings.TrimSuffix(rest, "/rollback"))
			if err != nil {
				writeErr(w, err)
				return
			}
			res := t.Result
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"id": t.ID, "state": t.State, "baseRev": t.BaseRev, "endRev": res.EndRev,
				"events": res.Events, "freezeMs": res.FreezeMs, "reverted": res.Reverted,
			})
		default:
			if r.Method != http.MethodGet {
				writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET required"})
				return
			}
			t, err := b.txnGet(rest)
			if err != nil {
				writeErr(w, err)
				return
			}
			writeJSON(w, http.StatusOK, t.public())
		}
	})
	return mux
}

// bearerAuth wraps h so every request must carry "Authorization: Bearer
// <token>"; anything else is 401. The comparison is constant-time and the
// token is never logged.
func bearerAuth(token string, h http.Handler) http.Handler {
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if token == "" || subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}` + "\n"))
			return
		}
		h.ServeHTTP(w, r)
	})
}

// serveBranchControl starts the control API on the unix socket (unauthenticated,
// local) and/or the TCP address (bearer-token guarded). Both serve the same
// mux. Listeners are closed when the driver context is cancelled.
func (b *beadsLog) serveBranchControl(ctx context.Context, socket, addr string) error {
	mux := b.controlMux()
	serve := func(ln net.Listener, h http.Handler, cleanup func()) {
		srv := &http.Server{Handler: h}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
			if cleanup != nil {
				cleanup()
			}
		}()
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				logrus.Errorf("Kine beads driver: control server: %v", err)
			}
		}()
	}
	if socket != "" {
		_ = os.Remove(socket)
		ln, err := net.Listen("unix", socket)
		if err != nil {
			return fmt.Errorf("branch control: listen on %q: %w", socket, err)
		}
		serve(ln, mux, func() { _ = os.Remove(socket) })
		logrus.Infof("Kine beads driver: control API listening on unix://%s", socket)
	}
	if addr != "" {
		if b.ctlToken == "" {
			return fmt.Errorf("branch control: TCP listener %q requires a bearer token", addr)
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("branch control: listen on %q: %w", addr, err)
		}
		serve(ln, bearerAuth(b.ctlToken, mux), nil)
		logrus.Infof("Kine beads driver: control API listening on tcp://%s (bearer token from %s)", addr, b.ctlTokenFile)
	}
	return nil
}
