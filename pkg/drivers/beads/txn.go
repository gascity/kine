package beads

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	beads "github.com/steveyegge/beads"

	"github.com/k3s-io/kine/pkg/server"
)

// Scoped transactions (kine control API v0, /txn/*). A transaction is a
// savepoint plus a scope: begin records the Dolt commit and revision at that
// moment, writes keep landing on the live branch, and rollback emits the
// inverse of every in-scope change since the savepoint as fresh revisions
// (created -> delete, modified -> the value as of the savepoint, deleted ->
// recreate). Out-of-scope keys are never touched. commit forgets the
// savepoint; nothing is merged because the writes were on the branch all
// along. The ledger (kine.txn.<id> config entries) makes begin idempotent and
// commit/rollback refuse a closed id, so a restart of kine or of the caller
// cannot double-begin or double-rollback.

// txnConfigPrefix is the store-config key prefix of the transaction ledger.
const txnConfigPrefix = "kine.txn."

// txnIDRE guards transaction ids: they become URL path segments and ledger
// config keys. Slashes are allowed so a caller can namespace ids
// (argo/<ns>/<app>/<revision>).
var txnIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$`)

// commitHashRE guards a Dolt commit hash before it is interpolated into a
// dolt_diff()/AS OF query.
var commitHashRE = regexp.MustCompile(`^[a-z0-9]{32}$`)

// txnScope is the scope object of the API: keys (group/resource/namespace/name
// or resource/namespace/name, mapped to etcd keys), raw etcd-key prefixes,
// namespaces, and whether the ownership cone is expanded at rollback time.
type txnScope struct {
	Keys         []string `json:"keys,omitempty"`
	Prefixes     []string `json:"prefixes,omitempty"`
	Namespaces   []string `json:"namespaces,omitempty"`
	ExpandOwners bool     `json:"expandOwners,omitempty"`
}

// txnResult is the outcome recorded on a rolled-back transaction.
type txnResult struct {
	Events   int      `json:"events"`
	FreezeMs int64    `json:"freezeMs"`
	Reverted []string `json:"reverted"`
	EndRev   int64    `json:"endRev,omitempty"`
}

// txnRecord is one ledger entry. etcdKeys is the mapped form of Scope.Keys,
// persisted so a restart does not need to re-map.
type txnRecord struct {
	ID         string     `json:"id"`
	State      string     `json:"state"`
	BaseRev    int64      `json:"baseRev"`
	BaseCommit string     `json:"baseCommit,omitempty"`
	Branch     string     `json:"branch,omitempty"`
	Scope      txnScope   `json:"scope"`
	EtcdKeys   []string   `json:"etcdKeys,omitempty"`
	StartedAt  string     `json:"startedAt"`
	FinishedAt string     `json:"finishedAt,omitempty"`
	Result     *txnResult `json:"result,omitempty"`
}

const (
	txnOpen       = "open"
	txnCommitted  = "committed"
	txnRolledBack = "rolledBack"
)

// txnLedger is the in-memory mirror of the ledger, guarded by its own mutex.
// Lock order: txnLedger.mu -> writeMu -> mu.
type txnLedger struct {
	mu   sync.Mutex
	txns map[string]*txnRecord
}

// httpError carries an HTTP status for the control server (404/409/400).
type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func errNotFound(format string, a ...interface{}) error {
	return &httpError{code: http.StatusNotFound, msg: fmt.Sprintf(format, a...)}
}

func errConflict(format string, a ...interface{}) error {
	return &httpError{code: http.StatusConflict, msg: fmt.Sprintf(format, a...)}
}

func errBadRequest(format string, a ...interface{}) error {
	return &httpError{code: http.StatusBadRequest, msg: fmt.Sprintf(format, a...)}
}

// --- scope key mapping ---------------------------------------------------------

// specialPrefixes are the resources the apiserver stores under a prefix that
// is not the resource name.
var specialPrefixes = map[string]string{
	"nodes":     "minions",
	"services":  "services/specs",
	"endpoints": "services/endpoints",
	"ingresses": "ingress",
}

// groupKeepsPrefix lists the built-in dotted groups whose etcd keys KEEP the
// group segment. Every other *.k8s.io built-in group (rbac, networking,
// storage, coordination, discovery, events, admissionregistration,
// scheduling, node, certificates, flowcontrol, policy, …) stores under the
// bare resource, the same as the core and dotless groups (apps, batch,
// autoscaling). Anything else with a dot is a CRD group and keeps its group.
var groupKeepsPrefix = map[string]bool{
	"apiextensions.k8s.io":   true,
	"apiregistration.k8s.io": true,
}

// mapScopeKey turns an API scope key into the etcd key the apiserver uses:
//
//	group/resource/namespace/name -> /registry/<group>/<resource>/<ns>/<name> (CRD group)
//	group/resource/name           -> /registry/<group>/<resource>/<name>      (cluster-scoped)
//	resource/namespace/name       -> /registry/<resource>/<ns>/<name>         (core / dotless group)
//	resource/name                 -> /registry/<resource>/<name>              (cluster-scoped)
//
// A dotless group (apps/deployments/ns/name) and a built-in *.k8s.io group
// drop the group segment; nodes map to minions. A key already starting with
// /registry/ is passed through verbatim. The rule for distinguishing the
// three-segment forms is the dot in the first segment: a group always has
// one, a resource never does.
func mapScopeKey(k string) (string, error) {
	k = strings.TrimSpace(k)
	if k == "" {
		return "", fmt.Errorf("empty scope key")
	}
	if strings.HasPrefix(k, "/registry/") {
		return k, nil
	}
	segs := strings.Split(strings.Trim(k, "/"), "/")
	for _, s := range segs {
		if s == "" {
			return "", fmt.Errorf("scope key %q has an empty segment", k)
		}
	}
	var group, resource string
	var rest []string
	if strings.Contains(segs[0], ".") {
		if len(segs) < 3 {
			return "", fmt.Errorf("scope key %q: group/resource/name needs at least 3 segments", k)
		}
		group, resource, rest = segs[0], segs[1], segs[2:]
	} else if len(segs) == 4 {
		// dotless group: apps/deployments/ns/name
		group, resource, rest = segs[0], segs[1], segs[2:]
	} else if len(segs) == 2 || len(segs) == 3 {
		resource, rest = segs[0], segs[1:]
	} else {
		return "", fmt.Errorf("scope key %q: want [group/]resource[/namespace]/name", k)
	}
	if len(rest) > 2 {
		return "", fmt.Errorf("scope key %q has too many segments", k)
	}
	prefix := resource
	if sp, ok := specialPrefixes[resource]; ok {
		prefix = sp
	}
	keepGroup := group != "" && strings.Contains(group, ".") &&
		(groupKeepsPrefix[group] || !strings.HasSuffix(group, ".k8s.io"))
	if keepGroup {
		prefix = group + "/" + resource
	}
	return "/registry/" + prefix + "/" + strings.Join(rest, "/"), nil
}

// keyNamespace returns the namespace segment of an etcd key, or "" for a
// cluster-scoped key or a key outside /registry/. Layouts:
//
//	/registry/<resource>/<ns>/<name>            (3 segments, dotless first)
//	/registry/<resource>/<name>                 (2 segments)
//	/registry/<group>/<resource>/<ns>/<name>    (4 segments, dotted first)
//	/registry/<group>/<resource>/<name>         (3 segments, dotted first)
//	/registry/services/{specs,endpoints}/<ns>/<name>
func keyNamespace(key string) string {
	const prefix = "/registry/"
	if !strings.HasPrefix(key, prefix) {
		return ""
	}
	segs := strings.Split(key[len(prefix):], "/")
	if segs[0] == "services" && len(segs) == 4 && (segs[1] == "specs" || segs[1] == "endpoints") {
		return segs[2]
	}
	if strings.Contains(segs[0], ".") {
		if len(segs) == 4 {
			return segs[2]
		}
		return ""
	}
	if len(segs) == 3 {
		return segs[1]
	}
	return ""
}

// keyInScope reports whether key is directly in the scope (keys, prefixes,
// namespaces); the ownership cone is handled separately at rollback time.
func keyInScope(key string, etcdKeys []string, sc *txnScope) bool {
	for _, k := range etcdKeys {
		if key == k {
			return true
		}
	}
	for _, p := range sc.Prefixes {
		if p != "" && strings.HasPrefix(key, p) {
			return true
		}
	}
	if len(sc.Namespaces) > 0 {
		if ns := keyNamespace(key); ns != "" {
			for _, n := range sc.Namespaces {
				if n == ns {
					return true
				}
			}
		}
	}
	return false
}

// scopesOverlap is the (deliberately conservative) overlap test used by begin:
// any shared namespace, any prefix that contains or is contained by the
// other's prefix, any equal key, or a key of one that falls into the other's
// namespaces or prefixes, counts as overlap.
func scopesOverlap(a, b *txnRecord) bool {
	for _, x := range a.Scope.Namespaces {
		for _, y := range b.Scope.Namespaces {
			if x == y {
				return true
			}
		}
	}
	for _, x := range a.Scope.Prefixes {
		for _, y := range b.Scope.Prefixes {
			if strings.HasPrefix(x, y) || strings.HasPrefix(y, x) {
				return true
			}
		}
	}
	for _, k := range a.EtcdKeys {
		if keyInScope(k, b.EtcdKeys, &b.Scope) {
			return true
		}
	}
	for _, k := range b.EtcdKeys {
		if keyInScope(k, a.EtcdKeys, &a.Scope) {
			return true
		}
	}
	// A prefix of one that lands inside a namespace of the other (e.g.
	// /registry/configmaps/ns-a/ vs namespace ns-a).
	for _, p := range a.Scope.Prefixes {
		if ns := keyNamespace(strings.TrimSuffix(p, "/") + "/x"); ns != "" {
			for _, n := range b.Scope.Namespaces {
				if n == ns {
					return true
				}
			}
		}
	}
	for _, p := range b.Scope.Prefixes {
		if ns := keyNamespace(strings.TrimSuffix(p, "/") + "/x"); ns != "" {
			for _, n := range a.Scope.Namespaces {
				if n == ns {
					return true
				}
			}
		}
	}
	return false
}

// --- ledger ------------------------------------------------------------------

func (b *beadsLog) ledger() *txnLedger {
	b.txnOnce.Do(func() {
		if b.txnLedger == nil {
			b.txnLedger = &txnLedger{txns: map[string]*txnRecord{}}
		}
	})
	return b.txnLedger
}

// loadTxnLedger reads every kine.txn.* config entry into memory at Start.
func (b *beadsLog) loadTxnLedger(ctx context.Context) {
	all, err := b.store.GetAllConfig(ctx)
	if err != nil {
		logrus.Warnf("Kine beads driver: load transaction ledger: %v", err)
		return
	}
	l := b.ledger()
	l.mu.Lock()
	defer l.mu.Unlock()
	open := 0
	for k, v := range all {
		if !strings.HasPrefix(k, txnConfigPrefix) {
			continue
		}
		var t txnRecord
		if err := json.Unmarshal([]byte(v), &t); err != nil || t.ID == "" {
			logrus.Warnf("Kine beads driver: skipping undecodable ledger entry %q", k)
			continue
		}
		l.txns[t.ID] = &t
		if t.State == txnOpen {
			open++
		}
	}
	if len(l.txns) > 0 {
		logrus.Infof("Kine beads driver: transaction ledger loaded: %d entries, %d open", len(l.txns), open)
	}
}

// persistTxn writes one ledger entry through the store's config.
func (b *beadsLog) persistTxn(ctx context.Context, t *txnRecord) error {
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := b.store.SetConfig(ctx, txnConfigPrefix+t.ID, string(raw)); err != nil {
		return fmt.Errorf("persist transaction %q: %w", t.ID, err)
	}
	return nil
}

func (t *txnRecord) public() map[string]interface{} {
	out := map[string]interface{}{
		"id":         t.ID,
		"state":      t.State,
		"baseRev":    t.BaseRev,
		"baseCommit": t.BaseCommit,
		"branch":     t.Branch,
		"scope":      t.Scope,
		"etcdKeys":   t.EtcdKeys,
		"startedAt":  t.StartedAt,
	}
	if t.FinishedAt != "" {
		out["finishedAt"] = t.FinishedAt
	}
	if t.Result != nil {
		out["result"] = t.Result
	}
	return out
}

// --- begin / list / get / commit ----------------------------------------------

type txnBeginRequest struct {
	ID    string   `json:"id"`
	Scope txnScope `json:"scope"`
}

// txnBegin opens a transaction: validates the id and scope, refuses overlap
// with any open transaction, records the current revision and the branch's
// commit as the savepoint (under writeMu so no write is in flight between the
// two), and persists the ledger entry. Idempotent on an existing open id.
func (b *beadsLog) txnBegin(ctx context.Context, req txnBeginRequest) (*txnRecord, error) {
	if b.ctl == nil {
		return nil, errBadRequest("transactions require branch control (SERVER mode with a control listener)")
	}
	if !txnIDRE.MatchString(req.ID) {
		return nil, errBadRequest("invalid transaction id %q", req.ID)
	}
	if len(req.Scope.Keys) == 0 && len(req.Scope.Prefixes) == 0 && len(req.Scope.Namespaces) == 0 {
		return nil, errBadRequest("transaction %q: scope selects nothing (keys, prefixes or namespaces required)", req.ID)
	}
	etcdKeys := make([]string, 0, len(req.Scope.Keys))
	for _, k := range req.Scope.Keys {
		m, err := mapScopeKey(k)
		if err != nil {
			return nil, errBadRequest("transaction %q: %v", req.ID, err)
		}
		etcdKeys = append(etcdKeys, m)
	}
	sort.Strings(etcdKeys)
	for _, p := range req.Scope.Prefixes {
		if !strings.HasPrefix(p, "/registry/") {
			return nil, errBadRequest("transaction %q: prefix %q must start with /registry/", req.ID, p)
		}
	}

	l := b.ledger()
	l.mu.Lock()
	defer l.mu.Unlock()

	if existing, ok := l.txns[req.ID]; ok {
		if existing.State == txnOpen {
			return existing, nil
		}
		return nil, errConflict("transaction %q is already %s", req.ID, existing.State)
	}
	cand := &txnRecord{ID: req.ID, State: txnOpen, Scope: req.Scope, EtcdKeys: etcdKeys}
	for _, other := range l.txns {
		if other.State != txnOpen {
			continue
		}
		if scopesOverlap(cand, other) {
			return nil, errConflict("transaction %q overlaps open transaction %q", req.ID, other.ID)
		}
	}

	// Savepoint: revision and commit must agree, so take the write lock for the
	// few milliseconds it takes to read both.
	b.writeMu.Lock()
	b.mu.RLock()
	rev := b.rev
	branch := b.curBranch
	b.mu.RUnlock()
	commit, err := b.ctl.BranchCommit(ctx, branch)
	b.writeMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("begin %q: read commit of %q: %w", req.ID, branch, err)
	}
	cand.BaseRev = rev
	cand.BaseCommit = commit
	cand.Branch = branch
	cand.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := b.persistTxn(ctx, cand); err != nil {
		return nil, err
	}
	l.txns[cand.ID] = cand
	logrus.Infof("Kine beads driver: txn %q begun at rev %d (commit %s, branch %q): keys=%d prefixes=%d namespaces=%v expandOwners=%v",
		cand.ID, rev, shortHash(commit), branch, len(etcdKeys), len(req.Scope.Prefixes), req.Scope.Namespaces, req.Scope.ExpandOwners)
	return cand, nil
}

func (b *beadsLog) txnList() []map[string]interface{} {
	l := b.ledger()
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]string, 0, len(l.txns))
	for id := range l.txns {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		t := l.txns[id]
		out = append(out, map[string]interface{}{
			"id": t.ID, "state": t.State, "baseRev": t.BaseRev, "startedAt": t.StartedAt,
		})
	}
	return out
}

func (b *beadsLog) txnGet(id string) (*txnRecord, error) {
	l := b.ledger()
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.txns[id]
	if !ok {
		return nil, errNotFound("no transaction %q", id)
	}
	return t, nil
}

// txnCommit forgets the savepoint. Refused (409) on a closed id.
func (b *beadsLog) txnCommit(ctx context.Context, id string) (*txnRecord, error) {
	l := b.ledger()
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.txns[id]
	if !ok {
		return nil, errNotFound("no transaction %q", id)
	}
	if t.State != txnOpen {
		return nil, errConflict("transaction %q is already %s", id, t.State)
	}
	t.State = txnCommitted
	t.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := b.persistTxn(ctx, t); err != nil {
		t.State = txnOpen
		t.FinishedAt = ""
		return nil, err
	}
	b.mu.RLock()
	rev := b.rev
	b.mu.RUnlock()
	logrus.Infof("Kine beads driver: txn %q committed (base rev %d, now rev %d)", id, t.BaseRev, rev)
	return t, nil
}

// --- rollback ------------------------------------------------------------------

// diffRow is one changed issues row between the savepoint and the branch
// head, as dolt_diff(base, branch, 'issues') reports it: the diff type, the
// bead id and key on each side, and the record as of the savepoint
// (from_description). The current record is taken from the live index, which
// is authoritative for the head.
type diffRow struct {
	diffType string // added | modified | removed
	fromID   string
	toID     string
	fromRef  string
	toRef    string
	fromDesc string
}

// keyChange is a changed key with its base and current records.
type keyChange struct {
	key    string
	base   rec
	baseOK bool // row existed at base
	cur    entry
	curOK  bool // row exists in the live index
	kind   string
}

// txnRollback reverts every in-scope change since the savepoint. Under writeMu
// (a privileged writer, like switch): diff the savepoint commit against the
// branch head via Dolt, filter to the scope and (with expandOwners) the
// ownership cone, write the base records back through beads in one
// transaction, then publish the synthetic events with fresh consecutive
// revisions and bump the high-water mark. Out-of-scope keys are untouched.
func (b *beadsLog) txnRollback(ctx context.Context, id string) (*txnRecord, error) {
	if b.ctl == nil {
		return nil, errBadRequest("transactions require branch control")
	}
	l := b.ledger()
	l.mu.Lock()
	defer l.mu.Unlock()
	t, ok := l.txns[id]
	if !ok {
		return nil, errNotFound("no transaction %q", id)
	}
	if t.State != txnOpen {
		return nil, errConflict("transaction %q is already %s", id, t.State)
	}
	if !commitHashRE.MatchString(t.BaseCommit) {
		return nil, fmt.Errorf("transaction %q has an unusable base commit %q", id, t.BaseCommit)
	}

	start := time.Now()
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	b.mu.RLock()
	branch := b.curBranch
	b.mu.RUnlock()
	if t.Branch != "" && t.Branch != branch {
		return nil, errConflict("transaction %q was begun on branch %q but %q is live", id, t.Branch, branch)
	}

	rows, err := b.ctl.DiffSince(ctx, t.BaseCommit, branch)
	if err != nil {
		return nil, fmt.Errorf("rollback %q: dolt diff since %s: %w", id, shortHash(t.BaseCommit), err)
	}
	changes := b.classifyChanges(rows)
	selected := b.selectForRollback(changes, t)

	// Durable write of the reverted records, one beads transaction (one Dolt
	// commit) for the whole set. The revisions are reserved first so the
	// records carry their final mr.
	b.mu.RLock()
	startRev := b.rev
	b.mu.RUnlock()
	events, updates, endRev := planRollback(selected, startRev)
	if len(updates) > 0 {
		if err := b.writeRevertedRecords(ctx, id, updates); err != nil {
			return nil, fmt.Errorf("rollback %q: durable write: %w", id, err)
		}
	}

	// Publish: index, events, revision — in memory, under mu, like switch.
	b.mu.Lock()
	for _, u := range updates {
		b.index[u.key] = entry{id: u.id, r: u.r}
	}
	b.rev = endRev
	b.events = append(b.events, events...)
	b.mu.Unlock()
	// Keep the uid->key map honest for recreated/restored objects (edges already
	// exist in Dolt; no AddDependency here).
	for _, u := range updates {
		if !u.r.Deleted {
			if uid, _, ok := parseK8sMeta(u.r.Value); ok && uid != "" {
				b.uidToKey[uid] = u.key
			}
		}
	}
	b.notifyRevision(endRev)
	b.persistHWM(ctx, endRev)
	freeze := time.Since(start).Milliseconds()

	reverted := make([]string, 0, len(updates))
	for _, u := range updates {
		reverted = append(reverted, u.key)
	}
	t.State = txnRolledBack
	t.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	t.Result = &txnResult{Events: len(events), FreezeMs: freeze, Reverted: reverted, EndRev: endRev}
	if err := b.persistTxn(ctx, t); err != nil {
		logrus.Errorf("Kine beads driver: txn %q rolled back but ledger write failed: %v", id, err)
	}
	logrus.Infof("Kine beads driver: txn %q rolled back: %d changed rows since %s, %d selected, %d events, rev %d -> %d, freeze %dms",
		id, len(rows), shortHash(t.BaseCommit), len(selected), len(events), startRev, endRev, freeze)
	return t, nil
}

// classifyChanges turns diff rows into keyChanges with the base record decoded
// from from_description and the current record from the live index.
func (b *beadsLog) classifyChanges(rows []diffRow) []keyChange {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]keyChange, 0, len(rows))
	for _, r := range rows {
		key := r.toRef
		if key == "" {
			key = r.fromRef
		}
		if !strings.HasPrefix(key, "/registry/") {
			continue
		}
		c := keyChange{key: key}
		if r.diffType != "added" {
			if br, err := decodeRec(r.fromDesc); err == nil {
				c.base = br
				c.baseOK = true
			} else {
				logrus.Warnf("Kine beads driver: txn diff: undecodable base record for %q: %v", key, err)
				continue
			}
		}
		if cur, ok := b.index[key]; ok {
			c.cur = cur
			c.curOK = true
		} else if r.diffType != "removed" {
			// Bead exists in Dolt but not in the index (should not happen; the
			// index is rebuilt from the same rows). Use the row's id as a tombstone.
			c.cur = entry{id: r.toID, r: rec{Deleted: true}}
			c.curOK = true
		}
		baseLive := c.baseOK && !c.base.Deleted
		curLive := c.curOK && !c.cur.r.Deleted
		switch {
		case !baseLive && curLive:
			c.kind = "created"
		case baseLive && !curLive:
			c.kind = "deleted"
		case baseLive && curLive && (string(c.base.Value) != string(c.cur.r.Value) || c.base.Lease != c.cur.r.Lease):
			c.kind = "modified"
		default:
			continue // tombstone-to-tombstone or no effective change
		}
		out = append(out, c)
	}
	return out
}

// selectForRollback filters changes to the scope and, with expandOwners, the
// ownership cone: a changed key whose owner chain (ownerReferences via the
// uid->key map, the CAPI node->machine annotation, the kubelet node-lease
// name link) reaches an in-scope key. Owner values for keys deleted since the
// base come from the base record.
func (b *beadsLog) selectForRollback(changes []keyChange, t *txnRecord) []keyChange {
	byKey := make(map[string]*keyChange, len(changes))
	for i := range changes {
		byKey[changes[i].key] = &changes[i]
	}
	valueOf := func(key string) ([]byte, bool) {
		if c, ok := byKey[key]; ok {
			if c.curOK && !c.cur.r.Deleted {
				return c.cur.r.Value, true
			}
			if c.baseOK {
				return c.base.Value, true
			}
		}
		b.mu.RLock()
		e, ok := b.index[key]
		b.mu.RUnlock()
		if ok && !e.r.Deleted {
			return e.r.Value, true
		}
		return nil, false
	}
	memo := map[string]bool{}
	var inCone func(key string, depth int) bool
	inCone = func(key string, depth int) bool {
		if keyInScope(key, t.EtcdKeys, &t.Scope) {
			return true
		}
		if !t.Scope.ExpandOwners || depth > 12 {
			return false
		}
		if v, ok := memo[key]; ok {
			return v
		}
		memo[key] = false // cycle guard
		val, ok := valueOf(key)
		if !ok {
			return false
		}
		for _, owner := range b.ownerKeys(key, val) {
			if inCone(owner, depth+1) {
				memo[key] = true
				return true
			}
		}
		return false
	}
	var out []keyChange
	for _, c := range changes {
		if inCone(c.key, 0) {
			out = append(out, c)
		}
	}
	return out
}

// ownerKeys returns the etcd keys of key's owners: ownerReferences resolved
// through the uid->key map, plus the two link shapes the ownership graph does
// not carry: a Node's cluster.x-k8s.io/machine annotation (CAPI never sets an
// ownerReference on a Node) and a kube-node-lease Lease's name (the kubelet's
// lease is named after its node). Reads uidToKey, which is writeMu-guarded;
// callers hold writeMu.
func (b *beadsLog) ownerKeys(key string, value []byte) []string {
	var out []string
	_, ownerUIDs, ann, ok := parseK8sMetaAnnotations(value)
	if ok {
		for _, uid := range ownerUIDs {
			if ok, found := b.uidToKey[uid]; found {
				out = append(out, ok)
			}
		}
	}
	if strings.HasPrefix(key, "/registry/minions/") {
		if m := ann["cluster.x-k8s.io/machine"]; m != "" {
			ns := ann["cluster.x-k8s.io/cluster-namespace"]
			if ns == "" {
				ns = "default"
			}
			out = append(out, "/registry/cluster.x-k8s.io/machines/"+ns+"/"+m)
		}
	}
	if strings.HasPrefix(key, "/registry/leases/kube-node-lease/") {
		out = append(out, "/registry/minions/"+strings.TrimPrefix(key, "/registry/leases/kube-node-lease/"))
	}
	return out
}

// revertUpdate is one reverted key: the bead id and the record to publish.
type revertUpdate struct {
	key string
	id  string
	r   rec
}

// planRollback builds the synthetic event burst and the records to write, with
// strictly consecutive revisions from startRev+1: deletes first (keys created
// since the base), then creates (keys deleted since the base, restored with
// the base value and a fresh CreateRevision; the uid lives in the value, so it
// is preserved), then updates (the base value, CreateRevision kept). Pure
// function: unit tested.
func planRollback(selected []keyChange, startRev int64) (events []*server.Event, updates []revertUpdate, endRev int64) {
	var deletes, creates, mods []keyChange
	for _, c := range selected {
		switch c.kind {
		case "created":
			deletes = append(deletes, c)
		case "deleted":
			creates = append(creates, c)
		case "modified":
			mods = append(mods, c)
		}
	}
	byKey := func(s []keyChange) { sort.Slice(s, func(i, j int) bool { return s[i].key < s[j].key }) }
	byKey(deletes)
	byKey(creates)
	byKey(mods)

	rev := startRev
	for _, c := range deletes {
		rev++
		cur := c.cur
		r := rec{CreateRevision: cur.r.CreateRevision, ModRevision: rev, Deleted: true}
		updates = append(updates, revertUpdate{key: c.key, id: cur.id, r: r})
		events = append(events, &server.Event{
			Delete: true,
			KV:     &server.KeyValue{Key: c.key, CreateRevision: cur.r.CreateRevision, ModRevision: rev},
			PrevKV: &server.KeyValue{Key: c.key, Value: cur.r.Value, CreateRevision: cur.r.CreateRevision, ModRevision: cur.r.ModRevision, Lease: cur.r.Lease},
		})
	}
	for _, c := range creates {
		rev++
		r := rec{Value: c.base.Value, CreateRevision: rev, ModRevision: rev, Lease: c.base.Lease}
		updates = append(updates, revertUpdate{key: c.key, id: c.cur.id, r: r})
		events = append(events, &server.Event{
			Create: true,
			KV:     &server.KeyValue{Key: c.key, Value: c.base.Value, CreateRevision: rev, ModRevision: rev, Lease: c.base.Lease},
		})
	}
	for _, c := range mods {
		rev++
		cur := c.cur
		r := rec{Value: c.base.Value, CreateRevision: cur.r.CreateRevision, ModRevision: rev, Lease: c.base.Lease}
		updates = append(updates, revertUpdate{key: c.key, id: cur.id, r: r})
		events = append(events, &server.Event{
			KV:     &server.KeyValue{Key: c.key, Value: c.base.Value, CreateRevision: cur.r.CreateRevision, ModRevision: rev, Lease: c.base.Lease},
			PrevKV: &server.KeyValue{Key: c.key, Value: cur.r.Value, CreateRevision: cur.r.CreateRevision, ModRevision: cur.r.ModRevision, Lease: cur.r.Lease},
		})
	}
	return events, updates, rev
}

// writeRevertedRecords persists the reverted records through beads in ONE
// RunInTransaction (one Dolt commit for the whole burst), so the durable side
// of a rollback is a single commit rather than one per key.
func (b *beadsLog) writeRevertedRecords(ctx context.Context, id string, updates []revertUpdate) error {
	return b.store.RunInTransaction(ctx, "kine: rollback txn "+id, func(tx beads.Transaction) error {
		for _, u := range updates {
			if u.id == "" {
				return fmt.Errorf("key %q has no bead id", u.key)
			}
			desc, err := encodeRec(u.r)
			if err != nil {
				return err
			}
			if err := tx.UpdateIssue(ctx, u.id, map[string]interface{}{"description": desc}, actor); err != nil {
				return fmt.Errorf("update bead %s for %q: %w", u.id, u.key, err)
			}
		}
		return nil
	})
}
