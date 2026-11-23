// Package beads implements a kine storage backend that runs k8s state THROUGH
// the beads work-graph library (github.com/steveyegge/beads), with Dolt embedded
// in-process. Every Kubernetes object kine stores becomes a bead (an Issue) via
// beads' public Go API — not a raw row in a Dolt SQL table. No `dolt` binary and
// no `dolt sql-server` process are required: beads opens embedded Dolt and
// everything compiles into the kine binary and runs in one process.
//
// This is the "k3s-on-beads" frankenstein. See docs/k3s-on-beads.md in the gas
// repo for the architecture ("What beads buys over Dolt alone", and Part 2).
//
// Division of labor (and the honest simplifications for this first cut):
//
//   - WRITES go through beads. Append maps each etcd key to one bead, keyed by
//     ExternalRef = the etcd key, with the kine record (value + revisions +
//     lease + delete tombstone) carried as JSON in the bead's Description. This
//     is the durable store of record, and what the in-process beads HTTP
//     endpoint (serve.go) exposes to gas city.
//   - kine's etcd READS and its gap-free WATCH stream are served from in-process
//     structures (an index of current state + an append log of events), which
//     are REBUILT FROM BEADS at Start. kine is the only writer (embedded Dolt is
//     single-writer), so the index cannot drift from beads under normal use; an
//     out-of-band write via the beads port would (reads-only there, by design).
//   - Time-travel reads (List at an arbitrary past revision) are best-effort:
//     the in-memory log covers [startRev, now]; older revisions return as if
//     compacted. Faithful historical reads over Dolt history are the follow-up
//     (the resourceVersion-reconciliation work the design note flags).
//
// Endpoint form:  beads://<beads-dir>
//   - <beads-dir>  filesystem directory holding the beads workspace (its .beads
//     dir / embedded Dolt data). Created/initialized if absent.
//
// Build note: embedded Dolt requires cgo and the gms_pure_go build tag
// (CGO_ENABLED=1, -tags gms_pure_go) to avoid linking ICU. See the gas repo's
// docs/k3s-on-beads.md and beads engdocs/ICU-POLICY.md.
package beads

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	beads "github.com/steveyegge/beads"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/server"
)

const (
	defaultDataDir = "./db/beads"

	// actor is stamped on every beads write for provenance ("who wrote this").
	// The design note's payoff is partly that the beads event log is
	// provenance-stamped; this is kine's signature on the control-plane writes.
	actor = "kine"

	// k8sLabel tags every bead kine creates, so a rebuild at Start can find them
	// all with one GetIssuesByLabel, and the beads port can filter to cluster
	// state. A label is an arbitrary string (not type-validated), which is why
	// the k8s identity lives here rather than in a custom IssueType.
	k8sLabel = "k8s"

	// issuePrefix seeds a fresh workspace's issue-id prefix (e.g. "k8s-tdr").
	issuePrefix = "k8s"

	// defaultPollInterval is how often the watch poll loop checks for new events
	// when kine's config does not specify one.
	defaultPollInterval = time.Second
)

// rec is the kine record carried as JSON in a bead's Description. json marshals
// Value ([]byte) as base64 automatically, so an opaque protobuf value round-trips
// as a string field.
type rec struct {
	Value          []byte `json:"v,omitempty"`
	CreateRevision int64  `json:"cr"`
	ModRevision    int64  `json:"mr"`
	Lease          int64  `json:"lease,omitempty"`
	Deleted        bool   `json:"del,omitempty"`
}

// entry is the in-process index value for one key: the bead's id (for
// UpdateIssue) and its current record.
type entry struct {
	id string
	r  rec
}

// beadsLog implements logstructured.Log over the beads library. See the package
// doc for the division of labor between beads (writes, durable truth) and the
// in-process structures (reads, watch).
type beadsLog struct {
	store beads.Storage
	wg    *sync.WaitGroup
	ctx   context.Context

	broadcaster  broadcaster.Broadcaster
	notify       chan int64
	pollInterval time.Duration

	// writeMu serializes the whole write path (CAS check + revision reservation +
	// the beads/Dolt write + the in-memory publish). Only one Append runs at a
	// time, which is what gives every successful write a total order and lets the
	// durable write happen WITHOUT holding mu — so CurrentRevision/After/List and
	// the poller never block behind a ~33 ms commit. The write-only bookkeeping
	// (types, uidToKey, pending) is guarded by writeMu alone, since no reader
	// touches it.
	writeMu sync.Mutex

	// mu guards the reader-visible in-memory state: rev, index, events and
	// eventsBase. It is held only for brief in-memory sections (the CAS snapshot
	// read and the post-write publish) and NEVER across a beads/Dolt call. Lock
	// order is always writeMu -> mu.
	mu       sync.RWMutex
	rev      int64               // global monotonic revision (last assigned)
	index    map[string]entry    // key -> current state
	events   []*server.Event     // append log, events[i].KV.ModRevision increases
	types    map[string]bool     // k8s kinds registered as beads custom types
	uidToKey map[string]string   // k8s object UID -> bead key (owner resolution)
	pending  map[string][]string // owner UID not-yet-seen -> waiting child bead ids
	// eventsBase is the revision just before events[0]; events are for
	// revisions (eventsBase, rev]. On a fresh process eventsBase == startRev.
	eventsBase int64

	// Branch control (k3s-on-beads-fork). dir/serverAddr/dbName let the store be
	// reopened onto a new Dolt branch; ctl is the MySQL control connection to the
	// shared sql-server (nil when branch control is disabled or in embedded mode).
	// curBranch/forkBase/forkCommit/parentBranch/parentCommit are the live branch
	// state, all guarded by mu. See branch.go.
	dir          string
	serverAddr   string
	dbName       string
	ctl          brancher
	ctlSocket    string
	ctlAddr      string // TCP host:port for the bearer-token-guarded control listener
	ctlTokenFile string
	ctlToken     string // bearer token (never logged)

	// Scoped-transaction ledger (txn.go). Lock order: txnLedger.mu -> writeMu -> mu.
	txnLedger    *txnLedger
	txnOnce      sync.Once
	curBranch    string
	forkBase     int64
	forkCommit   string
	parentBranch string
	parentCommit string
}

// parseEndpoint splits the kine endpoint into the beads data directory and the
// optional beads read-endpoint address. Form: beads://<dir>[?serve=<host:port>].
// parseEndpoint splits the kine endpoint into the beads workspace directory and
// an optional shared dolt sql-server address. Form:
// beads://<dir>[?server=<host:port>]. With ?server= kine runs in SERVER mode —
// the beads library connects over MySQL to that dolt sql-server (shared with bd
// and agents). Without it, kine opens the workspace embedded (single process).
func parseEndpoint(dataSourceName string) (dir, serverAddr string, err error) {
	dir = dataSourceName
	if dir == "" {
		dir = defaultDataDir
	}
	if i := strings.IndexByte(dir, '?'); i >= 0 {
		q, perr := url.ParseQuery(dir[i+1:])
		if perr != nil {
			return "", "", fmt.Errorf("parse beads endpoint query: %w", perr)
		}
		serverAddr = q.Get("server")
		dir = dir[:i]
	}
	abs, aerr := filepath.Abs(dir)
	if aerr != nil {
		return "", "", fmt.Errorf("resolve beads data dir: %w", aerr)
	}
	return abs, serverAddr, nil
}

// New opens (initializing if needed) the embedded beads workspace and returns a
// kine Backend that stores state through it. It returns leaderElect=true to
// match the other shared-store drivers: a single kine fronts a single store.
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	dir, serverAddr, err := parseEndpoint(cfg.DataSourceName)
	if err != nil {
		return false, nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, nil, fmt.Errorf("create beads data dir %q: %w", dir, err)
	}

	store, err := openOrInit(ctx, dir, serverAddr)
	if err != nil {
		return false, nil, err
	}
	if serverAddr != "" {
		logrus.Infof("Kine beads driver: SERVER mode — beads library -> dolt sql-server %s (workspace %s)", serverAddr, dir)
	} else {
		logrus.Infof("Kine beads driver: EMBEDDED mode — workspace %s", dir)
	}

	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	bl := &beadsLog{
		store:        store,
		wg:           wg,
		notify:       make(chan int64, 1024),
		pollInterval: pollInterval,
		index:        make(map[string]entry),
		types:        make(map[string]bool),
		uidToKey:     make(map[string]string),
		pending:      make(map[string][]string),
		dir:          dir,
		serverAddr:   serverAddr,
		curBranch:    "main",
	}

	// Branch control (k3s-on-beads-fork) is opt-in via the control socket and only
	// available in SERVER mode, where the branch flips go through a kine-owned
	// MySQL control connection to the shared dolt sql-server. The database name is
	// read from the workspace's .beads/metadata.json (dolt_database).
	bl.ctlSocket = os.Getenv("KINE_BRANCH_CONTROL_SOCKET")
	bl.ctlAddr = os.Getenv("KINE_BRANCH_CONTROL_ADDRESS")
	bl.ctlTokenFile = os.Getenv("KINE_BRANCH_CONTROL_TOKEN_FILE")
	if bl.ctlAddr != "" {
		// A TCP listener is only ever started with a bearer token: the control API
		// can rewrite cluster state. The token is read once and kept in memory.
		if bl.ctlTokenFile == "" {
			return false, nil, fmt.Errorf("beads branch control: --branch-control-address requires --branch-control-token-file")
		}
		raw, terr := os.ReadFile(bl.ctlTokenFile)
		if terr != nil {
			return false, nil, fmt.Errorf("beads branch control: read token file: %w", terr)
		}
		bl.ctlToken = strings.TrimSpace(string(raw))
		if bl.ctlToken == "" {
			return false, nil, fmt.Errorf("beads branch control: token file %q is empty", bl.ctlTokenFile)
		}
	}
	if bl.ctlSocket != "" || bl.ctlAddr != "" {
		if serverAddr == "" {
			logrus.Warn("Kine beads driver: branch control requested but not in SERVER mode (?server=); branch control disabled")
		} else {
			dbName, derr := doltDatabaseName(dir)
			if derr != nil {
				return false, nil, derr
			}
			ctl, cerr := newDoltControl(serverAddr, dbName)
			if cerr != nil {
				return false, nil, cerr
			}
			bl.dbName = dbName
			bl.ctl = ctl
			if cur, err := ctl.GetDefaultBranch(ctx); err == nil && cur != "" {
				bl.curBranch = cur
			}
			logrus.Infof("Kine beads driver: branch control enabled on %s (db %s), current branch %q", serverAddr, dbName, bl.curBranch)
		}
	}
	return true, logstructured.New(bl), nil
}

// doltDatabaseName reads dolt_database from the workspace's .beads/metadata.json,
// so the branch-control connection targets the same database the beads library
// opened. Defaults to "k3s_beads" if the field is absent.
func doltDatabaseName(dir string) (string, error) {
	metaPath := filepath.Join(dir, ".beads", "metadata.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		return "", fmt.Errorf("read beads metadata for db name: %w", err)
	}
	var meta struct {
		DoltDatabase string `json:"dolt_database"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return "", fmt.Errorf("parse beads metadata for db name: %w", err)
	}
	if meta.DoltDatabase == "" {
		return "k3s_beads", nil
	}
	return meta.DoltDatabase, nil
}

// openOrInit opens the beads workspace under dir, initializing it on first use.
// beads' OpenBestAvailable requires an existing workspace (a .beads directory
// with a metadata.json), so a missing one is bootstrapped here the way `bd init`
// would: write the backend marker, open, then seed the issue prefix a fresh
// workspace needs before any issue can be created.
func openOrInit(ctx context.Context, dir, serverAddr string) (beads.Storage, error) {
	beadsDir := filepath.Join(dir, ".beads")
	metaPath := filepath.Join(beadsDir, "metadata.json")
	fresh := false
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		if err := os.MkdirAll(beadsDir, 0o700); err != nil {
			return nil, fmt.Errorf("create beads workspace dir %q: %w", beadsDir, err)
		}
		if err := os.WriteFile(metaPath, []byte(`{"backend":"dolt"}`), 0o600); err != nil {
			return nil, fmt.Errorf("write beads metadata %q: %w", metaPath, err)
		}
		fresh = true
	}

	if serverAddr != "" {
		// Server mode: kine still speaks the beads library, but the library
		// connects over MySQL to a shared dolt sql-server instead of opening the
		// embedded file. Same Go API, different transport — and the shared server
		// is what lets bd and agents reach the same store concurrently.
		host, port, serr := net.SplitHostPort(serverAddr)
		if serr != nil {
			return nil, fmt.Errorf("invalid beads server address %q (want host:port): %w", serverAddr, serr)
		}
		os.Setenv("BEADS_DOLT_SERVER_MODE", "1")
		os.Setenv("BEADS_DOLT_SERVER_HOST", host)
		os.Setenv("BEADS_DOLT_SERVER_PORT", port)
	} else {
		// Embedded mode: clear the server switches so beads opens the local file.
		os.Unsetenv("BEADS_DOLT_SERVER_MODE")
		os.Unsetenv("BEADS_DOLT_SHARED_SERVER")
		os.Unsetenv("BEADS_DOLT_SERVER_HOST")
	}

	store, err := beads.OpenBestAvailable(ctx, beadsDir)
	if err != nil {
		return nil, fmt.Errorf("open beads workspace at %q: %w", beadsDir, err)
	}
	if fresh {
		if err := store.SetConfig(ctx, "issue_prefix", issuePrefix); err != nil {
			store.Close()
			return nil, fmt.Errorf("seed beads issue prefix: %w", err)
		}
		// Persist the versioned-history flag so `bd` (and any clone that pulls)
		// also records.
		_ = store.SetConfig(ctx, "versioned-history.enabled", "true")
	}
	// Flip the store's in-process versioned-history switch. The config flag alone
	// is inert for a library opener: bd's CLI calls this, OpenBestAvailable does
	// not, so without it kine's writes land in issue_versions unrecorded and
	// `bd versions`/`bd history`/`bd diff` show nothing. With it, every k8s object
	// gets full per-revision Dolt history (storage-layer time-travel), not just
	// the event log. The method is on the concrete store; assert structurally so
	// kine need not import beads' internal storage package.
	if vh, ok := store.(interface{ SetVersionedHistoryEnabled(bool) }); ok {
		vh.SetVersionedHistoryEnabled(true)
	} else {
		logrus.Warn("Kine beads driver: store does not expose SetVersionedHistoryEnabled; per-revision history disabled")
	}
	return store, nil
}

func (b *beadsLog) Start(ctx context.Context) error {
	b.ctx = ctx
	if err := b.rebuildFromBeads(ctx); err != nil {
		return err
	}
	// Recover the revision high-water mark: after a rollback kine's counter ran
	// ahead of the highest mr durably on this branch, so rev = max(max(mr), hwm).
	if hwm := b.readHWM(ctx); hwm > 0 {
		b.mu.Lock()
		if hwm > b.rev {
			logrus.Infof("Kine beads driver: recovered revision high-water mark %d (above max mr %d)", hwm, b.rev)
			b.rev = hwm
			b.eventsBase = hwm
		}
		b.mu.Unlock()
	}
	// Start the control server (branch + transaction API) and arrange to persist
	// the high-water mark on clean shutdown.
	if b.ctl != nil && (b.ctlSocket != "" || b.ctlAddr != "") {
		b.loadTxnLedger(ctx)
		if err := b.serveBranchControl(ctx, b.ctlSocket, b.ctlAddr); err != nil {
			return err
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			b.mu.RLock()
			rev := b.rev
			b.mu.RUnlock()
			b.persistHWM(sctx, rev)
			if err := b.ctl.Close(); err != nil {
				logrus.Warnf("Kine beads driver: close branch control connection: %v", err)
			}
		}()
	}
	return nil
}

// rebuildFromBeads loads every k8s bead into the in-process index and recovers
// the global revision counter from the max ModRevision found. The append log
// starts empty: new watches begin at the current revision, which is etcd's own
// behavior after compaction.
func (b *beadsLog) rebuildFromBeads(ctx context.Context) error {
	index, max, err := b.loadIndex(ctx, b.store)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// Load the kinds already registered as custom types, so a restart extends
	// the list rather than overwriting it.
	if raw, cerr := b.store.GetConfig(ctx, "types.custom"); cerr == nil {
		for _, t := range strings.Split(raw, ",") {
			if t = strings.TrimSpace(t); t != "" {
				b.types[t] = true
			}
		}
	}
	b.index = index
	b.rev = max
	b.eventsBase = max
	logrus.Infof("Kine beads driver: recovered %d keys, revision %d", len(b.index), b.rev)
	// Wire the ownership graph for everything already stored (nothing is
	// otherwise backfilled), so a restart lights up the whole cluster's edges.
	b.backfillOwnership(ctx)
	return nil
}

// loadIndex loads every k8s bead from a store into a fresh index map and returns
// it with the max ModRevision found. It performs no locking and mutates nothing
// on the receiver, so it is reused both for the Start rebuild (into b.index) and
// for building a candidate index from a just-reopened branch during a switch.
func (b *beadsLog) loadIndex(ctx context.Context, store beads.Storage) (map[string]entry, int64, error) {
	issues, err := store.GetIssuesByLabel(ctx, k8sLabel)
	if err != nil {
		return nil, 0, fmt.Errorf("beads: load existing state: %w", err)
	}
	index := make(map[string]entry, len(issues))
	var max int64
	for _, iss := range issues {
		r, derr := decodeRec(iss.Description)
		if derr != nil {
			logrus.Warnf("Kine beads driver: skipping bead %s with undecodable record: %v", iss.ID, derr)
			continue
		}
		key := ""
		if iss.ExternalRef != nil {
			key = *iss.ExternalRef
		}
		if key == "" {
			continue
		}
		index[key] = entry{id: iss.ID, r: r}
		if r.ModRevision > max {
			max = r.ModRevision
		}
	}
	return index, max, nil
}

func (b *beadsLog) CurrentRevision(ctx context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.rev, nil
}

func (b *beadsLog) CompactRevision(ctx context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// The oldest revision still replayable from the in-memory log.
	return b.eventsBase, nil
}

// Append is the write path. It performs the etcd compare-and-swap, writes the
// resulting state through beads, records the event in the in-process log, and
// returns the new global revision. A CAS failure returns (0, nil): logstructured
// interprets rev==0 as "precondition failed" and reports ErrKeyExists/conflict.
func (b *beadsLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	e := *event
	if e.KV == nil {
		e.KV = &server.KeyValue{}
	}
	if e.PrevKV == nil {
		e.PrevKV = &server.KeyValue{}
	}
	key := e.KV.Key

	// writeMu serializes the entire write path so every successful Append gets a
	// unique, strictly increasing revision and the events log stays gap-free. The
	// durable beads/Dolt write below (~33 ms) runs under writeMu but NOT under mu,
	// so readers (CurrentRevision, After, List) and the poller are not blocked
	// behind it — that lock contention was the source of the watch-cache lag.
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	// CAS snapshot. Read the latest committed state under mu. Because writeMu is
	// held, no other writer can change rev/index between here and the publish
	// below, so this snapshot stays valid across the write.
	b.mu.RLock()
	cur, exists := b.index[key]
	baseRev := b.rev
	b.mu.RUnlock()

	live := exists && !cur.r.Deleted

	// etcd compare-and-swap:
	//   - create: the key must not currently be live.
	//   - update/delete: the current ModRevision must equal the caller's
	//     PrevKV.ModRevision.
	if e.Create {
		if live {
			return 0, nil
		}
	} else {
		if !exists || cur.r.ModRevision != e.PrevKV.ModRevision {
			return 0, nil
		}
	}

	// Reserve the next revision. newRev is derived under writeMu but b.rev is not
	// advanced until the durable write succeeds (in the publish section). A failed
	// write therefore leaves b.rev untouched, so the next Append reuses this same
	// number — no revision is consumed by a failure and later skipped, and the
	// ordering across writers stays total.
	newRev := baseRev + 1

	newRec := rec{
		Value:       e.KV.Value,
		ModRevision: newRev,
		Lease:       e.KV.Lease,
		Deleted:     e.Delete,
	}
	if e.Create || !exists {
		newRec.CreateRevision = newRev
	} else {
		newRec.CreateRevision = cur.r.CreateRevision
	}

	// Durable write (and custom-type registration) under writeMu only — no mu
	// held, so reads stay live during the commit.
	id, err := b.writeBead(ctx, key, exists, cur.id, newRec)
	if err != nil {
		// kine's logstructured wrapper historically swallowed a non-nil Append
		// error through its CAS-miss fallback; that is now fixed there, but keep
		// logging here so a write failure is loud regardless of the caller.
		logrus.Errorf("Kine beads driver: write for key %q failed: %v", key, err)
		return 0, err
	}
	logrus.Tracef("APPEND key=%s create=%v delete=%v exists=%v => rev=%d id=%s", key, e.Create, e.Delete, exists, newRev, id)

	// Build the watch event (full KV + PrevKV) before taking mu, so the publish
	// section stays a pure in-memory update.
	ev := &server.Event{
		Create: e.Create,
		Delete: e.Delete,
		KV: &server.KeyValue{
			Key:            key,
			Value:          newRec.Value,
			CreateRevision: newRec.CreateRevision,
			ModRevision:    newRev,
			Lease:          newRec.Lease,
		},
	}
	if !e.Create {
		ev.PrevKV = &server.KeyValue{
			Key:            key,
			Value:          cur.r.Value,
			CreateRevision: cur.r.CreateRevision,
			ModRevision:    cur.r.ModRevision,
			Lease:          cur.r.Lease,
		}
	} else {
		ev.KV.CreateRevision = newRev
	}

	// Publish: advance the revision, update the index, and append the event — all
	// in memory, under mu, only after the durable write succeeded. Held briefly;
	// no beads/Dolt call happens under mu. Events are appended in strictly
	// increasing revision order (writeMu serializes writers, newRev == baseRev+1),
	// so the log stays consecutive and gap-free as the poller asserts.
	b.mu.Lock()
	b.rev = newRev
	b.index[key] = entry{id: id, r: newRec}
	b.events = append(b.events, ev)
	b.mu.Unlock()

	// Decompose ownerReferences into beads parent-child edges so the cluster's
	// ownership graph is traversable (Deployment -> ReplicaSet -> Pod). Done on
	// create AND update: CAPI and other controllers set ownerReferences on a
	// later update, not always at create time, so update must re-wire too.
	// (Delete carries an empty value, so parseK8sMeta no-ops on it.) Runs under
	// writeMu (not mu): it does beads/Dolt writes (AddDependency) and mutates the
	// write-only ownership maps, and reads b.index, which is stable because
	// writeMu serializes every index writer.
	if !e.Delete {
		b.wireOwnership(ctx, key, id, newRec.Value)
	}

	b.notifyRevision(newRev)
	return newRev, nil
}

// writeBead creates or updates the bead backing key, returning its beads id.
func (b *beadsLog) writeBead(ctx context.Context, key string, exists bool, id string, r rec) (string, error) {
	desc, err := encodeRec(r)
	if err != nil {
		return "", err
	}
	if !exists {
		ref := key
		// The bead's type is the Kubernetes resource (pods, configmaps,
		// deployments…) derived from the etcd key — the design note's
		// Kind->bead-type mapping. These are registered as beads custom types on
		// first sighting (beads reads types.custom live inside the create
		// transaction, so a just-registered type is accepted immediately). A key
		// with no resource segment falls back to the built-in default type.
		//
		// The type is squeezed through mappedType first: beads' issue_type column
		// is VARCHAR(32), so a kind over 32 characters (e.g.
		// validatingadmissionpolicybindings) would otherwise fail the write
		// silently. The full kind is preserved on a k8s-kind=<kind> label, kept on
		// every k8s bead for uniformity (not only the squeezed ones) so the real
		// resource name is always recoverable.
		labels := []string{k8sLabel}
		itype := beads.TypeTask
		if kind := kindFromKey(key); kind != "" {
			mt := mappedType(kind)
			if err := b.ensureType(ctx, mt); err != nil {
				return "", err
			}
			itype = beads.IssueType(mt)
			labels = append(labels, kindLabel(kind))
		}
		iss := &beads.Issue{
			Title:       key,
			Description: desc,
			Status:      beads.StatusOpen,
			IssueType:   itype,
			ExternalRef: &ref,
			Labels:      labels,
		}
		if err := b.store.CreateIssue(ctx, iss, actor); err != nil {
			return "", fmt.Errorf("beads: create bead for %q: %w", key, err)
		}
		newID := iss.ID
		if newID == "" {
			// CreateIssue did not populate the id; recover it by external ref.
			got, gerr := b.store.GetIssueByExternalRef(ctx, key)
			if gerr != nil || got == nil {
				return "", fmt.Errorf("beads: resolve id for new bead %q: %w", key, gerr)
			}
			newID = got.ID
		}
		return newID, nil
	}
	updates := map[string]interface{}{"description": desc}
	if err := b.store.UpdateIssue(ctx, id, updates, actor); err != nil {
		return "", fmt.Errorf("beads: update bead %s for %q: %w", id, key, err)
	}
	return id, nil
}

// List returns the current state of a key or key range. Past-revision reads are
// best-effort current state (see package doc).
func (b *beadsLog) List(ctx context.Context, key, end string, limit, revision int64, includeDeletes, keysOnly bool) (server.EventBatch, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	batch := server.EventBatch{CurrentRev: b.rev}
	var out []*server.Event
	for k, ent := range b.index {
		if !inRange(k, key, end) {
			continue
		}
		if ent.r.Deleted && !includeDeletes {
			continue
		}
		out = append(out, b.eventFor(k, ent, keysOnly))
	}
	sortEvents(out)
	if limit > 0 && int64(len(out)) > limit {
		out = out[:limit]
	}
	batch.Events = out
	b.notifyRevision(b.rev)
	return batch, nil
}

func (b *beadsLog) ListStream(ctx context.Context, key, end string, limit, revision int64, includeDeletes, keysOnly bool) server.ListResult {
	kvc := make(chan *server.KeyValue, 1)
	errc := make(chan error, 1)

	batch, err := b.List(ctx, key, end, limit, revision, includeDeletes, keysOnly)
	if err != nil {
		errc <- err
		close(kvc)
		close(errc)
		return server.ListResult{KVc: kvc, Errorc: errc}
	}

	go func() {
		defer close(kvc)
		defer close(errc)
		for _, ev := range batch.Events {
			select {
			case kvc <- ev.KV:
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			}
		}
	}()
	return server.ListResult{KVc: kvc, Errorc: errc, CurrentRevision: batch.CurrentRev}
}

func (b *beadsLog) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var n int64
	for k, ent := range b.index {
		if ent.r.Deleted {
			continue
		}
		if inRange(k, key, end) {
			n++
		}
	}
	return b.rev, n, nil
}

// After returns events strictly after revision in [key, end), from the in-memory
// append log. The log is contiguous, so the stream is gap-free — which the
// watch poll loop requires.
func (b *beadsLog) After(ctx context.Context, key, end string, revision, limit int64) (server.EventBatch, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	batch := server.EventBatch{CurrentRev: b.rev}
	// b.events is append-only and strictly ascending by ModRevision (Append holds
	// writeMu, so revisions are assigned in a total order and appended in order).
	// Binary-search to the first event with ModRevision > revision and scan only
	// the tail, instead of walking all 27k+ entries on every poll and every new
	// watch. For a revision below eventsBase (compacted history) sort.Search
	// returns 0, so the whole retained log is scanned — exactly today's behavior.
	start := sort.Search(len(b.events), func(i int) bool {
		return b.events[i].KV.ModRevision > revision
	})
	for _, ev := range b.events[start:] {
		if !inRange(ev.KV.Key, key, end) {
			continue
		}
		batch.Events = append(batch.Events, ev)
		if limit > 0 && int64(len(batch.Events)) >= limit {
			break
		}
	}
	return batch, nil
}

func (b *beadsLog) Watch(ctx context.Context) <-chan server.EventBatch {
	return b.broadcaster.Watch(ctx, b.startWatch)
}

func (b *beadsLog) startWatch() (chan server.EventBatch, error) {
	pollStart, _ := b.CurrentRevision(b.ctx)
	c := make(chan server.EventBatch)
	b.wg.Add(1)
	go b.poll(c, pollStart)
	return c, nil
}

// poll feeds the watch channel with new events as they are appended. Because the
// append log is gap-free, this is a straight replay from the last reported
// revision — no fill/skip machinery is needed.
func (b *beadsLog) poll(result chan server.EventBatch, pollStart int64) {
	wait := time.NewTicker(b.pollInterval)
	last := pollStart
	defer func() {
		wait.Stop()
		close(result)
		b.wg.Done()
	}()

	for {
		var trigger bool
		select {
		case <-b.ctx.Done():
			return
		case <-b.notify:
			trigger = true
		case <-wait.C:
			trigger = true
		}
		if !trigger {
			continue
		}

		// Coalesce a burst of writes into a single scan: drain any notifications
		// queued behind the one we just took, so N concurrent Appends become one
		// After() call instead of N. (After is now a tail scan, but under 8-way
		// concurrent create this still collapses the per-write wakeups into one.)
		drained := true
		for drained {
			select {
			case <-b.notify:
			default:
				drained = false
			}
		}

		batch, err := b.After(b.ctx, "", "", last, 0)
		if err != nil || len(batch.Events) == 0 {
			continue
		}
		last = batch.Events[len(batch.Events)-1].KV.ModRevision
		select {
		case result <- server.EventBatch{CurrentRev: last, Events: batch.Events}:
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *beadsLog) DbSize(ctx context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// Approximate: the serialized size of current state. A precise on-disk size
	// would come from a beads sizer; this is enough for kine's health surface.
	var n int64
	for _, ent := range b.index {
		n += int64(len(ent.r.Value)) + 64
	}
	return n, nil
}

// Compact drops replayable history below revision from the in-memory log. The
// durable beads store keeps current state regardless; historical versions live
// in Dolt's own history, which this does not touch.
func (b *beadsLog) Compact(ctx context.Context, revision int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if revision <= b.eventsBase {
		return b.rev, nil
	}
	kept := b.events[:0]
	for _, ev := range b.events {
		if ev.KV.ModRevision > revision {
			kept = append(kept, ev)
		}
	}
	b.events = append([]*server.Event(nil), kept...)
	if revision > b.eventsBase {
		b.eventsBase = revision
	}
	return b.rev, nil
}

func (b *beadsLog) notifyRevision(revision int64) {
	select {
	case b.notify <- revision:
	default:
	}
}

func (b *beadsLog) eventFor(key string, ent entry, keysOnly bool) *server.Event {
	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: ent.r.CreateRevision,
		ModRevision:    ent.r.ModRevision,
		Lease:          ent.r.Lease,
	}
	if !keysOnly {
		kv.Value = ent.r.Value
	}
	return &server.Event{
		Delete: ent.r.Deleted,
		KV:     kv,
	}
}

// kindFromKey derives the Kubernetes resource from an etcd key. k8s stores
// objects under /registry/<resource>/<namespace>/<name> (or
// /registry/<resource>/<name> for cluster-scoped), so the first segment after
// /registry/ is the resource: pods, configmaps, deployments, services… For a
// grouped resource or CRD the first segment is the API group, which is an
// acceptable approximation here; the exact Kind would need the object's GVK.
// Returns "" for a key not under /registry/, which maps to the default type.
func kindFromKey(key string) string {
	const prefix = "/registry/"
	if !strings.HasPrefix(key, prefix) {
		return ""
	}
	rest := key[len(prefix):]
	seg := strings.SplitN(rest, "/", 3)
	if len(seg) == 0 || seg[0] == "" {
		return ""
	}
	// Grouped resources are stored as /registry/<group>/<resource>/<ns>/<name>,
	// where the group contains a dot (cluster.x-k8s.io, apiextensions.k8s.io, a
	// CRD group…). Use the RESOURCE, not the group, so CAPI objects are typed
	// `machines`/`clusters` rather than `cluster.x-k8s.io`. Core resources
	// (/registry/pods/…) and dotless groups (apps -> /registry/deployments/…)
	// put the resource first already.
	if strings.Contains(seg[0], ".") && len(seg) > 1 && seg[1] != "" {
		return seg[1]
	}
	return seg[0]
}

// maxTypeLen is the width of beads' issue_type column (VARCHAR(32)). A kind
// longer than this fails at the SQL layer ("string '…' is too large for column
// 'issue_type'"), silently dropping the write, so kindFromKey's result must be
// squeezed to fit before it is used as an IssueType.
const maxTypeLen = 32

// mappedType returns a stable beads issue type for a Kubernetes kind that fits
// beads' VARCHAR(32) issue_type column. Kinds of 32 characters or fewer pass
// through unchanged. A longer kind (e.g. "validatingadmissionpolicybindings" or
// "managedresourceactivationpolicies", both 33 chars) is mapped to its first 24
// characters, a hyphen, and the first 7 hex characters of sha256(full kind) —
// exactly 32 characters, deterministic, and collision-resistant across the
// distinct full kinds. The full kind is never lost: writeBead records it on
// every bead as a k8s-kind=<kind> label (see kindLabel). Pure function: unit
// tested in beads_test.go.
func mappedType(kind string) string {
	if len(kind) <= maxTypeLen {
		return kind
	}
	sum := sha256.Sum256([]byte(kind))
	return kind[:24] + "-" + hex.EncodeToString(sum[:])[:7]
}

// kindLabelPrefix tags the full (un-squeezed) Kubernetes kind on every k8s bead,
// so the real resource name survives even when mappedType had to shorten the
// issue type, and so the beads port can filter/group by exact kind.
const kindLabelPrefix = "k8s-kind="

// kindLabel is the label that records a bead's full Kubernetes kind.
func kindLabel(kind string) string {
	return kindLabelPrefix + kind
}

// ensureType registers kind as a beads custom type if it is not already, by
// rewriting the types.custom config with the full set. Called from writeBead
// inside Append, under writeMu, so the read-modify-write of b.types is still
// serialized across writers (b.types is write-only state — no reader touches it).
// The kind passed here is already squeezed by mappedType, so what lands in
// types.custom is always a valid VARCHAR(32) issue type.
func (b *beadsLog) ensureType(ctx context.Context, kind string) error {
	if b.types[kind] {
		return nil
	}
	b.types[kind] = true
	list := make([]string, 0, len(b.types))
	for t := range b.types {
		list = append(list, t)
	}
	sort.Strings(list)
	if err := b.store.SetConfig(ctx, "types.custom", strings.Join(list, ",")); err != nil {
		delete(b.types, kind)
		return fmt.Errorf("beads: register custom type %q: %w", kind, err)
	}
	return nil
}

// inRange reports whether k falls in the query range, following kine's
// conventions:
//   - key=="" and end=="": all keys (the global watch/poll scan, e.g.
//     logstructured.Watch calls After(ctx, "", "", rev, 0)).
//   - key!="" and end=="": the single key `key`.
//   - end!="": the half-open range [key, end), where key=="" means from start.
func inRange(k, key, end string) bool {
	if key == "" && end == "" {
		return true
	}
	if end == "" {
		return k == key
	}
	if key == "" {
		return k < end
	}
	return k >= key && k < end
}

func sortEvents(evs []*server.Event) {
	// Insertion sort by key; event sets here are small (a List page).
	for i := 1; i < len(evs); i++ {
		for j := i; j > 0 && evs[j-1].KV.Key > evs[j].KV.Key; j-- {
			evs[j-1], evs[j] = evs[j], evs[j-1]
		}
	}
}

func encodeRec(r rec) (string, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("beads: encode record: %w", err)
	}
	return string(data), nil
}

func decodeRec(desc string) (rec, error) {
	var r rec
	if desc == "" {
		return r, nil
	}
	if err := json.Unmarshal([]byte(desc), &r); err != nil {
		return r, err
	}
	return r, nil
}

func init() {
	drivers.Register("beads", New)
}
