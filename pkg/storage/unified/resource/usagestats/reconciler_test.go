package usagestats

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/storage/unified/resource/lease"
)

func newTestReconciler(t *testing.T, store *Store, leases *lease.Manager, now func() time.Time) *Reconciler {
	t.Helper()
	if leases == nil {
		leases = newTestLeases(t)
	}
	r, err := NewReconciler(ReconcilerOptions{
		Store:  store,
		Leases: leases,
		Reg:    prometheus.NewRegistry(),
		Now:    now,
	})
	require.NoError(t, err)
	return r
}

// dayOffset returns the YYYY-MM-DD bucket for `days` before the fixed "today".
func dayOffset(today string, days int) string {
	base, _ := parseDay(today)
	return base.AddDate(0, 0, -days).Format(dayLayout)
}

func TestComputeAggregates(t *testing.T) {
	decl, _ := DefaultDeclarations().Lookup(dashboardsGroup, dashboardsResource) // windows 1,7,30
	today, _ := parseDay("2026-06-23")

	daily := map[string]map[string]uint64{
		"2026-06-23":   {"views": 5, "queries": 2}, // today
		"2026-06-20":   {"views": 3},               // within 7d
		"2026-06-01":   {"views": 7},               // within 30d, outside 7d
		"2026-05-01":   {"views": 100},             // outside 30d window
		overflowBucket: {"views": 1000},            // folded history, total only
	}

	got := computeAggregates(decl, daily, today)

	// views total = 5 + 3 + 7 + 100 + 1000
	require.Equal(t, uint64(1115), got["views_total"])
	require.Equal(t, uint64(5), got["views_last_1_days"])
	require.Equal(t, uint64(8), got["views_last_7_days"])   // 5 + 3
	require.Equal(t, uint64(15), got["views_last_30_days"]) // 5 + 3 + 7

	// queries only has a value today.
	require.Equal(t, uint64(2), got["queries_total"])
	require.Equal(t, uint64(2), got["queries_last_1_days"])
	require.Equal(t, uint64(2), got["queries_last_7_days"])
	require.Equal(t, uint64(2), got["queries_last_30_days"])

	// A metric with no data still emits zeroed fields so stale values reset.
	require.Equal(t, uint64(0), got["errors_total"])
	require.Equal(t, uint64(0), got["errors_last_7_days"])
}

func TestComputeAggregatesIgnoresFutureDays(t *testing.T) {
	decl, _ := DefaultDeclarations().Lookup(dashboardsGroup, dashboardsResource)
	today, _ := parseDay("2026-06-23")

	daily := map[string]map[string]uint64{
		"2026-06-23": {"views": 5},
		"2026-06-25": {"views": 9}, // clock skew / future bucket
	}
	got := computeAggregates(decl, daily, today)
	// Future days contribute to total but never to a rolling window.
	require.Equal(t, uint64(14), got["views_total"])
	require.Equal(t, uint64(5), got["views_last_1_days"])
	require.Equal(t, uint64(5), got["views_last_30_days"])
}

func TestReconcilerRecomputesWindowsAfterRollover(t *testing.T) {
	forEachBackend(t, func(t *testing.T, store *Store) {
		ctx := context.Background()
		const today = "2026-06-23"
		o := newTestObject("dash-a")

		// Daily source of truth: one view today, one view 10 days ago.
		require.NoError(t, store.IncrementDaily(ctx, o, today, map[string]uint64{"views": 1}))
		require.NoError(t, store.IncrementDaily(ctx, o, dayOffset(today, 10), map[string]uint64{"views": 1}))

		// Simulate drifted aggregates left by incremental flush bumps: the
		// 10-day-old view is still counted in last_7_days.
		require.NoError(t, store.WriteAggregates(ctx, o, map[string]uint64{
			"views_last_1_days":  2,
			"views_last_7_days":  2,
			"views_last_30_days": 2,
			"views_total":        2,
		}))

		r := newTestReconciler(t, store, nil, fixedNow(today))
		require.NoError(t, r.Reconcile(ctx))

		all, err := store.ScanAggregates(ctx, dashboardsGroup, dashboardsResource, "default")
		require.NoError(t, err)
		got := all["dash-a"]
		require.Equal(t, uint64(1), got["views_last_1_days"])  // only today
		require.Equal(t, uint64(1), got["views_last_7_days"])  // the 10-day-old view dropped out
		require.Equal(t, uint64(2), got["views_last_30_days"]) // both still in 30d
		require.Equal(t, uint64(2), got["views_total"])
	})
}

func TestReconcilerAcrossNamespaces(t *testing.T) {
	forEachBackend(t, func(t *testing.T, store *Store) {
		ctx := context.Background()
		const today = "2026-06-23"

		a := objectRef{Group: dashboardsGroup, Resource: dashboardsResource, Namespace: "ns-a", Name: "dash-a"}
		b := objectRef{Group: dashboardsGroup, Resource: dashboardsResource, Namespace: "ns-b", Name: "dash-b"}
		require.NoError(t, store.IncrementDaily(ctx, a, today, map[string]uint64{"views": 3}))
		require.NoError(t, store.IncrementDaily(ctx, b, today, map[string]uint64{"queries": 4}))

		r := newTestReconciler(t, store, nil, fixedNow(today))
		require.NoError(t, r.Reconcile(ctx))

		aggA, err := store.ScanAggregates(ctx, dashboardsGroup, dashboardsResource, "ns-a")
		require.NoError(t, err)
		require.Equal(t, uint64(3), aggA["dash-a"]["views_total"])

		aggB, err := store.ScanAggregates(ctx, dashboardsGroup, dashboardsResource, "ns-b")
		require.NoError(t, err)
		require.Equal(t, uint64(4), aggB["dash-b"]["queries_total"])
	})
}

func TestReconcilerSkipsNamespaceWithHeldLease(t *testing.T) {
	// Badger-only: a single shared KV so the externally-held lease is visible
	// to the reconciler.
	kvStore := newBadgerKV(t)
	store := NewStore(kvStore)
	leases := lease.NewManager(kvStore, "test-holder", nil,
		lease.WithInternalMinTTL(time.Second), lease.WithGarbageCollectionDisabled)
	t.Cleanup(leases.Stop)

	ctx := context.Background()
	const today = "2026-06-23"
	heldObj := objectRef{Group: dashboardsGroup, Resource: dashboardsResource, Namespace: "held", Name: "dash-a"}
	freeObj := objectRef{Group: dashboardsGroup, Resource: dashboardsResource, Namespace: "free", Name: "dash-b"}
	require.NoError(t, store.IncrementDaily(ctx, heldObj, today, map[string]uint64{"views": 5}))
	require.NoError(t, store.IncrementDaily(ctx, freeObj, today, map[string]uint64{"views": 9}))

	// Hold one namespace's flush lease so the reconciler must skip it, while
	// leaving the other namespace's lease free.
	scope := groupResourceNamespaceRef{Group: dashboardsGroup, Resource: dashboardsResource, Namespace: "held"}
	held, err := leases.Acquire(ctx, scope.leaseName(), lease.WithTTL(flushLeaseTTL))
	require.NoError(t, err)

	r := newTestReconciler(t, store, leases, fixedNow(today))
	require.NoError(t, r.Reconcile(ctx))

	// The held namespace was skipped: no aggregates written.
	heldAgg, err := store.ScanAggregates(ctx, dashboardsGroup, dashboardsResource, "held")
	require.NoError(t, err)
	require.Empty(t, heldAgg)

	// A namespace whose lease is free is reconciled in the same cycle.
	freeAgg, err := store.ScanAggregates(ctx, dashboardsGroup, dashboardsResource, "free")
	require.NoError(t, err)
	require.Equal(t, uint64(9), freeAgg["dash-b"]["views_total"])

	// After releasing, a subsequent reconcile writes the held namespace too.
	require.NoError(t, leases.Release(ctx, held))
	require.NoError(t, r.Reconcile(ctx))
	heldAgg, err = store.ScanAggregates(ctx, dashboardsGroup, dashboardsResource, "held")
	require.NoError(t, err)
	require.Equal(t, uint64(5), heldAgg["dash-a"]["views_total"])
}

func TestNewReconcilerRequiresLeaseManager(t *testing.T) {
	_, err := NewReconciler(ReconcilerOptions{Store: NewStore(newBadgerKV(t))})
	require.Error(t, err)
}

func TestNewReconcilerRequiresStore(t *testing.T) {
	_, err := NewReconciler(ReconcilerOptions{Leases: newTestLeases(t)})
	require.Error(t, err)
}
