package usagestats

import (
	"context"
	"errors"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/storage/unified/resource/lease"
)

const defaultReconcileInterval = time.Hour

type Reconciler struct {
	services.Service

	store   *Store
	decls   *Declarations
	leases  *lease.Manager
	metrics *reconcilerMetrics
	log     log.Logger
	now     func() time.Time

	interval time.Duration
}

type ReconcilerOptions struct {
	Store        *Store
	Declarations *Declarations
	Leases       *lease.Manager
	Reg          prometheus.Registerer
	Log          log.Logger

	Interval time.Duration
	// Now overrides the clock for testing; defaults to time.Now.
	Now func() time.Time
}

func NewReconciler(opts ReconcilerOptions) (*Reconciler, error) {
	if opts.Store == nil {
		return nil, errors.New("usage stats reconciler requires a store")
	}
	if opts.Leases == nil {
		return nil, errors.New("usage stats reconciler requires a lease manager")
	}
	decls := opts.Declarations
	if decls == nil {
		decls = DefaultDeclarations()
	}
	if err := decls.Validate(); err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	logger := opts.Log
	if logger == nil {
		logger = log.New("unified-storage.usagestats.reconciler")
	}
	r := &Reconciler{
		store:    opts.Store,
		decls:    decls,
		leases:   opts.Leases,
		metrics:  newReconcilerMetrics(opts.Reg),
		log:      logger,
		now:      now,
		interval: interval,
	}
	r.Service = services.NewBasicService(nil, r.running, nil)
	return r, nil
}

func (r *Reconciler) running(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.Reconcile(ctx); err != nil {
				r.log.Warn("usage stats reconcile failed", "error", err)
			}
		}
	}
}

// Reconcile recomputes aggregates for every declared resource across all
// namespaces. It returns the first error encountered but always attempts every
// namespace so one failure doesn't stall the rest.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	start := r.now()
	defer func() { r.metrics.reconcileDuration.Observe(r.now().Sub(start).Seconds()) }()

	// Anchor "today" to midnight in the same representation parseDay yields for
	// bucket keys, so window comparisons are exact.
	today, err := parseDay(r.now().Format(dayLayout))
	if err != nil {
		return err
	}

	var firstErr error
	for _, decl := range r.decls.All() {
		namespaces, err := r.store.listNamespaces(ctx, decl.Group, decl.Resource)
		if err != nil {
			r.log.Error("failed to list namespaces for usage stats reconcile",
				"group", decl.Group, "resource", decl.Resource, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, ns := range namespaces {
			scope := groupResourceNamespaceRef{Group: decl.Group, Resource: decl.Resource, Namespace: ns}
			if err := r.reconcileNamespace(ctx, scope, decl, today); err != nil {
				r.log.Error("failed to reconcile usage stats namespace",
					"group", decl.Group, "resource", decl.Resource, "namespace", ns, "error", err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	return firstErr
}

func (r *Reconciler) reconcileNamespace(ctx context.Context, scope groupResourceNamespaceRef, decl StatsDeclaration, today time.Time) error {
	l, err := r.leases.Acquire(ctx, scope.leaseName(), lease.WithTTL(flushLeaseTTL), lease.WithAutoRenew())
	if err != nil {
		if errors.Is(err, lease.ErrLeaseAlreadyHeld) {
			// A flush (or another reconcile) is working this namespace. Skip it
			// this cycle; the next tick, or another instance, picks it up.
			return nil
		}
		return err
	}
	defer func() {
		if releaseErr := r.leases.Release(context.WithoutCancel(ctx), l); releaseErr != nil {
			r.log.Warn("releasing usage stats reconcile lease failed", "lease", scope.leaseName(), "error", releaseErr)
		}
	}()

	// Stream one object at a time rather than materializing every object in the
	// namespace; each object's daily bucket set is bounded (days x metrics).
	for od, err := range r.store.StreamObjectDailies(ctx, scope.Group, scope.Resource, scope.Namespace) {
		if err != nil {
			return err
		}
		select {
		case <-l.Lost():
			r.log.Warn("usage stats reconcile lease lost; stopping namespace",
				"group", scope.Group, "resource", scope.Resource, "namespace", scope.Namespace)
			return nil
		default:
		}
		if err := r.store.WriteAggregates(ctx, od.Ref, computeAggregates(decl, od.Daily, today)); err != nil {
			return err
		}
	}
	return nil
}

// computeAggregates derives the aggregates cache for an object from its daily
// buckets. daily is day -> metric -> value and includes the overflow bucket.
//
//   - total = overflow + sum of every dated bucket
//   - last_N_days = sum of buckets in the inclusive range [today-(N-1), today]
//
// It always emits every metric/window/total field, so a stale (over-counted)
// aggregate is corrected back down to the true value on write.
func computeAggregates(decl StatsDeclaration, daily map[string]map[string]uint64, today time.Time) map[string]uint64 {
	// Precompute each window's inclusive start day.
	windowStart := make(map[int]time.Time, len(decl.Windows))
	for _, w := range decl.Windows {
		windowStart[w] = today.AddDate(0, 0, -(w - 1))
	}

	fields := make(map[string]uint64, len(decl.Metrics)*(len(decl.Windows)+1))
	for _, metric := range decl.Metrics {
		var total uint64
		windowSums := make(map[int]uint64, len(decl.Windows))
		for day, metrics := range daily {
			v := metrics[metric]
			total += v
			if day == overflowBucket {
				// Overflow feeds total but no window (it is older than MaxWindow).
				continue
			}
			d, err := parseDay(day)
			if err != nil {
				continue
			}
			if d.After(today) {
				continue
			}
			for _, w := range decl.Windows {
				if !d.Before(windowStart[w]) {
					windowSums[w] += v
				}
			}
		}
		fields[totalField(metric)] = total
		for _, w := range decl.Windows {
			fields[aggregateField(metric, w)] = windowSums[w]
		}
	}
	return fields
}
