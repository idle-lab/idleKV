package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type modeKind string

const (
	modeMemory modeKind = "memory"
	modeBench  modeKind = "bench"
)

type workloadKind string

const (
	workloadSingle workloadKind = "single"
	workloadMulti  workloadKind = "multi"
)

type benchOpKind string

const (
	opZAddInsert          benchOpKind = "zadd_insert"
	opZAddUpdate          benchOpKind = "zadd_update"
	opZRangeHead          benchOpKind = "zrange_head"
	opZRangeMid           benchOpKind = "zrange_mid"
	opZRangeDeep          benchOpKind = "zrange_deep"
	opZRangeHeadWithScore benchOpKind = "zrange_head_withscores"
)

type config struct {
	mode              modeKind
	server            string
	host              string
	port              int
	pid               int
	workload          workloadKind
	op                benchOpKind
	clients           int
	pipeline          int
	duration          time.Duration
	warmup            time.Duration
	ioTimeout         time.Duration
	reconnectDelay    time.Duration
	reportInterval    time.Duration
	errorLogLimit     int
	keyPrefix         string
	singleKey         string
	keyCount          int
	membersPerKey     int
	loadClients       int
	loadPipeline      int
	loadBatchMembers  int
	settle            time.Duration
	memorySamples     int
	memorySampleGap   time.Duration
	csvOut            string
	runID             string
	seed              uint64
	noVerify          bool
	verifyCorrectness bool
}

type opSpec struct {
	name            benchOpKind
	requiresLoad    bool
	rangeStart      int
	rangeStop       int
	withScores      bool
	returnedMembers int
}

type dataset struct {
	workload      workloadKind
	keyPrefix     string
	singleKey     string
	keyCount      int
	membersPerKey int
	totalMembers  int64
}

type memorySample struct {
	usedMemory     int64
	infoRSS        int64
	procRSS        int64
	usedMemoryPeak int64
}

type memoryRunResult struct {
	baseline       memorySample
	after          memorySample
	delta          int64
	bytesPerMember float64
}

type benchCounter struct {
	requests     int64
	errors       int64
	bytesSent    int64
	bytesRecv    int64
	latencyNsSum int64
}

type benchSharedStats struct {
	requests     atomic.Int64
	errors       atomic.Int64
	bytesSent    atomic.Int64
	bytesRecv    atomic.Int64
	latencyNsSum atomic.Int64
}

type benchState struct {
	measuring atomic.Bool
	stopCh    chan struct{}
	stopOnce  sync.Once
	startMu   sync.RWMutex
	start     time.Time
	end       time.Time

	errorLogLimit int64
	errorLogCount atomic.Int64
	errorLogMu    sync.Mutex
}

type workerStats struct {
	requests     int64
	errors       int64
	bytesSent    int64
	bytesRecv    int64
	latencyNsSum int64
	latencyHist  latencyHistogram
}

type benchResult struct {
	requests       int64
	errors         int64
	bytesSent      int64
	bytesRecv      int64
	latencyNsSum   int64
	avgLatencyUs   float64
	p50LatencyUs   float64
	p95LatencyUs   float64
	p99LatencyUs   float64
	opsPerSec      float64
	elementsPerSec float64
}

type latencyHistogram struct {
	boundsUs []int64
	counts   []uint64
	total    uint64
}

type respValue struct {
	kind      byte
	intValue  int64
	bulk      []byte
	bulkLen   int
	nilBulk   bool
	array     []respValue
	bytesRead int
}

type pendingRequest struct {
	reqBytes int
	key      string
	memberID int64
	score    string
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "flag error: %v\n", err)
		os.Exit(2)
	}

	ds := buildDataset(cfg)
	switch cfg.mode {
	case modeMemory:
		result, err := runMemoryMode(cfg, ds)
		if err != nil {
			fmt.Fprintf(os.Stderr, "memory mode failed: %v\n", err)
			os.Exit(1)
		}
		printMemorySummary(cfg, ds, result)
		if cfg.csvOut != "" {
			if err := appendCSV(cfg.csvOut, []string{
				"server", "workload", "op", "clients", "pipeline", "withscores", "key_count",
				"members_per_key", "total_members", "returned_members", "run", "ops_per_sec",
				"elements_per_sec", "avg_us", "p50_us", "p95_us", "p99_us",
				"used_memory_baseline", "used_memory_after", "used_memory_delta", "bytes_per_member",
				"rss_baseline", "rss_after",
			}, []string{
				cfg.server, string(cfg.workload), "memory", "0", "0", "false",
				strconv.Itoa(ds.keyCount), strconv.Itoa(ds.membersPerKey), strconv.FormatInt(ds.totalMembers, 10),
				"0", cfg.runID, "0", "0", "0", "0", "0", "0",
				strconv.FormatInt(result.baseline.usedMemory, 10),
				strconv.FormatInt(result.after.usedMemory, 10),
				strconv.FormatInt(result.delta, 10),
				fmt.Sprintf("%.6f", result.bytesPerMember),
				strconv.FormatInt(result.baseline.procRSS, 10),
				strconv.FormatInt(result.after.procRSS, 10),
			}); err != nil {
				fmt.Fprintf(os.Stderr, "append csv failed: %v\n", err)
				os.Exit(1)
			}
		}
	case modeBench:
		spec, err := resolveBenchOp(cfg.op)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bench mode failed: %v\n", err)
			os.Exit(1)
		}
		result, err := runBenchMode(cfg, ds, spec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bench mode failed: %v\n", err)
			os.Exit(1)
		}
		printBenchSummary(cfg, ds, spec, result)
		if cfg.csvOut != "" {
			if err := appendCSV(cfg.csvOut, []string{
				"server", "workload", "op", "clients", "pipeline", "withscores", "key_count",
				"members_per_key", "total_members", "returned_members", "run", "ops_per_sec",
				"elements_per_sec", "avg_us", "p50_us", "p95_us", "p99_us",
				"used_memory_baseline", "used_memory_after", "used_memory_delta", "bytes_per_member",
				"rss_baseline", "rss_after",
			}, []string{
				cfg.server, string(cfg.workload), string(spec.name),
				strconv.Itoa(cfg.clients), strconv.Itoa(cfg.pipeline), strconv.FormatBool(spec.withScores),
				strconv.Itoa(ds.keyCount), strconv.Itoa(ds.membersPerKey), strconv.FormatInt(ds.totalMembers, 10),
				strconv.Itoa(spec.returnedMembers), cfg.runID,
				fmt.Sprintf("%.6f", result.opsPerSec),
				fmt.Sprintf("%.6f", result.elementsPerSec),
				fmt.Sprintf("%.6f", result.avgLatencyUs),
				fmt.Sprintf("%.6f", result.p50LatencyUs),
				fmt.Sprintf("%.6f", result.p95LatencyUs),
				fmt.Sprintf("%.6f", result.p99LatencyUs),
				"", "", "", "", "", "",
			}); err != nil {
				fmt.Fprintf(os.Stderr, "append csv failed: %v\n", err)
				os.Exit(1)
			}
		}
	default:
		fmt.Fprintf(os.Stderr, "unsupported mode: %s\n", cfg.mode)
		os.Exit(2)
	}
}

func parseFlags() (config, error) {
	var cfg config

	flag.StringVar((*string)(&cfg.mode), "mode", string(modeBench),
		"mode: memory=preload dataset and sample memory, bench=run timed throughput/latency benchmark")
	flag.StringVar(&cfg.server, "server", "", "server label for reporting, e.g. redis or idlekv")
	flag.StringVar(&cfg.host, "host", "127.0.0.1", "target host")
	flag.IntVar(&cfg.port, "port", 6379, "target port")
	flag.IntVar(&cfg.pid, "pid", 0, "optional target process pid for /proc RSS sampling")
	flag.StringVar((*string)(&cfg.workload), "workload", string(workloadSingle), "workload: single or multi")
	flag.StringVar((*string)(&cfg.op), "op", string(opZRangeHead), "bench op: zadd_insert, zadd_update, zrange_head, zrange_mid, zrange_deep, zrange_head_withscores")
	flag.IntVar(&cfg.clients, "clients", 32, "number of benchmark clients")
	flag.IntVar(&cfg.pipeline, "pipeline", 1, "pipeline depth per benchmark client")
	flag.IntVar(&cfg.loadClients, "load-clients", 8, "number of loader clients for preloading data")
	flag.IntVar(&cfg.loadPipeline, "load-pipeline", 16, "pipeline depth per loader client")
	flag.IntVar(&cfg.loadBatchMembers, "load-batch-members", 16, "number of score/member pairs per preload ZADD")
	flag.StringVar(&cfg.keyPrefix, "key-prefix", "bench:zset", "key prefix for multi-key workload")
	flag.StringVar(&cfg.singleKey, "single-key", "bench:zset:single", "single-key workload key name")
	flag.IntVar(&cfg.keyCount, "key-count", 0, "override key count, default depends on workload")
	flag.IntVar(&cfg.membersPerKey, "members-per-key", 0, "override members per key, default depends on workload")
	flag.IntVar(&cfg.errorLogLimit, "error-log-limit", 16, "maximum number of worker errors to print")
	flag.StringVar(&cfg.csvOut, "csv-out", "", "optional csv file to append results to")
	flag.StringVar(&cfg.runID, "run-id", "", "optional run identifier for csv output")
	flag.BoolVar(&cfg.noVerify, "no-verify", false, "skip response validation")
	flag.BoolVar(&cfg.verifyCorrectness, "verify-correctness", false,
		"perform exact semantic verification of preloaded data and benchmark responses")

	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	warmup := flag.Duration("warmup", 10*time.Second, "warmup duration")
	ioTimeout := flag.Duration("io-timeout", 5*time.Second, "per-batch io timeout")
	reconnectDelay := flag.Duration("reconnect-delay", 200*time.Millisecond, "delay before reconnect")
	reportInterval := flag.Duration("report-interval", 2*time.Second, "progress report interval")
	settle := flag.Duration("settle", 10*time.Second, "settle time after preload before memory sampling")
	memorySampleGap := flag.Duration("memory-sample-gap", 500*time.Millisecond, "gap between repeated memory samples")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "base random seed")
	memorySamples := flag.Int("memory-samples", 5, "number of after-load memory samples")

	flag.Parse()

	cfg.duration = *duration
	cfg.warmup = *warmup
	cfg.ioTimeout = *ioTimeout
	cfg.reconnectDelay = *reconnectDelay
	cfg.reportInterval = *reportInterval
	cfg.settle = *settle
	cfg.memorySamples = *memorySamples
	cfg.memorySampleGap = *memorySampleGap
	cfg.seed = *seed

	if cfg.mode != modeMemory && cfg.mode != modeBench {
		return cfg, fmt.Errorf("--mode must be %q or %q", modeMemory, modeBench)
	}
	if cfg.workload != workloadSingle && cfg.workload != workloadMulti {
		return cfg, fmt.Errorf("--workload must be %q or %q", workloadSingle, workloadMulti)
	}
	if cfg.server == "" {
		return cfg, errors.New("--server is required")
	}
	if cfg.clients <= 0 {
		return cfg, errors.New("--clients must be > 0")
	}
	if cfg.pipeline <= 0 {
		return cfg, errors.New("--pipeline must be > 0")
	}
	if cfg.loadClients <= 0 {
		return cfg, errors.New("--load-clients must be > 0")
	}
	if cfg.loadPipeline <= 0 {
		return cfg, errors.New("--load-pipeline must be > 0")
	}
	if cfg.loadBatchMembers <= 0 {
		return cfg, errors.New("--load-batch-members must be > 0")
	}
	if cfg.duration <= 0 {
		return cfg, errors.New("--duration must be > 0")
	}
	if cfg.warmup < 0 {
		return cfg, errors.New("--warmup must be >= 0")
	}
	if cfg.ioTimeout <= 0 {
		return cfg, errors.New("--io-timeout must be > 0")
	}
	if cfg.reconnectDelay < 0 {
		return cfg, errors.New("--reconnect-delay must be >= 0")
	}
	if cfg.settle < 0 {
		return cfg, errors.New("--settle must be >= 0")
	}
	if cfg.memorySamples <= 0 {
		return cfg, errors.New("--memory-samples must be > 0")
	}
	if cfg.memorySampleGap < 0 {
		return cfg, errors.New("--memory-sample-gap must be >= 0")
	}
	if cfg.reportInterval <= 0 {
		return cfg, errors.New("--report-interval must be > 0")
	}

	switch cfg.workload {
	case workloadSingle:
		if cfg.keyCount == 0 {
			cfg.keyCount = 1
		}
		if cfg.membersPerKey == 0 {
			cfg.membersPerKey = 1_000_000
		}
	case workloadMulti:
		if cfg.keyCount == 0 {
			cfg.keyCount = 4096
		}
		if cfg.membersPerKey == 0 {
			cfg.membersPerKey = 1024
		}
	}

	if cfg.keyCount <= 0 {
		return cfg, errors.New("--key-count must be > 0")
	}
	if cfg.membersPerKey <= 0 {
		return cfg, errors.New("--members-per-key must be > 0")
	}

	return cfg, nil
}

func buildDataset(cfg config) dataset {
	total := int64(cfg.keyCount) * int64(cfg.membersPerKey)
	return dataset{
		workload:      cfg.workload,
		keyPrefix:     cfg.keyPrefix,
		singleKey:     cfg.singleKey,
		keyCount:      cfg.keyCount,
		membersPerKey: cfg.membersPerKey,
		totalMembers:  total,
	}
}

func resolveBenchOp(kind benchOpKind) (opSpec, error) {
	switch kind {
	case opZAddInsert:
		return opSpec{name: kind, requiresLoad: false}, nil
	case opZAddUpdate:
		return opSpec{name: kind, requiresLoad: true}, nil
	case opZRangeHead:
		return opSpec{name: kind, requiresLoad: true, rangeStart: 0, rangeStop: 9, returnedMembers: 10}, nil
	case opZRangeMid:
		return opSpec{name: kind, requiresLoad: true, rangeStart: 1000, rangeStop: 1009, returnedMembers: 10}, nil
	case opZRangeDeep:
		return opSpec{name: kind, requiresLoad: true, rangeStart: 100000, rangeStop: 100009, returnedMembers: 10}, nil
	case opZRangeHeadWithScore:
		return opSpec{
			name:            kind,
			requiresLoad:    true,
			rangeStart:      0,
			rangeStop:       9,
			returnedMembers: 10,
			withScores:      true,
		}, nil
	default:
		return opSpec{}, fmt.Errorf("unsupported --op %q", kind)
	}
}

func (d dataset) keyForIndex(idx int) string {
	if d.workload == workloadSingle {
		return d.singleKey
	}
	return fmt.Sprintf("%s:{%04d}", d.keyPrefix, idx)
}

func (d dataset) preloadKeyMember(id int64) (string, int) {
	if d.workload == workloadSingle {
		return d.singleKey, int(id)
	}
	keyIdx := int(id / int64(d.membersPerKey))
	localIdx := int(id % int64(d.membersPerKey))
	return d.keyForIndex(keyIdx), localIdx
}

func (d dataset) insertKeyMember(id int64) (string, int64) {
	if d.workload == workloadSingle {
		return d.singleKey, id
	}
	keyIdx := int(id % int64(d.keyCount))
	localID := id / int64(d.keyCount)
	return d.keyForIndex(keyIdx), localID
}

func memberName(localID int64) string {
	return fmt.Sprintf("member:%08d", localID+1)
}

func loadScore(localIdx int) string {
	return strconv.FormatInt(int64(localIdx+1), 10)
}

func updateScore(memberID int64, salt uint64) string {
	return strconv.FormatInt(10_000_000_000+memberID*17+int64(salt%997), 10)
}

func runMemoryMode(cfg config, ds dataset) (memoryRunResult, error) {
	baseline, err := sampleMemory(cfg)
	if err != nil {
		return memoryRunResult{}, fmt.Errorf("baseline sample failed: %w", err)
	}

	if err := preloadDataset(cfg, ds); err != nil {
		return memoryRunResult{}, fmt.Errorf("preload failed: %w", err)
	}
	if cfg.verifyCorrectness {
		if err := verifyPreloadedDataset(cfg, ds); err != nil {
			return memoryRunResult{}, fmt.Errorf("correctness verification failed: %w", err)
		}
	}
	if cfg.settle > 0 {
		time.Sleep(cfg.settle)
	}

	samples := make([]memorySample, 0, cfg.memorySamples)
	for i := 0; i < cfg.memorySamples; i++ {
		sample, err := sampleMemory(cfg)
		if err != nil {
			return memoryRunResult{}, fmt.Errorf("after-load sample %d failed: %w", i+1, err)
		}
		samples = append(samples, sample)
		if i+1 < cfg.memorySamples && cfg.memorySampleGap > 0 {
			time.Sleep(cfg.memorySampleGap)
		}
	}

	after := medianMemorySample(samples)
	delta := after.usedMemory - baseline.usedMemory
	bytesPerMember := 0.0
	if ds.totalMembers > 0 {
		bytesPerMember = float64(delta) / float64(ds.totalMembers)
	}

	return memoryRunResult{
		baseline:       baseline,
		after:          after,
		delta:          delta,
		bytesPerMember: bytesPerMember,
	}, nil
}

func runBenchMode(cfg config, ds dataset, spec opSpec) (benchResult, error) {
	if spec.returnedMembers > 0 && spec.rangeStop >= ds.membersPerKey {
		return benchResult{}, fmt.Errorf("op %s requires stop=%d but members-per-key=%d", spec.name, spec.rangeStop, ds.membersPerKey)
	}

	if spec.requiresLoad {
		if err := preloadDataset(cfg, ds); err != nil {
			return benchResult{}, fmt.Errorf("preload failed: %w", err)
		}
		if cfg.verifyCorrectness {
			if err := verifyPreloadedDataset(cfg, ds); err != nil {
				return benchResult{}, fmt.Errorf("correctness verification failed: %w", err)
			}
		}
		if cfg.settle > 0 {
			time.Sleep(cfg.settle)
		}
	}

	state := newBenchState(cfg.errorLogLimit)
	shared := &benchSharedStats{}
	results := make([]workerStats, cfg.clients)

	var nextInsertID atomic.Int64
	nextInsertID.Store(ds.totalMembers)

	var wg sync.WaitGroup
	for workerID := 0; workerID < cfg.clients; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results[id] = runBenchWorker(id, cfg, ds, spec, state, shared, &nextInsertID)
		}(workerID)
	}

	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		runBenchReporter(cfg.reportInterval, state, shared)
	}()

	if cfg.warmup > 0 {
		fmt.Printf("Warmup: %s\n", cfg.warmup)
		time.Sleep(cfg.warmup)
	}

	state.setMeasureStart(time.Now())
	state.measuring.Store(true)
	fmt.Printf(
		"Benchmarking: server=%s op=%s workload=%s duration=%s clients=%d pipeline=%d keys=%d members_per_key=%d\n",
		cfg.server, spec.name, cfg.workload, cfg.duration, cfg.clients, cfg.pipeline, ds.keyCount, ds.membersPerKey,
	)
	time.Sleep(cfg.duration)
	state.measuring.Store(false)
	state.setMeasureEnd(time.Now())
	state.stop()

	wg.Wait()
	<-reportDone

	merged := mergeWorkerStats(results)
	elapsed := state.measureEnd().Sub(state.measureStart())
	secs := math.Max(elapsed.Seconds(), 1e-9)
	avgUs := 0.0
	if merged.requests > 0 {
		avgUs = float64(merged.latencyNsSum) / float64(merged.requests) / 1e3
	}

	return benchResult{
		requests:       merged.requests,
		errors:         merged.errors,
		bytesSent:      merged.bytesSent,
		bytesRecv:      merged.bytesRecv,
		latencyNsSum:   merged.latencyNsSum,
		avgLatencyUs:   avgUs,
		p50LatencyUs:   merged.latencyHist.quantile(0.50),
		p95LatencyUs:   merged.latencyHist.quantile(0.95),
		p99LatencyUs:   merged.latencyHist.quantile(0.99),
		opsPerSec:      float64(merged.requests) / secs,
		elementsPerSec: float64(merged.requests*int64(spec.returnedMembersOrOne())) / secs,
	}, nil
}

func (s opSpec) returnedMembersOrOne() int {
	if s.returnedMembers > 0 {
		return s.returnedMembers
	}
	return 1
}

func newBenchState(errorLogLimit int) *benchState {
	if errorLogLimit < 0 {
		errorLogLimit = 0
	}
	return &benchState{
		stopCh:        make(chan struct{}),
		errorLogLimit: int64(errorLogLimit),
	}
}

func (s *benchState) stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *benchState) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

func (s *benchState) setMeasureStart(t time.Time) {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.start = t
}

func (s *benchState) setMeasureEnd(t time.Time) {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.end = t
}

func (s *benchState) measureStart() time.Time {
	s.startMu.RLock()
	defer s.startMu.RUnlock()
	return s.start
}

func (s *benchState) measureEnd() time.Time {
	s.startMu.RLock()
	defer s.startMu.RUnlock()
	return s.end
}

func (s *benchState) logWorkerError(workerID int, stage string, err error) {
	if err == nil || s.errorLogLimit <= 0 {
		return
	}
	seq := s.errorLogCount.Add(1)
	if seq > s.errorLogLimit {
		return
	}

	s.errorLogMu.Lock()
	defer s.errorLogMu.Unlock()
	fmt.Fprintf(os.Stderr, "[zset-bench error #%d] worker=%d stage=%s err=%v\n", seq, workerID, stage, err)
	if seq == s.errorLogLimit {
		fmt.Fprintf(os.Stderr, "[zset-bench error] reached --error-log-limit=%d, suppressing further logs\n", s.errorLogLimit)
	}
}

func preloadDataset(cfg config, ds dataset) error {
	fmt.Printf(
		"Preloading dataset: workload=%s keys=%d members_per_key=%d total_members=%d load_clients=%d load_pipeline=%d load_batch_members=%d\n",
		cfg.workload, ds.keyCount, ds.membersPerKey, ds.totalMembers, cfg.loadClients, cfg.loadPipeline, cfg.loadBatchMembers,
	)

	addr := fmt.Sprintf("%s:%d", cfg.host, cfg.port)
	var nextID atomic.Int64
	progressStep := maxInt64(ds.totalMembers/20, 1)
	var loaded atomic.Int64
	var firstErr error
	var errMu sync.Mutex
	var errOnce sync.Once
	var wg sync.WaitGroup

	for workerID := 0; workerID < cfg.loadClients; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := runLoadWorker(id, addr, cfg, ds, &nextID, &loaded); err != nil {
				errOnce.Do(func() {
					errMu.Lock()
					defer errMu.Unlock()
					firstErr = err
				})
			}
		}(workerID)
	}

	go func() {
		var lastBucket int64 = -1
		for {
			cur := loaded.Load()
			bucket := cur / progressStep
			if bucket != lastBucket {
				lastBucket = bucket
				fmt.Printf("  loaded=%d/%d (%.1f%%)\n", cur, ds.totalMembers, 100*float64(cur)/float64(maxInt64(ds.totalMembers, 1)))
			}
			if cur >= ds.totalMembers {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Wait()
	errMu.Lock()
	defer errMu.Unlock()
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func runLoadWorker(workerID int, addr string, cfg config, ds dataset, nextID, loaded *atomic.Int64) error {
	conn, err := net.DialTimeout("tcp", addr, cfg.ioTimeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 256*1024)
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	batch := bytes.NewBuffer(make([]byte, 0, cfg.loadPipeline*cfg.loadBatchMembers*64))
	expectedAdds := make([]int, 0, cfg.loadPipeline)

	for {
		batch.Reset()
		expectedAdds = expectedAdds[:0]

		for len(expectedAdds) < cfg.loadPipeline {
			start := nextID.Add(int64(cfg.loadBatchMembers)) - int64(cfg.loadBatchMembers)
			if start >= ds.totalMembers {
				break
			}

			key, localStart := ds.preloadKeyMember(start)
			remainingInKey := ds.membersPerKey - localStart
			remainingTotal := int(ds.totalMembers - start)
			pairs := minInt(cfg.loadBatchMembers, remainingInKey, remainingTotal)
			appendZAddBatch(batch, key, int64(localStart), pairs, loadScore)
			expectedAdds = append(expectedAdds, pairs)
		}

		if len(expectedAdds) == 0 {
			return nil
		}

		_ = conn.SetDeadline(time.Now().Add(cfg.ioTimeout))
		if _, err := conn.Write(batch.Bytes()); err != nil {
			return fmt.Errorf("write: %w", err)
		}

		for _, want := range expectedAdds {
			resp, err := readRESP(reader)
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			if !cfg.noVerify && (resp.kind != ':' || resp.intValue != int64(want)) {
				return fmt.Errorf("verify: expected integer %d, got kind=%q int=%d", want, resp.kind, resp.intValue)
			}
			loaded.Add(int64(want))
		}
	}
}

func runBenchWorker(workerID int, cfg config, ds dataset, spec opSpec, state *benchState, shared *benchSharedStats, nextInsertID *atomic.Int64) workerStats {
	local := workerStats{latencyHist: newLatencyHistogram()}
	addr := fmt.Sprintf("%s:%d", cfg.host, cfg.port)
	rng := newXorShift64(cfg.seed + uint64(workerID+1)*0x9e3779b97f4a7c15)
	batch := bytes.NewBuffer(make([]byte, 0, cfg.pipeline*256))
	pending := make([]pendingRequest, 0, cfg.pipeline)

	for !state.stopped() {
		conn, err := net.DialTimeout("tcp", addr, cfg.ioTimeout)
		if err != nil {
			if state.measuring.Load() {
				state.logWorkerError(workerID, "dial", err)
			}
			if !sleepOrStop(state.stopCh, cfg.reconnectDelay) {
				continue
			}
			return local
		}

		reader := bufio.NewReaderSize(conn, 256*1024)
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
		}

		for !state.stopped() {
			batch.Reset()
			pending = pending[:0]

			for i := 0; i < cfg.pipeline; i++ {
				req := buildPendingRequest(spec, ds, &rng, nextInsertID)
				req.reqBytes = appendBenchCommand(batch, spec, req)
				pending = append(pending, req)
			}

			_ = conn.SetDeadline(time.Now().Add(cfg.ioTimeout))
			start := time.Now()
			if _, err := conn.Write(batch.Bytes()); err != nil {
				if state.measuring.Load() {
					state.logWorkerError(workerID, "write", err)
				}
				break
			}

			for _, req := range pending {
				resp, err := readRESP(reader)
				if err != nil {
					if state.measuring.Load() {
						state.logWorkerError(workerID, "read", err)
					}
					goto reconnect
				}

				var verifyErr error
				if !cfg.noVerify {
					verifyErr = validateBenchResponse(spec, req, resp, cfg.verifyCorrectness)
				}
				ok := verifyErr == nil
				if verifyErr != nil && state.measuring.Load() {
					state.logWorkerError(workerID, "verify", verifyErr)
				}

				if state.measuring.Load() {
					lat := time.Since(start)
					if len(pending) > 0 {
						lat /= time.Duration(len(pending))
					}
					local.record(req.reqBytes, resp.bytesRead, lat, ok)
					shared.record(req.reqBytes, resp.bytesRead, lat, ok)
				}
			}
		}

	reconnect:
		_ = conn.Close()
		if state.stopped() {
			return local
		}
		if sleepOrStop(state.stopCh, cfg.reconnectDelay) {
			return local
		}
	}

	return local
}

func buildPendingRequest(spec opSpec, ds dataset, rng *xorShift64, nextInsertID *atomic.Int64) pendingRequest {
	switch spec.name {
	case opZAddInsert:
		id := nextInsertID.Add(1) - 1
		key, localID := ds.insertKeyMember(id)
		return pendingRequest{
			key:      key,
			memberID: localID,
			score:    strconv.FormatInt(localID+1, 10),
		}
	case opZAddUpdate:
		id := int64(rng.next() % uint64(ds.totalMembers))
		key, localID := ds.preloadKeyMember(id)
		return pendingRequest{
			key:      key,
			memberID: int64(localID),
			score:    updateScore(id, rng.next()),
		}
	default:
		if ds.workload == workloadSingle {
			return pendingRequest{key: ds.singleKey}
		}
		keyIdx := int(rng.next() % uint64(ds.keyCount))
		return pendingRequest{key: ds.keyForIndex(keyIdx)}
	}
}

func appendBenchCommand(buf *bytes.Buffer, spec opSpec, req pendingRequest) int {
	start := buf.Len()
	switch spec.name {
	case opZAddInsert:
		appendArray(buf, "ZADD", req.key, req.score, memberName(req.memberID))
	case opZAddUpdate:
		appendArray(buf, "ZADD", req.key, req.score, memberName(req.memberID))
	case opZRangeHead, opZRangeMid, opZRangeDeep:
		appendArray(buf, "ZRANGE", req.key, strconv.Itoa(spec.rangeStart), strconv.Itoa(spec.rangeStop))
	case opZRangeHeadWithScore:
		appendArray(buf, "ZRANGE", req.key, strconv.Itoa(spec.rangeStart), strconv.Itoa(spec.rangeStop), "WITHSCORES")
	}
	return buf.Len() - start
}

func validateBenchResponse(spec opSpec, req pendingRequest, resp respValue, verifyCorrectness bool) error {
	switch spec.name {
	case opZAddInsert, opZAddUpdate:
		if resp.kind != ':' {
			return fmt.Errorf("expected integer reply for %s, got kind=%q", spec.name, resp.kind)
		}
		if !verifyCorrectness {
			return nil
		}

		want := int64(1)
		if spec.name == opZAddUpdate {
			want = 0
		}
		if resp.intValue != want {
			return fmt.Errorf("unexpected ZADD result for key=%s member=%s: got=%d want=%d",
				req.key, memberName(req.memberID), resp.intValue, want)
		}
		return nil
	case opZRangeHead, opZRangeMid, opZRangeDeep:
		return validateRangeResponse(resp, spec.rangeStart, spec.returnedMembers, false, verifyCorrectness)
	case opZRangeHeadWithScore:
		return validateRangeResponse(resp, spec.rangeStart, spec.returnedMembers, true, verifyCorrectness)
	default:
		return fmt.Errorf("unsupported validation op %s", spec.name)
	}
}

func verifyPreloadedDataset(cfg config, ds dataset) error {
	sampleKeys := verificationSampleKeys(ds)
	fmt.Printf("Verifying dataset correctness: sample_keys=%d members_per_key=%d\n", len(sampleKeys), ds.membersPerKey)

	checks := []struct {
		start      int
		stop       int
		withScores bool
	}{
		{start: 0, stop: minInt(ds.membersPerKey, 10) - 1, withScores: false},
		{start: 0, stop: minInt(ds.membersPerKey, 10) - 1, withScores: true},
	}

	if ds.membersPerKey > 20 {
		midStart := ds.membersPerKey / 2
		midCount := minInt(10, ds.membersPerKey-midStart)
		checks = append(checks, struct {
			start      int
			stop       int
			withScores bool
		}{start: midStart, stop: midStart + midCount - 1, withScores: true})
	}

	if ds.membersPerKey > 10 {
		tailStart := ds.membersPerKey - minInt(ds.membersPerKey, 10)
		checks = append(checks, struct {
			start      int
			stop       int
			withScores bool
		}{start: tailStart, stop: ds.membersPerKey - 1, withScores: true})
	}

	for _, key := range sampleKeys {
		for _, check := range checks {
			args := []string{"ZRANGE", key, strconv.Itoa(check.start), strconv.Itoa(check.stop)}
			if check.withScores {
				args = append(args, "WITHSCORES")
			}

			resp, err := sendCommand(cfg.host, cfg.port, cfg.ioTimeout, args...)
			if err != nil {
				return fmt.Errorf("verify %s %d %d withscores=%t: %w", key, check.start, check.stop, check.withScores, err)
			}
			if err := validateRangeResponse(resp, check.start, check.stop-check.start+1, check.withScores, true); err != nil {
				return fmt.Errorf("verify %s %d %d withscores=%t: %w", key, check.start, check.stop, check.withScores, err)
			}
		}
	}

	fmt.Println("Correctness verification: OK")
	return nil
}

func verificationSampleKeys(ds dataset) []string {
	if ds.keyCount <= 1 {
		return []string{ds.keyForIndex(0)}
	}

	indexes := []int{0, ds.keyCount / 2, ds.keyCount - 1}
	keys := make([]string, 0, len(indexes))
	seen := make(map[int]struct{}, len(indexes))
	for _, idx := range indexes {
		if idx < 0 || idx >= ds.keyCount {
			continue
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		keys = append(keys, ds.keyForIndex(idx))
	}
	return keys
}

func validateRangeResponse(resp respValue, start, returnedMembers int, withScores, verifyCorrectness bool) error {
	if resp.kind != '*' {
		return fmt.Errorf("expected array reply, got kind=%q", resp.kind)
	}

	wantLen := returnedMembers
	if withScores {
		wantLen *= 2
	}
	if len(resp.array) != wantLen {
		return fmt.Errorf("unexpected array length: got=%d want=%d", len(resp.array), wantLen)
	}
	if !verifyCorrectness {
		return nil
	}

	for i := 0; i < returnedMembers; i++ {
		member := memberName(int64(start + i))
		if err := expectBulkString(resp.array[i*(1+boolToInt(withScores))], member, "member"); err != nil {
			return fmt.Errorf("range item %d: %w", i, err)
		}
		if withScores {
			score := loadScore(start + i)
			if err := expectBulkString(resp.array[i*2+1], score, "score"); err != nil {
				return fmt.Errorf("range item %d: %w", i, err)
			}
		}
	}
	return nil
}

func expectBulkString(value respValue, want, field string) error {
	if value.kind != '$' {
		return fmt.Errorf("expected bulk string for %s, got kind=%q", field, value.kind)
	}
	got := string(value.bulk)
	if got != want {
		return fmt.Errorf("unexpected %s: got=%q want=%q", field, got, want)
	}
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *workerStats) record(reqBytes, respBytes int, latency time.Duration, ok bool) {
	s.requests++
	s.bytesSent += int64(reqBytes)
	s.bytesRecv += int64(respBytes)
	s.latencyNsSum += latency.Nanoseconds()
	s.latencyHist.observe(latency)
	if !ok {
		s.errors++
	}
}

func (s *benchSharedStats) record(reqBytes, respBytes int, latency time.Duration, ok bool) {
	s.requests.Add(1)
	s.bytesSent.Add(int64(reqBytes))
	s.bytesRecv.Add(int64(respBytes))
	s.latencyNsSum.Add(latency.Nanoseconds())
	if !ok {
		s.errors.Add(1)
	}
}

func (s *benchSharedStats) snapshot() benchCounter {
	return benchCounter{
		requests:     s.requests.Load(),
		errors:       s.errors.Load(),
		bytesSent:    s.bytesSent.Load(),
		bytesRecv:    s.bytesRecv.Load(),
		latencyNsSum: s.latencyNsSum.Load(),
	}
}

func runBenchReporter(interval time.Duration, state *benchState, shared *benchSharedStats) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prev := benchCounter{}
	for {
		select {
		case <-state.stopCh:
			return
		case <-ticker.C:
			if !state.measuring.Load() {
				continue
			}
			cur := shared.snapshot()
			delta := benchCounter{
				requests:     cur.requests - prev.requests,
				errors:       cur.errors - prev.errors,
				bytesSent:    cur.bytesSent - prev.bytesSent,
				bytesRecv:    cur.bytesRecv - prev.bytesRecv,
				latencyNsSum: cur.latencyNsSum - prev.latencyNsSum,
			}
			prev = cur

			avgUs := 0.0
			if delta.requests > 0 {
				avgUs = float64(delta.latencyNsSum) / float64(delta.requests) / 1e3
			}
			fmt.Printf(
				"[%.1fs] qps=%.0f tx=%.2f MiB/s rx=%.2f MiB/s avg=%.2f us errors+=%d\n",
				time.Since(state.measureStart()).Seconds(),
				float64(delta.requests)/interval.Seconds(),
				float64(delta.bytesSent)/interval.Seconds()/(1024*1024),
				float64(delta.bytesRecv)/interval.Seconds()/(1024*1024),
				avgUs,
				delta.errors,
			)
		}
	}
}

func mergeWorkerStats(results []workerStats) workerStats {
	out := workerStats{latencyHist: newLatencyHistogram()}
	for i := range results {
		out.requests += results[i].requests
		out.errors += results[i].errors
		out.bytesSent += results[i].bytesSent
		out.bytesRecv += results[i].bytesRecv
		out.latencyNsSum += results[i].latencyNsSum
		out.latencyHist.merge(results[i].latencyHist)
	}
	return out
}

func sampleMemory(cfg config) (memorySample, error) {
	resp, err := sendCommand(cfg.host, cfg.port, cfg.ioTimeout, "INFO", "memory")
	if err != nil {
		return memorySample{}, err
	}
	if resp.kind != '$' {
		return memorySample{}, fmt.Errorf("expected bulk string from INFO memory, got kind=%q", resp.kind)
	}

	fields := parseInfoFields(resp.bulk)
	usedMemory, err := parseInfoInt(fields, "used_memory")
	if err != nil {
		return memorySample{}, err
	}
	usedMemoryRSS, err := parseInfoInt(fields, "used_memory_rss")
	if err != nil {
		return memorySample{}, err
	}
	usedMemoryPeak, _ := parseInfoInt(fields, "used_memory_peak")

	procRSS := int64(0)
	if cfg.pid > 0 {
		rss, err := readProcRSSBytes(cfg.pid)
		if err != nil {
			return memorySample{}, err
		}
		procRSS = rss
	}

	return memorySample{
		usedMemory:     usedMemory,
		infoRSS:        usedMemoryRSS,
		procRSS:        procRSS,
		usedMemoryPeak: usedMemoryPeak,
	}, nil
}

func sendCommand(host string, port int, ioTimeout time.Duration, parts ...string) (respValue, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", addr, ioTimeout)
	if err != nil {
		return respValue{}, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(ioTimeout)); err != nil {
		return respValue{}, err
	}

	var buf bytes.Buffer
	appendArray(&buf, parts...)
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return respValue{}, err
	}

	reader := bufio.NewReaderSize(conn, 256*1024)
	return readRESP(reader)
}

func parseInfoFields(raw []byte) map[string]string {
	fields := make(map[string]string)
	for _, line := range strings.Split(string(raw), "\r\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fields[parts[0]] = parts[1]
	}
	return fields
}

func parseInfoInt(fields map[string]string, key string) (int64, error) {
	raw, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("INFO memory missing field %q", key)
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", key, err)
	}
	return n, nil
}

func readProcRSSBytes(pid int) (int64, error) {
	statusPath := filepath.Join("/proc", strconv.Itoa(pid), "status")
	data, err := os.ReadFile(statusPath)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", statusPath, err)
	}

	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("unexpected VmRSS line: %q", line)
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse VmRSS: %w", err)
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("VmRSS not found in %s", statusPath)
}

func medianMemorySample(samples []memorySample) memorySample {
	used := make([]int64, 0, len(samples))
	infoRSS := make([]int64, 0, len(samples))
	procRSS := make([]int64, 0, len(samples))
	peak := make([]int64, 0, len(samples))
	for _, sample := range samples {
		used = append(used, sample.usedMemory)
		infoRSS = append(infoRSS, sample.infoRSS)
		procRSS = append(procRSS, sample.procRSS)
		peak = append(peak, sample.usedMemoryPeak)
	}
	return memorySample{
		usedMemory:     medianInt64(used),
		infoRSS:        medianInt64(infoRSS),
		procRSS:        medianInt64(procRSS),
		usedMemoryPeak: medianInt64(peak),
	}
}

func medianInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[len(values)/2]
}

func appendZAddBatch(buf *bytes.Buffer, key string, localStart int64, pairs int, scoreFn func(int) string) {
	args := make([]string, 0, 2+pairs*2)
	args = append(args, "ZADD", key)
	for i := 0; i < pairs; i++ {
		localIdx := int(localStart) + i
		args = append(args, scoreFn(localIdx), memberName(int64(localIdx)))
	}
	appendArray(buf, args...)
}

func appendArray(buf *bytes.Buffer, parts ...string) {
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(len(parts)))
	buf.WriteString("\r\n")
	for _, part := range parts {
		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(len(part)))
		buf.WriteString("\r\n")
		buf.WriteString(part)
		buf.WriteString("\r\n")
	}
}

func readRESP(reader *bufio.Reader) (respValue, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return respValue{}, err
	}

	switch prefix {
	case '+':
		line, bytesRead, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		return respValue{kind: '+', bulk: append([]byte(nil), line...), bulkLen: len(line), bytesRead: 1 + bytesRead}, nil
	case '-':
		line, _, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		return respValue{}, fmt.Errorf("server returned error: %s", string(line))
	case ':':
		line, bytesRead, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return respValue{}, err
		}
		return respValue{kind: ':', intValue: n, bytesRead: 1 + bytesRead}, nil
	case '$':
		line, bytesRead, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(string(line))
		if err != nil {
			return respValue{}, err
		}
		if n < 0 {
			return respValue{kind: '$', nilBulk: true, bytesRead: 1 + bytesRead}, nil
		}

		payload := make([]byte, n+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return respValue{}, err
		}
		if !bytes.HasSuffix(payload, []byte("\r\n")) {
			return respValue{}, errors.New("bulk string missing CRLF")
		}
		return respValue{
			kind:      '$',
			bulk:      append([]byte(nil), payload[:n]...),
			bulkLen:   n,
			bytesRead: 1 + bytesRead + len(payload),
		}, nil
	case '*':
		line, bytesRead, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(string(line))
		if err != nil {
			return respValue{}, err
		}
		if n < 0 {
			return respValue{kind: '*', bytesRead: 1 + bytesRead}, nil
		}

		values := make([]respValue, 0, n)
		totalBytes := 1 + bytesRead
		for i := 0; i < n; i++ {
			item, err := readRESP(reader)
			if err != nil {
				return respValue{}, err
			}
			totalBytes += item.bytesRead
			values = append(values, item)
		}
		return respValue{kind: '*', array: values, bytesRead: totalBytes}, nil
	default:
		return respValue{}, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func readLine(reader *bufio.Reader) ([]byte, int, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, 0, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, 0, errors.New("RESP line missing CRLF")
	}
	return line[:len(line)-2], len(line), nil
}

func newLatencyHistogram() latencyHistogram {
	var bounds []int64
	for v := int64(1); v <= 1_000; v++ {
		bounds = append(bounds, v)
	}
	for v := int64(1_010); v <= 10_000; v += 10 {
		bounds = append(bounds, v)
	}
	for v := int64(10_100); v <= 100_000; v += 100 {
		bounds = append(bounds, v)
	}
	for v := int64(101_000); v <= 1_000_000; v += 1_000 {
		bounds = append(bounds, v)
	}
	for v := int64(1_010_000); v <= 10_000_000; v += 10_000 {
		bounds = append(bounds, v)
	}
	return latencyHistogram{
		boundsUs: bounds,
		counts:   make([]uint64, len(bounds)+1),
	}
}

func (h *latencyHistogram) observe(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	idx := sort.Search(len(h.boundsUs), func(i int) bool {
		return h.boundsUs[i] >= us
	})
	h.counts[idx]++
	h.total++
}

func (h *latencyHistogram) merge(other latencyHistogram) {
	if len(h.counts) != len(other.counts) {
		return
	}
	for i := range h.counts {
		h.counts[i] += other.counts[i]
	}
	h.total += other.total
}

func (h latencyHistogram) quantile(q float64) float64 {
	if h.total == 0 {
		return 0
	}
	if q <= 0 {
		q = 0
	}
	if q >= 1 {
		q = 1
	}
	target := uint64(math.Ceil(q * float64(h.total)))
	if target == 0 {
		target = 1
	}
	var seen uint64
	for i, count := range h.counts {
		seen += count
		if seen < target {
			continue
		}
		if i >= len(h.boundsUs) {
			if len(h.boundsUs) == 0 {
				return 0
			}
			return float64(h.boundsUs[len(h.boundsUs)-1])
		}
		return float64(h.boundsUs[i])
	}
	if len(h.boundsUs) == 0 {
		return 0
	}
	return float64(h.boundsUs[len(h.boundsUs)-1])
}

type xorShift64 struct {
	state uint64
}

func newXorShift64(seed uint64) xorShift64 {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return xorShift64{state: seed}
}

func (r *xorShift64) next() uint64 {
	x := r.state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.state = x
	return x
}

func printMemorySummary(cfg config, ds dataset, result memoryRunResult) {
	fmt.Println("\n=== Memory Summary ===")
	fmt.Printf("server                  : %s\n", cfg.server)
	fmt.Printf("workload                : %s\n", cfg.workload)
	fmt.Printf("key_count               : %d\n", ds.keyCount)
	fmt.Printf("members_per_key         : %d\n", ds.membersPerKey)
	fmt.Printf("total_members           : %d\n", ds.totalMembers)
	fmt.Printf("used_memory_baseline    : %d\n", result.baseline.usedMemory)
	fmt.Printf("used_memory_after       : %d\n", result.after.usedMemory)
	fmt.Printf("used_memory_delta       : %d\n", result.delta)
	fmt.Printf("bytes_per_member        : %.6f\n", result.bytesPerMember)
	fmt.Printf("used_memory_rss_base    : %d\n", result.baseline.infoRSS)
	fmt.Printf("used_memory_rss_after   : %d\n", result.after.infoRSS)
	if cfg.pid > 0 {
		fmt.Printf("proc_rss_baseline       : %d\n", result.baseline.procRSS)
		fmt.Printf("proc_rss_after          : %d\n", result.after.procRSS)
	}
	fmt.Printf("used_memory_peak_after  : %d\n", result.after.usedMemoryPeak)
}

func printBenchSummary(cfg config, ds dataset, spec opSpec, result benchResult) {
	fmt.Println("\n=== Benchmark Summary ===")
	fmt.Printf("server                  : %s\n", cfg.server)
	fmt.Printf("workload                : %s\n", cfg.workload)
	fmt.Printf("op                      : %s\n", spec.name)
	fmt.Printf("clients                 : %d\n", cfg.clients)
	fmt.Printf("pipeline                : %d\n", cfg.pipeline)
	fmt.Printf("key_count               : %d\n", ds.keyCount)
	fmt.Printf("members_per_key         : %d\n", ds.membersPerKey)
	fmt.Printf("total_members           : %d\n", ds.totalMembers)
	fmt.Printf("returned_members        : %d\n", spec.returnedMembersOrOne())
	fmt.Printf("total_requests          : %d\n", result.requests)
	fmt.Printf("total_errors            : %d\n", result.errors)
	fmt.Printf("ops_per_sec             : %.6f\n", result.opsPerSec)
	fmt.Printf("elements_per_sec        : %.6f\n", result.elementsPerSec)
	fmt.Printf("avg_us                  : %.6f\n", result.avgLatencyUs)
	fmt.Printf("p50_us                  : %.6f\n", result.p50LatencyUs)
	fmt.Printf("p95_us                  : %.6f\n", result.p95LatencyUs)
	fmt.Printf("p99_us                  : %.6f\n", result.p99LatencyUs)
	fmt.Printf("tx_mib_per_sec          : %.6f\n", result.opsPerSec*float64(result.bytesSent)/math.Max(float64(result.requests), 1)/(1024*1024))
	fmt.Printf("rx_mib_per_sec          : %.6f\n", result.opsPerSec*float64(result.bytesRecv)/math.Max(float64(result.requests), 1)/(1024*1024))
}

func appendCSV(path string, header []string, row []string) error {
	if len(header) != len(row) {
		return fmt.Errorf("csv header/row length mismatch: %d vs %d", len(header), len(row))
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return err
	}

	needHeader := false
	if info, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		needHeader = true
	} else if info.Size() == 0 {
		needHeader = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if needHeader {
		if _, err := f.WriteString(strings.Join(header, ",") + "\n"); err != nil {
			return err
		}
	}
	if _, err := f.WriteString(strings.Join(escapeCSVRow(row), ",") + "\n"); err != nil {
		return err
	}
	return nil
}

func escapeCSVRow(row []string) []string {
	out := make([]string, len(row))
	for i, cell := range row {
		if strings.ContainsAny(cell, ",\"\n") {
			out[i] = `"` + strings.ReplaceAll(cell, `"`, `""`) + `"`
		} else {
			out[i] = cell
		}
	}
	return out
}

func sleepOrStop(stopCh <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-stopCh:
			return true
		default:
			return false
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-stopCh:
		return true
	case <-timer.C:
		return false
	}
}

func minInt(values ...int) int {
	out := values[0]
	for _, v := range values[1:] {
		if v < out {
			out = v
		}
	}
	return out
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
