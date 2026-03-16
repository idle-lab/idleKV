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
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type opKind int

const (
	opSet opKind = iota
	opGet
	opDel
	opCount
)

var opNames = [...]string{
	opSet: "SET",
	opGet: "GET",
	opDel: "DEL",
}

type config struct {
	host           string
	port           int
	clients        int
	pipeline       int
	duration       time.Duration
	warmup         time.Duration
	reportInterval time.Duration
	keyspace       int
	valueSize      int
	setRatio       int
	getRatio       int
	delRatio       int
	ioTimeout      time.Duration
	reconnectDelay time.Duration
	keyPrefix      string
	errorLogLimit  int
	seed           uint64
}

type opChooser struct {
	cumulative [opCount]int
	total      int
}

type workerRNG struct {
	state uint64
}

type opSnapshot struct {
	requests     int64
	errors       int64
	bytesSent    int64
	bytesRecv    int64
	latencyNsSum int64
}

type opStats struct {
	requests     atomic.Int64
	errors       atomic.Int64
	bytesSent    atomic.Int64
	bytesRecv    atomic.Int64
	latencyNsSum atomic.Int64
}

type benchStats struct {
	ops [opCount]opStats
}

type benchState struct {
	stats statsWrapper

	measuring atomic.Bool
	stopCh    chan struct{}
	stopOnce  sync.Once

	startMu sync.RWMutex
	start   time.Time
	end     time.Time

	errorLogLimit int64
	errorLogCount atomic.Int64
	errorLogMu    sync.Mutex
}

type statsWrapper struct {
	bench *benchStats
}

type reportSnapshot struct {
	total opSnapshot
	ops   [opCount]opSnapshot
}

type pendingOp struct {
	kind       opKind
	reqBytes   int
	keyIndex   int
	expectBulk bool
}

type respValue struct {
	kind      byte
	intValue  int64
	bulkLen   int
	nilBulk   bool
	bytesRead int
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "flag error: %v\n", err)
		os.Exit(2)
	}

	keys := buildKeys(cfg.keyPrefix, cfg.keyspace)
	value := bytes.Repeat([]byte("x"), cfg.valueSize)
	chooser := newOpChooser(cfg.setRatio, cfg.getRatio, cfg.delRatio)

	state := newBenchState()
	state.setErrorLogLimit(cfg.errorLogLimit)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\ninterrupt received, stopping benchmark...")
		state.stop()
	}()

	var wg sync.WaitGroup
	for i := 0; i < cfg.clients; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(workerID, cfg, keys, value, chooser, state)
		}(i)
	}

	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		runReporter(cfg.reportInterval, state)
	}()

	if cfg.warmup > 0 {
		fmt.Printf("Warmup: %s\n", cfg.warmup)
		if sleepOrStop(state, cfg.warmup) {
			state.setMeasureStart(time.Now())
			state.setMeasureEnd(time.Now())
			wg.Wait()
			<-reportDone
			printSummary(state.snapshot(), 0)
			return
		}
	}

	state.stats.reset()
	state.measuring.Store(true)
	state.setMeasureStart(time.Now())
	fmt.Printf(
		"Benchmarking: duration=%s clients=%d pipeline=%d keyspace=%d value_size=%d ratios(set/get/del)=%d/%d/%d\n",
		cfg.duration, cfg.clients, cfg.pipeline, cfg.keyspace, cfg.valueSize,
		cfg.setRatio, cfg.getRatio, cfg.delRatio,
	)

	if !sleepOrStop(state, cfg.duration) {
		state.stop()
	}

	state.measuring.Store(false)
	state.setMeasureEnd(time.Now())
	wg.Wait()
	<-reportDone

	elapsed := state.measureEnd().Sub(state.measureStart())
	printSummary(state.snapshot(), elapsed)
}

func parseFlags() (config, error) {
	var cfg config

	flag.StringVar(&cfg.host, "host", "127.0.0.1", "target Redis host")
	flag.IntVar(&cfg.port, "port", 6379, "target Redis port")
	flag.IntVar(&cfg.clients, "clients", 32, "number of concurrent TCP clients")
	flag.IntVar(&cfg.pipeline, "pipeline", 1, "pipeline depth per client")
	flag.IntVar(&cfg.keyspace, "keyspace", 10000, "number of hot keys to cycle through")
	flag.IntVar(&cfg.valueSize, "value-size", 1024, "SET payload size in bytes")
	flag.IntVar(&cfg.setRatio, "set-ratio", 40, "relative weight for SET requests")
	flag.IntVar(&cfg.getRatio, "get-ratio", 50, "relative weight for GET requests")
	flag.IntVar(&cfg.delRatio, "del-ratio", 10, "relative weight for DEL requests")
	flag.StringVar(&cfg.keyPrefix, "key-prefix", "bench:key:", "prefix for generated keys")
	flag.IntVar(&cfg.errorLogLimit, "error-log-limit", 16, "maximum number of worker errors to print")

	duration := flag.Duration("duration", 10*time.Second, "measurement duration")
	warmup := flag.Duration("warmup", 3*time.Second, "warmup duration before measuring")
	report := flag.Duration("report-interval", 1*time.Second, "progress report interval")
	ioTimeout := flag.Duration("io-timeout", 3*time.Second, "per-batch read/write timeout")
	reconnect := flag.Duration("reconnect-delay", 200*time.Millisecond, "delay before reconnect after error")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "base random seed")

	flag.Parse()

	cfg.duration = *duration
	cfg.warmup = *warmup
	cfg.reportInterval = *report
	cfg.ioTimeout = *ioTimeout
	cfg.reconnectDelay = *reconnect
	cfg.seed = *seed

	if cfg.clients <= 0 {
		return cfg, errors.New("--clients must be > 0")
	}
	if cfg.pipeline <= 0 {
		return cfg, errors.New("--pipeline must be > 0")
	}
	if cfg.keyspace <= 0 {
		return cfg, errors.New("--keyspace must be > 0")
	}
	if cfg.valueSize < 0 {
		return cfg, errors.New("--value-size must be >= 0")
	}
	if cfg.duration <= 0 {
		return cfg, errors.New("--duration must be > 0")
	}
	if cfg.reportInterval <= 0 {
		return cfg, errors.New("--report-interval must be > 0")
	}
	if cfg.ioTimeout <= 0 {
		return cfg, errors.New("--io-timeout must be > 0")
	}
	if cfg.setRatio < 0 || cfg.getRatio < 0 || cfg.delRatio < 0 {
		return cfg, errors.New("ratios must be >= 0")
	}
	if cfg.setRatio+cfg.getRatio+cfg.delRatio == 0 {
		return cfg, errors.New("at least one of set/get/del ratio must be > 0")
	}

	return cfg, nil
}

func buildKeys(prefix string, keyspace int) []string {
	keys := make([]string, keyspace)
	for i := 0; i < keyspace; i++ {
		keys[i] = prefix + strconv.Itoa(i)
	}
	return keys
}

func newOpChooser(setRatio, getRatio, delRatio int) opChooser {
	var chooser opChooser
	chooser.cumulative[opSet] = setRatio
	chooser.cumulative[opGet] = chooser.cumulative[opSet] + getRatio
	chooser.cumulative[opDel] = chooser.cumulative[opGet] + delRatio
	chooser.total = chooser.cumulative[opDel]
	return chooser
}

func (c opChooser) pick(r uint64) opKind {
	if c.total == 0 {
		return opGet
	}

	v := int(r % uint64(c.total))
	switch {
	case v < c.cumulative[opSet]:
		return opSet
	case v < c.cumulative[opGet]:
		return opGet
	default:
		return opDel
	}
}

func newWorkerRNG(seed uint64) workerRNG {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return workerRNG{state: seed}
}

func (r *workerRNG) next() uint64 {
	x := r.state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.state = x
	return x
}

func (r *workerRNG) keyIndex(keyspace int) int {
	return int(r.next() % uint64(keyspace))
}

func newBenchState() *benchState {
	stats := &benchStats{}
	return &benchState{
		stats:  statsWrapper{bench: stats},
		stopCh: make(chan struct{}),
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

func (s *benchState) setErrorLogLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	s.errorLogLimit = int64(limit)
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

	fmt.Fprintf(os.Stderr, "[worker-error #%d] worker=%d stage=%s err=%v\n", seq, workerID, stage, err)
	if seq == s.errorLogLimit {
		fmt.Fprintf(os.Stderr, "[worker-error] reached --error-log-limit=%d, suppressing further logs\n", s.errorLogLimit)
	}
}

func (s *benchState) snapshot() reportSnapshot {
	var snap reportSnapshot
	for i := opKind(0); i < opCount; i++ {
		opSnap := s.stats.bench.ops[i].snapshot()
		snap.ops[i] = opSnap
		snap.total.requests += opSnap.requests
		snap.total.errors += opSnap.errors
		snap.total.bytesSent += opSnap.bytesSent
		snap.total.bytesRecv += opSnap.bytesRecv
		snap.total.latencyNsSum += opSnap.latencyNsSum
	}
	return snap
}

func (s *opStats) snapshot() opSnapshot {
	return opSnapshot{
		requests:     s.requests.Load(),
		errors:       s.errors.Load(),
		bytesSent:    s.bytesSent.Load(),
		bytesRecv:    s.bytesRecv.Load(),
		latencyNsSum: s.latencyNsSum.Load(),
	}
}

func (s statsWrapper) reset() {
	for i := opKind(0); i < opCount; i++ {
		s.bench.ops[i].requests.Store(0)
		s.bench.ops[i].errors.Store(0)
		s.bench.ops[i].bytesSent.Store(0)
		s.bench.ops[i].bytesRecv.Store(0)
		s.bench.ops[i].latencyNsSum.Store(0)
	}
}

func (s statsWrapper) record(kind opKind, reqBytes, respBytes int, latency time.Duration, ok bool) {
	op := &s.bench.ops[kind]
	op.requests.Add(1)
	op.bytesSent.Add(int64(reqBytes))
	op.bytesRecv.Add(int64(respBytes))
	op.latencyNsSum.Add(latency.Nanoseconds())
	if !ok {
		op.errors.Add(1)
	}
}

func runWorker(workerID int, cfg config, keys []string, value []byte, chooser opChooser, state *benchState) {
	rng := newWorkerRNG(cfg.seed + uint64(workerID+1)*0x9e3779b97f4a7c15)
	addr := fmt.Sprintf("%s:%d", cfg.host, cfg.port)

	batch := bytes.NewBuffer(make([]byte, 0, cfg.pipeline*(cfg.valueSize+128)))
	pending := make([]pendingOp, 0, cfg.pipeline)

	for !state.stopped() {
		conn, err := net.DialTimeout("tcp", addr, cfg.ioTimeout)
		if err != nil {
			if state.measuring.Load() {
				state.logWorkerError(workerID, "dial", err)
			}
			if sleepOrStop(state, cfg.reconnectDelay) {
				return
			}
			continue
		}

		reader := bufio.NewReaderSize(conn, 64*1024)
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
		}

		for !state.stopped() {
			batch.Reset()
			pending = pending[:0]

			for i := 0; i < cfg.pipeline; i++ {
				keyIndex := rng.keyIndex(len(keys))
				kind := chooser.pick(rng.next())
				reqBytes := appendCommand(batch, kind, keys[keyIndex], value)
				pending = append(pending, pendingOp{
					kind:     kind,
					reqBytes: reqBytes,
					keyIndex: keyIndex,
				})
			}

			_ = conn.SetDeadline(time.Now().Add(cfg.ioTimeout))
			start := time.Now()
			if _, err := conn.Write(batch.Bytes()); err != nil {
				if state.measuring.Load() {
					state.logWorkerError(workerID, "write", err)
				}
				break
			}

			for i := range pending {
				resp, err := readRESP(reader)
				if err != nil {
					if state.measuring.Load() {
						state.logWorkerError(workerID, "read", err)
					}
					goto reconnect
				}

				ok := validateResponse(pending[i].kind, resp)
				if !ok && state.measuring.Load() {
					state.logWorkerError(
						workerID,
						"verify",
						fmt.Errorf("unexpected %s reply kind=%q int=%d bulk_len=%d nil=%v",
							opNames[pending[i].kind], resp.kind, resp.intValue, resp.bulkLen, resp.nilBulk),
					)
				}

				if state.measuring.Load() {
					elapsedPerOp := time.Since(start)
					if len(pending) > 0 {
						elapsedPerOp /= time.Duration(len(pending))
					}
					state.stats.record(
						pending[i].kind,
						pending[i].reqBytes,
						resp.bytesRead,
						elapsedPerOp,
						ok,
					)
				}
			}
		}

	reconnect:
		_ = conn.Close()
		if state.stopped() {
			return
		}
		if sleepOrStop(state, cfg.reconnectDelay) {
			return
		}
	}
}

func appendCommand(buf *bytes.Buffer, kind opKind, key string, value []byte) int {
	start := buf.Len()

	switch kind {
	case opSet:
		buf.WriteString("*3\r\n$3\r\nSET\r\n$")
		buf.WriteString(strconv.Itoa(len(key)))
		buf.WriteString("\r\n")
		buf.WriteString(key)
		buf.WriteString("\r\n$")
		buf.WriteString(strconv.Itoa(len(value)))
		buf.WriteString("\r\n")
		buf.Write(value)
		buf.WriteString("\r\n")
	case opGet:
		buf.WriteString("*2\r\n$3\r\nGET\r\n$")
		buf.WriteString(strconv.Itoa(len(key)))
		buf.WriteString("\r\n")
		buf.WriteString(key)
		buf.WriteString("\r\n")
	case opDel:
		buf.WriteString("*2\r\n$3\r\nDEL\r\n$")
		buf.WriteString(strconv.Itoa(len(key)))
		buf.WriteString("\r\n")
		buf.WriteString(key)
		buf.WriteString("\r\n")
	}

	return buf.Len() - start
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
		return respValue{kind: '+', bytesRead: 1 + bytesRead, bulkLen: len(line)}, nil
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
			bulkLen:   n,
			bytesRead: 1 + bytesRead + len(payload),
		}, nil
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

func validateResponse(kind opKind, resp respValue) bool {
	switch kind {
	case opSet:
		return resp.kind == '+'
	case opGet:
		return resp.kind == '$'
	case opDel:
		return resp.kind == ':'
	default:
		return false
	}
}

func runReporter(interval time.Duration, state *benchState) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prev := reportSnapshot{}
	for {
		select {
		case <-state.stopCh:
			return
		case <-ticker.C:
			if !state.measuring.Load() {
				continue
			}

			cur := state.snapshot()
			start := state.measureStart()
			elapsed := time.Since(start)
			printWindow(cur, prev, interval, elapsed)
			prev = cur
		}
	}
}

func printWindow(cur, prev reportSnapshot, window time.Duration, elapsed time.Duration) {
	dt := window.Seconds()
	if dt <= 0 {
		return
	}

	totalDelta := diffSnapshot(cur.total, prev.total)
	fmt.Printf(
		"[%7.2fs] total_qps=%10.0f tx=%8.2f MiB/s rx=%8.2f MiB/s errors+=%d\n",
		elapsed.Seconds(),
		float64(totalDelta.requests)/dt,
		float64(totalDelta.bytesSent)/dt/(1024*1024),
		float64(totalDelta.bytesRecv)/dt/(1024*1024),
		totalDelta.errors,
	)

	for i := opKind(0); i < opCount; i++ {
		delta := diffSnapshot(cur.ops[i], prev.ops[i])
		if delta.requests == 0 && delta.errors == 0 {
			continue
		}
		avgUs := 0.0
		if delta.requests > 0 {
			avgUs = float64(delta.latencyNsSum) / float64(delta.requests) / 1e3
		}
		fmt.Printf(
			"           %-3s qps=%10.0f avg_lat=%8.2f us errors+=%d\n",
			opNames[i],
			float64(delta.requests)/dt,
			avgUs,
			delta.errors,
		)
	}
}

func diffSnapshot(cur, prev opSnapshot) opSnapshot {
	return opSnapshot{
		requests:     cur.requests - prev.requests,
		errors:       cur.errors - prev.errors,
		bytesSent:    cur.bytesSent - prev.bytesSent,
		bytesRecv:    cur.bytesRecv - prev.bytesRecv,
		latencyNsSum: cur.latencyNsSum - prev.latencyNsSum,
	}
}

func printSummary(snap reportSnapshot, elapsed time.Duration) {
	secs := math.Max(elapsed.Seconds(), 1e-9)
	fmt.Println("\n=== Summary ===")
	fmt.Printf("duration_sec           : %.3f\n", secs)
	fmt.Printf("total_requests         : %d\n", snap.total.requests)
	fmt.Printf("total_errors           : %d\n", snap.total.errors)
	fmt.Printf("throughput_req_per_sec : %.0f\n", float64(snap.total.requests)/secs)
	fmt.Printf("tx_mib_per_sec         : %.2f\n", float64(snap.total.bytesSent)/secs/(1024*1024))
	fmt.Printf("rx_mib_per_sec         : %.2f\n", float64(snap.total.bytesRecv)/secs/(1024*1024))

	totalAvgUs := 0.0
	if snap.total.requests > 0 {
		totalAvgUs = float64(snap.total.latencyNsSum) / float64(snap.total.requests) / 1e3
	}
	fmt.Printf("avg_req_latency_us     : %.2f\n", totalAvgUs)

	fmt.Println("\n=== Per Operation ===")
	for i := opKind(0); i < opCount; i++ {
		opSnap := snap.ops[i]
		avgUs := 0.0
		if opSnap.requests > 0 {
			avgUs = float64(opSnap.latencyNsSum) / float64(opSnap.requests) / 1e3
		}
		fmt.Printf(
			"%-3s requests=%-10d qps=%-10.0f avg_lat_us=%-10.2f errors=%d\n",
			opNames[i],
			opSnap.requests,
			float64(opSnap.requests)/secs,
			avgUs,
			opSnap.errors,
		)
	}
}

func sleepOrStop(state *benchState, d time.Duration) bool {
	if d <= 0 {
		return state.stopped()
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-state.stopCh:
		return true
	case <-timer.C:
		return false
	}
}
