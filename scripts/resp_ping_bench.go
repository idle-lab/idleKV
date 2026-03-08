package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type optionalString struct {
	value string
	set   bool
}

func (o *optionalString) String() string {
	return o.value
}

func (o *optionalString) Set(v string) error {
	o.value = v
	o.set = true
	return nil
}

type args struct {
	host           string
	port           int
	clients        int
	pipeline       int
	duration       float64
	warmup         float64
	interval       float64
	reportWindow   float64
	message        optionalString
	ioTimeout      float64
	reconnectDelay float64
	errorLogLimit  int
	noVerify       bool
}

type statsSnapshot struct {
	requests     int64
	errors       int64
	bytesSent    int64
	bytesRecv    int64
	latencyNsSum int64
}

type stats struct {
	requests     atomic.Int64
	errors       atomic.Int64
	bytesSent    atomic.Int64
	bytesRecv    atomic.Int64
	latencyNsSum atomic.Int64
}

func (s *stats) reset() {
	s.requests.Store(0)
	s.errors.Store(0)
	s.bytesSent.Store(0)
	s.bytesRecv.Store(0)
	s.latencyNsSum.Store(0)
}

func (s *stats) snapshot() statsSnapshot {
	return statsSnapshot{
		requests:     s.requests.Load(),
		errors:       s.errors.Load(),
		bytesSent:    s.bytesSent.Load(),
		bytesRecv:    s.bytesRecv.Load(),
		latencyNsSum: s.latencyNsSum.Load(),
	}
}

type benchmarkState struct {
	stats     stats
	measuring atomic.Bool
	stopCh    chan struct{}
	stopOnce  sync.Once
	mu        sync.RWMutex
	start     time.Time
	end       time.Time

	errorLogLimit int64
	errorLogCount atomic.Int64
	errorLogMu    sync.Mutex
}

func newBenchmarkState() *benchmarkState {
	return &benchmarkState{
		stopCh: make(chan struct{}),
	}
}

func (s *benchmarkState) setErrorLogLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	s.errorLogLimit = int64(limit)
}

func (s *benchmarkState) reportWorkerError(workerID int, stage string, err error) {
	if err == nil {
		return
	}

	limit := s.errorLogLimit
	if limit <= 0 {
		return
	}

	seq := s.errorLogCount.Add(1)
	if seq > limit {
		return
	}

	s.errorLogMu.Lock()
	defer s.errorLogMu.Unlock()

	fmt.Fprintf(os.Stderr, "[bench-error #%d] worker=%d stage=%s err=%v\n", seq, workerID, stage, err)
	if seq == limit {
		fmt.Fprintf(os.Stderr, "[bench-error] reached --error-log-limit=%d, suppressing further logs\n", limit)
	}
}

func (s *benchmarkState) stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *benchmarkState) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

func (s *benchmarkState) isMeasuring() bool {
	return s.measuring.Load()
}

func (s *benchmarkState) setMeasureStart(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.start = t
}

func (s *benchmarkState) setMeasureEnd(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.end = t
}

func (s *benchmarkState) measureStart() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.start
}

func (s *benchmarkState) measureEnd() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.end
}

func buildPingRequest(message *string) ([]byte, []byte) {
	if message == nil {
		return []byte("*1\r\n$4\r\nPING\r\n"), []byte("+PONG\r\n")
	}

	payload := []byte(*message)
	req := make([]byte, 0, 32+len(payload))
	req = append(req, []byte("*2\r\n$4\r\nPING\r\n$")...)
	req = append(req, []byte(fmt.Sprintf("%d", len(payload)))...)
	req = append(req, []byte("\r\n")...)
	req = append(req, payload...)
	req = append(req, []byte("\r\n")...)

	expected := make([]byte, 0, len(payload)+3)
	expected = append(expected, '+')
	expected = append(expected, payload...)
	expected = append(expected, []byte("\r\n")...)
	return req, expected
}

func sendOneRoundtrip(host string, port int, ioTimeout time.Duration, request []byte) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", addr, ioTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(ioTimeout)); err != nil {
		return nil, err
	}
	if _, err := conn.Write(request); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return line, nil
}

func probePingCommand(host string, port int, ioTimeout time.Duration) ([]byte, []byte, string, error) {
	plainReq, plainExpected := buildPingRequest(nil)
	line, err := sendOneRoundtrip(host, port, ioTimeout, plainReq)
	if err != nil {
		return nil, nil, "", err
	}
	if bytes.Equal(line, plainExpected) {
		return plainReq, plainExpected, "PING", nil
	}

	if bytes.HasPrefix(line, []byte("-ERR wrong number of arguments")) {
		msg := "__idlekv_bench__"
		msgReq, msgExpected := buildPingRequest(&msg)
		line, err = sendOneRoundtrip(host, port, ioTimeout, msgReq)
		if err != nil {
			return nil, nil, "", err
		}
		if bytes.Equal(line, msgExpected) {
			return msgReq, msgExpected, "PING <message> (auto-probed)", nil
		}
	}

	return nil, nil, "", fmt.Errorf("probe failed, unexpected reply: %q", string(line))
}

func sleepOrStop(state *benchmarkState, d time.Duration) bool {
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

func worker(
	idx int,
	state *benchmarkState,
	host string,
	port int,
	request []byte,
	expectedReply []byte,
	pipeline int,
	verify bool,
	ioTimeout time.Duration,
	reconnectDelay time.Duration,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	addr := fmt.Sprintf("%s:%d", host, port)
	batchReq := bytes.Repeat(request, pipeline)
	reqSent := int64(len(request))

	for !state.stopped() {
		conn, err := net.DialTimeout("tcp", addr, ioTimeout)
		if err != nil {
			state.reportWorkerError(idx, "dial", err)
			if state.isMeasuring() {
				state.stats.errors.Add(1)
			}
			if sleepOrStop(state, reconnectDelay) {
				return
			}
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
		}
		reader := bufio.NewReader(conn)
		runErr := runWorkerConn(idx, state, conn, reader, batchReq, reqSent, expectedReply, pipeline, verify, ioTimeout)
		_ = conn.Close()

		if runErr == nil || state.stopped() {
			continue
		}

		state.reportWorkerError(idx, "run", runErr)
		if state.isMeasuring() {
			state.stats.errors.Add(1)
		}
		if sleepOrStop(state, reconnectDelay) {
			return
		}
	}
}

func runWorkerConn(
	idx int,
	state *benchmarkState,
	conn net.Conn,
	reader *bufio.Reader,
	batchReq []byte,
	reqSent int64,
	expectedReply []byte,
	pipeline int,
	verify bool,
	ioTimeout time.Duration,
) error {
	for !state.stopped() {
		start := time.Now()

		if err := conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
			return err
		}
		if _, err := conn.Write(batchReq); err != nil {
			return err
		}

		for i := 0; i < pipeline; i++ {
			if err := conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
				return err
			}
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return err
			}
			if verify && !bytes.Equal(line, expectedReply) {
				return fmt.Errorf("worker-%d: unexpected reply: %q, expect %q",
					idx, strings.TrimSpace(string(line)), strings.TrimSpace(string(expectedReply)))
			}

			if state.isMeasuring() {
				elapsed := time.Since(start).Nanoseconds()
				state.stats.requests.Add(1)
				state.stats.bytesSent.Add(reqSent)
				state.stats.bytesRecv.Add(int64(len(line)))
				state.stats.latencyNsSum.Add(elapsed)
			}
		}
	}
	return nil
}

type progressPoint struct {
	ts    time.Time
	stats statsSnapshot
}

func printProgress(base statsSnapshot, cur statsSnapshot, dt float64, elapsed float64, reportWindow float64) {
	if dt <= 0 {
		return
	}

	reqDelta := cur.requests - base.requests
	sentDelta := cur.bytesSent - base.bytesSent
	recvDelta := cur.bytesRecv - base.bytesRecv
	errDelta := cur.errors - base.errors

	qps := float64(reqDelta) / dt
	txMiB := float64(sentDelta) / dt / (1024 * 1024)
	rxMiB := float64(recvDelta) / dt / (1024 * 1024)

	fmt.Printf("[%7.2fs] qps(%.1fs)=%10.0f  tx=%8.2f MiB/s  rx=%8.2f MiB/s  errors+=%d\n",
		elapsed, reportWindow, qps, txMiB, rxMiB, errDelta)
}

func reporter(state *benchmarkState, interval time.Duration, reportWindow time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	for !state.isMeasuring() && !state.stopped() {
		time.Sleep(50 * time.Millisecond)
	}
	if state.stopped() {
		return
	}

	start := state.measureStart()
	history := []progressPoint{
		{ts: start, stats: statsSnapshot{}},
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-state.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			cur := state.stats.snapshot()
			history = append(history, progressPoint{ts: now, stats: cur})

			cutoff := now.Add(-reportWindow)
			for len(history) >= 2 && !history[1].ts.After(cutoff) {
				history = history[1:]
			}

			base := history[0]
			windowDt := now.Sub(base.ts).Seconds()
			elapsed := now.Sub(start).Seconds()
			window := reportWindow.Seconds()
			if elapsed < window {
				window = elapsed
			}
			printProgress(base.stats, cur, windowDt, elapsed, window)
		}
	}
}

func summarize(state *benchmarkState) {
	total := state.stats.snapshot()
	elapsed := state.measureEnd().Sub(state.measureStart()).Seconds()
	if elapsed <= 0 {
		elapsed = 1e-9
	}

	qps := float64(total.requests) / elapsed
	txMiB := float64(total.bytesSent) / elapsed / (1024 * 1024)
	rxMiB := float64(total.bytesRecv) / elapsed / (1024 * 1024)

	avgUs := 0.0
	if total.requests > 0 {
		avgUs = (float64(total.latencyNsSum) / float64(total.requests)) / 1e3
	}

	fmt.Println("\n=== Summary ===")
	fmt.Printf("duration_sec           : %.3f\n", elapsed)
	fmt.Printf("total_requests         : %d\n", total.requests)
	fmt.Printf("total_errors           : %d\n", total.errors)
	fmt.Printf("throughput_req_per_sec : %.0f\n", qps)
	fmt.Printf("tx_mib_per_sec         : %.2f\n", txMiB)
	fmt.Printf("rx_mib_per_sec         : %.2f\n", rxMiB)
	fmt.Printf("avg_req_latency_us     : %.2f\n", avgUs)
}

func parseArgs() (*args, error) {
	a := &args{}
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	fs.StringVar(&a.host, "host", "127.0.0.1", "Target host.")
	fs.IntVar(&a.port, "port", 4396, "Target port.")
	fs.IntVar(&a.clients, "clients", 64, "Number of concurrent TCP clients.")
	fs.IntVar(&a.clients, "c", 64, "Number of concurrent TCP clients.")
	fs.IntVar(&a.pipeline, "pipeline", 32, "Number of in-flight PING per round trip per client.")
	fs.IntVar(&a.pipeline, "P", 64, "Number of in-flight PING per round trip per client.")
	fs.Float64Var(&a.duration, "duration", 10.0, "Benchmark duration in seconds.")
	fs.Float64Var(&a.duration, "d", 20.0, "Benchmark duration in seconds.")
	fs.Float64Var(&a.warmup, "warmup", 0.0, "Warmup time in seconds.")
	fs.Float64Var(&a.warmup, "w", 0.0, "Warmup time in seconds.")

	fs.Float64Var(&a.interval, "interval", 1.0, "Progress report interval in seconds.")
	fs.Float64Var(&a.reportWindow, "report-window", 5.0, "Rolling window in seconds for progress metrics.")
	fs.Var(&a.message, "message", "Optional payload for `PING <message>`.")
	fs.Float64Var(&a.ioTimeout, "io-timeout", 5.0, "Socket read/write timeout in seconds.")
	fs.Float64Var(&a.reconnectDelay, "reconnect-delay", 0.05, "Delay before reconnect after I/O error in seconds.")
	fs.IntVar(&a.errorLogLimit, "error-log-limit", 20, "Print at most N worker error logs (0 disables).")
	fs.BoolVar(&a.noVerify, "no-verify", false, "Do not verify server reply payload.")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	if a.clients <= 0 {
		return nil, errors.New("--clients must be > 0")
	}
	if a.pipeline <= 0 {
		return nil, errors.New("--pipeline must be > 0")
	}
	if a.duration <= 0 {
		return nil, errors.New("--duration must be > 0")
	}
	if a.warmup < 0 {
		return nil, errors.New("--warmup must be >= 0")
	}
	if a.interval <= 0 {
		return nil, errors.New("--interval must be > 0")
	}
	if a.reportWindow <= 0 {
		return nil, errors.New("--report-window must be > 0")
	}
	if a.ioTimeout <= 0 {
		return nil, errors.New("--io-timeout must be > 0")
	}
	if a.reconnectDelay < 0 {
		return nil, errors.New("--reconnect-delay must be >= 0")
	}
	if a.errorLogLimit < 0 {
		return nil, errors.New("--error-log-limit must be >= 0")
	}

	return a, nil
}

func run(a *args) error {
	ioTimeout := time.Duration(a.ioTimeout * float64(time.Second))
	reconnectDelay := time.Duration(a.reconnectDelay * float64(time.Second))

	var (
		request       []byte
		expectedReply []byte
		commandText   string
		err           error
	)

	if !a.message.set {
		request, expectedReply, commandText, err = probePingCommand(a.host, a.port, ioTimeout)
		if err != nil {
			return err
		}
	} else {
		msg := a.message.value
		request, expectedReply = buildPingRequest(&msg)
		commandText = fmt.Sprintf("PING <%d bytes>", len([]byte(msg)))
	}

	verify := !a.noVerify
	state := newBenchmarkState()
	state.setErrorLogLimit(a.errorLogLimit)

	fmt.Printf("Target=%s:%d  clients=%d  pipeline=%d  verify=%t\n",
		a.host, a.port, a.clients, a.pipeline, verify)
	fmt.Printf("Command=%s\n", commandText)

	var wg sync.WaitGroup
	for i := 0; i < a.clients; i++ {
		wg.Add(1)
		go worker(i, state, a.host, a.port, request, expectedReply, a.pipeline, verify, ioTimeout, reconnectDelay, &wg)
	}

	wg.Add(1)
	go reporter(
		state,
		time.Duration(a.interval*float64(time.Second)),
		time.Duration(a.reportWindow*float64(time.Second)),
		&wg,
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	if a.warmup > 0 {
		fmt.Printf("Warmup: %.1fs\n", a.warmup)
		warmupTimer := time.NewTimer(time.Duration(a.warmup * float64(time.Second)))
		select {
		case <-sigCh:
			fmt.Println("\nInterrupted.")
			warmupTimer.Stop()
			state.stop()
			wg.Wait()
			return nil
		case <-warmupTimer.C:
		}
	}

	state.stats.reset()
	state.measuring.Store(true)
	state.setMeasureStart(time.Now())
	fmt.Printf("Benchmarking: %.1fs\n", a.duration)

	durationTimer := time.NewTimer(time.Duration(a.duration * float64(time.Second)))
	select {
	case <-sigCh:
		fmt.Println("\nInterrupted.")
	case <-durationTimer.C:
	}
	durationTimer.Stop()

	state.setMeasureEnd(time.Now())
	state.stop()
	wg.Wait()
	summarize(state)
	return nil
}

func main() {
	a, err := parseArgs()
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "argument error: %v\n", err)
		os.Exit(2)
	}

	if err := run(a); err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}
}
