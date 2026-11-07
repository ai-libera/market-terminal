// market-terminal-backend: QUBE terminal server.
//
// This is the backend for the high-density ("DENSE" preset) workspace:
// quote monitor + sparklines, security description, chart (GP), news,
// fundamentals, economic calendar, earnings and analyst ratings. Data is served
// from HTTP JSON endpoints and from JSON streams (SSE, NDJSON or WebSocket).
//
// The data itself comes from the collectors market-terminal-backend-*. The
// server runs them as child processes and reads their output, which is JSONL
// {data, provenance} envelopes:
//
//   build/bin/market-terminal-backend-yahoo quotes AAPL MSFT
//   build/bin/market-terminal-backend-news feed -every 90s -limit 200
//
// One-shot runs are cached (TTL with singleflight, and STALE data is served if
// a refresh fails). Stream topics share one long-running "-every" collector
// process per topic across all clients, with reference counting. That process
// is stopped `-linger` after the last subscriber leaves.
//
// HTTP API (same shapes as the TS routes, so the React panels work unchanged):
//   GET /api/yfin/watchlist?symbols=A,B          quote monitor, description
//   GET /api/yfin/chart/{sym}?range&interval     chart, sparklines (1d/5m)
//   GET /api/yfin/fundamentals?symbol=           FUND
//   GET /api/yfin/analyst-ratings?symbol=        ANALYST
//   GET /api/yfin/economic-calendar[?week=]      ECO
//   GET /api/v2/statements?symbol&type&period&limit   EARN
//   GET /api/v2/news?tickers&q&exclude&categories&sources&limit   NEWS
//   GET /api/yfin/news                           context headline list
//
// Extra routes (list commands answer {items, count, provenance}, single
// records answer the record plus provenance):
//   GET /api/sec/filings?ticker&forms&limit       sec filings
//   GET /api/v2/filing-doc?cik&accession&doc      sec filing-doc
//   GET /api/sec/insider?ticker|marketwide=1&limit   sec insider (Form 4)
//   GET /api/sec/institutional?ticker            sec institutional (13F filers)
//   GET /api/sec/13f?cik&top                     sec 13f (manager holdings)
//   GET /api/v2/ipos?month&market                nasdaq ipos
//   GET /api/v2/halts                            nasdaq halts
//   GET /api/yfin/options-chain?symbol&date      yahoo options (nasdaq fallback)
//   GET /api/v2/transcripts?symbol | ?id         ref transcripts / transcript
//   GET /api/yfin/article?url                    news article (reader text)
//   GET /api/yfin/fear-greed                     social feargreed
//   GET /api/wsb/trending[?subs]                 social reddit
//   GET /api/congress/trades?limit               social congress
//   GET /api/yfin/search?q                       yahoo search
//   GET /api/yfin/market-movers?type             yahoo screener
//   GET /api/yfin/trending[?region]              yahoo trending
//   GET /api/v2/quote-batch?symbols=             yahoo quote-batch (v7, 100/request)
//   GET /api/v2/universe?index&sector&exchange&sic&q&classified&limit&offset[&refresh=1]
//       all SEC tickers (EDGAR submissions.zip) + Yahoo sector / industry +
//       S&P 500 membership (`ref sp500`: iShares IVV holdings, Wikipedia fallback),
//       cached under edgarDir() / yahooDir() / ivvDir()
//   GET /api/v2/universe/status                  coverage / progress of the above
//
// Yahoo pacing: all yahoo collectors share -yahoo-pace-file; interactive
// requests are -yahoo-min-gap (500ms) apart, background profile fetches
// -yahoo-bg-gap (2.5s); a 429 / 999 pauses every yahoo process (5 min,
// doubling to 1 h).
//
// The SEC contact for the User-Agent is "email" in the terminal settings
// (settingsYAMLFile()); sec collectors cache under edgarDir()/cache.
//
// JSON streams:
//   GET /api/v2/stream?topics=t1,t2[&transport=sse|ndjson]   (also WebSocket)
//   GET /api/v2/dense?symbol=AAPL&watch=A,B&range=1y         NDJSON snapshot,
//       one line per panel payload, written as each payload completes
//   GET /api/v2/dense/stream?symbol=AAPL&watch=A,B&range=1y  live DENSE topics
//
// Topics:
//   quotes:SYM                       batched quote poll (one process per tick for all symbols)
//   chart:SYM[:RANGE[:INTERVAL]]     OHLCV, TS column layout
//   keystats:SYM | fundamentals:SYM  key statistics
//   analyst:SYM                      analyst consensus
//   statements:SYM[:TYPE[:PERIOD[:LIMIT]]]
//   econ                             economic calendar, one frame per event
//   news | news:breaking | breaking_news | news:SYM
//   halts                            Nasdaq trade halts
//   trades:SYM, depth:SYM            UNAVAILABLE marker (never simulated)
//
// Frames: {"key":topic,"id":recordKey,"data":...,"provenance":{...},"serverTimestamp":ms}
// Control frames: {"type":"connected"|"heartbeat"|"error",...}
//
// Flags/env: -listen (QUBE_LISTEN, default 127.0.0.1:4032), -launcher (QUBE_LAUNCHER,
// default ""), -collector-prefix (QUBE_COLLECTOR_PREFIX, default
// market-terminal-backend- next to this executable). Per-collector override:
// QUBE_COLLECTOR_YAHOO="/path/to/qube-yahoo" (split on whitespace). The collector
// env (QUBE_SEC_UA, QUBE_*_UA, ...) is inherited by the children.
//
// SEC EDGAR pacing: -sec-min-gap (QUBE_SEC_MIN_GAP, default 200ms = 5 req/s,
// never below 100ms) is enforced across all sec collector processes through
// the shared lock file -sec-pace-file; -sec-max-procs (default 2) caps the
// concurrent one-shot sec runs.
package main

import (
  "archive/zip"
  "bufio"
  "bytes"
  "context"
  "crypto/sha1"
  "encoding/base64"
  "encoding/binary"
  "encoding/hex"
  "encoding/json"
  "errors"
  "expvar"
  "flag"
  "fmt"
  "io"
  "log/slog"
  "net/http"
  "net/url"
  "os"
  "os/exec"
  "os/signal"
  "path/filepath"
  "regexp"
  "sort"
  "strconv"
  "strings"
  "sync"
  "sync/atomic"
  "syscall"
  "time"

  "gopkg.in/yaml.v3"

  common "market-terminal/common/go"
)

// appName names the config / cache / state dirs (see common.ConfigDir, ...).
const appName = "market-terminal"

// Settings (config dir): .settings.yaml with the SEC contact "email".
func settingsYAMLFile() string {
  return filepath.Join(common.ConfigDir(appName), ".settings.yaml")
}

// settingsString returns key of the settings file, "" if the file or the
// key is missing.
func settingsString(key string) string {
  b, err := os.ReadFile(settingsYAMLFile())
  if err != nil {
    return ""
  }
  var m map[string]any
  if yaml.Unmarshal(b, &m) != nil {
    return ""
  }
  if v, ok := m[key]; ok && v != nil {
    return fmt.Sprint(v)
  }
  return ""
}

// Everything below the cache dir can be downloaded again.
func yahooDir() string {
  return common.EnsureDir(filepath.Join(common.CacheDir(appName), "yahoo"))
}

func edgarDir() string {
  return common.EnsureDir(filepath.Join(common.CacheDir(appName), "edgar"))
}

func edgarSubmissionsDir() string {
  return common.EnsureDir(filepath.Join(edgarDir(), "submissions"))
}

// Parsed ticker index of a submissions zip: universe/YYYY-MM-DD.json
func edgarUniverseDir() string {
  return common.EnsureDir(filepath.Join(edgarDir(), "universe"))
}

// Disk cache of the sec collector (company_tickers, submissions, facts, ...).
func edgarCacheDir() string {
  return common.EnsureDir(filepath.Join(edgarDir(), "cache"))
}

// Lock file shared by every sec collector for request pacing (state dir).
func defaultEdgarPaceFile() string {
  return filepath.Join(common.EnsureDir(filepath.Join(common.StateDir(appName), "edgar")), ".pace")
}

// Per-ticker Yahoo profiles (sector, industry, ...), fetched once, cached.
func yahooProfilesFile() string {
  return filepath.Join(yahooDir(), "profiles.json")
}

// Lock file shared by every yahoo collector for pacing / rate-limit pauses
// (state dir).
func defaultYahooPaceFile() string {
  return filepath.Join(common.EnsureDir(filepath.Join(common.StateDir(appName), "yahoo")), ".pace")
}

// S&P 500 membership (`ref sp500`: IVV holdings, Wikipedia fallback).
func ivvDir() string {
  return common.EnsureDir(filepath.Join(common.CacheDir(appName), "ivv"))
}

// Current S&P 500 member list, refreshed by universe.sp500Loop.
func sp500ListFile() string {
  return filepath.Join(ivvDir(), "sp500.json")
}

// ---------------------------------------------------------------------------
// Config and main
// ---------------------------------------------------------------------------

type config struct {
  listen     string
  launcher   string
  prefix     string
  webDir     string
  cmdTimeout time.Duration
  maxProcs   int
  maxStreams int
  linger     time.Duration
  quoteEvery time.Duration

  // SEC EDGAR pacing (fair access: max 10 req/s per client). Passed to every
  // sec collector, which spaces its requests through a shared lock file.
  secMinGap   time.Duration
  secPaceFile string
  secMaxProcs int

  // Yahoo pacing, shared by all yahoo collectors through one lock file.
  yahooMinGap   time.Duration // interactive requests (GUI panels, quotes)
  yahooBgGap    time.Duration // background profile enrichment
  yahooPaceFile string

  // Universe (EDGAR submissions.zip + per-ticker Yahoo profiles), see universe.
  universeAuto    bool
  universeMaxAge  time.Duration // re-download submissions.zip after this
  universeTimeout time.Duration // one submissions.zip download
  enrichAuto      bool          // fetch missing Yahoo profiles in the background
  enrichChunk     int           // symbols per profile collector run

  // S&P 500 member list (`ref sp500`), cached in ivvDir().
  sp500Auto   bool
  sp500MaxAge time.Duration
}

// sp500MaxAgeFloor: never ask the sources more than once an hour.
const sp500MaxAgeFloor = time.Hour

// yahooMinGapFloor: never more than 5 interactive Yahoo requests per second.
const yahooMinGapFloor = 200 * time.Millisecond

// secMinGapFloor keeps us below the SEC hard limit of 10 req/s whatever the
// flags say.
const secMinGapFloor = 100 * time.Millisecond

func durationEnv(k string, def time.Duration) time.Duration {
  if v := strings.TrimSpace(os.Getenv(k)); v != "" {
    if d, err := time.ParseDuration(v); err == nil {
      return d
    }
  }
  return def
}

var collectorNames = map[string]bool{
  "yahoo": true, "sec": true, "nasdaq": true, "news": true, "social": true, "ref": true,
}

func envOr(k, def string) string {
  if v := strings.TrimSpace(os.Getenv(k)); v != "" {
    return v
  }
  return def
}

func main() {
  var cfg config
  flag.StringVar(&cfg.listen, "listen", envOr("QUBE_LISTEN", "127.0.0.1:4032"), "HTTP listen address (env QUBE_LISTEN)")
  flag.StringVar(&cfg.launcher, "launcher", os.Getenv("QUBE_LAUNCHER"), "command used to start collectors; empty = run prefix+name directly (env QUBE_LAUNCHER)")
  flag.StringVar(&cfg.prefix, "collector-prefix", envOr("QUBE_COLLECTOR_PREFIX", filepath.Join(common.ExeDir(), "market-terminal-backend-")), "collector command prefix (env QUBE_COLLECTOR_PREFIX)")
  flag.StringVar(&cfg.webDir, "web", "", "optional directory with the static UI build, served at /")
  flag.DurationVar(&cfg.cmdTimeout, "cmd-timeout", 60*time.Second, "timeout for one-shot collector runs")
  flag.IntVar(&cfg.maxProcs, "max-procs", 8, "max concurrent one-shot collector processes")
  flag.IntVar(&cfg.maxStreams, "max-streams", 64, "max concurrent stream sources (long-running collector processes)")
  flag.DurationVar(&cfg.linger, "linger", 30*time.Second, "keep a stream source running this long after its last subscriber leaves")
  flag.DurationVar(&cfg.quoteEvery, "quote-every", 5*time.Second, "quote stream poll interval")
  flag.DurationVar(&cfg.secMinGap, "sec-min-gap", durationEnv("QUBE_SEC_MIN_GAP", 200*time.Millisecond), "minimum gap between SEC EDGAR requests across all sec collectors (min 100ms; env QUBE_SEC_MIN_GAP)")
  flag.StringVar(&cfg.secPaceFile, "sec-pace-file", envOr("QUBE_SEC_PACE_FILE", defaultEdgarPaceFile()), "lock file the sec collectors share for pacing (env QUBE_SEC_PACE_FILE)")
  flag.IntVar(&cfg.secMaxProcs, "sec-max-procs", 2, "max concurrent one-shot sec collector processes (they share the -sec-min-gap budget)")
  flag.DurationVar(&cfg.yahooMinGap, "yahoo-min-gap", durationEnv("QUBE_YAHOO_MIN_GAP", 500*time.Millisecond), "minimum gap between Yahoo requests across all yahoo collectors (min 200ms; env QUBE_YAHOO_MIN_GAP)")
  flag.DurationVar(&cfg.yahooBgGap, "yahoo-bg-gap", 2500*time.Millisecond, "gap between background profile requests (they only take slots the interactive requests leave free)")
  flag.StringVar(&cfg.yahooPaceFile, "yahoo-pace-file", envOr("QUBE_YAHOO_PACE_FILE", defaultYahooPaceFile()), "lock file the yahoo collectors share for pacing and rate-limit pauses (env QUBE_YAHOO_PACE_FILE)")
  flag.BoolVar(&cfg.universeAuto, "universe-auto", true, "download / parse EDGAR submissions.zip in the background at startup when the newest copy is older than -universe-max-age")
  flag.DurationVar(&cfg.universeMaxAge, "universe-max-age", 7*24*time.Hour, "re-download EDGAR submissions.zip when the newest copy is older than this")
  flag.DurationVar(&cfg.universeTimeout, "universe-timeout", 2*time.Hour, "timeout of one submissions.zip download")
  flag.BoolVar(&cfg.enrichAuto, "enrich-auto", true, "fetch Yahoo sector / industry profiles of tickers without one in the background (S&P 500 first)")
  flag.IntVar(&cfg.enrichChunk, "enrich-chunk", 20, "symbols per background profile collector run")
  flag.BoolVar(&cfg.sp500Auto, "sp500-auto", true, "keep the S&P 500 member list current with `ref sp500` (iShares IVV holdings, Wikipedia fallback)")
  flag.DurationVar(&cfg.sp500MaxAge, "sp500-max-age", 24*time.Hour, "refresh the S&P 500 member list when it is older than this (min 1h)")
  verbose := flag.Bool("v", false, "debug logging")
  flag.Parse()
  if cfg.maxProcs < 1 {
    cfg.maxProcs = 1
  }
  if cfg.quoteEvery < time.Second {
    cfg.quoteEvery = time.Second
  }
  if cfg.secMinGap < secMinGapFloor {
    cfg.secMinGap = secMinGapFloor
  }
  if cfg.secMaxProcs < 1 {
    cfg.secMaxProcs = 1
  }
  if cfg.yahooMinGap < yahooMinGapFloor {
    cfg.yahooMinGap = yahooMinGapFloor
  }
  if cfg.yahooBgGap < cfg.yahooMinGap {
    cfg.yahooBgGap = cfg.yahooMinGap
  }
  if cfg.enrichChunk < 1 {
    cfg.enrichChunk = 1
  }
  if cfg.sp500MaxAge < sp500MaxAgeFloor {
    cfg.sp500MaxAge = sp500MaxAgeFloor
  }

  level := slog.LevelInfo
  if *verbose {
    level = slog.LevelDebug
  }
  logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level, ReplaceAttr: dropTime}))
  respLog = logger

  ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
  defer stop()

  s := newServer(cfg, logger)
  srv := &http.Server{
    Addr:              cfg.listen,
    Handler:           s.routes(),
    ReadHeaderTimeout: 10 * time.Second,
    IdleTimeout:       120 * time.Second,
    // No WriteTimeout: streams are long-lived; per-write deadlines are set instead.
  }
  srv.RegisterOnShutdown(func() { s.closeOnce.Do(func() { close(s.closing) }) })
  go s.janitor(ctx)
  go s.uni.start(ctx)

  done := make(chan struct{})
  go func() {
    defer close(done)
    <-ctx.Done()
    logger.Info("shutting down")
    shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    srv.Shutdown(shCtx)
    s.hub.shutdown(8 * time.Second)
  }()

  logger.Info("qube terminal server listening", "addr", cfg.listen, "launcher", cfg.launcher, "prefix", cfg.prefix,
    "secMinGap", cfg.secMinGap.String(), "secPaceFile", cfg.secPaceFile, "secMaxProcs", cfg.secMaxProcs)
  if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
    logger.Error("listen failed", "err", err)
    os.Exit(1)
  }
  <-done
}

// ---------------------------------------------------------------------------
// Envelope (collector output contract)
// ---------------------------------------------------------------------------

const (
  qLive        = "LIVE"
  qDelayed     = "DELAYED"
  qStale       = "STALE"
  qDerived     = "DERIVED"
  qUnavailable = "UNAVAILABLE"
)

type Provenance struct {
  Source      string `json:"source"`
  Endpoint    string `json:"endpoint"`
  Key         string `json:"key,omitempty"`
  RetrievedAt string `json:"retrievedAt"`
  Quality     string `json:"quality"`
  CacheAgeMs  int64  `json:"cacheAgeMs"`
  Error       string `json:"error"`
}

type Envelope struct {
  Data       json.RawMessage `json:"data"`
  Provenance Provenance      `json:"provenance"`
}

var jsonNull = json.RawMessage("null")

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

func failEnv(source, endpoint, key string, err error) Envelope {
  msg := "unavailable"
  if err != nil {
    msg = err.Error()
  }
  return Envelope{Data: jsonNull, Provenance: Provenance{
    Source: source, Endpoint: endpoint, Key: key, RetrievedAt: nowRFC3339(), Quality: qUnavailable, Error: msg,
  }}
}

func staleEnv(e Envelope, age time.Duration, errMsg string) Envelope {
  e.Provenance.Quality = qStale
  e.Provenance.CacheAgeMs = age.Milliseconds()
  if errMsg != "" {
    e.Provenance.Error = errMsg
  }
  return e
}

func (e Envelope) ok() bool {
  return e.Provenance.Quality != qUnavailable && len(e.Data) > 0 && string(e.Data) != "null"
}

func allUnavailable(envs []Envelope) bool {
  for _, e := range envs {
    if e.ok() {
      return false
    }
  }
  return true
}

// collErr tags an error with the collector command that produced it. The
// message is unchanged, so what the frontend sees stays the same.
type collErr struct {
  ref collRef
  err error
}

func (e *collErr) Error() string { return e.err.Error() }
func (e *collErr) Unwrap() error { return e.err }

// collRef names a collector run: the command and its arguments.
type collRef struct {
  cmd  string
  args string
}

func (s *server) collRef(coll string, args ...string) collRef {
  return collRef{cmd: s.run.command(coll), args: strings.Join(args, " ")}
}

func collectorOf(err error) collRef {
  var ce *collErr
  if errors.As(err, &ce) {
    return ce.ref
  }
  return collRef{}
}

// taggedBody carries the collector responsible for an error body. It
// marshals exactly like the wrapped body; the tag is only used for logging.
type taggedBody struct {
  body any
  ref  collRef
}

func (t taggedBody) MarshalJSON() ([]byte, error) { return json.Marshal(t.body) }

func tagColl(body any, ref collRef) any {
  if ref.cmd == "" {
    return body
  }
  return taggedBody{body: body, ref: ref}
}

// dropTime removes the time attribute from log lines.
func dropTime(groups []string, a slog.Attr) slog.Attr {
  if len(groups) == 0 && a.Key == slog.TimeKey {
    return slog.Attr{}
  }
  return a
}

func firstError(envs []Envelope) string {
  for _, e := range envs {
    if e.Provenance.Error != "" {
      return e.Provenance.Error
    }
  }
  return "no data"
}

// pick returns the envelope for key, or the only envelope if there is one.
func pick(envs []Envelope, key string) (Envelope, bool) {
  for _, e := range envs {
    if e.Provenance.Key == key {
      return e, true
    }
  }
  if len(envs) == 1 {
    return envs[0], true
  }
  return Envelope{}, false
}

func errString(err error) string {
  if err == nil {
    return ""
  }
  return err.Error()
}

// ---------------------------------------------------------------------------
// Collector runner
// ---------------------------------------------------------------------------

type collStat struct {
  mu        sync.Mutex
  runs      int64
  failures  int64
  records   int64
  active    int64
  lastOK    string
  lastFail  string
  lastError string
}

func (st *collStat) begin() {
  st.mu.Lock()
  st.runs++
  st.active++
  st.mu.Unlock()
}

func (st *collStat) end(records int, err error) {
  st.mu.Lock()
  defer st.mu.Unlock()
  st.active--
  st.records += int64(records)
  if err != nil {
    st.failures++
    st.lastFail = nowRFC3339()
    st.lastError = err.Error()
  } else {
    st.lastOK = nowRFC3339()
  }
}

type runner struct {
  cfg   config
  log   *slog.Logger
  sem   chan struct{}
  stats map[string]*collStat
  // Per-collector caps for one-shot runs (taken before the global -max-procs
  // slot, so waiting sec runs do not block other collectors).
  collSem map[string]chan struct{}
}

func newRunner(cfg config, log *slog.Logger) *runner {
  r := &runner{cfg: cfg, log: log, sem: make(chan struct{}, cfg.maxProcs), stats: map[string]*collStat{},
    collSem: map[string]chan struct{}{"sec": make(chan struct{}, cfg.secMaxProcs)}}
  for name := range collectorNames {
    r.stats[name] = &collStat{}
  }
  expvar.Publish("collectors", expvar.Func(func() any { return r.snapshot() }))
  return r
}

func (r *runner) snapshot() map[string]any {
  out := map[string]any{}
  for name, st := range r.stats {
    argv, _ := r.argv(name)
    st.mu.Lock()
    out[name] = map[string]any{
      "command":   strings.Join(argv, " "),
      "runs":      st.runs,
      "failures":  st.failures,
      "records":   st.records,
      "active":    st.active,
      "lastOK":    st.lastOK,
      "lastFail":  st.lastFail,
      "lastError": st.lastError,
    }
    st.mu.Unlock()
  }
  return out
}

// command is the collector command that is run, e.g.
// "build/bin/market-terminal-backend-yahoo" (or the QUBE_COLLECTOR_<NAME> override).
func (r *runner) command(name string) string {
  if v := strings.Fields(os.Getenv("QUBE_COLLECTOR_" + strings.ToUpper(name))); len(v) > 0 {
    return strings.Join(v, " ")
  }
  return r.cfg.prefix + name
}

// argv returns the command line that starts a collector, without arguments.
func (r *runner) argv(name string) ([]string, error) {
  if !collectorNames[name] {
    return nil, fmt.Errorf("unknown collector %q", name)
  }
  if v := strings.Fields(os.Getenv("QUBE_COLLECTOR_" + strings.ToUpper(name))); len(v) > 0 {
    return v, nil
  }
  argv := strings.Fields(r.cfg.launcher)
  if len(argv) == 0 {
    // run prefix+name directly: add ".exe" on Windows
    return []string{common.WithExeSuffix(r.cfg.prefix + name)}, nil
  }
  return append(argv, r.cfg.prefix+name), nil
}

// tailBuf keeps the last max bytes written to it (collector stderr).
type tailBuf struct {
  mu  sync.Mutex
  b   []byte
  max int
}

func (t *tailBuf) Write(p []byte) (int, error) {
  t.mu.Lock()
  t.b = append(t.b, p...)
  if len(t.b) > t.max {
    t.b = append([]byte(nil), t.b[len(t.b)-t.max:]...)
  }
  t.mu.Unlock()
  return len(p), nil
}

func (t *tailBuf) lastLine() string {
  t.mu.Lock()
  defer t.mu.Unlock()
  lines := strings.Split(strings.TrimSpace(string(t.b)), "\n")
  for i := len(lines) - 1; i >= 0; i-- {
    if l := strings.TrimSpace(lines[i]); l != "" {
      if len(l) > 300 {
        l = l[:300]
      }
      return l
    }
  }
  return ""
}

// exec runs one collector process and calls fn for every envelope it prints.
// It returns when the process exits or ctx is cancelled (the whole process
// group is sent SIGTERM so launcher children go away too).
func (r *runner) exec(ctx context.Context, name string, args []string, fn func(Envelope)) (int, error) {
  return r.execEnv(ctx, name, args, nil, fn)
}

// collectorEnv is the environment of a collector process: shared pacing
// (SEC and Yahoo), the SEC contact and cache dir, then extra (which wins).
func (r *runner) collectorEnv(name string, extra []string) []string {
  var env []string
  switch name {
  case "sec":
    // Every sec process (one-shot and stream) paces through the same file,
    // identifies with the e-mail from the terminal settings and caches under
    // edgarDir().
    env = append(os.Environ(),
      "QUBE_SEC_MIN_GAP="+r.cfg.secMinGap.String(),
      "QUBE_SEC_PACE_FILE="+r.cfg.secPaceFile)
    if ua := secUserAgent(); ua != "" {
      env = append(env, "QUBE_SEC_UA="+ua)
    }
    if os.Getenv("QUBE_SEC_CACHE_DIR") == "" {
      env = append(env, "QUBE_SEC_CACHE_DIR="+edgarCacheDir())
    }
  case "yahoo":
    // All yahoo processes share one pace file and one rate-limit pause.
    env = append(os.Environ(),
      "QUBE_YAHOO_MIN_GAP="+r.cfg.yahooMinGap.String(),
      "QUBE_YAHOO_PACE_FILE="+r.cfg.yahooPaceFile)
  }
  if len(extra) > 0 {
    if env == nil {
      env = os.Environ()
    }
    env = append(env, extra...) // later duplicates win in os/exec
  }
  return env
}

// execEnv is exec with extra environment variables (KEY=VALUE).
func (r *runner) execEnv(ctx context.Context, name string, args []string, extra []string, fn func(Envelope)) (int, error) {
  argv, err := r.argv(name)
  if err != nil {
    return 0, err
  }
  full := append(append([]string{}, argv[1:]...), args...)
  cmd := exec.CommandContext(ctx, argv[0], full...)
  common.SetProcessGroup(cmd)
  cmd.Cancel = func() error {
    if cmd.Process == nil {
      return nil
    }
    return common.TerminateProcessGroup(cmd)
  }
  cmd.WaitDelay = 5 * time.Second
  cmd.Env = r.collectorEnv(name, extra) // nil = inherit
  tail := &tailBuf{max: 4096}
  cmd.Stderr = tail
  out, err := cmd.StdoutPipe()
  if err != nil {
    return 0, err
  }
  st := r.stats[name]
  st.begin()
  r.log.Debug("collector start", "cmd", argv[0], "args", strings.Join(full, " "))
  if err := cmd.Start(); err != nil {
    err = fmt.Errorf("start %s: %w", strings.Join(argv, " "), err)
    st.end(0, err)
    return 0, err
  }
  sc := bufio.NewScanner(out)
  sc.Buffer(make([]byte, 64<<10), 64<<20)
  n := 0
  for sc.Scan() {
    line := bytes.TrimSpace(sc.Bytes())
    if len(line) == 0 || line[0] != '{' {
      continue
    }
    var env Envelope
    if err := json.Unmarshal(line, &env); err != nil {
      r.log.Warn("bad collector record", "collector", name, "err", err)
      continue
    }
    if len(env.Data) == 0 {
      env.Data = jsonNull
    }
    n++
    fn(env)
  }
  scanErr := sc.Err()
  if scanErr != nil {
    io.Copy(io.Discard, out)
  }
  werr := cmd.Wait()
  switch {
  case ctx.Err() != nil:
    werr = ctx.Err()
  case werr != nil:
    sub := ""
    if len(args) > 0 {
      sub = " " + args[0]
    }
    if l := tail.lastLine(); l != "" {
      werr = fmt.Errorf("%s%s: %v: %s", name, sub, werr, l)
    } else {
      werr = fmt.Errorf("%s%s: %v", name, sub, werr)
    }
  case scanErr != nil:
    werr = scanErr
  }
  // A non-zero exit after records just means some records were UNAVAILABLE.
  if n > 0 && ctx.Err() == nil {
    st.end(n, nil)
  } else {
    st.end(n, werr)
  }
  return n, werr
}

// once runs a collector to completion (bounded by -max-procs and -cmd-timeout)
// and returns its envelopes.
func (r *runner) once(ctx context.Context, name string, args ...string) ([]Envelope, error) {
  if cs := r.collSem[name]; cs != nil {
    select {
    case cs <- struct{}{}:
    case <-ctx.Done():
      return nil, ctx.Err()
    }
    defer func() { <-cs }()
  }
  select {
  case r.sem <- struct{}{}:
  case <-ctx.Done():
    return nil, ctx.Err()
  }
  defer func() { <-r.sem }()
  ctx, cancel := context.WithTimeout(ctx, r.cfg.cmdTimeout)
  defer cancel()
  var envs []Envelope
  _, err := r.exec(ctx, name, args, func(e Envelope) { envs = append(envs, e) })
  if len(envs) > 0 {
    return envs, nil
  }
  if errors.Is(ctx.Err(), context.DeadlineExceeded) {
    return nil, fmt.Errorf("%s %s: timed out after %s", name, strings.Join(args, " "), r.cfg.cmdTimeout)
  }
  if err == nil {
    err = fmt.Errorf("%s: no records", name)
  }
  return nil, err
}

// ---------------------------------------------------------------------------
// Memo: TTL cache with singleflight, stale-on-error and a short negative cache
// ---------------------------------------------------------------------------

const negativeTTL = 15 * time.Second

type memoEntry struct {
  v       any
  at      time.Time
  has     bool
  lastErr error
  errAt   time.Time
  flight  chan struct{}
}

func (e *memoEntry) result() (any, time.Duration, bool, error) {
  if e.lastErr != nil {
    if e.has {
      return e.v, time.Since(e.at), true, e.lastErr
    }
    return nil, 0, false, e.lastErr
  }
  return e.v, time.Since(e.at), false, nil
}

type memo struct {
  mu sync.Mutex
  m  map[string]*memoEntry
}

func newMemo() *memo { return &memo{m: map[string]*memoEntry{}} }

// get returns (value, age, stale, err). stale is true when an older value is
// returned because the refresh failed; err then carries the refresh error.
func (c *memo) get(key string, ttl time.Duration, fn func() (any, error)) (any, time.Duration, bool, error) {
  c.mu.Lock()
  e := c.m[key]
  if e == nil {
    e = &memoEntry{}
    c.m[key] = e
  }
  if e.has && e.lastErr == nil && time.Since(e.at) < ttl {
    v, age := e.v, time.Since(e.at)
    c.mu.Unlock()
    return v, age, false, nil
  }
  if e.flight != nil {
    w := e.flight
    c.mu.Unlock()
    <-w
    c.mu.Lock()
    defer c.mu.Unlock()
    return e.result()
  }
  if e.lastErr != nil && time.Since(e.errAt) < negativeTTL {
    defer c.mu.Unlock()
    return e.result()
  }
  w := make(chan struct{})
  e.flight = w
  c.mu.Unlock()

  v, err := fn()

  c.mu.Lock()
  defer c.mu.Unlock()
  if err == nil {
    e.v, e.at, e.has, e.lastErr = v, time.Now(), true, nil
  } else {
    e.lastErr, e.errAt = err, time.Now()
  }
  e.flight = nil
  close(w)
  return e.result()
}

func (c *memo) prune(maxAge time.Duration) {
  c.mu.Lock()
  defer c.mu.Unlock()
  for k, e := range c.m {
    if e.flight == nil && time.Since(e.at) > maxAge && time.Since(e.errAt) > maxAge {
      delete(c.m, k)
    }
  }
}

// ---------------------------------------------------------------------------
// Server state, cached collector calls, quote store
// ---------------------------------------------------------------------------

type server struct {
  cfg       config
  log       *slog.Logger
  run       *runner
  memo      *memo
  qs        *quoteStore
  hub       *hub
  started   time.Time
  clients   atomic.Int64
  closing   chan struct{}
  closeOnce sync.Once
  uni       *universe
}

func newServer(cfg config, log *slog.Logger) *server {
  s := &server{
    cfg:     cfg,
    log:     log,
    run:     newRunner(cfg, log),
    memo:    newMemo(),
    qs:      &quoteStore{m: map[string]quoteEntry{}},
    started: time.Now(),
    closing: make(chan struct{}),
  }
  s.hub = newHub(cfg, log)
  s.uni = newUniverse(cfg, log, s.run)
  expvar.Publish("streams", expvar.Func(func() any { return s.hub.snapshot() }))
  expvar.Publish("clients", expvar.Func(func() any { return s.clients.Load() }))
  return s
}

func (s *server) janitor(ctx context.Context) {
  t := time.NewTicker(5 * time.Minute)
  defer t.Stop()
  for {
    select {
    case <-ctx.Done():
      return
    case <-t.C:
      s.memo.prune(24 * time.Hour)
      s.qs.prune(time.Hour)
    }
  }
}

// cached runs a one-shot collector command through the memo. Runs that only
// produced UNAVAILABLE records count as failures, so they never replace good
// cached data. Envelopes served from an older run are marked STALE.
func (s *server) cached(ttl time.Duration, coll string, args ...string) ([]Envelope, error) {
  key := coll + " " + strings.Join(args, " ")
  v, age, stale, err := s.memo.get(key, ttl, func() (any, error) {
    envs, err := s.run.once(context.Background(), coll, args...)
    if err != nil {
      return nil, err
    }
    if allUnavailable(envs) {
      return nil, errors.New(firstError(envs))
    }
    return envs, nil
  })
  src, _ := v.([]Envelope)
  if src == nil {
    if err == nil {
      err = errors.New("no data")
    }
    return nil, &collErr{ref: s.collRef(coll, args...), err: err}
  }
  out := make([]Envelope, len(src))
  for i, e := range src {
    if stale {
      e = staleEnv(e, age, errString(err))
    } else {
      e.Provenance.CacheAgeMs += age.Milliseconds()
    }
    out[i] = e
  }
  return out, nil
}

type quoteEntry struct {
  env Envelope
  at  time.Time
}

// quoteStore is a per-symbol quote cache shared by the watchlist route, the
// dense snapshot and the quotes stream, so all of them batch into one
// `yahoo quotes` process per refresh.
type quoteStore struct {
  mu sync.Mutex
  m  map[string]quoteEntry
}

func (q *quoteStore) prune(maxAge time.Duration) {
  q.mu.Lock()
  defer q.mu.Unlock()
  for k, e := range q.m {
    if time.Since(e.at) > maxAge {
      delete(q.m, k)
    }
  }
}

// quoteChartFallbackMax: at most this many symbols per batch are retried
// with per-symbol chart requests when quote-batch did not answer them.
const quoteChartFallbackMax = 10

// quotes returns one envelope per symbol (symbols must already be validated).
func (s *server) quotes(ctx context.Context, syms []string, maxAge time.Duration) map[string]Envelope {
  out := map[string]Envelope{}
  var missing []string
  s.qs.mu.Lock()
  for _, sym := range syms {
    if e, ok := s.qs.m[sym]; ok && time.Since(e.at) < maxAge {
      env := e.env
      env.Provenance.CacheAgeMs = time.Since(e.at).Milliseconds()
      out[sym] = env
    } else {
      missing = append(missing, sym)
    }
  }
  s.qs.mu.Unlock()
  var runErr error
  for start := 0; start < len(missing); start += 100 {
    end := start + 100
    if end > len(missing) {
      end = len(missing)
    }
    // One v7 request per 100 symbols; symbols it could not answer fall back
    // to one chart request each (few, so the shared Yahoo pacing holds).
    part := missing[start:end]
    envs, err := s.run.once(ctx, "yahoo", append([]string{"quote-batch"}, part...)...)
    if err != nil {
      runErr = err
    }
    okSyms := map[string]bool{}
    for _, e := range envs {
      if e.ok() {
        okSyms[e.Provenance.Key] = true
      }
    }
    var retry []string
    for _, sym := range part {
      if !okSyms[sym] {
        retry = append(retry, sym)
      }
    }
    if len(retry) > 0 && len(retry) <= quoteChartFallbackMax {
      more, err := s.run.once(ctx, "yahoo", append([]string{"quotes", "-concurrency", "2"}, retry...)...)
      if err != nil && runErr == nil {
        runErr = err
      }
      envs = append(envs, more...) // later (fallback) envelopes win below
    }
    now := time.Now()
    s.qs.mu.Lock()
    for _, e := range envs {
      sym := e.Provenance.Key
      switch old, had := s.qs.m[sym]; {
      case e.ok():
        s.qs.m[sym] = quoteEntry{env: e, at: now}
        out[sym] = e
      case had:
        out[sym] = staleEnv(old.env, now.Sub(old.at), e.Provenance.Error)
      default:
        out[sym] = e
      }
    }
    s.qs.mu.Unlock()
  }
  s.qs.mu.Lock()
  defer s.qs.mu.Unlock()
  for _, sym := range missing {
    if _, ok := out[sym]; ok {
      continue
    }
    if old, had := s.qs.m[sym]; had {
      out[sym] = staleEnv(old.env, time.Since(old.at), errString(runErr))
    } else {
      out[sym] = failEnv("yahoo", "quote", sym, runErr)
    }
  }
  return out
}

// ---------------------------------------------------------------------------
// Stream hub: one source (producer) per unique upstream topic, shared by all
// subscribers. Sources keep the last frame per record id for replay, drop
// unchanged frames (delta-only push) and stop -linger after the last
// subscriber leaves.
// ---------------------------------------------------------------------------

type frame struct {
  Key             string          `json:"key"`
  ID              string          `json:"id,omitempty"`
  Data            json.RawMessage `json:"data"`
  Provenance      *Provenance     `json:"provenance,omitempty"`
  ServerTimestamp int64           `json:"serverTimestamp"`
  hash            string
  tags            []string // filter tags, e.g. "sym:AAPL", "urgency:high"; "*" matches any filter
}

func (f *frame) bytesFor(topic string) []byte {
  g := *f
  g.Key = topic
  b, err := json.Marshal(&g)
  if err != nil {
    b, _ = json.Marshal(map[string]any{"key": topic, "id": f.ID, "data": nil, "error": err.Error()})
  }
  return b
}

func hashBytes(parts ...[]byte) string {
  h := sha1.New()
  for _, p := range parts {
    h.Write(p)
    h.Write([]byte{0})
  }
  return hex.EncodeToString(h.Sum(nil)[:10])
}

func frameHash(f *frame) string {
  q, e := "", ""
  if f.Provenance != nil {
    q, e = f.Provenance.Quality, f.Provenance.Error
  }
  return hashBytes(f.Data, []byte(q), []byte(e))
}

// ctrl builds a control frame such as connected/heartbeat/error.
func ctrl(typ string, fields map[string]any) []byte {
  if fields == nil {
    fields = map[string]any{}
  }
  fields["type"] = typ
  b, _ := json.Marshal(fields)
  return b
}

// client is one streaming connection. Sources never block on it: if its
// buffer overflows the connection is dropped (the browser reconnects and gets
// a fresh replay).
type client struct {
  out  chan []byte
  done chan struct{}
  once sync.Once
}

func newClient() *client {
  return &client{out: make(chan []byte, 4096), done: make(chan struct{})}
}

func (c *client) kill() { c.once.Do(func() { close(c.done) }) }

func (c *client) send(b []byte) {
  select {
  case <-c.done:
  case c.out <- b:
  default:
    c.kill()
  }
}

type sub struct {
  c     *client
  topic string
  need  string
  arg   string
  src   *source
}

func (sb *sub) wants(f *frame) bool {
  if sb.need == "" {
    return true
  }
  for _, t := range f.tags {
    if t == sb.need || t == "*" {
      return true
    }
  }
  return false
}

type source struct {
  h       *hub
  key     string
  maxLast int
  produce func(ctx context.Context, src *source)

  mu     sync.Mutex
  subs   map[*sub]bool
  last   map[string]*frame
  order  []string
  cancel context.CancelFunc
  idle   *time.Timer
  wake   chan struct{}
}

func (src *source) poke() {
  select {
  case src.wake <- struct{}{}:
  default:
  }
}

// args returns the sorted union of subscriber arguments (e.g. quote symbols).
func (src *source) args() []string {
  src.mu.Lock()
  defer src.mu.Unlock()
  seen := map[string]bool{}
  var out []string
  for sb := range src.subs {
    if sb.arg != "" && !seen[sb.arg] {
      seen[sb.arg] = true
      out = append(out, sb.arg)
    }
  }
  sort.Strings(out)
  return out
}

// publish delivers f to matching subscribers. With store, f is remembered
// for replay and suppressed if identical to the last frame with the same id.
func (src *source) publish(f *frame, store bool) {
  if f.ServerTimestamp == 0 {
    f.ServerTimestamp = time.Now().UnixMilli()
  }
  if f.hash == "" {
    f.hash = frameHash(f)
  }
  src.mu.Lock()
  defer src.mu.Unlock()
  if store {
    if prev, ok := src.last[f.ID]; ok {
      if prev.hash == f.hash {
        return
      }
    } else {
      src.order = append(src.order, f.ID)
      if src.maxLast > 0 && len(src.order) > src.maxLast {
        delete(src.last, src.order[0])
        src.order = append([]string(nil), src.order[1:]...)
      }
    }
    src.last[f.ID] = f
  }
  for sb := range src.subs {
    if sb.wants(f) {
      sb.c.send(f.bytesFor(sb.topic))
    }
  }
}

type hub struct {
  cfg       config
  log       *slog.Logger
  ctx       context.Context
  cancelAll context.CancelFunc
  wg        sync.WaitGroup

  mu      sync.Mutex
  sources map[string]*source
  running int
}

func newHub(cfg config, log *slog.Logger) *hub {
  ctx, cancel := context.WithCancel(context.Background())
  return &hub{cfg: cfg, log: log, ctx: ctx, cancelAll: cancel, sources: map[string]*source{}}
}

// topicSpec describes how a client topic maps onto a shared source.
type topicSpec struct {
  source  string
  need    string
  arg     string
  maxLast int
  produce func(ctx context.Context, src *source)
  static  *frame // answered once, no source (e.g. UNAVAILABLE entitlements)
}

func (h *hub) attach(c *client, topic string, sp topicSpec) (*sub, error) {
  h.mu.Lock()
  defer h.mu.Unlock()
  src := h.sources[sp.source]
  if src == nil {
    if h.running >= h.cfg.maxStreams {
      return nil, fmt.Errorf("stream capacity reached (%d sources)", h.cfg.maxStreams)
    }
    src = &source{
      h: h, key: sp.source, maxLast: sp.maxLast, produce: sp.produce,
      subs: map[*sub]bool{}, last: map[string]*frame{}, wake: make(chan struct{}, 1),
    }
    h.sources[sp.source] = src
  }
  sb := &sub{c: c, topic: topic, need: sp.need, arg: sp.arg, src: src}
  src.mu.Lock()
  if src.idle != nil {
    src.idle.Stop()
    src.idle = nil
  }
  src.subs[sb] = true
  if src.cancel == nil {
    ctx, cancel := context.WithCancel(h.ctx)
    src.cancel = cancel
    h.running++
    h.wg.Add(1)
    h.log.Debug("stream source start", "source", src.key)
    go func() {
      defer h.wg.Done()
      src.produce(ctx, src)
    }()
  }
  for _, id := range src.order {
    if f := src.last[id]; f != nil && sb.wants(f) {
      c.send(f.bytesFor(topic))
    }
  }
  src.mu.Unlock()
  src.poke()
  return sb, nil
}

func (h *hub) detach(sb *sub) {
  src := sb.src
  src.mu.Lock()
  delete(src.subs, sb)
  if len(src.subs) == 0 && src.idle == nil {
    src.idle = time.AfterFunc(h.cfg.linger, func() { h.reap(src) })
  }
  src.mu.Unlock()
  src.poke()
}

func (h *hub) reap(src *source) {
  h.mu.Lock()
  defer h.mu.Unlock()
  src.mu.Lock()
  defer src.mu.Unlock()
  if len(src.subs) > 0 {
    return
  }
  src.idle = nil
  if src.cancel != nil {
    src.cancel()
    src.cancel = nil
    h.running--
    h.log.Debug("stream source stop", "source", src.key)
  }
  if h.sources[src.key] == src {
    delete(h.sources, src.key)
  }
}

func (h *hub) snapshot() map[string]any {
  h.mu.Lock()
  defer h.mu.Unlock()
  srcs := map[string]any{}
  for k, src := range h.sources {
    src.mu.Lock()
    srcs[k] = map[string]any{"subscribers": len(src.subs), "records": len(src.last), "running": src.cancel != nil}
    src.mu.Unlock()
  }
  return map[string]any{"running": h.running, "max": h.cfg.maxStreams, "sources": srcs}
}

func (h *hub) shutdown(wait time.Duration) {
  h.cancelAll()
  done := make(chan struct{})
  go func() {
    h.wg.Wait()
    close(done)
  }()
  select {
  case <-done:
  case <-time.After(wait):
    h.log.Warn("stream sources did not stop in time")
  }
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
  t := time.NewTimer(d)
  defer t.Stop()
  select {
  case <-ctx.Done():
    return false
  case <-t.C:
    return true
  }
}

// ---------------------------------------------------------------------------
// Topics and producers
// ---------------------------------------------------------------------------

var (
  symRe    = regexp.MustCompile(`^[A-Z0-9^][A-Z0-9^=.\-]{0,23}$`)
  equityRe = regexp.MustCompile(`^[A-Z][A-Z0-9.\-]{0,9}$`)
  cikRe    = regexp.MustCompile(`^[0-9]{1,10}$`)

  validRanges = map[string]bool{
    "1d": true, "5d": true, "1mo": true, "3mo": true, "6mo": true, "ytd": true,
    "1y": true, "2y": true, "5y": true, "10y": true, "max": true,
  }
  validIntervals = map[string]bool{
    "1m": true, "2m": true, "5m": true, "15m": true, "30m": true, "60m": true, "90m": true,
    "1h": true, "1d": true, "5d": true, "1wk": true, "1mo": true, "3mo": true,
  }
  statementTypes   = map[string]bool{"INCOME_STATEMENT": true, "BALANCE_SHEET": true, "CASH_FLOW": true}
  statementPeriods = map[string]bool{"ANNUAL": true, "QUARTERLY": true, "TTM": true}
)

func normSym(s string) (string, bool) {
  s = strings.ToUpper(strings.TrimSpace(s))
  return s, symRe.MatchString(s)
}

func intraday(interval string) bool {
  return strings.HasSuffix(interval, "m") && interval != "1mo" && interval != "3mo" || interval == "1h"
}

func chartTTL(interval string) time.Duration {
  if intraday(interval) {
    return 30 * time.Second
  }
  return 10 * time.Minute
}

// parseTopic maps a client topic onto a shared source.
func (s *server) parseTopic(topic string) (topicSpec, error) {
  parts := strings.Split(strings.TrimSpace(topic), ":")
  kind := strings.ToLower(parts[0])
  arg := func(i int) string {
    if i < len(parts) {
      return strings.TrimSpace(parts[i])
    }
    return ""
  }
  needSym := func() (string, error) {
    sym, ok := normSym(arg(1))
    if !ok {
      return "", fmt.Errorf("topic %q: invalid symbol", topic)
    }
    return sym, nil
  }
  switch kind {
  case "quotes":
    sym, err := needSym()
    if err != nil {
      return topicSpec{}, err
    }
    return topicSpec{source: "quotes", need: "sym:" + sym, arg: sym, maxLast: 5000, produce: s.quoteProducer}, nil

  case "trades", "depth":
    sym, err := needSym()
    if err != nil {
      return topicSpec{}, err
    }
    ent := "Consolidated Tape (CTA/UTP) tick feed"
    if kind == "depth" {
      ent = "Level 2 depth (TotalView / depth-of-book)"
    }
    data, _ := json.Marshal(map[string]any{
      "topic": kind, "symbol": sym, "quality": qUnavailable, "entitlementRequired": ent,
      "message": "Real exchange feed not configured. Qube never simulates trades or book depth.",
    })
    p := Provenance{Source: "terminal", Endpoint: kind, Key: sym, RetrievedAt: nowRFC3339(), Quality: qUnavailable, Error: "entitlement required"}
    return topicSpec{static: &frame{ID: sym, Data: data, Provenance: &p, ServerTimestamp: time.Now().UnixMilli()}}, nil

  case "chart":
    sym, err := needSym()
    if err != nil {
      return topicSpec{}, err
    }
    rng, iv := strings.ToLower(arg(2)), strings.ToLower(arg(3))
    if rng == "" {
      rng = "1y"
    }
    if iv == "" {
      iv = "1d"
    }
    if !validRanges[rng] || !validIntervals[iv] {
      return topicSpec{}, fmt.Errorf("topic %q: invalid range or interval", topic)
    }
    every := "5m"
    if intraday(iv) {
      every = "30s"
    }
    key := "chart:" + sym + ":" + rng + ":" + iv
    args := []string{"chart", "-every", every, "-range", rng, "-interval", iv, sym}
    return topicSpec{source: key, maxLast: 1, produce: s.procProducer("yahoo", args, chartFrame)}, nil

  case "keystats", "fundamentals":
    sym, err := needSym()
    if err != nil {
      return topicSpec{}, err
    }
    if !equityRe.MatchString(sym) {
      return topicSpec{}, fmt.Errorf("topic %q: not an equity symbol", topic)
    }
    args := []string{"keystats", "-every", "30m", sym}
    return topicSpec{source: "keystats:" + sym, maxLast: 1, produce: s.procProducer("yahoo", args, passFrame)}, nil

  case "analyst":
    sym, err := needSym()
    if err != nil {
      return topicSpec{}, err
    }
    if !equityRe.MatchString(sym) {
      return topicSpec{}, fmt.Errorf("topic %q: not an equity symbol", topic)
    }
    args := []string{"analyst", "-every", "6h", sym}
    return topicSpec{source: "analyst:" + sym, maxLast: 1, produce: s.procProducer("ref", args, passFrame)}, nil

  case "statements":
    sym, err := needSym()
    if err != nil {
      return topicSpec{}, err
    }
    if !equityRe.MatchString(sym) && !cikRe.MatchString(sym) {
      return topicSpec{}, fmt.Errorf("topic %q: not an equity symbol or CIK", topic)
    }
    typ, per, lim := strings.ToUpper(arg(2)), strings.ToUpper(arg(3)), arg(4)
    if typ == "" {
      typ = "INCOME_STATEMENT"
    }
    if per == "" {
      per = "ANNUAL"
    }
    n, err := strconv.Atoi(lim)
    if err != nil || n < 1 {
      n = 5
    }
    if n > 12 {
      n = 12
    }
    if !statementTypes[typ] || !statementPeriods[per] {
      return topicSpec{}, fmt.Errorf("topic %q: invalid statement type or period", topic)
    }
    key := fmt.Sprintf("statements:%s:%s:%s:%d", sym, typ, per, n)
    args := []string{"statements", "-every", "6h", "-type", typ, "-period", per, "-limit", strconv.Itoa(n), sym}
    return topicSpec{source: key, maxLast: 1, produce: s.procProducer("sec", args, statementFrame)}, nil

  case "econ":
    week := strings.ToLower(arg(1))
    if week == "" {
      week = "thisweek"
    }
    if week != "thisweek" && week != "nextweek" && week != "lastweek" {
      return topicSpec{}, fmt.Errorf("topic %q: week must be thisweek, nextweek or lastweek", topic)
    }
    args := []string{"econ-calendar", "-every", "30m", "-week", week}
    return topicSpec{source: "econ:" + week, maxLast: 1000, produce: s.procProducer("ref", args, passFrame)}, nil

  case "news", "breaking_news":
    need := ""
    switch a := arg(1); {
    case kind == "breaking_news" || strings.EqualFold(a, "breaking"):
      need = "urgency:high"
    case a != "":
      sym, ok := normSym(a)
      if !ok {
        return topicSpec{}, fmt.Errorf("topic %q: invalid ticker", topic)
      }
      need = "ticker:" + sym
    }
    args := []string{"feed", "-every", "90s", "-limit", "200"}
    return topicSpec{source: "news", need: need, maxLast: 400, produce: s.procProducer("news", args, newsFrame)}, nil

  case "halts":
    args := []string{"halts", "-every", "30s"}
    return topicSpec{source: "halts", maxLast: 1000, produce: s.procProducer("nasdaq", args, passFrame)}, nil
  }
  return topicSpec{}, fmt.Errorf("unknown topic %q", topic)
}

func provPtr(p Provenance) *Provenance { return &p }

// passFrame forwards an envelope unchanged. Failures match every filter.
func passFrame(env Envelope) (*frame, bool) {
  f := &frame{ID: env.Provenance.Key, Data: env.Data, Provenance: provPtr(env.Provenance)}
  if !env.ok() {
    f.tags = []string{"*"}
  }
  return f, true
}

func chartFrame(env Envelope) (*frame, bool) {
  if !env.ok() {
    return passFrame(env)
  }
  m, err := compatChart(env)
  if err != nil {
    return nil, false
  }
  b, err := json.Marshal(m)
  if err != nil {
    return nil, false
  }
  return &frame{ID: env.Provenance.Key, Data: b, Provenance: provPtr(env.Provenance)}, true
}

func statementFrame(env Envelope) (*frame, bool) {
  if !env.ok() {
    return passFrame(env)
  }
  st, err := compatStatement(env)
  if err != nil {
    return nil, false
  }
  b, err := json.Marshal(st)
  if err != nil {
    return nil, false
  }
  return &frame{ID: env.Provenance.Key, Data: b, Provenance: provPtr(env.Provenance)}, true
}

// newsFrame forwards articles tagged by urgency and tickers; per-feed status
// records stay internal.
func newsFrame(env Envelope) (*frame, bool) {
  if env.Provenance.Endpoint != "article" || !env.ok() {
    return nil, false
  }
  var a struct {
    Urgency string   `json:"urgency"`
    Tickers []string `json:"tickers"`
  }
  if json.Unmarshal(env.Data, &a) != nil {
    return nil, false
  }
  tags := []string{"urgency:" + a.Urgency}
  for _, t := range a.Tickers {
    tags = append(tags, "ticker:"+strings.ToUpper(t))
  }
  return &frame{ID: env.Provenance.Key, Data: env.Data, Provenance: provPtr(env.Provenance), tags: tags}, true
}

// procProducer keeps one long-running `-every` collector process alive for a
// source and restarts it with backoff if it exits.
func (s *server) procProducer(coll string, args []string, tr func(Envelope) (*frame, bool)) func(context.Context, *source) {
  return func(ctx context.Context, src *source) {
    backoff := 2 * time.Second
    for {
      started := time.Now()
      _, err := s.run.exec(ctx, coll, args, func(env Envelope) {
        if f, ok := tr(env); ok {
          src.publish(f, true)
        }
      })
      if ctx.Err() != nil {
        return
      }
      if time.Since(started) > time.Minute {
        backoff = 2 * time.Second
      }
      msg := "collector exited"
      if err != nil {
        msg = err.Error()
      }
      s.log.Warn("collector stream ended", "source", src.key, "err", msg, "retry", backoff)
      p := Provenance{Source: coll, Endpoint: "stream", Key: src.key, RetrievedAt: nowRFC3339(), Quality: qUnavailable, Error: msg}
      src.publish(&frame{ID: "_status", Data: jsonNull, Provenance: &p, tags: []string{"*"}}, false)
      if !sleepCtx(ctx, backoff) {
        return
      }
      backoff *= 2
      if backoff > time.Minute {
        backoff = time.Minute
      }
    }
  }
}

// quoteProducer polls the union of subscribed symbols with one batched
// `yahoo quotes` run per tick (shared with the watchlist route's cache) and
// publishes only quotes that changed.
func (s *server) quoteProducer(ctx context.Context, src *source) {
  maxAge := s.cfg.quoteEvery - s.cfg.quoteEvery/10
  for {
    if syms := src.args(); len(syms) > 0 {
      res := s.quotes(ctx, syms, maxAge)
      if ctx.Err() != nil {
        return
      }
      for _, sym := range syms {
        if env, ok := res[sym]; ok {
          src.publish(quoteFrame(sym, env), true)
        }
      }
    }
    t := time.NewTimer(s.cfg.quoteEvery)
    select {
    case <-ctx.Done():
      t.Stop()
      return
    case <-src.wake:
      t.Stop()
    case <-t.C:
    }
  }
}

// ---------------------------------------------------------------------------
// Converters to the TS route shapes
// ---------------------------------------------------------------------------

func providerName(p Provenance) string {
  switch p.Source {
  case "yahoo":
    return "Yahoo Finance"
  case "sec":
    return "SEC-EDGAR"
  case "nasdaq":
    return "Nasdaq"
  case "news":
    return "RSS aggregation"
  case "social":
    return "Social"
  case "ref":
    switch p.Endpoint {
    case "analyst":
      return "stockanalysis.com"
    case "econ-event":
      return "ForexFactory (faireconomy.media)"
    case "transcripts", "transcript", "transcript-meta":
      return "fool.com"
    }
    return "Reference"
  }
  if p.Source == "" {
    return "unknown"
  }
  return p.Source
}

// tsProv renders lib/types/provenance.ts DataProvenance plus the collector
// extras (cacheAgeMs, error, endpoint).
func tsProv(p Provenance, currency string) map[string]any {
  if currency == "" {
    currency = "USD"
  }
  m := map[string]any{
    "sourceProvider":     providerName(p),
    "retrievalTimestamp": p.RetrievedAt,
    "quality":            p.Quality,
    "currency":           currency,
    "source":             p.Source,
    "endpoint":           p.Endpoint,
    "cacheAgeMs":         p.CacheAgeMs,
  }
  if p.Error != "" {
    m["error"] = p.Error
  }
  return m
}

func nilIfEmpty(s string) any {
  if s == "" {
    return nil
  }
  return s
}

// yQuote is the qube-yahoo `quote` record.
type yQuote struct {
  Symbol        string   `json:"symbol"`
  Name          string   `json:"name"`
  Currency      string   `json:"currency"`
  Exchange      string   `json:"exchange"`
  Type          string   `json:"type"`
  Price         *float64 `json:"price"`
  PreviousClose *float64 `json:"previousClose"`
  Change        *float64 `json:"change"`
  ChangePercent *float64 `json:"changePercent"`
  DayHigh       *float64 `json:"dayHigh"`
  DayLow        *float64 `json:"dayLow"`
  Volume        *float64 `json:"volume"`
  High52w       *float64 `json:"high52w"`
  Low52w        *float64 `json:"low52w"`
  MarketTime    string   `json:"marketTime"`
}

// yChart is the qube-yahoo `chart` record (quote fields + columns).
type yChart struct {
  yQuote
  Range        string          `json:"range"`
  Interval     string          `json:"interval"`
  Timestamps   []int64         `json:"timestamps"`
  Open         []*float64      `json:"open"`
  High         []*float64      `json:"high"`
  Low          []*float64      `json:"low"`
  Close        []*float64      `json:"close"`
  AdjClose     []*float64      `json:"adjClose"`
  VolumeSeries []*float64      `json:"volume"`
  Dividends    json.RawMessage `json:"dividends"`
  Splits       json.RawMessage `json:"splits"`
}

// watchItem renders one /api/yfin/watchlist item.
func watchItem(sym string, env Envelope) map[string]any {
  it := map[string]any{
    "symbol": sym, "price": nil, "previousClose": nil, "change": nil, "changePercent": nil,
    "high52w": nil, "low52w": nil, "quality": env.Provenance.Quality,
  }
  var q yQuote
  if env.ok() && json.Unmarshal(env.Data, &q) == nil {
    it["name"] = nilIfEmpty(q.Name)
    it["price"], it["previousClose"] = q.Price, q.PreviousClose
    it["change"], it["changePercent"] = q.Change, q.ChangePercent
    it["high52w"], it["low52w"] = q.High52w, q.Low52w
    it["dayHigh"], it["dayLow"], it["volume"] = q.DayHigh, q.DayLow, q.Volume
    it["currency"], it["exchange"] = nilIfEmpty(q.Currency), nilIfEmpty(q.Exchange)
    it["marketTime"] = nilIfEmpty(q.MarketTime)
  }
  if env.Provenance.Error != "" {
    it["error"] = env.Provenance.Error
  }
  return it
}

// quoteFrame renders the TS v2/stream QuoteFrame. The hash covers only the
// collector data and quality, so an unchanged quote is not pushed again.
func quoteFrame(sym string, env Envelope) *frame {
  qf := map[string]any{
    "symbol": sym, "price": nil, "previousClose": nil, "change": nil, "changePercent": nil,
    "currency": nil, "exchange": nil, "retrievalTimestamp": env.Provenance.RetrievedAt, "quality": env.Provenance.Quality,
  }
  var q yQuote
  if env.ok() && json.Unmarshal(env.Data, &q) == nil {
    qf["price"], qf["previousClose"] = q.Price, q.PreviousClose
    qf["change"], qf["changePercent"] = q.Change, q.ChangePercent
    qf["currency"], qf["exchange"] = nilIfEmpty(q.Currency), nilIfEmpty(q.Exchange)
    qf["dayHigh"], qf["dayLow"], qf["volume"] = q.DayHigh, q.DayLow, q.Volume
    qf["high52w"], qf["low52w"] = q.High52w, q.Low52w
    qf["marketTime"] = nilIfEmpty(q.MarketTime)
  }
  b, _ := json.Marshal(qf)
  p := env.Provenance
  return &frame{
    ID: sym, Data: b, Provenance: &p, tags: []string{"sym:" + sym},
    hash: hashBytes(env.Data, []byte(p.Quality), []byte(p.Error)),
  }
}

// compatChart renders the TS chart shape: meta fields plus column arrays.
// Bars without a close are dropped (instead of becoming 0 as in the TS code),
// missing open/high/low fall back to the close, and missing volume is 0.
func compatChart(env Envelope) (map[string]any, error) {
  var c yChart
  if err := json.Unmarshal(env.Data, &c); err != nil {
    return nil, err
  }
  at := func(a []*float64, i int) *float64 {
    if i < len(a) {
      return a[i]
    }
    return nil
  }
  or := func(v *float64, def float64) float64 {
    if v == nil {
      return def
    }
    return *v
  }
  n := len(c.Timestamps)
  ts := make([]int64, 0, n)
  o, h, l, cl, v := make([]float64, 0, n), make([]float64, 0, n), make([]float64, 0, n), make([]float64, 0, n), make([]float64, 0, n)
  var adj []float64
  if len(c.AdjClose) > 0 {
    adj = make([]float64, 0, n)
  }
  for i, t := range c.Timestamps {
    cv := at(c.Close, i)
    if cv == nil {
      continue
    }
    ts = append(ts, t)
    cl = append(cl, *cv)
    o = append(o, or(at(c.Open, i), *cv))
    h = append(h, or(at(c.High, i), *cv))
    l = append(l, or(at(c.Low, i), *cv))
    v = append(v, or(at(c.VolumeSeries, i), 0))
    if adj != nil {
      adj = append(adj, or(at(c.AdjClose, i), *cv))
    }
  }
  m := map[string]any{
    "symbol":        c.Symbol,
    "name":          nilIfEmpty(c.Name),
    "currency":      nilIfEmpty(c.Currency),
    "exchange":      nilIfEmpty(c.Exchange),
    "price":         c.Price,
    "previousClose": c.PreviousClose,
    "change":        c.Change,
    "changePercent": c.ChangePercent,
    "high52w":       c.High52w,
    "low52w":        c.Low52w,
    "marketTime":    nilIfEmpty(c.MarketTime),
    "range":         c.Range,
    "interval":      c.Interval,
    "timestamps":    ts,
    "open":          o,
    "high":          h,
    "low":           l,
    "close":         cl,
    "volume":        v,
    "provenance":    tsProv(env.Provenance, c.Currency),
  }
  if adj != nil {
    m["adjClose"] = adj
  }
  if len(c.Dividends) > 0 && string(c.Dividends) != "null" {
    m["dividends"] = c.Dividends
  }
  if len(c.Splits) > 0 && string(c.Splits) != "null" {
    m["splits"] = c.Splits
  }
  return m, nil
}

// compatStatement adds the TS per-statement provenance to a qube-sec statement.
func compatStatement(env Envelope) (map[string]any, error) {
  var st map[string]any
  if err := json.Unmarshal(env.Data, &st); err != nil {
    return nil, err
  }
  cur, _ := st["currency"].(string)
  st["provenance"] = tsProv(env.Provenance, cur)
  return st, nil
}

// article is the qube-news `article` record (NewsArticle in contracts.ts).
type article struct {
  ID           string   `json:"id"`
  Title        string   `json:"title"`
  URL          string   `json:"url"`
  Source       string   `json:"source"`
  Category     string   `json:"category"`
  Urgency      string   `json:"urgency"`
  PublishedAt  string   `json:"publishedAt"`
  Summary      string   `json:"summary"`
  Tickers      []string `json:"tickers"`
  TickerSource string   `json:"tickerSource,omitempty"`
  published    time.Time
}

// newsFilter mirrors the news collector's feedFilter (and NewsQuery).
type newsFilter struct {
  tickers    map[string]bool
  keyword    string
  exclude    []string
  categories map[string]bool
  sources    map[string]bool
  urgency    map[string]bool
}

func splitList(s string) []string {
  var out []string
  seen := map[string]bool{}
  for _, p := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' }) {
    if p = strings.TrimSpace(p); p != "" && !seen[p] {
      seen[p] = true
      out = append(out, p)
    }
  }
  return out
}

func setOf(s string, upper bool) map[string]bool {
  m := map[string]bool{}
  for _, x := range splitList(s) {
    if upper {
      m[strings.ToUpper(x)] = true
    } else {
      m[strings.ToLower(x)] = true
    }
  }
  return m
}

func newsFilterFrom(q url.Values) *newsFilter {
  f := &newsFilter{
    tickers:    setOf(q.Get("tickers"), true),
    keyword:    strings.ToLower(strings.TrimSpace(q.Get("q"))),
    categories: setOf(q.Get("categories"), false),
    sources:    setOf(q.Get("sources"), false),
    urgency:    setOf(q.Get("urgency"), false),
  }
  for x := range setOf(q.Get("exclude"), false) {
    f.exclude = append(f.exclude, x)
  }
  return f
}

func (f *newsFilter) keep(a *article) bool {
  if len(f.tickers) > 0 && !f.tickers["ALL"] {
    hit := false
    for _, t := range a.Tickers {
      hit = hit || f.tickers[strings.ToUpper(t)]
    }
    if !hit {
      return false
    }
  }
  lt, ls := strings.ToLower(a.Title), strings.ToLower(a.Summary)
  if f.keyword != "" && !strings.Contains(lt, f.keyword) && !strings.Contains(ls, f.keyword) {
    return false
  }
  for _, x := range f.exclude {
    if strings.Contains(lt, x) {
      return false
    }
  }
  if len(f.categories) > 0 && !f.categories["all"] && !f.categories[strings.ToLower(a.Category)] {
    return false
  }
  if len(f.sources) > 0 && !f.sources["all"] && !f.sources[strings.ToLower(a.Source)] {
    return false
  }
  if len(f.urgency) > 0 && !f.urgency[strings.ToLower(a.Urgency)] {
    return false
  }
  return true
}

// ---------------------------------------------------------------------------
// TS-compatible bodies for the DENSE panels. Each returns (status, body) so
// the HTTP routes and the /api/v2/dense snapshot share them.
// ---------------------------------------------------------------------------

func errStatus(msg string) int {
  if strings.Contains(msg, "HTTP 429") || strings.Contains(strings.ToLower(msg), "rate limit") {
    return http.StatusTooManyRequests
  }
  return http.StatusBadGateway
}

func errBody(msg string) map[string]any {
  return map[string]any{"error": msg, "quality": qUnavailable}
}

// watchlistBody: GET /api/yfin/watchlist?symbols=
func (s *server) watchlistBody(ctx context.Context, raw []string) (int, any) {
  if len(raw) > 200 {
    raw = raw[:200]
  }
  var valid []string
  for _, r := range raw {
    if sym, ok := normSym(r); ok {
      valid = append(valid, sym)
    }
  }
  res := s.quotes(ctx, valid, 5*time.Second)
  items := make([]any, 0, len(raw))
  for _, r := range raw {
    sym, ok := normSym(r)
    if !ok {
      items = append(items, watchItem(r, failEnv("terminal", "quote", r, errors.New("invalid symbol"))))
      continue
    }
    items = append(items, watchItem(sym, res[sym]))
  }
  return http.StatusOK, map[string]any{"items": items}
}

// chartBody: GET /api/yfin/chart/{sym}?range&interval&events
func (s *server) chartBody(rawSym, rng, iv, events string) (int, any) {
  sym, ok := normSym(rawSym)
  if !ok {
    return http.StatusBadRequest, map[string]any{"error": "invalid symbol"}
  }
  if rng == "" {
    rng = "6mo"
  }
  if iv == "" {
    iv = "1d"
  }
  rng, iv = strings.ToLower(rng), strings.ToLower(iv)
  if !validRanges[rng] || !validIntervals[iv] {
    return http.StatusBadRequest, map[string]any{"error": "invalid range or interval"}
  }
  var evs []string
  for _, e := range splitList(strings.ToLower(events)) {
    if e != "div" && e != "split" {
      return http.StatusBadRequest, map[string]any{"error": "invalid events (div, split)"}
    }
    evs = append(evs, e)
  }
  args := []string{"chart", "-range", rng, "-interval", iv}
  if len(evs) > 0 {
    args = append(args, "-events", strings.Join(evs, ","))
  }
  args = append(args, sym)
  cmd := s.collRef("yahoo", args...)
  envs, err := s.cached(chartTTL(iv), "yahoo", args...)
  if err != nil {
    return errStatus(err.Error()), tagColl(errBody(err.Error()), cmd)
  }
  env, ok := pick(envs, sym)
  if !ok || !env.ok() {
    msg := firstError(envs)
    return errStatus(msg), tagColl(errBody(msg), cmd)
  }
  m, err := compatChart(env)
  if err != nil {
    return http.StatusBadGateway, tagColl(errBody("decode chart: "+err.Error()), cmd)
  }
  return http.StatusOK, m
}

// singleBody runs a one-record collector command and decodes it into a map
// with TS provenance attached.
func (s *server) singleBody(ttl time.Duration, key string, coll string, args ...string) (int, any) {
  cmd := s.collRef(coll, args...)
  envs, err := s.cached(ttl, coll, args...)
  if err != nil {
    return errStatus(err.Error()), tagColl(errBody(err.Error()), cmd)
  }
  env, ok := pick(envs, key)
  if !ok || !env.ok() {
    msg := firstError(envs)
    return errStatus(msg), tagColl(errBody(msg), cmd)
  }
  var m map[string]any
  if err := json.Unmarshal(env.Data, &m); err != nil {
    return http.StatusBadGateway, tagColl(errBody("decode: "+err.Error()), cmd)
  }
  cur, _ := m["currency"].(string)
  m["provenance"] = tsProv(env.Provenance, cur)
  return http.StatusOK, m
}

// fundamentalsBody: GET /api/yfin/fundamentals?symbol=
func (s *server) fundamentalsBody(rawSym string) (int, any) {
  sym := strings.ToUpper(strings.TrimSpace(rawSym))
  if sym == "" {
    sym = "AAPL"
  }
  if !equityRe.MatchString(sym) {
    return http.StatusBadRequest, map[string]any{"error": "Invalid symbol"}
  }
  return s.singleBody(time.Hour, sym, "yahoo", "keystats", sym)
}

// analystBody: GET /api/yfin/analyst-ratings?symbol=
func (s *server) analystBody(rawSym string) (int, any) {
  sym := strings.ToUpper(strings.TrimSpace(rawSym))
  if sym == "" {
    sym = "AAPL"
  }
  if !equityRe.MatchString(sym) {
    return http.StatusBadRequest, map[string]any{"error": "Invalid symbol"}
  }
  return s.singleBody(6*time.Hour, sym, "ref", "analyst", sym)
}

// econBody: GET /api/yfin/economic-calendar[?week=thisweek|nextweek|lastweek]
// The TS route answered {items: []} on failure; that is kept, plus provenance.
func (s *server) econBody(week string) (int, any) {
  week = strings.ToLower(strings.TrimSpace(week))
  if week == "" {
    week = "thisweek"
  }
  if week != "thisweek" && week != "nextweek" && week != "lastweek" {
    return http.StatusBadRequest, map[string]any{"error": "week must be thisweek, nextweek or lastweek"}
  }
  envs, err := s.cached(time.Hour, "ref", "econ-calendar", "-week", week)
  if err != nil {
    return http.StatusOK, tagColl(map[string]any{"items": []any{}, "quality": qUnavailable, "error": err.Error(),
      "provenance": tsProv(Provenance{Source: "ref", Endpoint: "econ-event", RetrievedAt: nowRFC3339(), Quality: qUnavailable, Error: err.Error()}, "")},
      collectorOf(err))
  }
  type ev struct {
    raw  json.RawMessage
    date string
  }
  var evs []ev
  var prov Provenance
  for _, e := range envs {
    if !e.ok() {
      continue
    }
    var d struct {
      Date string `json:"date"`
    }
    json.Unmarshal(e.Data, &d)
    evs = append(evs, ev{raw: e.Data, date: d.Date})
    prov = e.Provenance
  }
  sort.SliceStable(evs, func(i, j int) bool { return evs[i].date < evs[j].date })
  items := make([]json.RawMessage, len(evs))
  for i, e := range evs {
    items[i] = e.raw
  }
  prov.Key = ""
  return http.StatusOK, map[string]any{"items": items, "provenance": tsProv(prov, "")}
}

// statementsBody: GET /api/v2/statements?symbol&type&period&limit
func (s *server) statementsBody(rawSym, typ, per, lim string) (int, any) {
  sym := strings.ToUpper(strings.TrimSpace(rawSym))
  if sym == "" {
    return http.StatusBadRequest, map[string]any{"error": "symbol required"}
  }
  if !equityRe.MatchString(sym) && !cikRe.MatchString(sym) {
    return http.StatusBadRequest, map[string]any{"error": "invalid symbol"}
  }
  typ, per = strings.ToUpper(typ), strings.ToUpper(per)
  if typ == "" {
    typ = "INCOME_STATEMENT"
  }
  if per == "" {
    per = "ANNUAL"
  }
  if !statementTypes[typ] {
    return http.StatusBadRequest, map[string]any{"error": "invalid type"}
  }
  if !statementPeriods[per] {
    return http.StatusBadRequest, map[string]any{"error": "invalid period"}
  }
  n, err := strconv.Atoi(lim)
  if err != nil || n < 1 {
    n = 5
  }
  if n > 8 {
    n = 8
  }
  args := []string{"statements", "-type", typ, "-period", per, "-limit", strconv.Itoa(n), sym}
  cmd := s.collRef("sec", args...)
  envs, err := s.cached(6*time.Hour, "sec", args...)
  if err != nil {
    return errStatus(err.Error()), tagColl(errBody(err.Error()), cmd)
  }
  env, ok := pick(envs, sym+":"+typ+":"+per)
  if !ok || !env.ok() {
    msg := firstError(envs)
    return errStatus(msg), tagColl(errBody(msg), cmd)
  }
  st, err := compatStatement(env)
  if err != nil {
    return http.StatusBadGateway, tagColl(errBody("decode statement: "+err.Error()), cmd)
  }
  return http.StatusOK, map[string]any{"statements": []any{st}, "provenance": st["provenance"]}
}

// newsSnapshot returns the de-duplicated headline set, newest first. The
// collector run is shared and cached for 60s; filtering happens here.
func (s *server) newsSnapshot() ([]*article, Provenance, error) {
  envs, err := s.cached(60*time.Second, "news", "feed", "-limit", "200")
  if err != nil {
    return nil, Provenance{}, err
  }
  var out []*article
  var prov Provenance
  for _, e := range envs {
    if e.Provenance.Endpoint != "article" || !e.ok() {
      continue
    }
    a := &article{}
    if json.Unmarshal(e.Data, a) != nil {
      continue
    }
    a.published, _ = time.Parse(time.RFC3339, a.PublishedAt)
    out = append(out, a)
    prov = e.Provenance
  }
  if len(out) == 0 {
    return nil, Provenance{}, &collErr{ref: s.collRef("news", "feed", "-limit", "200"), err: errors.New("no articles from any feed")}
  }
  sort.SliceStable(out, func(i, j int) bool { return out[i].published.After(out[j].published) })
  prov.Key = ""
  return out, prov, nil
}

// newsBody: GET /api/v2/news?tickers&q&exclude&categories&sources&limit
func (s *server) newsBody(q url.Values) (int, any) {
  limit, err := strconv.Atoi(q.Get("limit"))
  if err != nil || limit < 1 {
    limit = 40
  }
  if limit > 100 {
    limit = 100
  }
  all, prov, err := s.newsSnapshot()
  if err != nil {
    return http.StatusBadGateway, tagColl(map[string]any{"error": err.Error(), "items": []any{}, "total": 0, "quality": qUnavailable}, collectorOf(err))
  }
  f := newsFilterFrom(q)
  items := make([]*article, 0, limit)
  total := 0
  for _, a := range all {
    if !f.keep(a) {
      continue
    }
    total++
    if len(items) < limit {
      items = append(items, a)
    }
  }
  return http.StatusOK, map[string]any{"items": items, "total": total, "provenance": tsProv(prov, "")}
}

// yfinNewsBody: GET /api/yfin/news (legacy {title, link, pubDate, source} list)
func (s *server) yfinNewsBody() (int, any) {
  all, _, err := s.newsSnapshot()
  if err != nil {
    return http.StatusOK, map[string]any{"items": []any{}}
  }
  items := make([]map[string]any, 0, 30)
  for _, a := range all {
    if len(items) == 30 {
      break
    }
    items = append(items, map[string]any{"title": a.Title, "link": a.URL, "pubDate": a.PublishedAt, "source": a.Source})
  }
  return http.StatusOK, map[string]any{"items": items}
}

// respLog writes "error response" lines to stderr; main replaces it with the
// server logger once the log level is known.
var respLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{ReplaceAttr: dropTime}))

// logRespErr mirrors an error sent to the frontend onto stderr: any status
// >= 400, or a body carrying a non-empty top-level "error" string. Bodies
// wrapped by tagColl also name the collector command that failed.
func logRespErr(r *http.Request, status int, body any, attrs ...any) {
  var ref collRef
  if t, ok := body.(taggedBody); ok {
    body, ref = t.body, t.ref
  }
  msg := ""
  if m, ok := body.(map[string]any); ok {
    if e, ok := m["error"].(string); ok {
      msg = e
    }
  }
  if msg == "" && status < 400 {
    return
  }
  level := slog.LevelWarn
  if status >= 500 {
    level = slog.LevelError
  }
  args := []any{"status", status}
  if ref.cmd != "" {
    args = append(args, "collector", ref.cmd, "args", ref.args)
  }
  args = append(args, "error", msg)
  if r != nil {
    args = append(args, "path", r.URL.Path, "query", r.URL.RawQuery, "remote", r.RemoteAddr)
  }
  args = append(args, attrs...)
  respLog.Log(context.Background(), level, "error response", args...)
}

// httpError is http.Error plus logRespErr.
func httpError(w http.ResponseWriter, r *http.Request, msg string, status int) {
  logRespErr(r, status, map[string]any{"error": msg})
  http.Error(w, msg, status)
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
  b, err := json.Marshal(v)
  if err != nil {
    logRespErr(r, http.StatusInternalServerError, map[string]any{"error": "encode failed: " + err.Error()})
    status, b = http.StatusInternalServerError, []byte(`{"error":"encode failed"}`)
  } else {
    logRespErr(r, status, v)
  }
  h := w.Header()
  h.Set("Content-Type", "application/json; charset=utf-8")
  h.Set("Cache-Control", "no-store")
  w.WriteHeader(status)
  w.Write(b)
  w.Write([]byte("\n"))
}

func (s *server) handleWatchlist(w http.ResponseWriter, r *http.Request) {
  syms := splitList(r.URL.Query().Get("symbols"))
  if len(syms) == 0 {
    writeJSON(w, r, http.StatusOK, map[string]any{"items": []any{}})
    return
  }
  st, body := s.watchlistBody(r.Context(), syms)
  writeJSON(w, r, st, body)
}

func (s *server) handleChart(w http.ResponseWriter, r *http.Request) {
  sym := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/yfin/chart/"), "/")
  q := r.URL.Query()
  st, body := s.chartBody(sym, q.Get("range"), q.Get("interval"), q.Get("events"))
  writeJSON(w, r, st, body)
}

func (s *server) handleFundamentals(w http.ResponseWriter, r *http.Request) {
  st, body := s.fundamentalsBody(r.URL.Query().Get("symbol"))
  writeJSON(w, r, st, body)
}

func (s *server) handleAnalyst(w http.ResponseWriter, r *http.Request) {
  st, body := s.analystBody(r.URL.Query().Get("symbol"))
  writeJSON(w, r, st, body)
}

func (s *server) handleEcon(w http.ResponseWriter, r *http.Request) {
  st, body := s.econBody(r.URL.Query().Get("week"))
  writeJSON(w, r, st, body)
}

func (s *server) handleStatements(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  st, body := s.statementsBody(q.Get("symbol"), q.Get("type"), q.Get("period"), q.Get("limit"))
  writeJSON(w, r, st, body)
}

func (s *server) handleNews(w http.ResponseWriter, r *http.Request) {
  st, body := s.newsBody(r.URL.Query())
  writeJSON(w, r, st, body)
}

func (s *server) handleYfinNews(w http.ResponseWriter, r *http.Request) {
  st, body := s.yfinNewsBody()
  writeJSON(w, r, st, body)
}

// ---------------------------------------------------------------------------
// Extra routes: the remaining free collector subcommands (SEC, Nasdaq, ref,
// news, social, yahoo). Multi-record commands answer
// {items: [data...], count, provenance}; single-record commands answer the
// record itself plus provenance (singleBody). Nothing is ever simulated: a
// failed upstream is an error body with quality UNAVAILABLE.
// ---------------------------------------------------------------------------

var (
  formsRe     = regexp.MustCompile(`^[0-9A-Z/\-]{1,12}(,[0-9A-Z/\-]{1,12}){0,11}$`)
  accessionRe = regexp.MustCompile(`^[0-9]{10}-?[0-9]{2}-?[0-9]{6}$`)
  docNameRe   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._\-]{0,127}$`)
  monthRe     = regexp.MustCompile(`^[0-9]{4}-(0[1-9]|1[0-2])$`)
  dateRe      = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
  transcrRe   = regexp.MustCompile(`^(mf:)?/[A-Za-z0-9/_.\-]{1,200}$`)
  subsRe      = regexp.MustCompile(`^[A-Za-z0-9_]{2,21}(,[A-Za-z0-9_]{2,21}){0,4}$`)

  ipoMarkets = map[string]bool{"upcoming": true, "priced": true, "filed": true, "withdrawn": true, "all": true}

  // market-movers ?type= -> Yahoo predefined screener id
  moverScreens = map[string]string{
    "gainers":       "day_gainers",
    "losers":        "day_losers",
    "active":        "most_actives",
    "unusual":       "most_actives",
    "small_gainers": "small_cap_gainers",
    "undervalued":   "undervalued_large_caps",
    "growth_tech":   "growth_technology_stocks",
  }
)

// qInt reads an integer query parameter clamped to [lo, hi].
func qInt(q url.Values, key string, def, lo, hi int) int {
  n, err := strconv.Atoi(strings.TrimSpace(q.Get(key)))
  if err != nil {
    n = def
  }
  if n < lo {
    n = lo
  }
  if n > hi {
    n = hi
  }
  return n
}

// listBody runs a multi-record collector command through the cache and
// returns the data of every usable record whose endpoint matches ("" = all).
// emptyOK: a run without records is an empty list, not an error (halts, IPOs).
func (s *server) listBody(ttl time.Duration, endpoint string, emptyOK bool, coll string, args ...string) (int, any) {
  cmd := s.collRef(coll, args...)
  envs, err := s.cached(ttl, coll, args...)
  if err != nil {
    msg := err.Error()
    if emptyOK && strings.HasSuffix(msg, ": no records") {
      prov := Provenance{Source: coll, Endpoint: endpoint, RetrievedAt: nowRFC3339(), Quality: qLive}
      return http.StatusOK, map[string]any{"items": []any{}, "count": 0, "provenance": tsProv(prov, "")}
    }
    return errStatus(msg), tagColl(map[string]any{"error": msg, "items": []any{}, "count": 0, "quality": qUnavailable}, cmd)
  }
  items := make([]json.RawMessage, 0, len(envs))
  var prov Provenance
  failed := 0
  for _, e := range envs {
    if endpoint != "" && e.Provenance.Endpoint != endpoint {
      continue
    }
    if !e.ok() {
      failed++
      continue
    }
    items = append(items, e.Data)
    prov = e.Provenance
  }
  if len(items) == 0 && !emptyOK {
    msg := firstError(envs)
    return errStatus(msg), tagColl(map[string]any{"error": msg, "items": []any{}, "count": 0, "quality": qUnavailable}, cmd)
  }
  if prov.Source == "" {
    prov = Provenance{Source: coll, Endpoint: endpoint, RetrievedAt: nowRFC3339(), Quality: qLive}
  }
  prov.Key = ""
  body := map[string]any{"items": items, "count": len(items), "provenance": tsProv(prov, "")}
  if failed > 0 {
    body["failed"] = failed
  }
  return http.StatusOK, body
}

// tickerOrCIK validates a SEC issuer argument.
func tickerOrCIK(raw string) (string, bool) {
  sym := strings.ToUpper(strings.TrimSpace(raw))
  return sym, equityRe.MatchString(sym) || cikRe.MatchString(sym)
}

// GET /api/sec/filings?ticker=&forms=10-K,10-Q&limit=
func (s *server) handleSecFilings(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  sym, ok := tickerOrCIK(q.Get("ticker"))
  if !ok {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "ticker or CIK required"})
    return
  }
  args := []string{"filings", "-limit", strconv.Itoa(qInt(q, "limit", 40, 1, 200))}
  if forms := strings.ToUpper(strings.ReplaceAll(q.Get("forms"), " ", "")); forms != "" {
    if !formsRe.MatchString(forms) {
      writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid forms"})
      return
    }
    args = append(args, "-forms", forms)
  }
  args = append(args, sym)
  st, body := s.listBody(30*time.Minute, "filing", true, "sec", args...)
  writeJSON(w, r, st, body)
}

// GET /api/v2/filing-doc?cik=&accession=&doc=
func (s *server) handleFilingDoc(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  cik, acc, doc := strings.TrimSpace(q.Get("cik")), strings.TrimSpace(q.Get("accession")), strings.TrimSpace(q.Get("doc"))
  if !cikRe.MatchString(cik) || !accessionRe.MatchString(acc) || !docNameRe.MatchString(doc) {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "cik, accession and doc required"})
    return
  }
  st, body := s.singleBody(24*time.Hour, "", "sec", "filing-doc", "-cik", cik, "-accession", acc, "-doc", doc)
  writeJSON(w, r, st, body)
}

// GET /api/sec/insider?ticker=&limit=   or   ?marketwide=1&limit=
func (s *server) handleSecInsider(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  args := []string{"insider", "-limit", strconv.Itoa(qInt(q, "limit", 25, 1, 100))}
  if mw := q.Get("marketwide"); mw == "1" || strings.EqualFold(mw, "true") {
    args = append(args, "-marketwide")
  } else {
    sym, ok := tickerOrCIK(q.Get("ticker"))
    if !ok {
      writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "ticker or CIK required (or marketwide=1)"})
      return
    }
    args = append(args, sym)
  }
  st, body := s.listBody(15*time.Minute, "insider", true, "sec", args...)
  writeJSON(w, r, st, body)
}

// GET /api/sec/institutional?ticker=
func (s *server) handleSecInstitutional(w http.ResponseWriter, r *http.Request) {
  sym := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("ticker")))
  if !equityRe.MatchString(sym) {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "ticker required"})
    return
  }
  st, body := s.singleBody(12*time.Hour, sym, "sec", "institutional", sym)
  writeJSON(w, r, st, body)
}

// GET /api/sec/13f?cik=&top=   (13F-HR information table of one manager)
func (s *server) handleSec13F(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  cik := strings.TrimSpace(q.Get("cik"))
  if !cikRe.MatchString(cik) {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "numeric manager cik required"})
    return
  }
  top := strconv.Itoa(qInt(q, "top", 100, 1, 1000))
  st, body := s.singleBody(12*time.Hour, "", "sec", "13f", "-top", top, cik)
  writeJSON(w, r, st, body)
}

// GET /api/v2/ipos?month=YYYY-MM&market=upcoming|priced|filed|withdrawn|all
func (s *server) handleIPOs(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  market := strings.ToLower(strings.TrimSpace(q.Get("market")))
  if market == "" {
    market = "all"
  }
  if !ipoMarkets[market] {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid market"})
    return
  }
  args := []string{"ipos", "-market", market}
  if month := strings.TrimSpace(q.Get("month")); month != "" {
    if !monthRe.MatchString(month) {
      writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "month must be YYYY-MM"})
      return
    }
    args = append(args, "-month", month)
  }
  st, body := s.listBody(time.Hour, "ipo", true, "nasdaq", args...)
  writeJSON(w, r, st, body)
}

// GET /api/v2/halts
func (s *server) handleHalts(w http.ResponseWriter, r *http.Request) {
  st, body := s.listBody(30*time.Second, "halt", true, "nasdaq", "halts")
  writeJSON(w, r, st, body)
}

// GET /api/yfin/options-chain?symbol=&date=YYYY-MM-DD
// Yahoo (with IV) first; Nasdaq's chain is the fallback for equities.
func (s *server) handleOptionsChain(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  sym, ok := normSym(q.Get("symbol"))
  if !ok {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid symbol"})
    return
  }
  args := []string{"options"}
  date := strings.TrimSpace(q.Get("date"))
  if date != "" {
    if !dateRe.MatchString(date) {
      writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "date must be YYYY-MM-DD"})
      return
    }
    args = append(args, "-date", date)
  }
  args = append(args, sym)
  st, body := s.singleBody(2*time.Minute, sym, "yahoo", args...)
  if st != http.StatusOK && date == "" && equityRe.MatchString(sym) {
    if st2, body2 := s.singleBody(5*time.Minute, sym, "nasdaq", "options-chain", sym); st2 == http.StatusOK {
      st, body = st2, body2
    }
  }
  writeJSON(w, r, st, body)
}

// GET /api/v2/transcripts?symbol=   (index)   or   ?id=mf:/...   (one call)
func (s *server) handleTranscripts(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  if id := strings.TrimSpace(q.Get("id")); id != "" {
    if !transcrRe.MatchString(id) || strings.Contains(id, "..") {
      writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid transcript id"})
      return
    }
    st, body := s.singleBody(24*time.Hour, "", "ref", "transcript", id)
    writeJSON(w, r, st, body)
    return
  }
  sym := strings.ToUpper(strings.TrimSpace(q.Get("symbol")))
  if !equityRe.MatchString(sym) {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "symbol or id required"})
    return
  }
  st, body := s.listBody(12*time.Hour, "transcript-meta", true, "ref", "transcripts", sym)
  writeJSON(w, r, st, body)
}

// GET /api/yfin/article?url=   readable text of a news article. The news
// collector only fetches the configured publishers (SSRF-guarded).
func (s *server) handleArticle(w http.ResponseWriter, r *http.Request) {
  raw := strings.TrimSpace(r.URL.Query().Get("url"))
  u, err := url.Parse(raw)
  if raw == "" || len(raw) > 2048 || err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "absolute http(s) url required"})
    return
  }
  st, body := s.singleBody(time.Hour, "", "news", "article", u.String())
  writeJSON(w, r, st, body)
}

// GET /api/yfin/fear-greed   (CNN Fear & Greed via the social collector)
func (s *server) handleFearGreed(w http.ResponseWriter, r *http.Request) {
  st, body := s.singleBody(10*time.Minute, "", "social", "feargreed")
  writeJSON(w, r, st, body)
}

// GET /api/wsb/trending[?subs=wallstreetbets,stocks]
func (s *server) handleWSB(w http.ResponseWriter, r *http.Request) {
  args := []string{"reddit"}
  if subs := strings.ReplaceAll(r.URL.Query().Get("subs"), " ", ""); subs != "" {
    if !subsRe.MatchString(subs) {
      writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid subs"})
      return
    }
    args = append(args, "-subs", subs)
  }
  st, body := s.singleBody(5*time.Minute, "", "social", args...)
  writeJSON(w, r, st, body)
}

// GET /api/congress/trades?limit=
func (s *server) handleCongress(w http.ResponseWriter, r *http.Request) {
  lim := strconv.Itoa(qInt(r.URL.Query(), "limit", 150, 1, 1000))
  st, body := s.listBody(6*time.Hour, "congress-trade", false, "social", "congress", "-limit", lim)
  writeJSON(w, r, st, body)
}

// GET /api/yfin/search?q=
func (s *server) handleSearch(w http.ResponseWriter, r *http.Request) {
  q := strings.TrimSpace(r.URL.Query().Get("q"))
  if q == "" || len(q) > 64 || strings.HasPrefix(q, "-") || strings.Contains(q, " -") || strings.ContainsAny(q, "\r\n\t") {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "query required (max 64 chars)"})
    return
  }
  st, body := s.singleBody(10*time.Minute, "", "yahoo", "search", "-count", "10", q)
  writeJSON(w, r, st, body)
}

// GET /api/yfin/market-movers?type=gainers|losers|active|unusual|small_gainers|undervalued|growth_tech
func (s *server) handleMovers(w http.ResponseWriter, r *http.Request) {
  typ := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("type")))
  if typ == "" {
    typ = "gainers"
  }
  scr, ok := moverScreens[typ]
  if !ok {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid type"})
    return
  }
  st, body := s.singleBody(2*time.Minute, "", "yahoo", "screener", "-count", "60", scr)
  if m, ok := body.(map[string]any); ok && st == http.StatusOK {
    m["type"] = typ
  }
  writeJSON(w, r, st, body)
}

// GET /api/yfin/trending[?region=US]
func (s *server) handleTrending(w http.ResponseWriter, r *http.Request) {
  region := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("region")))
  if region == "" {
    region = "US"
  }
  if len(region) != 2 || strings.Trim(region, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") != "" {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid region"})
    return
  }
  st, body := s.singleBody(5*time.Minute, "", "yahoo", "trending", "-region", region, "-count", "20")
  writeJSON(w, r, st, body)
}

// GET /api/v2/quote-batch?symbols=A,B,...   (yahoo quote-batch: v7, 100
// symbols per upstream request, with market cap). Up to quoteBatchMax symbols.
const quoteBatchMax = 1000

func (s *server) handleQuoteBatch(w http.ResponseWriter, r *http.Request) {
  raw := splitList(r.URL.Query().Get("symbols"))
  if len(raw) == 0 || len(raw) > quoteBatchMax {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("1..%d symbols required", quoteBatchMax)})
    return
  }
  args := []string{"quote-batch"}
  for _, x := range raw {
    sym, ok := normSym(x)
    if !ok {
      writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "invalid symbol " + x})
      return
    }
    args = append(args, sym)
  }
  st, body := s.listBody(15*time.Second, "quote", false, "yahoo", args...)
  writeJSON(w, r, st, body)
}

// ---------------------------------------------------------------------------
// Universe: every SEC-registered ticker (EDGAR bulk submissions.zip) joined
// with per-ticker Yahoo profiles (sector, industry, country, market cap).
//
// Disk cache:
//   edgar/submissions/YYYY-MM-DD.zip  raw bulk archive (one paced request,
//                                     re-downloaded after -universe-max-age)
//   edgar/universe/YYYY-MM-DD.json    tickers + SIC etc. parsed from it
//   yahoo/profiles.json               one profile per ticker, fetched once
//   ivv/sp500.json                    S&P 500 members (`ref sp500`), refreshed
//                                     after -sp500-max-age (default 24h)
//
// Profiles of tickers without one are fetched in the background (S&P 500
// first, then NYSE / Nasdaq, then the rest) by `yahoo profile` runs at the
// background gap (-yahoo-bg-gap); the shared Yahoo pacer gives interactive
// requests priority and pauses everything after a rate-limit answer.
//
// GET /api/v2/universe?index=all|sp500&sector=&exchange=&sic=&q=&classified=1
//     &limit=&offset=    the joined list (largest market cap first) + status
// GET /api/v2/universe?refresh=1   re-download today's submissions.zip and
//                                  refresh the S&P 500 list
// GET /api/v2/universe/status      EDGAR state + profile coverage / progress
// Builds run in the background, one at a time; requests never wait for one.
// ---------------------------------------------------------------------------

const (
  submissionsZipURL   = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
  isoDay              = "2006-01-02"
  universeKeep        = 2 // dated files kept per cache directory
  downloadLogEvery    = 256 << 20
  universeLimitMax    = 50000
  universeProgressLog = 100000 // report every N zip entries while parsing
)

var datedFileRe = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})\.(zip|json)$`)

// secUserAgent: "qube-terminal <email>" from the terminal settings
// (settingsString("email")), else $QUBE_SEC_UA.
func secUserAgent() string {
  if email := strings.TrimSpace(settingsString("email")); strings.Contains(email, "@") {
    return "qube-terminal " + email
  }
  return strings.TrimSpace(os.Getenv("QUBE_SEC_UA"))
}

// secPaceWait spaces SEC requests of this process against all sec
// collectors (same lock file protocol as the collector: flock + UnixNano of
// the last request start).
func secPaceWait(ctx context.Context, path string, gap time.Duration) error {
  f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
  if err != nil {
    return err
  }
  defer f.Close()
  if err := common.LockFile(f); err != nil {
    return err
  }
  defer common.UnlockFile(f)
  buf := make([]byte, 32)
  n, _ := f.ReadAt(buf, 0)
  if last, err := strconv.ParseInt(strings.TrimSpace(string(buf[:n])), 10, 64); err == nil && last > 0 {
    d := time.Until(time.Unix(0, last).Add(gap))
    if d > gap {
      d = gap
    }
    if d > 0 {
      t := time.NewTimer(d)
      select {
      case <-ctx.Done():
        t.Stop()
        return ctx.Err()
      case <-t.C:
      }
    }
  }
  if err := f.Truncate(0); err == nil {
    f.WriteAt([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)), 0)
  }
  return nil
}

// datedFiles returns the YYYY-MM-DD.<ext> names in dir, newest first.
func datedFiles(dir, ext string) []string {
  ents, err := os.ReadDir(dir)
  if err != nil {
    return nil
  }
  var out []string
  for _, e := range ents {
    if m := datedFileRe.FindStringSubmatch(e.Name()); m != nil && m[2] == ext && !e.IsDir() {
      out = append(out, e.Name())
    }
  }
  sort.Sort(sort.Reverse(sort.StringSlice(out)))
  return out
}

func dayOf(name string) string {
  if m := datedFileRe.FindStringSubmatch(name); m != nil {
    return m[1]
  }
  return ""
}

// freshDay: day is today, or no older than maxAge (maxAge 0 = today only).
func freshDay(day string, maxAge time.Duration) bool {
  t, err := time.Parse(isoDay, day)
  if err != nil {
    return false
  }
  today := time.Now().UTC().Format(isoDay)
  return day == today || (maxAge > 0 && time.Since(t) <= maxAge)
}

func pruneDated(dir, ext string, keep int, log *slog.Logger) {
  for i, n := range datedFiles(dir, ext) {
    if i < keep {
      continue
    }
    if err := os.Remove(filepath.Join(dir, n)); err == nil {
      log.Info("universe: removed old cache file", "file", filepath.Join(dir, n))
    }
  }
}

// uniWriteJSON: encoding/json -> os.CreateTemp in the target dir -> Rename.
func uniWriteJSON(fn string, v any) error {
  b, err := json.Marshal(v)
  if err != nil {
    return err
  }
  tmp, err := os.CreateTemp(filepath.Dir(fn), "."+filepath.Base(fn)+".*.tmp")
  if err != nil {
    return err
  }
  if _, err := tmp.Write(b); err != nil {
    tmp.Close()
    os.Remove(tmp.Name())
    return err
  }
  if err := tmp.Close(); err != nil {
    os.Remove(tmp.Name())
    return err
  }
  return os.Rename(tmp.Name(), fn)
}

func uniReadJSON(fn string, v any) error {
  b, err := os.ReadFile(fn)
  if err != nil {
    return err
  }
  return json.Unmarshal(b, v)
}

// secListing is one ticker of one SEC filer (a filer can have several).
type secListing struct {
  Ticker               string `json:"ticker"`
  Exchange             string `json:"exchange,omitempty"`
  CIK                  string `json:"cik"`
  Name                 string `json:"name"`
  EntityType           string `json:"entityType,omitempty"`
  SIC                  string `json:"sic,omitempty"`
  SICDescription       string `json:"sicDescription,omitempty"`
  Category             string `json:"category,omitempty"`
  StateOfIncorporation string `json:"stateOfIncorporation,omitempty"`
  FiscalYearEnd        string `json:"fiscalYearEnd,omitempty"`
}

type secUniverseFile struct {
  Date        string       `json:"date"`
  Source      string       `json:"source"`
  GeneratedAt string       `json:"generatedAt"`
  Filers      int          `json:"filers"`
  Count       int          `json:"count"`
  Items       []secListing `json:"items"`
}

// yProfile is one cached yahoo `profile` record (sector / industry etc.).
type yProfile struct {
  Symbol      string   `json:"symbol"`
  Name        string   `json:"name,omitempty"`
  QuoteType   string   `json:"quoteType,omitempty"`
  Exchange    string   `json:"exchange,omitempty"`
  Currency    string   `json:"currency,omitempty"`
  Sector      string   `json:"sector,omitempty"`
  SectorKey   string   `json:"sectorKey,omitempty"`
  Industry    string   `json:"industry,omitempty"`
  IndustryKey string   `json:"industryKey,omitempty"`
  Country     string   `json:"country,omitempty"`
  Website     string   `json:"website,omitempty"`
  Employees   *float64 `json:"employees,omitempty"`
  MarketCap   *float64 `json:"marketCap,omitempty"`
  NotFound    bool     `json:"notFound,omitempty"`
  FetchedAt   string   `json:"fetchedAt"`
}

// universeItem is one row of /api/v2/universe.
type universeItem struct {
  secListing
  SICDivision string   `json:"sicDivision,omitempty"`
  Sector      string   `json:"sector,omitempty"`
  SectorKey   string   `json:"sectorKey,omitempty"`
  Industry    string   `json:"industry,omitempty"`
  IndustryKey string   `json:"industryKey,omitempty"`
  Country     string   `json:"country,omitempty"`
  QuoteType   string   `json:"quoteType,omitempty"`
  MarketCap   *float64 `json:"marketCap"` // as of the profile fetch
  Currency    string   `json:"currency,omitempty"`
  Profiled    bool     `json:"profiled"` // a Yahoo profile was fetched
  SP500       bool     `json:"sp500,omitempty"`
  SP500Sector string   `json:"sp500Sector,omitempty"` // GICS sector from the S&P 500 list
  SP500Weight *float64 `json:"sp500Weight,omitempty"` // % of IVV (IVV source only)
}

// sicDivision maps a SIC code to its division.
func sicDivision(sic string) string {
  n, err := strconv.Atoi(sic)
  if err != nil || n <= 0 {
    return ""
  }
  switch major := n / 100; {
  case major <= 9:
    return "Agriculture, Forestry, Fishing"
  case major <= 14:
    return "Mining"
  case major <= 17:
    return "Construction"
  case major <= 39:
    return "Manufacturing"
  case major <= 49:
    return "Transportation, Communications, Utilities"
  case major <= 51:
    return "Wholesale Trade"
  case major <= 59:
    return "Retail Trade"
  case major <= 67:
    return "Finance, Insurance, Real Estate"
  case major <= 89:
    return "Services"
  case major <= 99:
    return "Public Administration"
  }
  return ""
}

// normTicker: SEC and Yahoo both use BRK-B; lists often say BRK.B.
func normTicker(t string) string {
  return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(t), ".", "-"))
}

func uniStr(v any) string {
  switch t := v.(type) {
  case string:
    return t
  case float64:
    return strconv.FormatInt(int64(t), 10)
  case nil:
    return ""
  }
  return fmt.Sprint(v)
}

// readSubmissionHead decodes the top-level fields of one CIK##########.json
// and stops early: at an empty "tickers" list (most filers) or at "filings"
// (the bulky tail) once the tickers are known.
func readSubmissionHead(f *zip.File) ([]secListing, error) {
  rc, err := f.Open()
  if err != nil {
    return nil, err
  }
  defer rc.Close()
  dec := json.NewDecoder(bufio.NewReaderSize(rc, 32<<10))
  if t, err := dec.Token(); err != nil || t != json.Delim('{') {
    return nil, fmt.Errorf("%s: not a JSON object", f.Name)
  }
  var (
    cik, sic                                       any
    name, entityType, sicDesc, category, state, fy string
    tickers, exchanges                             []string
    haveTickers                                    bool
  )
  for dec.More() {
    t, err := dec.Token()
    if err != nil {
      return nil, err
    }
    key, _ := t.(string)
    if key == "filings" && haveTickers {
      break
    }
    switch key {
    case "cik":
      err = dec.Decode(&cik)
    case "sic":
      err = dec.Decode(&sic)
    case "name":
      err = dec.Decode(&name)
    case "entityType":
      err = dec.Decode(&entityType)
    case "sicDescription":
      err = dec.Decode(&sicDesc)
    case "category":
      err = dec.Decode(&category)
    case "stateOfIncorporation":
      err = dec.Decode(&state)
    case "fiscalYearEnd":
      err = dec.Decode(&fy)
    case "exchanges":
      err = dec.Decode(&exchanges)
    case "tickers":
      err = dec.Decode(&tickers)
      haveTickers = true
      if err == nil && len(tickers) == 0 {
        return nil, nil
      }
    default:
      var skip json.RawMessage
      err = dec.Decode(&skip)
    }
    if err != nil {
      return nil, err
    }
  }
  if len(tickers) == 0 {
    return nil, nil
  }
  base := secListing{
    CIK: strings.TrimLeft(uniStr(cik), "0"), Name: name, EntityType: entityType,
    SIC: uniStr(sic), SICDescription: sicDesc, Category: category,
    StateOfIncorporation: state, FiscalYearEnd: fy,
  }
  out := make([]secListing, 0, len(tickers))
  for i, tk := range tickers {
    if tk = strings.TrimSpace(tk); tk == "" {
      continue
    }
    l := base
    l.Ticker = strings.ToUpper(tk)
    if i < len(exchanges) {
      l.Exchange = exchanges[i]
    }
    out = append(out, l)
  }
  return out, nil
}

// parseSubmissionsZip extracts every ticker from the bulk archive.
func parseSubmissionsZip(ctx context.Context, fn string, log *slog.Logger, progress func(string)) ([]secListing, int, error) {
  zr, err := zip.OpenReader(fn)
  if err != nil {
    return nil, 0, err
  }
  defer zr.Close()
  var out []secListing
  filers, bad := 0, 0
  for i, f := range zr.File {
    if i%universeProgressLog == 0 {
      if ctx.Err() != nil {
        return nil, filers, ctx.Err()
      }
      progress(fmt.Sprintf("parsing submissions.zip %d/%d", i, len(zr.File)))
    }
    base := filepath.Base(f.Name)
    if !strings.HasPrefix(base, "CIK") || !strings.HasSuffix(base, ".json") || strings.Contains(base, "-submissions-") {
      continue
    }
    ls, err := readSubmissionHead(f)
    if err != nil {
      bad++
      continue
    }
    if len(ls) > 0 {
      filers++
      out = append(out, ls...)
    }
  }
  if bad > 0 {
    log.Warn("universe: unreadable submissions entries", "count", bad)
  }
  if len(out) == 0 {
    return nil, 0, errors.New("submissions.zip: no tickers found")
  }
  return out, filers, nil
}

// Profile enrichment tunables.
const (
  profileRetryAfter    = time.Hour           // after a failed fetch
  profileNotFoundRetry = 30 * 24 * time.Hour // re-ask for unknown symbols
  profileIdleWait      = 10 * time.Minute    // nothing missing: look again
  profileErrorWait     = time.Minute         // after a failed collector run
  profileRunSlack      = 10 * time.Minute    // run timeout beyond chunk*gap
)

// Exchanges whose tickers are enriched before the rest (OTC etc. last).
var profileExchangeRank = map[string]int{"nyse": 0, "nasdaq": 0, "cboe": 1, "nyse american": 1, "nyse arca": 1}

const profileOtherRank = 2 // every exchange not in profileExchangeRank

type universe struct {
  cfg config
  log *slog.Logger
  run *runner

  mu         sync.Mutex
  sec        secUniverseFile
  items      []universeItem
  edgarDate  string
  building   bool
  phase      string
  lastError  string
  startedAt  time.Time
  finishedAt time.Time

  // Yahoo profiles (yahooProfilesFile()), keyed by
  // normTicker. Missing ones are fetched by enrichLoop.
  profiles     map[string]yProfile
  failedAt     map[string]time.Time // transient failures, retried later
  enrichActive bool
  enrichSyms   []string // symbols of the running collector run
  enrichErr    string
  enrichDone   int // profiles fetched since start
  enrichSince  time.Time
  kick         chan struct{}

  // S&P 500 members (sp500ListFile()), refreshed by
  // sp500Loop. spKeys / spInfo are replaced, never mutated.
  sp500     sp500File
  spKeys    []string // normTicker keys, list order
  spInfo    map[string]sp500Member
  sp500Err  string
  sp500Busy bool
  sp500Kick chan struct{}
}

func newUniverse(cfg config, log *slog.Logger, run *runner) *universe {
  return &universe{
    cfg: cfg, log: log, run: run, phase: "idle",
    profiles: map[string]yProfile{}, failedAt: map[string]time.Time{},
    kick: make(chan struct{}, 1), spInfo: map[string]sp500Member{},
    sp500Kick: make(chan struct{}, 1),
  }
}

func (u *universe) setPhase(p string) {
  u.mu.Lock()
  u.phase = p
  u.mu.Unlock()
  u.log.Info("universe: " + p)
}

// start loads the cached universe and profiles, refreshes submissions.zip in
// the background when it is stale (-universe-auto) and starts the profile
// enrichment (-enrich-auto), which begins with the S&P 500 right away, and
// keeps the S&P 500 member list current (-sp500-auto).
func (u *universe) start(ctx context.Context) {
  u.loadCached()
  if u.cfg.sp500Auto {
    go u.sp500Loop(ctx)
  }
  if u.cfg.universeAuto {
    u.trigger(ctx, false)
  }
  if u.cfg.enrichAuto {
    go u.enrichLoop(ctx)
  }
}

// loadCached reads the newest parsed index and the profile cache (no
// network access).
func (u *universe) loadCached() {
  var sec secUniverseFile
  dir := edgarUniverseDir()
  if fs := datedFiles(dir, "json"); len(fs) > 0 {
    if err := uniReadJSON(filepath.Join(dir, fs[0]), &sec); err != nil {
      u.log.Warn("universe: cached index unreadable", "file", fs[0], "err", err)
    }
  }
  var pf struct {
    Items map[string]yProfile `json:"items"`
  }
  if fn := yahooProfilesFile(); common.FileExists(fn) {
    if err := uniReadJSON(fn, &pf); err != nil {
      u.log.Warn("universe: profile cache unreadable", "file", fn, "err", err)
    }
  }
  if fn := sp500ListFile(); common.FileExists(fn) {
    var sp sp500File
    if err := uniReadJSON(fn, &sp); err != nil {
      u.log.Warn("universe: S&P 500 list unreadable", "file", fn, "err", err)
    } else {
      u.setSP500(sp)
    }
  }
  u.mu.Lock()
  for k, p := range pf.Items {
    u.profiles[normTicker(k)] = p
  }
  u.sec = sec
  u.mu.Unlock()
  u.publish()
}

// saveProfiles writes the profile cache atomically.
func (u *universe) saveProfiles() {
  u.mu.Lock()
  items := make(map[string]yProfile, len(u.profiles))
  for k, p := range u.profiles {
    items[k] = p
  }
  u.mu.Unlock()
  doc := map[string]any{"updatedAt": nowRFC3339(), "count": len(items), "items": items}
  if err := uniWriteJSON(yahooProfilesFile(), doc); err != nil {
    u.log.Warn("universe: writing profile cache failed", "err", err)
  }
}

// wake makes enrichLoop look for missing profiles now.
func (u *universe) wake() {
  select {
  case u.kick <- struct{}{}:
  default:
  }
}

// ---------------------------------------------------------------------------
// S&P 500 members: the `ref sp500` record (iShares IVV holdings CSV, else the
// Wikipedia constituents table), cached as sp500ListFile()
// and refreshed after -sp500-max-age. A failed refresh keeps the last list.
// Until the first list arrives nothing is flagged sp500.
// ---------------------------------------------------------------------------

const (
  sp500MinMembers = 480 // a shorter list is a broken source, not a new index
  sp500RunTimeout = 5 * time.Minute
  sp500RetryWait  = time.Hour
)

type sp500Member struct {
  Symbol      string   `json:"symbol"`
  Name        string   `json:"name,omitempty"`
  Sector      string   `json:"sector,omitempty"`      // GICS sector
  SubIndustry string   `json:"subIndustry,omitempty"` // GICS sub-industry (wikipedia)
  Weight      *float64 `json:"weight,omitempty"`      // % of IVV
  Exchange    string   `json:"exchange,omitempty"`
  DateAdded   string   `json:"dateAdded,omitempty"`
  CIK         string   `json:"cik,omitempty"`
}

type sp500File struct {
  FetchedAt       string        `json:"fetchedAt"`
  Source          string        `json:"source"` // "ivv" | "wikipedia"
  SourceURL       string        `json:"sourceUrl"`
  AsOf            string        `json:"asOf,omitempty"`
  Count           int           `json:"count"`
  Members         []sp500Member `json:"members"`
  FallbackReasons []string      `json:"fallbackReasons,omitempty"`
}

// setSP500 installs a member list (normalized, de-duplicated, list order).
func (u *universe) setSP500(f sp500File) {
  keys := make([]string, 0, len(f.Members))
  info := make(map[string]sp500Member, len(f.Members))
  for _, m := range f.Members {
    k := normTicker(m.Symbol)
    if _, dup := info[k]; k == "" || dup {
      continue
    }
    info[k] = m
    keys = append(keys, k)
  }
  u.mu.Lock()
  u.sp500, u.spKeys, u.spInfo = f, keys, info
  u.mu.Unlock()
}

// kickSP500 makes sp500Loop refresh the list now.
func (u *universe) kickSP500() {
  select {
  case u.sp500Kick <- struct{}{}:
  default:
  }
}

// sp500Loop refreshes the list whenever it is older than -sp500-max-age (or
// on kickSP500), retrying failures after sp500RetryWait.
func (u *universe) sp500Loop(ctx context.Context) {
  for ctx.Err() == nil {
    u.mu.Lock()
    fetched := u.sp500.FetchedAt
    u.mu.Unlock()
    var wait time.Duration
    if t, err := time.Parse(time.RFC3339, fetched); err == nil {
      wait = time.Until(t.Add(u.cfg.sp500MaxAge))
    }
    if wait > 0 {
      select {
      case <-ctx.Done():
        return
      case <-u.sp500Kick:
      case <-time.After(wait):
      }
    }
    if err := u.refreshSP500(ctx); err != nil {
      if ctx.Err() != nil {
        return
      }
      u.log.Warn("universe: S&P 500 refresh failed, keeping the last list", "err", err)
      select {
      case <-ctx.Done():
        return
      case <-u.sp500Kick:
      case <-time.After(sp500RetryWait):
      }
    }
  }
}

// refreshSP500 runs `ref sp500` once and installs + caches its list.
func (u *universe) refreshSP500(ctx context.Context) error {
  u.mu.Lock()
  u.sp500Busy = true
  u.mu.Unlock()
  defer func() {
    u.mu.Lock()
    u.sp500Busy = false
    u.mu.Unlock()
  }()
  rctx, cancel := context.WithTimeout(ctx, sp500RunTimeout)
  defer cancel()
  var got *sp500File
  var lastFail string
  _, runErr := u.run.exec(rctx, "ref", []string{"sp500"}, func(e Envelope) {
    if e.Provenance.Endpoint != "sp500" {
      return
    }
    if !e.ok() {
      lastFail = e.Provenance.Error
      return
    }
    var f sp500File
    if err := json.Unmarshal(e.Data, &f); err != nil {
      lastFail = "decode sp500 record: " + err.Error()
      return
    }
    got = &f
  })
  var err error
  switch {
  case got != nil && len(got.Members) < sp500MinMembers:
    err = fmt.Errorf("ref sp500: only %d members", len(got.Members))
  case got != nil:
  case lastFail != "":
    err = errors.New(lastFail)
  case runErr != nil:
    err = runErr
  default:
    err = errors.New("ref sp500: no record")
  }
  u.mu.Lock()
  if err != nil {
    u.sp500Err = err.Error()
  } else {
    u.sp500Err = ""
  }
  u.mu.Unlock()
  if err != nil {
    return err
  }
  got.FetchedAt = nowRFC3339()
  got.Count = len(got.Members)
  if werr := uniWriteJSON(sp500ListFile(), got); werr != nil {
    u.log.Warn("universe: writing S&P 500 list failed", "err", werr)
  }
  u.setSP500(*got)
  u.log.Info("universe: S&P 500 list updated", "source", got.Source, "asOf", got.AsOf, "members", got.Count)
  u.publish()
  u.wake() // new members need profiles
  return nil
}

// needsProfile: no profile yet (or a stale "not found"), and no recent
// transient failure. Caller holds u.mu.
func (u *universe) needsProfile(key string, now time.Time) bool {
  if _, ok := normSym(key); !ok {
    return false
  }
  if p, ok := u.profiles[key]; ok {
    if !p.NotFound {
      return false
    }
    t, err := time.Parse(time.RFC3339, p.FetchedAt)
    if err == nil && now.Sub(t) < profileNotFoundRetry {
      return false
    }
  }
  if t, ok := u.failedAt[key]; ok && now.Sub(t) < profileRetryAfter {
    return false
  }
  return true
}

// pendingProfiles returns up to n symbols to enrich: S&P 500 first, then the
// SEC universe (NYSE / Nasdaq before the others), each in list order.
func (u *universe) pendingProfiles(n int) []string {
  now := time.Now()
  u.mu.Lock()
  defer u.mu.Unlock()
  var out []string
  seen := map[string]bool{}
  add := func(k string) bool {
    if !seen[k] && u.needsProfile(k, now) {
      out = append(out, k)
    }
    seen[k] = true
    return len(out) >= n
  }
  for _, k := range u.spKeys {
    if add(k) {
      return out
    }
  }
  rank := func(ex string) int {
    if r, ok := profileExchangeRank[strings.ToLower(ex)]; ok {
      return r
    }
    return profileOtherRank
  }
  for pass := 0; pass <= profileOtherRank; pass++ {
    for i := range u.sec.Items {
      l := &u.sec.Items[i]
      if rank(l.Exchange) == pass {
        if add(normTicker(l.Ticker)) {
          return out
        }
      }
    }
  }
  return out
}

// enrichLoop fetches missing profiles chunk by chunk with the yahoo
// `profile` collector at the background gap; the shared pacer gives the
// interactive requests priority and makes the run sleep through rate-limit
// pauses. Results are cached after every chunk.
func (u *universe) enrichLoop(ctx context.Context) {
  u.mu.Lock()
  u.enrichSince = time.Now()
  u.mu.Unlock()
  for ctx.Err() == nil {
    syms := u.pendingProfiles(u.cfg.enrichChunk)
    if len(syms) == 0 {
      u.mu.Lock()
      u.enrichActive, u.enrichSyms = false, nil
      u.mu.Unlock()
      select {
      case <-ctx.Done():
        return
      case <-u.kick:
      case <-time.After(profileIdleWait):
      }
      continue
    }
    u.mu.Lock()
    u.enrichActive, u.enrichSyms = true, syms
    u.mu.Unlock()
    err := u.fetchProfiles(ctx, syms)
    u.mu.Lock()
    u.enrichSyms = nil
    if err != nil {
      u.enrichErr = err.Error()
    } else {
      u.enrichErr = ""
    }
    u.mu.Unlock()
    u.saveProfiles()
    u.publish()
    if err != nil {
      u.log.Warn("universe: profile run failed", "err", err)
      select {
      case <-ctx.Done():
        return
      case <-time.After(profileErrorWait):
      }
    }
  }
}

// fetchProfiles runs one `yahoo profile` collector for syms.
func (u *universe) fetchProfiles(ctx context.Context, syms []string) error {
  timeout := time.Duration(len(syms))*u.cfg.yahooBgGap*2 + profileRunSlack
  rctx, cancel := context.WithTimeout(ctx, timeout)
  defer cancel()
  extra := []string{
    "QUBE_YAHOO_MIN_GAP=" + u.cfg.yahooBgGap.String(),
    "QUBE_YAHOO_WAIT_PAUSE=1",
  }
  got := map[string]bool{}
  var lastFail string
  _, runErr := u.run.execEnv(rctx, "yahoo", append([]string{"profile"}, syms...), extra, func(e Envelope) {
    if e.Provenance.Endpoint != "profile" {
      return
    }
    key := normTicker(e.Provenance.Key)
    now := time.Now()
    if !e.ok() {
      lastFail = e.Provenance.Error
      u.mu.Lock()
      u.failedAt[key] = now
      u.mu.Unlock()
      got[key] = true
      return
    }
    var p yProfile
    if json.Unmarshal(e.Data, &p) != nil {
      return
    }
    p.FetchedAt = now.UTC().Format(time.RFC3339)
    u.mu.Lock()
    u.profiles[key] = p
    delete(u.failedAt, key)
    u.enrichDone++
    u.mu.Unlock()
    got[key] = true
  })
  // Symbols without any answer (run cut short) count as transient failures.
  u.mu.Lock()
  for _, k := range syms {
    if !got[k] {
      u.failedAt[k] = time.Now()
    }
  }
  u.mu.Unlock()
  if len(got) == 0 && runErr != nil {
    return runErr
  }
  if lastFail != "" && len(got) > 0 {
    u.log.Debug("universe: some profiles failed", "last", lastFail)
  }
  return nil
}

// trigger starts a background build unless one is running. force: accept
// only today's archive / screen (re-download if needed).
func (u *universe) trigger(ctx context.Context, force bool) bool {
  u.mu.Lock()
  if u.building {
    u.mu.Unlock()
    return false
  }
  u.building, u.startedAt, u.lastError = true, time.Now(), ""
  u.mu.Unlock()
  go func() {
    err := u.build(ctx, force)
    u.mu.Lock()
    u.building, u.finishedAt = false, time.Now()
    if err != nil {
      u.lastError = err.Error()
      u.phase = "failed"
    } else {
      u.phase = "ready"
    }
    u.mu.Unlock()
    if err != nil {
      u.log.Error("universe: build failed", "err", err)
    }
  }()
  return true
}

func (u *universe) build(ctx context.Context, force bool) error {
  sec, err := u.ensureEdgar(ctx, force)
  if err != nil {
    return err
  }
  u.mu.Lock()
  u.sec = sec
  u.mu.Unlock()
  u.publish()
  u.wake() // new tickers may need profiles
  return nil
}

// ensureEdgar returns a parsed ticker index no older than -universe-max-age:
// cached index, else parse a cached zip, else download today's zip.
func (u *universe) ensureEdgar(ctx context.Context, force bool) (secUniverseFile, error) {
  maxAge := u.cfg.universeMaxAge
  if force {
    maxAge = 0
  }
  uniDir := edgarUniverseDir()
  subDir := edgarSubmissionsDir()
  var idx secUniverseFile
  if fs := datedFiles(uniDir, "json"); len(fs) > 0 && freshDay(dayOf(fs[0]), maxAge) {
    if err := uniReadJSON(filepath.Join(uniDir, fs[0]), &idx); err == nil && len(idx.Items) > 0 {
      return idx, nil
    }
  }
  zipName := ""
  if zs := datedFiles(subDir, "zip"); len(zs) > 0 && freshDay(dayOf(zs[0]), maxAge) {
    zipName = zs[0]
  } else {
    zipName = time.Now().UTC().Format(isoDay) + ".zip"
    if err := u.download(ctx, filepath.Join(subDir, zipName)); err != nil {
      return idx, err
    }
    pruneDated(subDir, "zip", universeKeep, u.log)
  }
  items, filers, err := parseSubmissionsZip(ctx, filepath.Join(subDir, zipName), u.log, u.setPhase)
  if err != nil {
    return idx, err
  }
  idx = secUniverseFile{
    Date: dayOf(zipName), Source: submissionsZipURL, GeneratedAt: nowRFC3339(),
    Filers: filers, Count: len(items), Items: items,
  }
  if err := uniWriteJSON(filepath.Join(uniDir, idx.Date+".json"), idx); err != nil {
    u.log.Warn("universe: writing index failed", "err", err)
  }
  pruneDated(uniDir, "json", universeKeep, u.log)
  u.log.Info("universe: edgar index ready", "date", idx.Date, "filers", filers, "tickers", len(items))
  return idx, nil
}

// download fetches submissions.zip (a single, paced SEC request) into fn via
// a temp file in the same directory.
func (u *universe) download(ctx context.Context, fn string) error {
  ua := secUserAgent()
  if ua == "" {
    return fmt.Errorf("SEC needs a contact e-mail: set email in %s (or QUBE_SEC_UA)", settingsYAMLFile())
  }
  ctx, cancel := context.WithTimeout(ctx, u.cfg.universeTimeout)
  defer cancel()
  if err := secPaceWait(ctx, u.cfg.secPaceFile, u.cfg.secMinGap); err != nil {
    u.log.Warn("universe: shared SEC pacing unavailable", "err", err)
  }
  u.setPhase("downloading " + submissionsZipURL)
  req, err := http.NewRequestWithContext(ctx, http.MethodGet, submissionsZipURL, nil)
  if err != nil {
    return err
  }
  req.Header.Set("User-Agent", ua)
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    return err
  }
  defer resp.Body.Close()
  if resp.StatusCode != http.StatusOK {
    return fmt.Errorf("submissions.zip: HTTP %d", resp.StatusCode)
  }
  tmp, err := os.CreateTemp(filepath.Dir(fn), "."+filepath.Base(fn)+".*.part")
  if err != nil {
    return err
  }
  defer os.Remove(tmp.Name()) // no-op after the rename
  var total int64
  next := int64(downloadLogEvery)
  buf := make([]byte, 1<<20)
  for {
    n, rerr := resp.Body.Read(buf)
    if n > 0 {
      if _, werr := tmp.Write(buf[:n]); werr != nil {
        tmp.Close()
        return werr
      }
      total += int64(n)
      if total >= next {
        u.setPhase(fmt.Sprintf("downloading submissions.zip: %d of %d MiB", total>>20, resp.ContentLength>>20))
        next += downloadLogEvery
      }
    }
    if rerr == io.EOF {
      break
    }
    if rerr != nil {
      tmp.Close()
      return rerr
    }
  }
  if resp.ContentLength > 0 && total != resp.ContentLength {
    tmp.Close()
    return fmt.Errorf("submissions.zip: got %d of %d bytes", total, resp.ContentLength)
  }
  if err := tmp.Close(); err != nil {
    return err
  }
  if err := os.Rename(tmp.Name(), fn); err != nil {
    return err
  }
  u.log.Info("universe: submissions.zip saved", "file", fn, "bytes", total)
  return nil
}

// publish joins the SEC index with the Yahoo profiles and the S&P 500 list.
// S&P 500 members are always listed (also before the first submissions.zip
// is parsed).
func (u *universe) publish() {
  u.mu.Lock()
  sec := u.sec
  profiles := u.profiles
  spKeys, spInfo := u.spKeys, u.spInfo
  seen := make(map[string]bool, len(sec.Items)+len(spKeys))
  items := make([]universeItem, 0, len(sec.Items)+len(spKeys))
  join := func(it universeItem, key string) {
    if m, ok := spInfo[key]; ok {
      it.SP500, it.SP500Sector, it.SP500Weight = true, m.Sector, m.Weight
      if it.Name == "" {
        it.Name = m.Name
      }
    }
    if p, ok := profiles[key]; ok {
      it.Profiled = true
      it.Sector, it.SectorKey, it.Industry, it.IndustryKey = p.Sector, p.SectorKey, p.Industry, p.IndustryKey
      it.Country, it.QuoteType, it.MarketCap, it.Currency = p.Country, p.QuoteType, p.MarketCap, p.Currency
      if it.Name == "" {
        it.Name = p.Name
      }
      if it.Exchange == "" {
        it.Exchange = p.Exchange
      }
    }
    items = append(items, it)
  }
  for _, l := range sec.Items {
    key := normTicker(l.Ticker)
    if key == "" || seen[key] {
      continue
    }
    seen[key] = true
    join(universeItem{secListing: l, SICDivision: sicDivision(l.SIC)}, key)
  }
  for _, key := range spKeys {
    if !seen[key] {
      seen[key] = true
      join(universeItem{secListing: secListing{Ticker: key}}, key)
    }
  }
  u.mu.Unlock()

  sort.SliceStable(items, func(i, j int) bool {
    a, b := items[i].MarketCap, items[j].MarketCap
    switch {
    case a != nil && b != nil && *a != *b:
      return *a > *b
    case (a == nil) != (b == nil):
      return a != nil
    }
    return items[i].Ticker < items[j].Ticker
  })
  u.mu.Lock()
  u.items, u.edgarDate = items, sec.Date
  u.mu.Unlock()
}

// yahooPausedUntil reads the shared Yahoo rate-limit pause from the pace file
// ("<last> <pausedUntil> <backoff>", UnixNano); zero when not paused.
func yahooPausedUntil(path string) time.Time {
  b, err := os.ReadFile(path)
  if err != nil {
    return time.Time{}
  }
  f := strings.Fields(string(b))
  if len(f) < 2 {
    return time.Time{}
  }
  n, err := strconv.ParseInt(f[1], 10, 64)
  if err != nil || n <= time.Now().UnixNano() {
    return time.Time{}
  }
  return time.Unix(0, n)
}

// status is the build / enrichment state for /api/v2/universe/status:
//   edgar:    submissions.zip index (date, tickers, building, phase, error)
//   profiles: coverage of the S&P 500 and of all tickers ("have" = profile
//             fetched, "classified" = with a sector), running state, pause
func (u *universe) status() map[string]any {
  pausedUntil := yahooPausedUntil(u.cfg.yahooPaceFile)
  u.mu.Lock()
  defer u.mu.Unlock()
  edgar := map[string]any{
    "date": nilIfEmpty(u.edgarDate), "tickers": len(u.sec.Items), "building": u.building,
    "phase": u.phase, "secContactConfigured": secUserAgent() != "",
    "settingsFile": settingsYAMLFile(),
  }
  if u.lastError != "" {
    edgar["error"] = u.lastError
  }
  if !u.startedAt.IsZero() {
    edgar["startedAt"] = u.startedAt.UTC().Format(time.RFC3339)
  }
  if !u.finishedAt.IsZero() {
    edgar["finishedAt"] = u.finishedAt.UTC().Format(time.RFC3339)
  }

  gap := u.cfg.yahooBgGap
  cover := func(keys []string) map[string]any {
    have, classified := 0, 0
    for _, k := range keys {
      if p, ok := u.profiles[k]; ok {
        have++
        if p.Sector != "" {
          classified++
        }
      }
    }
    rem := len(keys) - have
    return map[string]any{
      "total": len(keys), "have": have, "classified": classified, "remaining": rem,
      "etaSeconds": int64((time.Duration(rem) * gap).Seconds()),
    }
  }
  allKeys := make([]string, 0, len(u.items))
  for i := range u.items {
    allKeys = append(allKeys, normTicker(u.items[i].Ticker))
  }
  prof := map[string]any{
    "sp500": cover(u.spKeys), "all": cover(allKeys),
    "active": u.enrichActive, "current": u.enrichSyms, "fetched": u.enrichDone,
    "cached": len(u.profiles), "gapMs": gap.Milliseconds(), "enabled": u.cfg.enrichAuto,
  }
  if mins := time.Since(u.enrichSince).Minutes(); !u.enrichSince.IsZero() && mins > 0 {
    prof["perMinute"] = float64(u.enrichDone) / mins
  }
  if u.enrichErr != "" {
    prof["error"] = u.enrichErr
  }
  if !pausedUntil.IsZero() {
    prof["pausedUntil"] = pausedUntil.UTC().Format(time.RFC3339)
  }
  sp := map[string]any{
    "members": len(u.spKeys), "source": nilIfEmpty(u.sp500.Source), "sourceUrl": nilIfEmpty(u.sp500.SourceURL),
    "asOf": nilIfEmpty(u.sp500.AsOf), "fetchedAt": nilIfEmpty(u.sp500.FetchedAt), "refreshing": u.sp500Busy,
    "enabled": u.cfg.sp500Auto, "maxAgeHours": u.cfg.sp500MaxAge.Hours(), "file": sp500ListFile(),
  }
  if len(u.sp500.FallbackReasons) > 0 {
    sp["fallbackReasons"] = u.sp500.FallbackReasons
  }
  if u.sp500Err != "" {
    sp["error"] = u.sp500Err
  }
  return map[string]any{"edgar": edgar, "profiles": prof, "sp500": sp}
}

// GET /api/v2/universe/status
func (s *server) handleUniverseStatus(w http.ResponseWriter, r *http.Request) {
  writeJSON(w, r, http.StatusOK, s.uni.status())
}

func (s *server) handleUniverse(w http.ResponseWriter, r *http.Request) {
  q := r.URL.Query()
  if v := q.Get("refresh"); v == "1" || strings.EqualFold(v, "true") {
    s.uni.trigger(context.Background(), true)
    s.uni.kickSP500()
  }
  index := strings.ToLower(strings.TrimSpace(q.Get("index")))
  if index != "" && index != "all" && index != "sp500" {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": "index must be all or sp500"})
    return
  }
  sectors := setOf(q.Get("sector"), false)
  exchanges := setOf(q.Get("exchange"), false)
  sicPrefix := strings.TrimSpace(q.Get("sic"))
  needle := strings.ToLower(strings.TrimSpace(q.Get("q")))
  classified := q.Get("classified") == "1"
  limit := qInt(q, "limit", universeLimitMax, 1, universeLimitMax)
  offset := qInt(q, "offset", 0, 0, universeLimitMax)

  s.uni.mu.Lock()
  all := s.uni.items // replaced, never mutated, by publish
  s.uni.mu.Unlock()
  page := make([]universeItem, 0, 64)
  total := 0
  for i := range all {
    it := &all[i]
    if index == "sp500" && !it.SP500 {
      continue
    }
    if classified && it.Sector == "" {
      continue
    }
    if len(sectors) > 0 && !sectors[strings.ToLower(it.Sector)] && !sectors[strings.ToLower(it.SectorKey)] {
      continue
    }
    if len(exchanges) > 0 && !exchanges[strings.ToLower(it.Exchange)] {
      continue
    }
    if sicPrefix != "" && !strings.HasPrefix(it.SIC, sicPrefix) {
      continue
    }
    if needle != "" && !strings.Contains(strings.ToLower(it.Ticker), needle) && !strings.Contains(strings.ToLower(it.Name), needle) {
      continue
    }
    total++
    if total > offset && len(page) < limit {
      page = append(page, *it)
    }
  }
  st := s.uni.status()
  if len(all) == 0 {
    writeJSON(w, r, http.StatusServiceUnavailable, map[string]any{
      "error": "universe not built yet", "status": st, "items": []any{}, "total": 0, "quality": qUnavailable,
    })
    return
  }
  writeJSON(w, r, http.StatusOK, map[string]any{"status": st, "total": total, "count": len(page), "items": page})
}

// ---------------------------------------------------------------------------
// Streaming: SSE, NDJSON and WebSocket transports over the hub
// ---------------------------------------------------------------------------

const (
  maxTopicsPerConn = 256
  heartbeatEvery   = 15 * time.Second
)

func splitTopics(s string) []string {
  var out []string
  seen := map[string]bool{}
  for _, t := range strings.Split(s, ",") {
    if t = strings.TrimSpace(t); t != "" && !seen[t] {
      seen[t] = true
      out = append(out, t)
    }
  }
  if len(out) > maxTopicsPerConn {
    out = out[:maxTopicsPerConn]
  }
  return out
}

func streamMode(r *http.Request) string {
  switch strings.ToLower(r.URL.Query().Get("transport")) {
  case "ndjson", "jsonl":
    return "ndjson"
  case "sse":
    return "sse"
  }
  acc := r.Header.Get("Accept")
  if strings.Contains(acc, "application/x-ndjson") || strings.Contains(acc, "application/jsonl") {
    return "ndjson"
  }
  return "sse"
}

// subscribe attaches c to one topic. Static topics are answered directly and
// return a nil sub.
func (s *server) subscribe(c *client, topic string) (*sub, error) {
  sp, err := s.parseTopic(topic)
  if err != nil {
    return nil, err
  }
  if sp.static != nil {
    c.send(sp.static.bytesFor(topic))
    return nil, nil
  }
  return s.hub.attach(c, topic, sp)
}

func (s *server) subscribeAll(c *client, topics []string, subs map[string]*sub) {
  for _, t := range topics {
    if _, dup := subs[t]; dup {
      continue
    }
    if len(subs) >= maxTopicsPerConn {
      respLog.Warn("error response", "kind", "subscribe", "topic", t, "error", "too many topics on this connection")
      c.send(ctrl("error", map[string]any{"topic": t, "error": "too many topics on this connection"}))
      continue
    }
    sb, err := s.subscribe(c, t)
    if err != nil {
      respLog.Warn("error response", "kind", "subscribe", "topic", t, "error", err.Error())
      c.send(ctrl("error", map[string]any{"topic": t, "error": err.Error()}))
      continue
    }
    if sb != nil {
      subs[t] = sb
    }
  }
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
  if isWebSocket(r) {
    s.serveWS(w, r, splitTopics(r.URL.Query().Get("topics")))
    return
  }
  s.serveFrames(w, r, streamMode(r), splitTopics(r.URL.Query().Get("topics")))
}

// serveFrames streams hub frames as SSE ("data: {...}\n\n") or NDJSON
// ("{...}\n") until the client goes away.
func (s *server) serveFrames(w http.ResponseWriter, r *http.Request, mode string, topics []string) {
  rc := http.NewResponseController(w)
  h := w.Header()
  if mode == "sse" {
    h.Set("Content-Type", "text/event-stream; charset=utf-8")
  } else {
    h.Set("Content-Type", "application/x-ndjson; charset=utf-8")
  }
  h.Set("Cache-Control", "no-cache, no-transform")
  h.Set("X-Accel-Buffering", "no")
  w.WriteHeader(http.StatusOK)

  s.clients.Add(1)
  defer s.clients.Add(-1)
  c := newClient()
  defer c.kill()

  write := func(b []byte) error {
    rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
    var err error
    if mode == "sse" {
      if _, err = io.WriteString(w, "data: "); err == nil {
        if _, err = w.Write(b); err == nil {
          _, err = io.WriteString(w, "\n\n")
        }
      }
    } else {
      if _, err = w.Write(b); err == nil {
        _, err = io.WriteString(w, "\n")
      }
    }
    return err
  }

  if write(ctrl("connected", map[string]any{"transport": mode, "topics": topics, "serverTime": nowRFC3339()})) != nil {
    return
  }
  subs := map[string]*sub{}
  s.subscribeAll(c, topics, subs)
  defer func() {
    for _, sb := range subs {
      s.hub.detach(sb)
    }
  }()
  if rc.Flush() != nil {
    return
  }

  hb := time.NewTicker(heartbeatEvery)
  defer hb.Stop()
  for {
    select {
    case <-r.Context().Done():
      return
    case <-s.closing:
      return
    case <-c.done:
      return
    case b := <-c.out:
      if write(b) != nil {
        return
      }
    drain:
      for i := 0; i < 512; i++ {
        select {
        case b = <-c.out:
          if write(b) != nil {
            return
          }
        default:
          break drain
        }
      }
      if rc.Flush() != nil {
        return
      }
    case <-hb.C:
      if write(ctrl("heartbeat", map[string]any{"t": time.Now().UnixMilli()})) != nil || rc.Flush() != nil {
        return
      }
    }
  }
}

func headerHasToken(h http.Header, name, token string) bool {
  for _, v := range h.Values(name) {
    for _, p := range strings.Split(v, ",") {
      if strings.EqualFold(strings.TrimSpace(p), token) {
        return true
      }
    }
  }
  return false
}

func isWebSocket(r *http.Request) bool {
  return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && headerHasToken(r.Header, "Connection", "upgrade")
}

// sameOrigin enforces same-origin for browser WebSocket upgrades.
func sameOrigin(r *http.Request) bool {
  o := r.Header.Get("Origin")
  if o == "" {
    return true
  }
  u, err := url.Parse(o)
  return err == nil && strings.EqualFold(u.Host, r.Host)
}

const (
  wsText  = 0x1
  wsClose = 0x8
  wsPing  = 0x9
  wsPong  = 0xA
)

func wsWrite(w *bufio.Writer, op byte, p []byte) error {
  hdr := []byte{0x80 | op}
  n := len(p)
  switch {
  case n < 126:
    hdr = append(hdr, byte(n))
  case n < 1<<16:
    hdr = append(hdr, 126, byte(n>>8), byte(n))
  default:
    var b [8]byte
    binary.BigEndian.PutUint64(b[:], uint64(n))
    hdr = append(hdr, 127)
    hdr = append(hdr, b[:]...)
  }
  if _, err := w.Write(hdr); err != nil {
    return err
  }
  if _, err := w.Write(p); err != nil {
    return err
  }
  return w.Flush()
}

func wsRead(r *bufio.Reader) (op byte, fin bool, p []byte, err error) {
  var h [2]byte
  if _, err = io.ReadFull(r, h[:]); err != nil {
    return
  }
  fin = h[0]&0x80 != 0
  op = h[0] & 0x0f
  if h[1]&0x80 == 0 {
    err = errors.New("ws: unmasked client frame")
    return
  }
  n := uint64(h[1] & 0x7f)
  switch n {
  case 126:
    var b [2]byte
    if _, err = io.ReadFull(r, b[:]); err != nil {
      return
    }
    n = uint64(binary.BigEndian.Uint16(b[:]))
  case 127:
    var b [8]byte
    if _, err = io.ReadFull(r, b[:]); err != nil {
      return
    }
    n = binary.BigEndian.Uint64(b[:])
  }
  if n > 1<<20 {
    err = errors.New("ws: frame too large")
    return
  }
  var mask [4]byte
  if _, err = io.ReadFull(r, mask[:]); err != nil {
    return
  }
  p = make([]byte, n)
  if _, err = io.ReadFull(r, p); err != nil {
    return
  }
  for i := range p {
    p[i] ^= mask[i%4]
  }
  return
}

// serveWS speaks the stream-client.ts protocol: client sends
// {"action":"SUBSCRIBE"|"UNSUBSCRIBE","key":topic}; server sends frames.
func (s *server) serveWS(w http.ResponseWriter, r *http.Request, topics []string) {
  if !sameOrigin(r) {
    httpError(w, r, "forbidden origin", http.StatusForbidden)
    return
  }
  key := r.Header.Get("Sec-WebSocket-Key")
  if key == "" || r.Header.Get("Sec-WebSocket-Version") != "13" {
    httpError(w, r, "bad websocket handshake", http.StatusBadRequest)
    return
  }
  hj, ok := w.(http.Hijacker)
  if !ok {
    httpError(w, r, "websocket unsupported", http.StatusInternalServerError)
    return
  }
  conn, brw, err := hj.Hijack()
  if err != nil {
    return
  }
  defer conn.Close()
  sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
  resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
    base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n"
  conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
  if _, err := conn.Write([]byte(resp)); err != nil {
    return
  }

  s.clients.Add(1)
  defer s.clients.Add(-1)
  c := newClient()
  defer c.kill()
  bw := bufio.NewWriter(conn)
  var wmu sync.Mutex
  send := func(op byte, p []byte) error {
    wmu.Lock()
    defer wmu.Unlock()
    conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
    return wsWrite(bw, op, p)
  }

  var smu sync.Mutex
  subs := map[string]*sub{}
  defer func() {
    smu.Lock()
    for _, sb := range subs {
      s.hub.detach(sb)
    }
    smu.Unlock()
  }()

  c.send(ctrl("connected", map[string]any{"transport": "ws", "topics": topics, "serverTime": nowRFC3339()}))
  smu.Lock()
  s.subscribeAll(c, topics, subs)
  smu.Unlock()

  go func() {
    defer c.kill()
    var msg []byte
    for {
      conn.SetReadDeadline(time.Now().Add(90 * time.Second))
      op, fin, p, err := wsRead(brw.Reader)
      if err != nil {
        return
      }
      switch op {
      case 0x0, wsText, 0x2:
        msg = append(msg, p...)
        if len(msg) > 1<<20 {
          return
        }
        if !fin {
          continue
        }
        var m struct {
          Action string   `json:"action"`
          Key    string   `json:"key"`
          Topics []string `json:"topics"`
        }
        if json.Unmarshal(msg, &m) == nil {
          keys := m.Topics
          if m.Key != "" {
            keys = append(keys, m.Key)
          }
          smu.Lock()
          switch strings.ToUpper(m.Action) {
          case "SUBSCRIBE":
            s.subscribeAll(c, keys, subs)
          case "UNSUBSCRIBE":
            for _, k := range keys {
              if sb := subs[k]; sb != nil {
                s.hub.detach(sb)
                delete(subs, k)
              }
            }
          }
          smu.Unlock()
        }
        msg = nil
      case wsClose:
        if len(p) >= 2 {
          p = p[:2]
        }
        send(wsClose, p)
        return
      case wsPing:
        send(wsPong, p)
      case wsPong:
      }
    }
  }()

  hb := time.NewTicker(heartbeatEvery)
  defer hb.Stop()
  for {
    select {
    case <-c.done:
      return
    case <-s.closing:
      send(wsClose, []byte{0x03, 0xe9}) // 1001 going away
      return
    case b := <-c.out:
      if send(wsText, b) != nil {
        return
      }
    case <-hb.C:
      if send(wsText, ctrl("heartbeat", map[string]any{"t": time.Now().UnixMilli()})) != nil {
        return
      }
      if send(wsPing, nil) != nil {
        return
      }
    }
  }
}

// ---------------------------------------------------------------------------
// DENSE workspace bundle
// ---------------------------------------------------------------------------

var defaultDenseWatch = []string{"SPY", "QQQ", "DIA", "IWM", "^VIX", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"}

type denseParams struct {
  symbol string
  watch  []string
  rng    string
}

func parseDense(q url.Values) (denseParams, error) {
  p := denseParams{rng: strings.ToLower(q.Get("range"))}
  sym, ok := normSym(q.Get("symbol"))
  if q.Get("symbol") == "" {
    sym, ok = "AAPL", true
  }
  if !ok {
    return p, errors.New("invalid symbol")
  }
  p.symbol = sym
  if p.rng == "" {
    p.rng = "1y"
  }
  if !validRanges[p.rng] {
    return p, errors.New("invalid range")
  }
  raw := splitList(q.Get("watch"))
  if len(raw) == 0 {
    raw = defaultDenseWatch
  }
  for _, r := range raw {
    if w, ok := normSym(r); ok {
      p.watch = append(p.watch, w)
    }
    if len(p.watch) == 40 {
      break
    }
  }
  return p, nil
}

// denseTopics lists the stream topics behind the DENSE preset: MONITOR
// (quotes + 1d/5m sparklines), DESCRIPTION + GP (quote + chart), NEWS, FUND,
// ECO, EARN (quarterly income statement) and ANALYST.
func denseTopics(p denseParams) []string {
  var t []string
  seen := map[string]bool{}
  add := func(s string) {
    if !seen[s] {
      seen[s] = true
      t = append(t, s)
    }
  }
  add("quotes:" + p.symbol)
  add("chart:" + p.symbol + ":" + p.rng + ":1d")
  for _, w := range p.watch {
    add("quotes:" + w)
    add("chart:" + w + ":1d:5m")
  }
  if equityRe.MatchString(p.symbol) {
    add("keystats:" + p.symbol)
    add("analyst:" + p.symbol)
    add("statements:" + p.symbol + ":INCOME_STATEMENT:QUARTERLY:8")
  }
  add("news")
  add("econ")
  return t
}

// handleDense writes one NDJSON line per panel payload as soon as it is ready:
// {"key":"watchlist"|"chart"|"spark:SYM"|"fundamentals"|"analyst"|"earnings"|
// "news"|"econ","status":200,"data":{...TS route body...},"serverTimestamp":ms}
// and finally {"type":"done","ms":N}.
func (s *server) handleDense(w http.ResponseWriter, r *http.Request) {
  p, err := parseDense(r.URL.Query())
  if err != nil {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": err.Error()})
    return
  }
  rc := http.NewResponseController(w)
  w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
  w.Header().Set("Cache-Control", "no-store")
  w.Header().Set("X-Accel-Buffering", "no")
  w.WriteHeader(http.StatusOK)
  start := time.Now()
  ctx := r.Context()

  var mu sync.Mutex
  emitLine := func(b []byte) {
    mu.Lock()
    defer mu.Unlock()
    if ctx.Err() != nil {
      return
    }
    rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
    w.Write(b)
    w.Write([]byte("\n"))
    rc.Flush()
  }
  part := func(key string, fn func() (int, any)) func() {
    return func() {
      st, body := fn()
      b, err := json.Marshal(map[string]any{"key": key, "status": st, "data": body, "serverTimestamp": time.Now().UnixMilli()})
      if err != nil {
        logRespErr(r, http.StatusInternalServerError, errBody("encode failed: "+err.Error()), "part", key)
        b, _ = json.Marshal(map[string]any{"key": key, "status": 500, "data": errBody(err.Error())})
      } else {
        logRespErr(r, st, body, "part", key)
      }
      emitLine(b)
    }
  }

  watch := p.watch
  if !contains(watch, p.symbol) {
    watch = append([]string{p.symbol}, watch...)
  }
  parts := []func(){
    part("watchlist", func() (int, any) { return s.watchlistBody(ctx, watch) }),
    part("chart", func() (int, any) { return s.chartBody(p.symbol, p.rng, "1d", "") }),
    part("news", func() (int, any) { return s.newsBody(url.Values{"limit": {"40"}}) }),
    part("econ", func() (int, any) { return s.econBody("thisweek") }),
  }
  if equityRe.MatchString(p.symbol) {
    parts = append(parts,
      part("fundamentals", func() (int, any) { return s.fundamentalsBody(p.symbol) }),
      part("analyst", func() (int, any) { return s.analystBody(p.symbol) }),
      part("earnings", func() (int, any) { return s.statementsBody(p.symbol, "INCOME_STATEMENT", "QUARTERLY", "8") }),
    )
  }
  for _, sym := range p.watch {
    sym := sym
    parts = append(parts, part("spark:"+sym, func() (int, any) { return s.chartBody(sym, "1d", "5m", "") }))
  }
  var wg sync.WaitGroup
  for _, fn := range parts {
    wg.Add(1)
    go func(fn func()) {
      defer wg.Done()
      fn()
    }(fn)
  }
  wg.Wait()
  emitLine(ctrl("done", map[string]any{"ms": time.Since(start).Milliseconds(), "parts": len(parts)}))
}

// handleDenseStream subscribes to every DENSE topic on one SSE/NDJSON/WS stream.
func (s *server) handleDenseStream(w http.ResponseWriter, r *http.Request) {
  p, err := parseDense(r.URL.Query())
  if err != nil {
    writeJSON(w, r, http.StatusBadRequest, map[string]any{"error": err.Error()})
    return
  }
  topics := denseTopics(p)
  if isWebSocket(r) {
    s.serveWS(w, r, topics)
    return
  }
  s.serveFrames(w, r, streamMode(r), topics)
}

func contains(list []string, s string) bool {
  for _, x := range list {
    if x == s {
      return true
    }
  }
  return false
}

// ---------------------------------------------------------------------------
// Routing, middleware, health
// ---------------------------------------------------------------------------

func (s *server) routes() http.Handler {
  mux := http.NewServeMux()
  mux.HandleFunc("/healthz", s.handleHealth)
  mux.HandleFunc("/api/v2/collectors", s.handleCollectors)
  mux.Handle("/debug/vars", expvar.Handler())

  mux.HandleFunc("/api/yfin/watchlist", s.handleWatchlist)
  mux.HandleFunc("/api/yfin/chart/", s.handleChart)
  mux.HandleFunc("/api/yfin/fundamentals", s.handleFundamentals)
  mux.HandleFunc("/api/yfin/analyst-ratings", s.handleAnalyst)
  mux.HandleFunc("/api/yfin/economic-calendar", s.handleEcon)
  mux.HandleFunc("/api/yfin/news", s.handleYfinNews)
  mux.HandleFunc("/api/v2/statements", s.handleStatements)
  mux.HandleFunc("/api/v2/news", s.handleNews)

  mux.HandleFunc("/api/sec/filings", s.handleSecFilings)
  mux.HandleFunc("/api/v2/filing-doc", s.handleFilingDoc)
  mux.HandleFunc("/api/sec/insider", s.handleSecInsider)
  mux.HandleFunc("/api/sec/institutional", s.handleSecInstitutional)
  mux.HandleFunc("/api/sec/13f", s.handleSec13F)
  mux.HandleFunc("/api/v2/ipos", s.handleIPOs)
  mux.HandleFunc("/api/v2/halts", s.handleHalts)
  mux.HandleFunc("/api/yfin/options-chain", s.handleOptionsChain)
  mux.HandleFunc("/api/v2/transcripts", s.handleTranscripts)
  mux.HandleFunc("/api/yfin/article", s.handleArticle)
  mux.HandleFunc("/api/yfin/fear-greed", s.handleFearGreed)
  mux.HandleFunc("/api/wsb/trending", s.handleWSB)
  mux.HandleFunc("/api/congress/trades", s.handleCongress)
  mux.HandleFunc("/api/yfin/search", s.handleSearch)
  mux.HandleFunc("/api/yfin/market-movers", s.handleMovers)
  mux.HandleFunc("/api/yfin/trending", s.handleTrending)
  mux.HandleFunc("/api/v2/quote-batch", s.handleQuoteBatch)
  mux.HandleFunc("/api/v2/universe", s.handleUniverse)
  mux.HandleFunc("/api/v2/universe/status", s.handleUniverseStatus)

  mux.HandleFunc("/api/v2/stream", s.handleStream)
  mux.HandleFunc("/api/v2/dense", s.handleDense)
  mux.HandleFunc("/api/v2/dense/stream", s.handleDenseStream)

  mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, r, http.StatusNotFound, map[string]any{"error": "not ported yet: " + r.URL.Path, "quality": qUnavailable})
  })
  if s.cfg.webDir != "" {
    mux.Handle("/", http.FileServer(http.Dir(s.cfg.webDir)))
  } else {
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
      writeJSON(w, r, http.StatusNotFound, map[string]any{"error": "no UI configured (-web)"})
    })
  }
  return s.middleware(mux)
}

// middleware: GET/HEAD only, panic recovery, basic hardening headers. It does
// not wrap the ResponseWriter, so Flush/Hijack keep working for streams.
func (s *server) middleware(next http.Handler) http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    defer func() {
      if v := recover(); v != nil {
        if v == http.ErrAbortHandler {
          panic(v)
        }
        s.log.Error("handler panic", "path", r.URL.Path, "panic", fmt.Sprint(v))
        httpError(w, r, "internal error", http.StatusInternalServerError)
      }
    }()
    if r.Method != http.MethodGet && r.Method != http.MethodHead {
      w.Header().Set("Allow", "GET, HEAD")
      writeJSON(w, r, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
      return
    }
    w.Header().Set("X-Content-Type-Options", "nosniff")
    w.Header().Set("Referrer-Policy", "same-origin")
    s.log.Debug("request", "path", r.URL.Path, "query", r.URL.RawQuery)
    next.ServeHTTP(w, r)
  })
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
  writeJSON(w, r, http.StatusOK, map[string]any{
    "ok":      true,
    "uptime":  time.Since(s.started).Round(time.Second).String(),
    "clients": s.clients.Load(),
    "streams": s.hub.snapshot()["running"],
  })
}

func (s *server) handleCollectors(w http.ResponseWriter, r *http.Request) {
  writeJSON(w, r, http.StatusOK, map[string]any{
    "collectors": s.run.snapshot(),
    "streams":    s.hub.snapshot(),
  })
}
