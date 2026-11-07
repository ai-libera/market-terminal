// qube-sec: SEC EDGAR collector.
//
// Ports lib/sec-edgar.ts, providers/financial-statements-provider.ts,
// /api/sec/*, /api/v2/filing-doc and /api/v2/statements. Output is a JSONL
// stream of {data, provenance} envelopes on stdout; logs go to stderr.
//
// SEC requires a descriptive User-Agent with a contact address:
//   export QUBE_SEC_UA="Jane Doe jane@example.org"
//
//   go run sec.go ticker AAPL MSFT
//   go run sec.go filings -forms 10-K,10-Q,8-K -limit 20 AAPL
//   go run sec.go filings -every 2m AAPL            # stream new filings
//   go run sec.go keyfin AAPL
//   go run sec.go facts AAPL                         # raw companyfacts
//   go run sec.go statements -type income -period quarterly -limit 8 AAPL
//   go run sec.go insider AAPL
//   go run sec.go insider -marketwide -limit 40
//   go run sec.go institutional AAPL
//   go run sec.go 13f 0001067983
//   go run sec.go filing-doc -cik 320193 -accession 0000320193-24-000123 -doc aapl-20240928.htm
//
// Pacing: request starts are spaced by -min-gap (QUBE_SEC_MIN_GAP, default
// 200ms = 5 req/s) across *all* qube-sec processes sharing -pace-file
// (QUBE_SEC_PACE_FILE, default <state dir>/qube-sec-<uid>.pace, file-locked).
//
// Large, slow-changing responses (company_tickers, submissions, companyfacts,
// archives) are cached on disk under -cache-dir (CreateTemp + Rename).
package main

import (
  "bufio"
  "bytes"
  "context"
  "crypto/sha1"
  "encoding/hex"
  "encoding/json"
  "encoding/xml"
  "errors"
  "flag"
  "fmt"
  "io"
  "log/slog"
  "math/rand"
  "net/http"
  "net/url"
  "os"
  "os/signal"
  "path/filepath"
  "regexp"
  "sort"
  "strconv"
  "strings"
  "sync"
  "syscall"
  "time"

  common "market-terminal/common/go"
)

const (
  progName   = "qube-sec"
  sourceName = "sec-edgar"
  envPrefix  = "QUBE_SEC_"
  defaultUA  = "Terminal Bot contact@example.com" // same as terminal/src/lib/sec-edgar.ts
)

func main() {
  runMain([]command{
    {"ticker", "TICKER...", "ticker -> CIK and company name", cmdTicker},
    {"filings", "[-forms 10-K,10-Q] [-limit N] TICKER|CIK", "one record per filing (newest first)", cmdFilings},
    {"keyfin", "TICKER|CIK...", "latest annual key financials from XBRL facts", cmdKeyfin},
    {"facts", "TICKER|CIK", "raw XBRL companyfacts", cmdFacts},
    {"statements", "[-type] [-period] [-limit N] TICKER|CIK", "standardized income/balance/cashflow", cmdStatements},
    {"insider", "[-limit N] [-details] TICKER|CIK | -marketwide", "Form 4 filings with parsed transactions", cmdInsider},
    {"institutional", "TICKER", "13F-HR filers mentioning the issuer + shares outstanding", cmdInstitutional},
    {"13f", "[-accession A] CIK", "13F-HR information table of a manager", cmd13F},
    {"filing-doc", "-cik C -accession A -doc D", "sanitized filing document, item TOC, exhibits", cmdFilingDoc},
  })
}

// ---------------------------------------------------------------------------
// SEC client: rate limit, memory + disk cache, staleness tracking
// ---------------------------------------------------------------------------

type secClient struct {
  f        *fetcher
  cacheDir string
  every    time.Duration
  mu       sync.Mutex
  mem      map[string]memBody
  tickers  *tickerIndex
}

type memBody struct {
  b  []byte
  at time.Time
}

// callInfo accumulates cache age / staleness of every upstream body used to
// build one record, so the envelope can report STALE + cacheAgeMs.
type callInfo struct {
  mu    sync.Mutex
  age   time.Duration
  stale bool
  err   error
}

func (ci *callInfo) note(age time.Duration, stale bool, err error) {
  if ci == nil {
    return
  }
  ci.mu.Lock()
  defer ci.mu.Unlock()
  if age > ci.age {
    ci.age = age
  }
  if stale {
    ci.stale = true
    ci.err = err
  }
}

func (ci *callInfo) env(endpoint, key string, data any) Envelope {
  e := envOK(endpoint, key, qLive, data)
  e.Provenance.CacheAgeMs = ci.age.Milliseconds()
  if ci.stale {
    e.Provenance.Quality = qStale
    if ci.err != nil {
      e.Provenance.Error = ci.err.Error()
    }
  }
  return e
}

// Request pacing shared by every qube-sec process (SEC fair access: max 10
// req/s per client; we stay at 5 by default).
const defaultMinGap = 200 * time.Millisecond

var (
  secMinGap   time.Duration
  secPaceFile string
)

func defaultPaceFile() string {
  return filepath.Join(common.StateDir("market-terminal"), fmt.Sprintf("qube-sec-%d.pace", os.Getuid()))
}

func addSecFlags(fs *flag.FlagSet) *string {
  def := os.Getenv(envPrefix + "CACHE_DIR")
  if def == "" {
    def = filepath.Join(common.CacheDir("market-terminal"), "qube-sec")
  }
  gap := defaultMinGap
  if v := strings.TrimSpace(os.Getenv(envPrefix + "MIN_GAP")); v != "" {
    if d, err := time.ParseDuration(v); err == nil && d >= 0 {
      gap = d
    }
  }
  fs.DurationVar(&secMinGap, "min-gap", gap, "minimum gap between SEC requests across all qube-sec processes (env "+envPrefix+"MIN_GAP)")
  fs.StringVar(&secPaceFile, "pace-file", envOr(envPrefix+"PACE_FILE", defaultPaceFile()), "lock file shared by all qube-sec processes for -min-gap pacing, empty = this process only (env "+envPrefix+"PACE_FILE)")
  return fs.String("cache-dir", def, "disk cache directory, empty to disable (env "+envPrefix+"CACHE_DIR)")
}

// filePacer spaces request starts by at least gap across every process that
// uses the same pace file. The file holds the UnixNano time of the last
// request start and is guarded by flock, so concurrent collectors (the
// backend runs several) queue up behind each other. If the file cannot be
// used, pacing falls back to this process only.
type filePacer struct {
  path  string
  gap   time.Duration
  mu    sync.Mutex
  local *limiter
  warn  sync.Once
}

func newFilePacer(path string, gap time.Duration) *filePacer {
  p := &filePacer{path: path, gap: gap, local: &limiter{gap: gap}}
  return p
}

func (p *filePacer) fallback(ctx context.Context, err error) error {
  p.warn.Do(func() {
    logger.Warn("shared SEC pacing unavailable, pacing this process only", "file", p.path, "err", err)
  })
  return p.local.wait(ctx)
}

func (p *filePacer) wait(ctx context.Context) error {
  if p.gap <= 0 {
    return nil
  }
  if p.path == "" {
    return p.local.wait(ctx)
  }
  p.mu.Lock()
  defer p.mu.Unlock()
  f, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0o600)
  if err != nil {
    return p.fallback(ctx, err)
  }
  defer f.Close()
  if err := common.LockFile(f); err != nil {
    return p.fallback(ctx, err)
  }
  defer common.UnlockFile(f)

  buf := make([]byte, 32)
  n, _ := f.ReadAt(buf, 0)
  if last, err := strconv.ParseInt(strings.TrimSpace(string(buf[:n])), 10, 64); err == nil && last > 0 {
    d := time.Until(time.Unix(0, last).Add(p.gap))
    if d > p.gap {
      d = p.gap // clock moved backwards
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
  stamp := []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
  if err := f.Truncate(0); err == nil {
    f.WriteAt(stamp, 0)
  }
  return nil
}

func newSEC(o *opts, cacheDir string) (*secClient, error) {
  ua := strings.TrimSpace(o.ua)
  if ua == "" || !strings.Contains(ua, "@") {
    return nil, errors.New("SEC requires a descriptive User-Agent with a contact e-mail: pass -ua \"Name you@domain\" or set " + envPrefix + "UA")
  }
  if strings.Contains(ua, "example.com") {
    logger.Warn("using placeholder SEC User-Agent; set -ua or " + envPrefix + "UA to a real contact", "ua", ua)
  }
  // SEC hard limit is 10 req/s per client: every request (including
  // retries) waits for the shared pacer, default 200 ms apart = 5 req/s.
  f := newFetcher(o, nil, 0)
  f.pacer = newFilePacer(secPaceFile, secMinGap)
  f.maxBody = 96 << 20 // companyfacts of large filers is big
  return &secClient{f: f, cacheDir: cacheDir, every: o.every, mem: map[string]memBody{}}, nil
}

// ttl shortens cache TTLs in -every mode so each poll can see new data.
func (s *secClient) ttl(base time.Duration) time.Duration {
  if s.every > 0 && s.every*9/10 < base {
    return s.every * 9 / 10
  }
  return base
}

func (s *secClient) cachePath(u string) string {
  if s.cacheDir == "" {
    return ""
  }
  sum := sha1.Sum([]byte(u))
  return filepath.Join(s.cacheDir, hex.EncodeToString(sum[:])+".cache")
}

func (s *secClient) remember(u string, b []byte, at time.Time) {
  s.mu.Lock()
  s.mem[u] = memBody{b, at}
  s.mu.Unlock()
}

// raw returns the body for u from memory, disk or network (in that order).
// On a failed refresh an older copy is served and recorded as stale in ci.
func (s *secClient) raw(ctx context.Context, ci *callInfo, u string, ttl time.Duration) ([]byte, error) {
  s.mu.Lock()
  m, inMem := s.mem[u]
  s.mu.Unlock()
  if inMem && time.Since(m.at) < ttl {
    ci.note(time.Since(m.at), false, nil)
    return m.b, nil
  }
  var disk []byte
  var diskAt time.Time
  path := s.cachePath(u)
  if path != "" {
    if st, err := os.Stat(path); err == nil {
      if b, err := os.ReadFile(path); err == nil {
        disk, diskAt = b, st.ModTime()
        if time.Since(diskAt) < ttl {
          s.remember(u, b, diskAt)
          ci.note(time.Since(diskAt), false, nil)
          return b, nil
        }
      }
    }
  }
  b, err := s.f.get(ctx, u, nil)
  if err != nil {
    st := httpStatus(err)
    if st == http.StatusTooManyRequests {
      err = fmt.Errorf("SEC rate limited: %w", err)
    }
    if st != http.StatusNotFound && ctx.Err() == nil {
      if inMem {
        ci.note(time.Since(m.at), true, err)
        return m.b, nil
      }
      if disk != nil {
        ci.note(time.Since(diskAt), true, err)
        return disk, nil
      }
    }
    return nil, err
  }
  s.remember(u, b, time.Now())
  if path != "" {
    if err := writeFileAtomic(path, b); err != nil {
      logger.Warn("cache write failed", "path", path, "err", err)
    }
  }
  return b, nil
}

func (s *secClient) getJSON(ctx context.Context, ci *callInfo, u string, ttl time.Duration, v any) error {
  b, err := s.raw(ctx, ci, u, ttl)
  if err != nil {
    return err
  }
  if err := json.Unmarshal(b, v); err != nil {
    return fmt.Errorf("SEC invalid JSON from %s: %w", u, err)
  }
  return nil
}

func writeFileAtomic(path string, b []byte) error {
  dir := filepath.Dir(path)
  if err := os.MkdirAll(dir, 0o755); err != nil {
    return err
  }
  f, err := os.CreateTemp(dir, ".tmp-*")
  if err != nil {
    return err
  }
  tmp := f.Name()
  if _, err := f.Write(b); err != nil {
    f.Close()
    os.Remove(tmp)
    return err
  }
  if err := f.Close(); err != nil {
    os.Remove(tmp)
    return err
  }
  if err := os.Rename(tmp, path); err != nil {
    os.Remove(tmp)
    return err
  }
  return nil
}

// ---------------------------------------------------------------------------
// Ticker <-> CIK
// ---------------------------------------------------------------------------

type CompanyInfo struct {
  CIK    string `json:"cik"`
  CIK10  string `json:"cik10"`
  Ticker string `json:"ticker"`
  Name   string `json:"name"`
}

type tickerIndex struct {
  at       time.Time
  byTicker map[string]CompanyInfo
  byCIK    map[string]CompanyInfo
}

func (s *secClient) loadTickers(ctx context.Context, ci *callInfo) (*tickerIndex, error) {
  s.mu.Lock()
  idx := s.tickers
  s.mu.Unlock()
  if idx != nil && time.Since(idx.at) < 24*time.Hour {
    return idx, nil
  }
  var raw map[string]struct {
    CIK    any    `json:"cik_str"`
    Ticker string `json:"ticker"`
    Title  string `json:"title"`
  }
  if err := s.getJSON(ctx, ci, "https://www.sec.gov/files/company_tickers.json", 24*time.Hour, &raw); err != nil {
    return nil, err
  }
  idx = &tickerIndex{at: time.Now(), byTicker: map[string]CompanyInfo{}, byCIK: map[string]CompanyInfo{}}
  for _, e := range raw {
    cik := strings.TrimLeft(str(e.CIK), "0")
    info := CompanyInfo{CIK: cik, CIK10: padCIK(cik), Ticker: strings.ToUpper(e.Ticker), Name: e.Title}
    idx.byTicker[info.Ticker] = info
    if _, ok := idx.byCIK[cik]; !ok {
      idx.byCIK[cik] = info
    }
  }
  s.mu.Lock()
  s.tickers = idx
  s.mu.Unlock()
  return idx, nil
}

var (
  tickerRe = regexp.MustCompile(`^[A-Z][A-Z0-9.\-]{0,9}$`)
  cikRe    = regexp.MustCompile(`^\d{1,10}$`)
)

// resolve accepts a ticker or a numeric CIK.
func (s *secClient) resolve(ctx context.Context, ci *callInfo, arg string) (CompanyInfo, error) {
  a := strings.ToUpper(strings.TrimSpace(arg))
  if cikRe.MatchString(a) {
    cik := strings.TrimLeft(a, "0")
    if idx, err := s.loadTickers(ctx, ci); err == nil {
      if info, ok := idx.byCIK[cik]; ok {
        return info, nil
      }
    }
    return CompanyInfo{CIK: cik, CIK10: padCIK(cik)}, nil
  }
  if !tickerRe.MatchString(a) {
    return CompanyInfo{}, fmt.Errorf("invalid ticker %q", arg)
  }
  idx, err := s.loadTickers(ctx, ci)
  if err != nil {
    return CompanyInfo{}, err
  }
  info, ok := idx.byTicker[a]
  if !ok {
    info, ok = idx.byTicker[strings.ReplaceAll(a, ".", "-")]
  }
  if !ok {
    return CompanyInfo{}, fmt.Errorf("ticker %s not found in SEC database", a)
  }
  return info, nil
}

func padCIK(cik string) string {
  cik = strings.TrimLeft(cik, "0")
  if len(cik) >= 10 {
    return cik
  }
  return strings.Repeat("0", 10-len(cik)) + cik
}

func cmdTicker(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("ticker", &o)
  cacheDir := addSecFlags(fs)
  list := splitList(parseArgs(fs, args))
  if len(list) == 0 {
    return errors.New("ticker: at least one ticker required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, t := range list {
      ci := &callInfo{}
      info, err := s.resolve(ctx, ci, t)
      if err != nil {
        d.publish(envFail("ticker", strings.ToUpper(t), err))
        continue
      }
      d.publish(ci.env("ticker", strings.ToUpper(t), info))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Submissions / filings
// ---------------------------------------------------------------------------

type submissions struct {
  CIK     any      `json:"cik"`
  Name    string   `json:"name"`
  Tickers []string `json:"tickers"`
  Filings struct {
    Recent struct {
      AccessionNumber       []string `json:"accessionNumber"`
      FilingDate            []string `json:"filingDate"`
      ReportDate            []string `json:"reportDate"`
      AcceptanceDateTime    []string `json:"acceptanceDateTime"`
      Form                  []string `json:"form"`
      PrimaryDocument       []string `json:"primaryDocument"`
      PrimaryDocDescription []string `json:"primaryDocDescription"`
    } `json:"recent"`
  } `json:"filings"`
}

type Filing struct {
  AccessionNumber       string `json:"accessionNumber"`
  Form                  string `json:"form"`
  FilingDate            string `json:"filingDate"`
  ReportDate            string `json:"reportDate"`
  AcceptedAt            string `json:"acceptedAt,omitempty"`
  PrimaryDocument       string `json:"primaryDocument"`
  PrimaryDocDescription string `json:"primaryDocDescription"`
  EntityName            string `json:"entityName"`
  CIK                   string `json:"cik"`
  Ticker                string `json:"ticker,omitempty"`
  URL                   string `json:"url"`
}

func (s *secClient) submissions(ctx context.Context, ci *callInfo, cik string) (*submissions, error) {
  var sub submissions
  u := "https://data.sec.gov/submissions/CIK" + padCIK(cik) + ".json"
  if err := s.getJSON(ctx, ci, u, s.ttl(10*time.Minute), &sub); err != nil {
    return nil, err
  }
  return &sub, nil
}

func archiveURL(cik, accession, doc string) string {
  u := "https://www.sec.gov/Archives/edgar/data/" + strings.TrimLeft(cik, "0") + "/" + strings.ReplaceAll(accession, "-", "")
  if doc != "" {
    u += "/" + doc
  }
  return u
}

func (s *secClient) filings(ctx context.Context, ci *callInfo, info CompanyInfo, forms map[string]bool) ([]Filing, error) {
  sub, err := s.submissions(ctx, ci, info.CIK)
  if err != nil {
    return nil, err
  }
  r := sub.Filings.Recent
  out := []Filing{}
  for i, form := range r.Form {
    if len(forms) > 0 && !forms[form] {
      continue
    }
    acc := at(r.AccessionNumber, i)
    doc := at(r.PrimaryDocument, i)
    out = append(out, Filing{
      AccessionNumber:       acc,
      Form:                  form,
      FilingDate:            at(r.FilingDate, i),
      ReportDate:            at(r.ReportDate, i),
      AcceptedAt:            at(r.AcceptanceDateTime, i),
      PrimaryDocument:       doc,
      PrimaryDocDescription: at(r.PrimaryDocDescription, i),
      EntityName:            sub.Name,
      CIK:                   info.CIK10,
      Ticker:                info.Ticker,
      URL:                   archiveURL(info.CIK, acc, doc),
    })
  }
  return out, nil
}

func cmdFilings(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("filings", &o)
  cacheDir := addSecFlags(fs)
  formsFlag := fs.String("forms", "", "comma list of form types to keep (e.g. 10-K,10-Q,8-K)")
  limit := fs.Int("limit", 100, "max filings per company")
  list := splitList(parseArgs(fs, args))
  if len(list) == 0 {
    return errors.New("filings: ticker or CIK required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  forms := map[string]bool{}
  for _, f := range splitList([]string{*formsFlag}) {
    forms[strings.ToUpper(f)] = true
  }
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, t := range list {
      ci := &callInfo{}
      info, err := s.resolve(ctx, ci, t)
      if err == nil {
        var fl []Filing
        if fl, err = s.filings(ctx, ci, info, forms); err == nil {
          if len(fl) > *limit {
            fl = fl[:*limit]
          }
          // oldest first so a stream consumer sees them in filing order
          for i := len(fl) - 1; i >= 0; i-- {
            d.publish(ci.env("filing", fl[i].AccessionNumber, fl[i]))
          }
          continue
        }
      }
      d.publish(envFail("filing", strings.ToUpper(t), err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// XBRL company facts, key financials, standardized statements
// ---------------------------------------------------------------------------

type factEntry struct {
  Start string  `json:"start"`
  End   string  `json:"end"`
  Val   float64 `json:"val"`
  FY    int     `json:"fy"`
  FP    string  `json:"fp"`
  Form  string  `json:"form"`
  Frame string  `json:"frame"`
  Filed string  `json:"filed"`
}

type conceptFacts struct {
  Label string                 `json:"label"`
  Units map[string][]factEntry `json:"units"`
}

type companyFacts struct {
  CIK        any                                `json:"cik"`
  EntityName string                             `json:"entityName"`
  Facts      map[string]map[string]conceptFacts `json:"facts"`
}

func factsURL(cik string) string {
  return "https://data.sec.gov/api/xbrl/companyfacts/CIK" + padCIK(cik) + ".json"
}

func (s *secClient) companyFacts(ctx context.Context, ci *callInfo, cik string) (*companyFacts, error) {
  var cf companyFacts
  if err := s.getJSON(ctx, ci, factsURL(cik), s.ttl(6*time.Hour), &cf); err != nil {
    if httpStatus(err) == http.StatusNotFound {
      return nil, fmt.Errorf("no XBRL company facts for CIK %s", cik)
    }
    return nil, err
  }
  return &cf, nil
}

// concept looks a concept up in us-gaap first, then dei.
func (cf *companyFacts) concept(name string) (conceptFacts, bool) {
  for _, tax := range []string{"us-gaap", "dei", "ifrs-full"} {
    if c, ok := cf.Facts[tax][name]; ok {
      return c, true
    }
  }
  return conceptFacts{}, false
}

type KeyFigure struct {
  Value   float64 `json:"value"`
  End     string  `json:"end"`
  Form    string  `json:"form"`
  Concept string  `json:"concept"`
  Unit    string  `json:"unit"`
}

var keyFinancialConcepts = []struct {
  key      string
  concepts []string
}{
  {"revenues", []string{"Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"}},
  {"netIncome", []string{"NetIncomeLoss"}},
  {"totalAssets", []string{"Assets"}},
  {"totalLiabilities", []string{"Liabilities"}},
  {"stockholdersEquity", []string{"StockholdersEquity"}},
  {"cashAndEquivalents", []string{"CashAndCashEquivalentsAtCarryingValue"}},
  {"operatingIncome", []string{"OperatingIncomeLoss"}},
  {"basicEPS", []string{"EarningsPerShareBasic"}},
  {"dilutedEPS", []string{"EarningsPerShareDiluted"}},
  {"commonSharesOutstanding", []string{"EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding"}},
  {"grossProfit", []string{"GrossProfit"}},
  {"researchAndDevelopment", []string{"ResearchAndDevelopmentExpense"}},
}

// pickUnit prefers USD, then shares, then USD/shares, then anything.
func pickUnit(c conceptFacts) (string, []factEntry) {
  for _, u := range []string{"USD", "shares", "USD/shares"} {
    if xs := c.Units[u]; len(xs) > 0 {
      return u, xs
    }
  }
  keys := make([]string, 0, len(c.Units))
  for k := range c.Units {
    keys = append(keys, k)
  }
  sort.Strings(keys)
  for _, k := range keys {
    if len(c.Units[k]) > 0 {
      return k, c.Units[k]
    }
  }
  return "", nil
}

func extractKeyFinancials(cf *companyFacts) map[string]*KeyFigure {
  out := map[string]*KeyFigure{}
  for _, kc := range keyFinancialConcepts {
    out[kc.key] = nil
    for _, name := range kc.concepts {
      c, ok := cf.concept(name)
      if !ok {
        continue
      }
      unit, xs := pickUnit(c)
      if len(xs) == 0 {
        continue
      }
      // latest 10-K value by period end; else the last reported value
      best := -1
      for i, e := range xs {
        if e.Form != "10-K" {
          continue
        }
        if best < 0 || e.End > xs[best].End || (e.End == xs[best].End && e.Filed >= xs[best].Filed) {
          best = i
        }
      }
      if best < 0 {
        best = len(xs) - 1
      }
      e := xs[best]
      out[kc.key] = &KeyFigure{Value: e.Val, End: e.End, Form: e.Form, Concept: name, Unit: unit}
      break
    }
  }
  return out
}

func cmdKeyfin(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("keyfin", &o)
  cacheDir := addSecFlags(fs)
  list := splitList(parseArgs(fs, args))
  if len(list) == 0 {
    return errors.New("keyfin: ticker or CIK required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, t := range list {
      ci := &callInfo{}
      info, err := s.resolve(ctx, ci, t)
      if err == nil {
        var cf *companyFacts
        if cf, err = s.companyFacts(ctx, ci, info.CIK); err == nil {
          d.publish(ci.env("keyfin", info.Ticker+info.CIK10, map[string]any{
            "ticker": info.Ticker, "cik": info.CIK10, "name": firstNonEmpty(cf.EntityName, info.Name),
            "keyFinancials": extractKeyFinancials(cf),
          }))
          continue
        }
      }
      d.publish(envFail("keyfin", strings.ToUpper(t), err))
    }
    return nil
  })
}

func cmdFacts(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("facts", &o)
  cacheDir := addSecFlags(fs)
  list := splitList(parseArgs(fs, args))
  if len(list) != 1 {
    return errors.New("facts: exactly one ticker or CIK required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    ci := &callInfo{}
    info, err := s.resolve(ctx, ci, list[0])
    if err == nil {
      var b []byte
      if b, err = s.raw(ctx, ci, factsURL(info.CIK), s.ttl(6*time.Hour)); err == nil {
        if json.Valid(b) {
          d.publish(ci.env("facts", info.CIK10, json.RawMessage(b)))
          return nil
        }
        err = errors.New("SEC returned invalid JSON")
      }
    }
    d.publish(envFail("facts", strings.ToUpper(list[0]), err))
    return nil
  })
}

type conceptLine struct {
  Key       string
  Label     string
  Concepts  []string
  PercentOf string
  Indent    int
}

var incomeLines = []conceptLine{
  {"revenue", "Total Revenue", []string{"Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"}, "revenue", 0},
  {"cogs", "Cost of Revenue", []string{"CostOfRevenue", "CostOfGoodsAndServicesSold"}, "revenue", 1},
  {"grossProfit", "Gross Profit", []string{"GrossProfit"}, "revenue", 0},
  {"rnd", "R&D Expense", []string{"ResearchAndDevelopmentExpense"}, "revenue", 1},
  {"sga", "SG&A Expense", []string{"SellingGeneralAndAdministrativeExpense"}, "revenue", 1},
  {"opex", "Total Operating Expenses", []string{"OperatingExpenses"}, "revenue", 1},
  {"operatingIncome", "Operating Income (EBIT)", []string{"OperatingIncomeLoss"}, "revenue", 0},
  {"interestExpense", "Interest Expense", []string{"InterestExpense", "InterestExpenseNonoperating"}, "revenue", 1},
  {"pretaxIncome", "Pretax Income", []string{"IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"}, "revenue", 0},
  {"taxExpense", "Income Tax Expense", []string{"IncomeTaxExpenseBenefit"}, "revenue", 1},
  {"netIncome", "Net Income", []string{"NetIncomeLoss", "ProfitLoss"}, "revenue", 0},
  {"epsBasic", "EPS (Basic)", []string{"EarningsPerShareBasic"}, "", 0},
  {"epsDiluted", "EPS (Diluted)", []string{"EarningsPerShareDiluted"}, "", 0},
  {"sharesDiluted", "Diluted Shares Out.", []string{"WeightedAverageNumberOfDilutedSharesOutstanding", "WeightedAverageNumberOfSharesOutstandingBasic"}, "", 0},
}

var balanceLines = []conceptLine{
  {"cash", "Cash & Equivalents", []string{"CashAndCashEquivalentsAtCarryingValue"}, "", 0},
  {"shortTermInvestments", "Short-Term Investments", []string{"ShortTermInvestments"}, "", 1},
  {"receivables", "Accounts Receivable", []string{"AccountsReceivableNetCurrent"}, "", 1},
  {"inventory", "Inventory", []string{"InventoryNet"}, "", 1},
  {"currentAssets", "Total Current Assets", []string{"AssetsCurrent"}, "", 0},
  {"ppe", "Property, Plant & Equip.", []string{"PropertyPlantAndEquipmentNet"}, "", 1},
  {"goodwill", "Goodwill", []string{"Goodwill"}, "", 1},
  {"intangibles", "Intangibles", []string{"IntangibleAssetsNetExcludingGoodwill"}, "", 1},
  {"totalAssets", "Total Assets", []string{"Assets"}, "", 0},
  {"currentLiabilities", "Total Current Liabilities", []string{"LiabilitiesCurrent"}, "", 0},
  {"shortTermDebt", "Short-Term Debt", []string{"LongTermDebtCurrent", "DebtCurrent"}, "", 1},
  {"payables", "Accounts Payable", []string{"AccountsPayableCurrent"}, "", 1},
  {"longTermDebt", "Long-Term Debt", []string{"LongTermDebtNoncurrent", "LongTermDebt"}, "", 1},
  {"totalLiabilities", "Total Liabilities", []string{"Liabilities"}, "", 0},
  {"retainedEarnings", "Retained Earnings", []string{"RetainedEarningsAccumulatedDeficit"}, "", 1},
  {"equity", "Total Shareholder Equity", []string{"StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"}, "", 0},
}

var cashflowLines = []conceptLine{
  {"cfo", "Cash from Operations", []string{"NetCashProvidedByUsedInOperatingActivities"}, "", 0},
  {"capex", "Capital Expenditures", []string{"PaymentsToAcquirePropertyPlantAndEquipment"}, "", 1},
  {"fcf", "Free Cash Flow", nil, "", 0}, // computed: cfo - capex
  {"acquisitions", "Acquisitions", []string{"PaymentsToAcquireBusinessesNetOfCashAcquired"}, "", 1},
  {"cfi", "Cash from Investing", []string{"NetCashProvidedByUsedInInvestingActivities"}, "", 0},
  {"debtIssued", "Debt Issued", []string{"ProceedsFromIssuanceOfLongTermDebt"}, "", 1},
  {"debtRepaid", "Debt Repaid", []string{"RepaymentsOfLongTermDebt"}, "", 1},
  {"buybacks", "Share Repurchases", []string{"PaymentsForRepurchaseOfCommonStock"}, "", 1},
  {"dividendsPaid", "Dividends Paid", []string{"PaymentsOfDividendsCommonStock", "PaymentsOfDividends"}, "", 1},
  {"cff", "Cash from Financing", []string{"NetCashProvidedByUsedInFinancingActivities"}, "", 0},
  {"netChangeCash", "Net Change in Cash", []string{"CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect", "CashCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"}, "", 0},
}

type StatementLine struct {
  Key       string              `json:"key"`
  Label     string              `json:"label"`
  Values    map[string]*float64 `json:"values"`
  PercentOf string              `json:"percentOf,omitempty"`
  Indent    int                 `json:"indent,omitempty"`
}

type Statement struct {
  Symbol        string          `json:"symbol"`
  CIK           string          `json:"cik"`
  EntityName    string          `json:"entityName"`
  StatementType string          `json:"statementType"`
  PeriodType    string          `json:"periodType"`
  Currency      string          `json:"currency"`
  Periods       []string        `json:"periods"`
  Lines         []StatementLine `json:"lines"`
}

func parseDay(s string) (time.Time, bool) {
  t, err := time.Parse("2006-01-02", s)
  return t, err == nil
}

func durationDays(e factEntry) float64 {
  if e.Start == "" {
    return 0 // instant fact (balance sheet)
  }
  s, ok1 := parseDay(e.Start)
  t, ok2 := parseDay(e.End)
  if !ok1 || !ok2 {
    return -1
  }
  return t.Sub(s).Hours() / 24
}

func periodLabel(end string, annual bool) (string, bool) {
  t, ok := parseDay(end)
  if !ok {
    return "", false
  }
  if annual {
    return fmt.Sprintf("FY%d", t.Year()), true
  }
  // quarter of the period end; fiscal quarters ending in the first half of a
  // month are attributed to the previous calendar quarter (52/53-week years).
  q := int(t.Month()-1) / 3
  if t.Day() > 15 {
    q++
  }
  y := t.Year()
  if q == 0 {
    q, y = 4, y-1
  }
  return fmt.Sprintf("Q%d %d", q, y), true
}

// factsForPeriods buckets a concept's facts by period label. Balance-sheet
// (instant) facts are bucketed by their date; duration facts must match the
// period length. As in the TS code the last (most recently filed) value wins.
func factsForPeriods(c conceptFacts, period string, instant bool) map[string]float64 {
  out := map[string]float64{}
  annual := period == "ANNUAL" || period == "TTM"
  var entries []factEntry
  for _, u := range []string{"USD", "shares", "USD/shares"} {
    entries = append(entries, c.Units[u]...)
  }
  for _, e := range entries {
    dur := durationDays(e)
    if instant {
      if dur != 0 {
        continue
      }
      if annual && e.Form != "" && !strings.HasPrefix(e.Form, "10-K") {
        continue
      }
      if !annual && e.Form != "" && !strings.HasPrefix(e.Form, "10-Q") && !strings.HasPrefix(e.Form, "10-K") {
        continue
      }
    } else if annual {
      if dur < 300 || dur > 400 {
        continue
      }
      if period == "ANNUAL" && e.Form != "" && !strings.HasPrefix(e.Form, "10-K") {
        continue
      }
    } else {
      if dur < 60 || dur > 100 {
        continue
      }
      if e.Form != "" && !strings.HasPrefix(e.Form, "10-Q") && !strings.HasPrefix(e.Form, "10-K") {
        continue
      }
    }
    if label, ok := periodLabel(e.End, annual); ok {
      out[label] = e.Val
    }
  }
  return out
}

var (
  yearRe    = regexp.MustCompile(`(\d{4})`)
  quarterRe = regexp.MustCompile(`Q(\d)`)
)

func periodOrder(s string) int {
  y, q := 0, 0
  if m := yearRe.FindStringSubmatch(s); m != nil {
    y, _ = strconv.Atoi(m[1])
  }
  if m := quarterRe.FindStringSubmatch(s); m != nil {
    q, _ = strconv.Atoi(m[1])
  }
  return y*10 + q
}

func normalizeStatementType(s string) (string, bool) {
  switch strings.ToUpper(strings.ReplaceAll(s, "-", "_")) {
  case "INCOME", "INCOME_STATEMENT", "IS":
    return "INCOME_STATEMENT", true
  case "BALANCE", "BALANCE_SHEET", "BS":
    return "BALANCE_SHEET", true
  case "CASHFLOW", "CASH_FLOW", "CF":
    return "CASH_FLOW", true
  }
  return "", false
}

func buildStatement(cf *companyFacts, info CompanyInfo, stType, period string, limit int) (*Statement, error) {
  defs := incomeLines
  instant := false
  switch stType {
  case "BALANCE_SHEET":
    defs, instant = balanceLines, true
  case "CASH_FLOW":
    defs = cashflowLines
  }
  maps := make([]map[string]float64, len(defs))
  all := map[string]bool{}
  for i, def := range defs {
    maps[i] = map[string]float64{}
    for _, name := range def.Concepts {
      c, ok := cf.Facts["us-gaap"][name]
      if !ok {
        continue
      }
      if got := factsForPeriods(c, period, instant); len(got) > 0 {
        maps[i] = got // first concept with data wins (priority order)
        break
      }
    }
    for p := range maps[i] {
      all[p] = true
    }
  }
  periods := make([]string, 0, len(all))
  for p := range all {
    periods = append(periods, p)
  }
  sort.Slice(periods, func(a, b int) bool { return periodOrder(periods[a]) < periodOrder(periods[b]) })
  if len(periods) > limit {
    periods = periods[len(periods)-limit:]
  }
  if len(periods) == 0 {
    return nil, fmt.Errorf("no %s facts available for %s", period, firstNonEmpty(info.Ticker, info.CIK))
  }
  st := &Statement{
    Symbol: info.Ticker, CIK: info.CIK10, EntityName: firstNonEmpty(cf.EntityName, info.Name),
    StatementType: stType, PeriodType: period, Currency: "USD", Periods: periods,
  }
  idx := map[string]int{}
  for i, def := range defs {
    vals := map[string]*float64{}
    for _, p := range periods {
      if v, ok := maps[i][p]; ok {
        vv := v
        vals[p] = &vv
      } else {
        vals[p] = nil
      }
    }
    idx[def.Key] = len(st.Lines)
    st.Lines = append(st.Lines, StatementLine{Key: def.Key, Label: def.Label, Values: vals, PercentOf: def.PercentOf, Indent: def.Indent})
  }
  if stType == "CASH_FLOW" {
    // PaymentsToAcquirePropertyPlantAndEquipment is reported as a positive
    // outflow, so FCF = CFO - capex (the TS code added them).
    cfo, capex, fcf := st.Lines[idx["cfo"]], st.Lines[idx["capex"]], idx["fcf"]
    for _, p := range periods {
      if o, x := cfo.Values[p], capex.Values[p]; o != nil && x != nil {
        v := *o - *x
        st.Lines[fcf].Values[p] = &v
      }
    }
  }
  return st, nil
}

func cmdStatements(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("statements", &o)
  cacheDir := addSecFlags(fs)
  typ := fs.String("type", "INCOME_STATEMENT", "income | balance | cashflow (or INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW)")
  period := fs.String("period", "ANNUAL", "ANNUAL | QUARTERLY | TTM")
  limit := fs.Int("limit", 5, "number of most recent periods (max 12)")
  list := splitList(parseArgs(fs, args))
  if len(list) == 0 {
    return errors.New("statements: ticker or CIK required")
  }
  stType, ok := normalizeStatementType(*typ)
  if !ok {
    return fmt.Errorf("statements: invalid -type %q", *typ)
  }
  per := strings.ToUpper(*period)
  if per != "ANNUAL" && per != "QUARTERLY" && per != "TTM" {
    return fmt.Errorf("statements: invalid -period %q", *period)
  }
  lim := *limit
  if lim < 1 {
    lim = 1
  }
  if lim > 12 {
    lim = 12
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, t := range list {
      ci := &callInfo{}
      key := strings.ToUpper(t) + ":" + stType + ":" + per
      info, err := s.resolve(ctx, ci, t)
      if err == nil {
        var cf *companyFacts
        if cf, err = s.companyFacts(ctx, ci, info.CIK); err == nil {
          var st *Statement
          if st, err = buildStatement(cf, info, stType, per, lim); err == nil {
            d.publish(ci.env("statements", key, st))
            continue
          }
        }
      }
      d.publish(envFail("statements", key, err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Insider (Form 4): submissions per issuer, or EDGAR "latest filings" feed
// ---------------------------------------------------------------------------

type InsiderTxn struct {
  Security         string   `json:"security"`
  Date             string   `json:"date"`
  Code             string   `json:"code"`
  AcquiredDisposed string   `json:"acquiredDisposed"`
  Shares           *float64 `json:"shares"`
  Price            *float64 `json:"price"`
  Value            *float64 `json:"value"`
  SharesOwnedAfter *float64 `json:"sharesOwnedAfter"`
  DirectIndirect   string   `json:"directOrIndirect"`
  Derivative       bool     `json:"derivative"`
}

type InsiderFiling struct {
  AccessionNumber string       `json:"accessionNumber"`
  Form            string       `json:"form"`
  FilingDate      string       `json:"filingDate"`
  PeriodOfReport  string       `json:"periodOfReport,omitempty"`
  IssuerCIK       string       `json:"issuerCik"`
  IssuerName      string       `json:"issuerName"`
  IssuerTicker    string       `json:"issuerTicker,omitempty"`
  ReportingName   string       `json:"reportingName"`
  ReportingCIK    string       `json:"reportingCik,omitempty"`
  ReportingTitle  string       `json:"reportingTitle"`
  URL             string       `json:"url"`
  Transactions    []InsiderTxn `json:"transactions"`
  DetailsError    string       `json:"detailsError,omitempty"`
}

type f4Value struct {
  Value string `xml:"value"`
}

type f4Txn struct {
  Security f4Value `xml:"securityTitle"`
  Date     f4Value `xml:"transactionDate"`
  Coding   struct {
    Code string `xml:"transactionCode"`
  } `xml:"transactionCoding"`
  Amounts struct {
    Shares f4Value `xml:"transactionShares"`
    Price  f4Value `xml:"transactionPricePerShare"`
    AD     f4Value `xml:"transactionAcquiredDisposedCode"`
  } `xml:"transactionAmounts"`
  Post struct {
    Owned f4Value `xml:"sharesOwnedFollowingTransaction"`
  } `xml:"postTransactionAmounts"`
  Nature struct {
    DI f4Value `xml:"directOrIndirectOwnership"`
  } `xml:"ownershipNature"`
}

type ownershipDoc struct {
  PeriodOfReport string `xml:"periodOfReport"`
  Issuer         struct {
    CIK    string `xml:"issuerCik"`
    Name   string `xml:"issuerName"`
    Ticker string `xml:"issuerTradingSymbol"`
  } `xml:"issuer"`
  Owners []struct {
    ID struct {
      CIK  string `xml:"rptOwnerCik"`
      Name string `xml:"rptOwnerName"`
    } `xml:"reportingOwnerId"`
    Rel struct {
      IsDirector   string `xml:"isDirector"`
      IsOfficer    string `xml:"isOfficer"`
      IsTenPct     string `xml:"isTenPercentOwner"`
      IsOther      string `xml:"isOther"`
      OfficerTitle string `xml:"officerTitle"`
      OtherText    string `xml:"otherText"`
    } `xml:"reportingOwnerRelationship"`
  } `xml:"reportingOwner"`
  NonDeriv []f4Txn `xml:"nonDerivativeTable>nonDerivativeTransaction"`
  Deriv    []f4Txn `xml:"derivativeTable>derivativeTransaction"`
}

func xmlTrue(s string) bool {
  s = strings.TrimSpace(strings.ToLower(s))
  return s == "1" || s == "true"
}

func parseFloatPtr(s string) *float64 {
  s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
  if s == "" {
    return nil
  }
  f, err := strconv.ParseFloat(s, 64)
  if err != nil {
    return nil
  }
  return &f
}

func applyForm4(f *InsiderFiling, doc *ownershipDoc) {
  f.PeriodOfReport = doc.PeriodOfReport
  f.IssuerName = firstNonEmpty(f.IssuerName, doc.Issuer.Name)
  f.IssuerTicker = strings.ToUpper(strings.TrimSpace(doc.Issuer.Ticker))
  if doc.Issuer.CIK != "" {
    f.IssuerCIK = padCIK(doc.Issuer.CIK)
  }
  var names, titles []string
  for _, ow := range doc.Owners {
    names = append(names, strings.TrimSpace(ow.ID.Name))
    if f.ReportingCIK == "" {
      f.ReportingCIK = padCIK(ow.ID.CIK)
    }
    var rel []string
    if t := strings.TrimSpace(ow.Rel.OfficerTitle); t != "" {
      rel = append(rel, t)
    } else if xmlTrue(ow.Rel.IsOfficer) {
      rel = append(rel, "Officer")
    }
    if xmlTrue(ow.Rel.IsDirector) {
      rel = append(rel, "Director")
    }
    if xmlTrue(ow.Rel.IsTenPct) {
      rel = append(rel, "10% Owner")
    }
    if xmlTrue(ow.Rel.IsOther) {
      rel = append(rel, firstNonEmpty(strings.TrimSpace(ow.Rel.OtherText), "Other"))
    }
    titles = append(titles, strings.Join(rel, ", "))
  }
  if len(names) > 0 {
    f.ReportingName = strings.Join(names, "; ")
    f.ReportingTitle = strings.Join(titles, "; ")
  }
  conv := func(list []f4Txn, deriv bool) {
    for _, t := range list {
      tx := InsiderTxn{
        Security:         strings.TrimSpace(t.Security.Value),
        Date:             strings.TrimSpace(t.Date.Value),
        Code:             strings.TrimSpace(t.Coding.Code),
        AcquiredDisposed: strings.TrimSpace(t.Amounts.AD.Value),
        Shares:           parseFloatPtr(t.Amounts.Shares.Value),
        Price:            parseFloatPtr(t.Amounts.Price.Value),
        SharesOwnedAfter: parseFloatPtr(t.Post.Owned.Value),
        DirectIndirect:   strings.TrimSpace(t.Nature.DI.Value),
        Derivative:       deriv,
      }
      if len(tx.Date) > 10 {
        tx.Date = tx.Date[:10]
      }
      if tx.Shares != nil && tx.Price != nil {
        v := *tx.Shares * *tx.Price
        tx.Value = &v
      }
      f.Transactions = append(f.Transactions, tx)
    }
  }
  conv(doc.NonDeriv, false)
  conv(doc.Deriv, true)
}

type archiveIndex struct {
  Directory struct {
    Item []struct {
      Name         string `json:"name"`
      Type         string `json:"type"`
      Size         any    `json:"size"`
      LastModified string `json:"last-modified"`
    } `json:"item"`
  } `json:"directory"`
}

func (s *secClient) archiveIndex(ctx context.Context, ci *callInfo, cik, accession string) (*archiveIndex, error) {
  var idx archiveIndex
  if err := s.getJSON(ctx, ci, archiveURL(cik, accession, "index.json"), 24*time.Hour, &idx); err != nil {
    return nil, err
  }
  return &idx, nil
}

// form4XML locates and parses the ownership XML of a Form 4 filing.
func (s *secClient) form4XML(ctx context.Context, ci *callInfo, cik, accession, primaryDoc string) (*ownershipDoc, error) {
  doc := ""
  if strings.HasSuffix(strings.ToLower(primaryDoc), ".xml") {
    doc = primaryDoc[strings.LastIndex(primaryDoc, "/")+1:] // strip the xslF345X0n/ rendering dir
  } else {
    idx, err := s.archiveIndex(ctx, ci, cik, accession)
    if err != nil {
      return nil, err
    }
    for _, it := range idx.Directory.Item {
      if strings.HasSuffix(strings.ToLower(it.Name), ".xml") {
        doc = it.Name
        break
      }
    }
    if doc == "" {
      return nil, errors.New("no ownership XML in filing")
    }
  }
  b, err := s.raw(ctx, ci, archiveURL(cik, accession, doc), 24*time.Hour)
  if err != nil {
    return nil, err
  }
  var od ownershipDoc
  if err := xmlDecode(b, &od); err != nil {
    return nil, fmt.Errorf("form 4 XML: %w", err)
  }
  return &od, nil
}

type atomFeed struct {
  Entries []struct {
    Title   string `xml:"title"`
    Summary string `xml:"summary"`
    Updated string `xml:"updated"`
    ID      string `xml:"id"`
    Link    struct {
      Href string `xml:"href,attr"`
    } `xml:"link"`
  } `xml:"entry"`
}

var (
  currentTitleRe = regexp.MustCompile(`^(\S+)\s+-\s+(.*?)\s+\((\d{10})\)\s+\((\w+)\)`)
  accessionRe    = regexp.MustCompile(`(\d{10}-\d{2}-\d{6})`)
  filedRe        = regexp.MustCompile(`Filed:(?:</b>)?\s*(\d{4}-\d{2}-\d{2})`)
)

// latestForm4 reads EDGAR's "current events" Atom feed. Every Form 4 appears
// twice (Issuer and Reporting); entries are merged per accession number.
func (s *secClient) latestForm4(ctx context.Context, ci *callInfo, count int) ([]*InsiderFiling, error) {
  q := url.Values{}
  q.Set("action", "getcurrent")
  q.Set("type", "4")
  q.Set("owner", "include")
  q.Set("start", "0")
  q.Set("count", strconv.Itoa(count))
  q.Set("output", "atom")
  b, err := s.raw(ctx, ci, "https://www.sec.gov/cgi-bin/browse-edgar?"+q.Encode(), s.ttl(2*time.Minute))
  if err != nil {
    return nil, err
  }
  var feed atomFeed
  if err := xmlDecode(b, &feed); err != nil {
    return nil, fmt.Errorf("EDGAR current feed: %w", err)
  }
  byAcc := map[string]*InsiderFiling{}
  var order []*InsiderFiling
  for _, e := range feed.Entries {
    m := currentTitleRe.FindStringSubmatch(strings.TrimSpace(e.Title))
    acc := accessionRe.FindString(e.ID)
    if acc == "" {
      acc = accessionRe.FindString(e.Link.Href)
    }
    if m == nil || acc == "" {
      continue
    }
    f := byAcc[acc]
    if f == nil {
      f = &InsiderFiling{AccessionNumber: acc, Form: m[1], Transactions: []InsiderTxn{}}
      if fm := filedRe.FindStringSubmatch(e.Summary); fm != nil {
        f.FilingDate = fm[1]
      } else if len(e.Updated) >= 10 {
        f.FilingDate = e.Updated[:10]
      }
      byAcc[acc] = f
      order = append(order, f)
    }
    switch strings.ToLower(m[4]) {
    case "issuer":
      f.IssuerName, f.IssuerCIK = m[2], m[3]
    case "reporting":
      f.ReportingName, f.ReportingCIK = m[2], m[3]
    }
  }
  for _, f := range order {
    cik := firstNonEmpty(f.IssuerCIK, f.ReportingCIK)
    f.URL = archiveURL(cik, f.AccessionNumber, "")
  }
  return order, nil
}

func cmdInsider(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("insider", &o)
  cacheDir := addSecFlags(fs)
  marketwide := fs.Bool("marketwide", false, "latest Form 4 filings across all issuers")
  limit := fs.Int("limit", 40, "max filings")
  details := fs.Bool("details", true, "fetch and parse each Form 4 XML for transactions")
  list := splitList(parseArgs(fs, args))
  if !*marketwide && len(list) == 0 {
    return errors.New("insider: ticker/CIK or -marketwide required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  d := newDelta(o.all)
  publish := func(ci *callInfo, fl []*InsiderFiling) {
    for i := len(fl) - 1; i >= 0; i-- { // oldest first
      f := fl[i]
      if *details {
        cik := strings.TrimLeft(firstNonEmpty(f.IssuerCIK, f.ReportingCIK), "0")
        primary := ""
        if strings.HasPrefix(f.URL, "https://www.sec.gov/Archives/") && strings.HasSuffix(strings.ToLower(f.URL), ".xml") {
          primary = f.URL[strings.LastIndex(f.URL, "/")+1:]
        }
        if doc, err := s.form4XML(ctx, ci, cik, f.AccessionNumber, primary); err == nil {
          applyForm4(f, doc)
        } else {
          f.DetailsError = err.Error()
        }
      }
      d.publish(ci.env("insider", f.AccessionNumber, f))
    }
  }
  return loop(ctx, &o, func(ctx context.Context) error {
    if *marketwide {
      ci := &callInfo{}
      fl, err := s.latestForm4(ctx, ci, minInt(100, maxInt(*limit*2, 10)))
      if err != nil {
        d.publish(envFail("insider", "marketwide", err))
        return nil
      }
      if len(fl) > *limit {
        fl = fl[:*limit]
      }
      publish(ci, fl)
      return nil
    }
    for _, t := range list {
      ci := &callInfo{}
      info, err := s.resolve(ctx, ci, t)
      if err != nil {
        d.publish(envFail("insider", strings.ToUpper(t), err))
        continue
      }
      filings, err := s.filings(ctx, ci, info, map[string]bool{"4": true, "4/A": true})
      if err != nil {
        d.publish(envFail("insider", strings.ToUpper(t), err))
        continue
      }
      if len(filings) > *limit {
        filings = filings[:*limit]
      }
      var fl []*InsiderFiling
      for _, f := range filings {
        fl = append(fl, &InsiderFiling{
          AccessionNumber: f.AccessionNumber, Form: f.Form, FilingDate: f.FilingDate,
          PeriodOfReport: f.ReportDate, IssuerCIK: info.CIK10, IssuerName: f.EntityName,
          IssuerTicker: info.Ticker, URL: f.URL, Transactions: []InsiderTxn{},
        })
      }
      publish(ci, fl)
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Institutional: EFTS full-text search for 13F-HR filings naming the issuer
// ---------------------------------------------------------------------------

type eftsResponse struct {
  Hits struct {
    Total struct {
      Value int `json:"value"`
    } `json:"total"`
    Hits []struct {
      ID     string `json:"_id"`
      Source struct {
        Ciks         []string `json:"ciks"`
        DisplayNames []string `json:"display_names"`
        FileDate     string   `json:"file_date"`
        Adsh         string   `json:"adsh"`
        Form         string   `json:"form"`
        PeriodEnding string   `json:"period_ending"`
      } `json:"_source"`
    } `json:"hits"`
  } `json:"hits"`
}

type Filer struct {
  CIK          string `json:"cik"`
  Name         string `json:"name"`
  FilingDate   string `json:"filingDate"`
  PeriodEnding string `json:"periodEnding,omitempty"`
  Accession    string `json:"accessionNumber"`
}

var (
  cikSuffixRe = regexp.MustCompile(`\s*\(CIK\s+\d+\)\s*`)
  punctRe     = regexp.MustCompile(`[^A-Z0-9 &]+`)
)

func cleanDisplayName(s string) string {
  return strings.TrimSpace(cikSuffixRe.ReplaceAllString(s, " "))
}

// latestValue returns the most recent fact (by period end) of a concept.
func latestValue(cf *companyFacts, names ...string) (*KeyFigure, bool) {
  for _, n := range names {
    c, ok := cf.concept(n)
    if !ok {
      continue
    }
    unit, xs := pickUnit(c)
    best := -1
    for i, e := range xs {
      if best < 0 || e.End > xs[best].End || (e.End == xs[best].End && e.Filed >= xs[best].Filed) {
        best = i
      }
    }
    if best >= 0 {
      e := xs[best]
      return &KeyFigure{Value: e.Val, End: e.End, Form: e.Form, Concept: n, Unit: unit}, true
    }
  }
  return nil, false
}

func cmdInstitutional(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("institutional", &o)
  cacheDir := addSecFlags(fs)
  days := fs.Int("days", 200, "look-back window for 13F-HR filings")
  list := splitList(parseArgs(fs, args))
  if len(list) == 0 {
    return errors.New("institutional: ticker required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, t := range list {
      ci := &callInfo{}
      key := strings.ToUpper(t)
      info, err := s.resolve(ctx, ci, t)
      if err != nil {
        d.publish(envFail("institutional", key, err))
        continue
      }
      // 13F information tables list issuer names (not tickers), so search
      // for the normalized company name.
      name := strings.Join(strings.Fields(punctRe.ReplaceAllString(strings.ToUpper(info.Name), " ")), " ")
      now := time.Now().UTC()
      q := url.Values{}
      q.Set("q", `"`+name+`"`)
      q.Set("forms", "13F-HR")
      q.Set("dateRange", "custom")
      q.Set("startdt", now.AddDate(0, 0, -*days).Format("2006-01-02"))
      q.Set("enddt", now.Format("2006-01-02"))
      var er eftsResponse
      if err := s.getJSON(ctx, ci, "https://efts.sec.gov/LATEST/search-index?"+q.Encode(), s.ttl(30*time.Minute), &er); err != nil {
        d.publish(envFail("institutional", key, err))
        continue
      }
      filers := []Filer{}
      for _, h := range er.Hits.Hits {
        src := h.Source
        f := Filer{FilingDate: src.FileDate, PeriodEnding: src.PeriodEnding, Accession: src.Adsh}
        if len(src.Ciks) > 0 {
          f.CIK = src.Ciks[0]
        }
        if len(src.DisplayNames) > 0 {
          f.Name = cleanDisplayName(src.DisplayNames[0])
        }
        filers = append(filers, f)
      }
      out := map[string]any{
        "ticker": info.Ticker, "cik": info.CIK10, "name": info.Name, "searchName": name,
        "totalFilers": er.Hits.Total.Value, "filers": filers,
        "sharesOutstanding": nil, "sharesOutstandingDate": nil,
      }
      // Shares outstanding for % calculations in the backend. The TS route's
      // simulated quarterly trend is intentionally not reproduced.
      if cf, err := s.companyFacts(ctx, ci, info.CIK); err == nil {
        if kf, ok := latestValue(cf, "EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding"); ok {
          out["sharesOutstanding"], out["sharesOutstandingDate"] = kf.Value, kf.End
        }
      }
      d.publish(ci.env("institutional", key, out))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// 13F-HR information table
// ---------------------------------------------------------------------------

type infoTable struct {
  Entries []struct {
    Name     string `xml:"nameOfIssuer"`
    Class    string `xml:"titleOfClass"`
    CUSIP    string `xml:"cusip"`
    Value    string `xml:"value"`
    PutCall  string `xml:"putCall"`
    Discret  string `xml:"investmentDiscretion"`
    ShrsOrPr struct {
      Amount string `xml:"sshPrnamt"`
      Type   string `xml:"sshPrnamtType"`
    } `xml:"shrsOrPrnAmt"`
    Voting struct {
      Sole   string `xml:"Sole"`
      Shared string `xml:"Shared"`
      None   string `xml:"None"`
    } `xml:"votingAuthority"`
  } `xml:"infoTable"`
}

type Holding struct {
  Issuer       string  `json:"issuer"`
  Class        string  `json:"class"`
  CUSIP        string  `json:"cusip"`
  PutCall      string  `json:"putCall,omitempty"`
  Value        float64 `json:"value"`
  Shares       float64 `json:"shares"`
  ShareType    string  `json:"shareType"`
  PctPortfolio float64 `json:"pctPortfolio"`
}

func cmd13F(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("13f", &o)
  cacheDir := addSecFlags(fs)
  accFlag := fs.String("accession", "", "specific 13F-HR accession (default latest)")
  top := fs.Int("top", 0, "only the N largest positions (0 = all)")
  list := splitList(parseArgs(fs, args))
  if len(list) == 0 {
    return errors.New("13f: manager CIK required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, arg := range list {
      ci := &callInfo{}
      if !cikRe.MatchString(arg) {
        d.publish(envFail("13f", arg, fmt.Errorf("invalid CIK %q", arg)))
        continue
      }
      cik := strings.TrimLeft(arg, "0")
      out, err := s.thirteenF(ctx, ci, cik, *accFlag, *top)
      if err != nil {
        d.publish(envFail("13f", padCIK(cik), err))
        continue
      }
      d.publish(ci.env("13f", padCIK(cik), out))
    }
    return nil
  })
}

func (s *secClient) thirteenF(ctx context.Context, ci *callInfo, cik, wantAcc string, top int) (map[string]any, error) {
  sub, err := s.submissions(ctx, ci, cik)
  if err != nil {
    return nil, err
  }
  r := sub.Filings.Recent
  idx := -1
  for i, form := range r.Form {
    acc := at(r.AccessionNumber, i)
    if wantAcc != "" {
      if strings.ReplaceAll(acc, "-", "") == strings.ReplaceAll(wantAcc, "-", "") {
        idx = i
        break
      }
      continue
    }
    if form == "13F-HR" {
      idx = i
      break
    }
  }
  if idx < 0 {
    return nil, errors.New("no 13F-HR filing found in recent submissions")
  }
  acc := at(r.AccessionNumber, idx)
  filed := at(r.FilingDate, idx)
  aidx, err := s.archiveIndex(ctx, ci, cik, acc)
  if err != nil {
    return nil, err
  }
  doc := ""
  for _, it := range aidx.Directory.Item {
    n := strings.ToLower(it.Name)
    if strings.HasSuffix(n, ".xml") && n != "primary_doc.xml" {
      doc = it.Name
      break
    }
  }
  if doc == "" {
    return nil, errors.New("information table XML not found in filing")
  }
  b, err := s.raw(ctx, ci, archiveURL(cik, acc, doc), 24*time.Hour)
  if err != nil {
    return nil, err
  }
  var tbl infoTable
  if err := xmlDecode(b, &tbl); err != nil {
    return nil, fmt.Errorf("13F information table: %w", err)
  }
  // Filings made from 2023-01-03 report value in dollars, earlier ones in
  // thousands; normalize to dollars.
  mult := 1.0
  if filed < "2023-01-03" {
    mult = 1000
  }
  agg := map[string]*Holding{}
  var order []string
  total := 0.0
  for _, e := range tbl.Entries {
    val := 0.0
    if v := parseFloatPtr(e.Value); v != nil {
      val = *v * mult
    }
    sh := 0.0
    if v := parseFloatPtr(e.ShrsOrPr.Amount); v != nil {
      sh = *v
    }
    k := strings.ToUpper(strings.TrimSpace(e.CUSIP)) + "|" + strings.ToUpper(strings.TrimSpace(e.PutCall))
    h := agg[k]
    if h == nil {
      h = &Holding{Issuer: strings.TrimSpace(e.Name), Class: strings.TrimSpace(e.Class), CUSIP: strings.TrimSpace(e.CUSIP),
        PutCall: strings.TrimSpace(e.PutCall), ShareType: strings.TrimSpace(e.ShrsOrPr.Type)}
      agg[k] = h
      order = append(order, k)
    }
    h.Value += val
    h.Shares += sh
    total += val
  }
  holdings := make([]Holding, 0, len(order))
  for _, k := range order {
    h := agg[k]
    if total > 0 {
      h.PctPortfolio = h.Value / total * 100
    }
    holdings = append(holdings, *h)
  }
  sort.SliceStable(holdings, func(a, b int) bool { return holdings[a].Value > holdings[b].Value })
  if top > 0 && len(holdings) > top {
    holdings = holdings[:top]
  }
  return map[string]any{
    "cik": padCIK(cik), "filer": sub.Name, "accessionNumber": acc, "form": at(r.Form, idx),
    "filingDate": filed, "reportDate": at(r.ReportDate, idx), "totalValue": total,
    "positions": len(order), "holdings": holdings, "url": archiveURL(cik, acc, doc),
  }, nil
}

// ---------------------------------------------------------------------------
// Filing document reader
// ---------------------------------------------------------------------------

type Section struct {
  ID     string `json:"id"`
  Title  string `json:"title"`
  Anchor string `json:"anchor"`
}

type Exhibit struct {
  Name string `json:"name"`
  Type string `json:"type"`
  Size any    `json:"size,omitempty"`
  URL  string `json:"url"`
}

var sanitizers = []struct {
  re   *regexp.Regexp
  with string
}{
  {regexp.MustCompile(`(?is)<script.*?</script\s*>`), ""},
  {regexp.MustCompile(`(?i)<script[^>]*/>`), ""},
  {regexp.MustCompile(`(?is)<style.*?</style\s*>`), ""},
  {regexp.MustCompile(`(?is)<iframe.*?</iframe\s*>`), ""},
  {regexp.MustCompile(`(?is)<form.*?</form\s*>`), ""},
  {regexp.MustCompile(`(?is)<object.*?</object\s*>`), ""},
  {regexp.MustCompile(`(?i)<(?:embed|base|link|meta)\b[^>]*>`), ""},
  {regexp.MustCompile(`(?i)\son\w+\s*=\s*"[^"]*"`), ""},
  {regexp.MustCompile(`(?i)\son\w+\s*=\s*'[^']*'`), ""},
  {regexp.MustCompile(`(?i)\son\w+\s*=\s*[^\s>]+`), ""},
  {regexp.MustCompile(`(?i)javascript:`), "#"},
}

func sanitizeHTML(h string) string {
  for _, s := range sanitizers {
    h = s.re.ReplaceAllString(h, s.with)
  }
  return h
}

var (
  sectionRe = regexp.MustCompile(`(?i)item\s+(\d+[A-C]?)\.?\s+([^<]{0,90})`)
  anchorRe  = regexp.MustCompile(`<a[^>]+href="#([^"]+)"[^>]*>\s*(?:<[^>]+>\s*)*([Ii][Tt][Ee][Mm]\s+\d+[A-Ca-c]?[\s.:\x{2013}\x{2014}-]*[^<]{0,80})`)
  headRe    = regexp.MustCompile(`<(?:h[1-4]|b|p)[^>]*>\s*([Ii][Tt][Ee][Mm]\s+\d+[A-Ca-c]?[\s.:\x{2013}\x{2014}-][^<]{2,80})\s*</(?:h[1-4]|b|p)>`)
  spacesRe  = regexp.MustCompile(`\s+`)
  itemPfxRe = regexp.MustCompile(`(?i)^item\s+`)
)

func clip(s string, n int) string {
  r := []rune(s)
  if len(r) > n {
    return string(r[:n])
  }
  return s
}

func extractSections(h string) []Section {
  sections := []Section{}
  seen := map[string]bool{}
  for _, m := range anchorRe.FindAllStringSubmatch(h, -1) {
    raw := strings.TrimSpace(spacesRe.ReplaceAllString(m[2], " "))
    id := fmt.Sprintf("sec-%d", len(sections))
    if p := sectionRe.FindStringSubmatch(raw); p != nil {
      id = "item-" + strings.ToLower(p[1])
    }
    title := raw
    if len(raw) > 2 {
      title = clip(itemPfxRe.ReplaceAllString(raw, "Item "), 90)
    }
    if title != "" && !seen[id] {
      seen[id] = true
      sections = append(sections, Section{ID: id, Title: title, Anchor: m[1]})
    }
    if len(sections) >= 40 {
      return sections
    }
  }
  if len(sections) == 0 {
    for _, m := range headRe.FindAllStringSubmatch(h, -1) {
      raw := strings.TrimSpace(spacesRe.ReplaceAllString(m[1], " "))
      p := sectionRe.FindStringSubmatch(raw)
      if p == nil {
        continue
      }
      id := "item-" + strings.ToLower(p[1])
      if !seen[id] {
        seen[id] = true
        sections = append(sections, Section{ID: id, Title: clip(raw, 90), Anchor: ""})
      }
      if len(sections) >= 40 {
        break
      }
    }
  }
  return sections
}

var (
  accFlagRe = regexp.MustCompile(`^\d{10}-?\d{2}-?\d{6}$`)
  docRe     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._\-]{0,200}$`)
  exhibitRe = regexp.MustCompile(`(?i)\.(htm|html|txt)$`)
)

func cmdFilingDoc(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("filing-doc", &o)
  cacheDir := addSecFlags(fs)
  cikF := fs.String("cik", "", "filer CIK")
  accF := fs.String("accession", "", "accession number (with or without dashes)")
  docF := fs.String("doc", "", "document file name within the filing")
  noHTML := fs.Bool("no-html", false, "omit the sanitized HTML body (TOC and exhibits only)")
  parseArgs(fs, args)
  cik := strings.TrimLeft(*cikF, "0")
  if !cikRe.MatchString(*cikF) || cik == "" || !accFlagRe.MatchString(*accF) || !docRe.MatchString(*docF) || strings.Contains(*docF, "..") {
    return errors.New("filing-doc: valid -cik, -accession and -doc are required")
  }
  s, err := newSEC(&o, *cacheDir)
  if err != nil {
    return err
  }
  key := strings.ReplaceAll(*accF, "-", "") + "/" + *docF
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    ci := &callInfo{}
    docURL := archiveURL(cik, *accF, *docF)
    b, err := s.raw(ctx, ci, docURL, 24*time.Hour)
    if err != nil {
      d.publish(envFail("filing-doc", key, fmt.Errorf("document fetch failed: %w", err)))
      return nil
    }
    h := string(b)
    exhibits := []Exhibit{}
    if idx, err := s.archiveIndex(ctx, ci, cik, *accF); err == nil {
      for _, it := range idx.Directory.Item {
        if exhibitRe.MatchString(it.Name) {
          exhibits = append(exhibits, Exhibit{Name: it.Name, Type: it.Type, Size: it.Size, URL: archiveURL(cik, *accF, it.Name)})
        }
        if len(exhibits) >= 40 {
          break
        }
      }
    }
    out := map[string]any{"url": docURL, "sections": extractSections(h), "exhibits": exhibits}
    if !*noHTML {
      out["html"] = sanitizeHTML(h)
    }
    d.publish(ci.env("filing-doc", key, out))
    return nil
  })
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

// xmlDecode decodes XML, accepting the ISO-8859-1 declarations EDGAR uses.
func xmlDecode(b []byte, v any) error {
  dec := xml.NewDecoder(bytes.NewReader(b))
  dec.CharsetReader = charsetReader
  return dec.Decode(v)
}

func charsetReader(charset string, in io.Reader) (io.Reader, error) {
  switch strings.ToLower(strings.TrimSpace(charset)) {
  case "", "utf-8", "utf8", "us-ascii", "ascii":
    return in, nil
  case "iso-8859-1", "iso8859-1", "latin1", "latin-1", "iso-8859-15", "windows-1252", "cp1252":
    b, err := io.ReadAll(in)
    if err != nil {
      return nil, err
    }
    var sb strings.Builder
    for _, c := range b {
      sb.WriteRune(rune(c))
    }
    return strings.NewReader(sb.String()), nil
  }
  return nil, fmt.Errorf("unsupported XML charset %q", charset)
}

func at(xs []string, i int) string {
  if i >= 0 && i < len(xs) {
    return xs[i]
  }
  return ""
}

func minInt(a, b int) int {
  if a < b {
    return a
  }
  return b
}

func maxInt(a, b int) int {
  if a > b {
    return a
  }
  return b
}

func firstNonEmpty(xs ...string) string {
  for _, x := range xs {
    if strings.TrimSpace(x) != "" {
      return x
    }
  }
  return ""
}

func str(v any) string {
  switch t := v.(type) {
  case nil:
    return ""
  case string:
    return t
  case float64:
    return strconv.FormatFloat(t, 'f', -1, 64)
  case json.Number:
    return t.String()
  }
  return fmt.Sprint(v)
}

// ---------------------------------------------------------------------------
// Shared collector plumbing. Duplicated in every collector on purpose so each
// file stays a standalone project: envelope, flags, poll loop, delta filter,
// HTTP fetcher with retries/limits, and a small TTL memo.
// ---------------------------------------------------------------------------

const (
  qLive        = "LIVE"
  qDelayed     = "DELAYED"
  qStale       = "STALE"
  qDerived     = "DERIVED"
  qUnavailable = "UNAVAILABLE"
)

// Provenance mirrors lib/types/provenance.ts in a collector-neutral shape.
type Provenance struct {
  Source      string `json:"source"`
  Endpoint    string `json:"endpoint"`
  Key         string `json:"key,omitempty"`
  RetrievedAt string `json:"retrievedAt"`
  Quality     string `json:"quality"`
  CacheAgeMs  int64  `json:"cacheAgeMs"`
  Error       string `json:"error"`
}

// Envelope is one JSONL record on stdout.
type Envelope struct {
  Data       any        `json:"data"`
  Provenance Provenance `json:"provenance"`
}

var (
  outMu    sync.Mutex
  outW     = bufio.NewWriterSize(os.Stdout, 64<<10)
  failures int
  logger   = slog.New(slog.NewTextHandler(os.Stderr, nil))
)

func emit(env Envelope) {
  b, err := json.Marshal(env)
  if err != nil {
    b, _ = json.Marshal(envFail(env.Provenance.Endpoint, env.Provenance.Key, fmt.Errorf("encode: %w", err)))
  }
  outMu.Lock()
  defer outMu.Unlock()
  if env.Provenance.Quality == qUnavailable {
    failures++
  }
  outW.Write(b)
  outW.WriteByte('\n')
  if err := outW.Flush(); err != nil {
    os.Exit(0) // reader went away (e.g. piped into head)
  }
}

func envOK(endpoint, key, quality string, data any) Envelope {
  return Envelope{Data: data, Provenance: Provenance{
    Source:      sourceName,
    Endpoint:    endpoint,
    Key:         key,
    RetrievedAt: time.Now().UTC().Format(time.RFC3339),
    Quality:     quality,
  }}
}

func envFail(endpoint, key string, err error) Envelope {
  e := envOK(endpoint, key, qUnavailable, nil)
  e.Provenance.Error = err.Error()
  return e
}

// resultEnv turns a memo lookup into an envelope: fresh, cached, STALE (an
// older value served because the refresh failed) or UNAVAILABLE.
func resultEnv(endpoint, key, quality string, v any, age time.Duration, stale bool, err error) Envelope {
  switch {
  case stale:
    e := envOK(endpoint, key, qStale, v)
    e.Provenance.CacheAgeMs = age.Milliseconds()
    if err != nil {
      e.Provenance.Error = err.Error()
    }
    return e
  case err != nil:
    return envFail(endpoint, key, err)
  default:
    e := envOK(endpoint, key, quality, v)
    e.Provenance.CacheAgeMs = age.Milliseconds()
    return e
  }
}

type command struct {
  name, args, help string
  run              func(ctx context.Context, args []string) error
}

func runMain(cmds []command) {
  ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
  defer stop()
  usage := func() {
    fmt.Fprintf(os.Stderr, "usage: %s <command> [flags] [args]\n\n", progName)
    fmt.Fprintf(os.Stderr, "Writes JSONL {data, provenance} envelopes to stdout; logs to stderr.\n")
    fmt.Fprintf(os.Stderr, "Common flags: -every DURATION (poll and stream changed records), -all, -timeout DURATION, -ua STRING\n\ncommands:\n")
    for _, c := range cmds {
      fmt.Fprintf(os.Stderr, "  %-14s %-46s %s\n", c.name, c.args, c.help)
    }
  }
  if len(os.Args) < 2 {
    usage()
    os.Exit(2)
  }
  name := os.Args[1]
  if name == "help" || name == "-h" || name == "-help" || name == "--help" {
    usage()
    return
  }
  for _, c := range cmds {
    if c.name != name {
      continue
    }
    err := c.run(ctx, os.Args[2:])
    outMu.Lock()
    outW.Flush()
    failed := failures
    outMu.Unlock()
    if err != nil && ctx.Err() == nil {
      logger.Error(name+" failed", "err", err)
      stop()
      os.Exit(1)
    }
    if failed > 0 {
      stop()
      os.Exit(1)
    }
    return
  }
  fmt.Fprintf(os.Stderr, "unknown command %q\n\n", name)
  usage()
  os.Exit(2)
}

type opts struct {
  every   time.Duration
  timeout time.Duration
  all     bool
  ua      string
}

func newFlags(name string, o *opts) *flag.FlagSet {
  fs := flag.NewFlagSet(progName+" "+name, flag.ExitOnError)
  fs.DurationVar(&o.every, "every", 0, "poll interval; keeps running and emits changed records (0 = run once)")
  fs.DurationVar(&o.timeout, "timeout", 20*time.Second, "per-request timeout")
  fs.BoolVar(&o.all, "all", false, "with -every, emit all records on every poll, not only changed ones")
  fs.StringVar(&o.ua, "ua", envOr(envPrefix+"UA", defaultUA), "User-Agent header (env "+envPrefix+"UA)")
  return fs
}

// parseArgs allows flags and positional arguments to be interleaved.
func parseArgs(fs *flag.FlagSet, args []string) []string {
  var pos []string
  for {
    fs.Parse(args)
    rest := fs.Args()
    if len(rest) == 0 {
      return pos
    }
    pos = append(pos, rest[0])
    args = rest[1:]
  }
}

func envOr(k, def string) string {
  if v := strings.TrimSpace(os.Getenv(k)); v != "" {
    return v
  }
  return def
}

// splitList splits arguments on commas/whitespace, trims and de-duplicates.
func splitList(in []string) []string {
  seen := map[string]bool{}
  var out []string
  for _, a := range in {
    for _, p := range strings.FieldsFunc(a, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' }) {
      if p = strings.TrimSpace(p); p != "" && !seen[p] {
        seen[p] = true
        out = append(out, p)
      }
    }
  }
  return out
}

func loop(ctx context.Context, o *opts, fn func(context.Context) error) error {
  for {
    err := fn(ctx)
    if o.every <= 0 || ctx.Err() != nil {
      return err
    }
    if err != nil {
      logger.Warn("poll failed", "err", err)
    }
    t := time.NewTimer(o.every)
    select {
    case <-ctx.Done():
      t.Stop()
      return nil
    case <-t.C:
    }
  }
}

// deltaFilter suppresses records whose content did not change since they were
// last emitted under the same endpoint+key (used in -every mode).
type deltaFilter struct {
  mu   sync.Mutex
  last map[string]string
  all  bool
}

func newDelta(all bool) *deltaFilter {
  return &deltaFilter{last: map[string]string{}, all: all}
}

func (d *deltaFilter) publish(env Envelope) {
  if !d.all {
    b, _ := json.Marshal([]any{env.Data, env.Provenance.Quality, env.Provenance.Error})
    sum := sha1.Sum(b)
    h := hex.EncodeToString(sum[:10])
    k := env.Provenance.Endpoint + "|" + env.Provenance.Key
    d.mu.Lock()
    same := d.last[k] == h
    if !same {
      if len(d.last) > 200000 {
        d.last = map[string]string{}
      }
      d.last[k] = h
    }
    d.mu.Unlock()
    if same {
      return
    }
  }
  emit(env)
}

type limiter struct {
  mu   sync.Mutex
  next time.Time
  gap  time.Duration
}

func newLimiter(rps float64) *limiter {
  if rps <= 0 {
    return &limiter{}
  }
  return &limiter{gap: time.Duration(float64(time.Second) / rps)}
}

func (l *limiter) wait(ctx context.Context) error {
  if l == nil || l.gap == 0 {
    return nil
  }
  l.mu.Lock()
  now := time.Now()
  t := l.next
  if t.Before(now) {
    t = now
  }
  l.next = t.Add(l.gap)
  l.mu.Unlock()
  d := time.Until(t)
  if d <= 0 {
    return nil
  }
  timer := time.NewTimer(d)
  defer timer.Stop()
  select {
  case <-ctx.Done():
    return ctx.Err()
  case <-timer.C:
    return nil
  }
}

type httpError struct {
  Status int
  URL    string
}

func (e *httpError) Error() string { return fmt.Sprintf("HTTP %d from %s", e.Status, e.URL) }

func httpStatus(err error) int {
  var he *httpError
  if errors.As(err, &he) {
    return he.Status
  }
  return 0
}

type fetcher struct {
  client  *http.Client
  ua      string
  headers map[string]string
  limiter *limiter
  pacer   *filePacer // cross-process pacing (SEC); replaces limiter when set
  maxBody int64
  retries int
}

func newFetcher(o *opts, jar http.CookieJar, rps float64) *fetcher {
  tr := http.DefaultTransport.(*http.Transport).Clone()
  tr.MaxIdleConnsPerHost = 16
  return &fetcher{
    client:  &http.Client{Timeout: o.timeout, Jar: jar, Transport: tr},
    ua:      o.ua,
    headers: map[string]string{},
    limiter: newLimiter(rps),
    maxBody: 16 << 20,
    retries: 2,
  }
}

// do performs a single request and returns the status and size-capped body.
func (f *fetcher) do(ctx context.Context, method, u string, body []byte, hdr map[string]string) (int, []byte, error) {
  if f.pacer != nil {
    if err := f.pacer.wait(ctx); err != nil {
      return 0, nil, err
    }
  } else if err := f.limiter.wait(ctx); err != nil {
    return 0, nil, err
  }
  var rd io.Reader
  if body != nil {
    rd = bytes.NewReader(body)
  }
  req, err := http.NewRequestWithContext(ctx, method, u, rd)
  if err != nil {
    return 0, nil, err
  }
  req.Header.Set("User-Agent", f.ua)
  for k, v := range f.headers {
    req.Header.Set(k, v)
  }
  for k, v := range hdr {
    req.Header.Set(k, v)
  }
  resp, err := f.client.Do(req)
  if err != nil {
    return 0, nil, err
  }
  defer resp.Body.Close()
  b, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBody+1))
  if err != nil {
    return resp.StatusCode, nil, err
  }
  if int64(len(b)) > f.maxBody {
    return resp.StatusCode, nil, fmt.Errorf("response from %s exceeds %d bytes", u, f.maxBody)
  }
  return resp.StatusCode, b, nil
}

// fetch retries transport errors and 5xx with jittered backoff; any other
// non-2xx status is returned immediately as *httpError.
func (f *fetcher) fetch(ctx context.Context, method, u string, body []byte, hdr map[string]string) ([]byte, error) {
  var last error
  for attempt := 0; attempt <= f.retries; attempt++ {
    if attempt > 0 {
      d := time.Duration(250<<attempt)*time.Millisecond + time.Duration(rand.Int63n(int64(250*time.Millisecond)))
      t := time.NewTimer(d)
      select {
      case <-ctx.Done():
        t.Stop()
        return nil, ctx.Err()
      case <-t.C:
      }
    }
    st, b, err := f.do(ctx, method, u, body, hdr)
    switch {
    case err != nil:
      if ctx.Err() != nil {
        return nil, ctx.Err()
      }
      last = err
    case st >= 500:
      last = &httpError{st, u}
    case st < 200 || st > 299:
      return nil, &httpError{st, u}
    default:
      return b, nil
    }
  }
  return nil, last
}

func (f *fetcher) get(ctx context.Context, u string, hdr map[string]string) ([]byte, error) {
  return f.fetch(ctx, http.MethodGet, u, nil, hdr)
}

func (f *fetcher) getJSON(ctx context.Context, u string, hdr map[string]string, v any) error {
  b, err := f.get(ctx, u, hdr)
  if err != nil {
    return err
  }
  if err := json.Unmarshal(b, v); err != nil {
    return fmt.Errorf("decode %s: %w", u, err)
  }
  return nil
}

type memoEntry struct {
  v  any
  at time.Time
}

// memo is a TTL cache that also keeps the last good value so a failed refresh
// can be served as STALE.
type memo struct {
  mu sync.Mutex
  m  map[string]memoEntry
}

func newMemo() *memo { return &memo{m: map[string]memoEntry{}} }

func (c *memo) get(key string, ttl time.Duration, fn func() (any, error)) (any, time.Duration, bool, error) {
  c.mu.Lock()
  e, ok := c.m[key]
  c.mu.Unlock()
  if ok && time.Since(e.at) < ttl {
    return e.v, time.Since(e.at), false, nil
  }
  v, err := fn()
  if err == nil {
    c.mu.Lock()
    c.m[key] = memoEntry{v, time.Now()}
    c.mu.Unlock()
    return v, 0, false, nil
  }
  if ok {
    return e.v, time.Since(e.at), true, err
  }
  return nil, 0, false, err
}
