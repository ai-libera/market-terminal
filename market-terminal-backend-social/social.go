// qube-social: crowd-sentiment collector.
//
// Ports /api/wsb/trending, /api/yfin/fear-greed and /api/congress/trades.
// The TS fallbacks (mock WSB tickers, seeded-random fear & greed, mock
// congress trades) are gone: failures are reported as UNAVAILABLE. Output is a
// JSONL stream of {data, provenance} envelopes on stdout; logs go to stderr.
//
//   go run social.go reddit -subs wallstreetbets,stocks
//   go run social.go reddit -every 5m -posts           # also stream posts
//   go run social.go feargreed
//   go run social.go congress -limit 100
//
// Env: QUBE_SOCIAL_UA overrides the User-Agent. Reddit asks for a descriptive
// UA, e.g. "linux:qube-social:1.0 (by /u/yourname)".
package main

import (
  "bufio"
  "bytes"
  "context"
  "crypto/sha1"
  "encoding/hex"
  "encoding/json"
  "errors"
  "flag"
  "fmt"
  "io"
  "log/slog"
  "math"
  "math/rand"
  "net/http"
  "os"
  "os/signal"
  "regexp"
  "sort"
  "strconv"
  "strings"
  "sync"
  "syscall"
  "time"
)

const (
  progName   = "qube-social"
  sourceName = "social"
  envPrefix  = "QUBE_SOCIAL_"
  defaultUA  = "Mozilla/5.0 (compatible; qube-social/1.0; market sentiment collector)"
)

func main() {
  runMain([]command{
    {"reddit", "[-subs a,b] [-limit N] [-posts]", "ticker mentions + bull/bear lexicon from subreddit hot posts", cmdReddit},
    {"feargreed", "", "CNN Fear & Greed index with history and components", cmdFearGreed},
    {"congress", "[-limit N] [-url U]", "House Stock Watcher disclosures, one record per trade", cmdCongress},
  })
}

// ---------------------------------------------------------------------------
// Reddit
// ---------------------------------------------------------------------------

var (
  subRe        = regexp.MustCompile(`^[A-Za-z0-9_]{2,21}$`)
  redditTicker = regexp.MustCompile(`\b([A-Z]{2,5})\b`)
  blacklist    = map[string]bool{}
)

func init() {
  for _, w := range strings.Fields(`THE AND FOR YOU NOT ARE BUT ALL CAN WAS HAS CEO USA NYSE WSB YOLO DD ETF IMO LOL FAQ
    FOMO PUMP MOON HODL ATH FUD BULL BEAR GAIN LOSS USD BUY SELL CALL PUT`) {
    blacklist[w] = true
  }
}

type redditListing struct {
  Data struct {
    Children []struct {
      Data struct {
        Title       string  `json:"title"`
        Selftext    string  `json:"selftext"`
        Score       float64 `json:"score"`
        NumComments float64 `json:"num_comments"`
        Permalink   string  `json:"permalink"`
        CreatedUTC  float64 `json:"created_utc"`
        Stickied    bool    `json:"stickied"`
        Flair       string  `json:"link_flair_text"`
      } `json:"data"`
    } `json:"children"`
  } `json:"data"`
}

type RedditPost struct {
  Sub         string  `json:"sub"`
  Title       string  `json:"title"`
  Score       float64 `json:"score"`
  NumComments float64 `json:"num_comments"`
  URL         string  `json:"url"`
  CreatedUTC  float64 `json:"created_utc"`
  CreatedAt   string  `json:"createdAt"`
  Flair       string  `json:"flair,omitempty"`
}

type Mention struct {
  Symbol    string `json:"symbol"`
  Mentions  int    `json:"mentions"`
  Bullish   int    `json:"bullish"`
  Bearish   int    `json:"bearish"`
  Sentiment string `json:"sentiment"`
  Score     int    `json:"score"`
  Sample    string `json:"sample"`
}

func clip(s string, n int) string {
  r := []rune(s)
  if len(r) > n {
    return string(r[:n])
  }
  return s
}

// countMentions ports the TS heuristic: every uppercase 2-5 letter token not
// on the blacklist is a mention; the whole post's wording moves sentiment.
func countMentions(texts []string) []Mention {
  type acc struct {
    count, bull, bear int
    sample           string
  }
  m := map[string]*acc{}
  for _, txt := range texts {
    lower := strings.ToLower(txt)
    bull := strings.Contains(lower, "bull") || strings.Contains(lower, "buy") || strings.Contains(lower, "moon") || strings.Contains(lower, "calls")
    bear := strings.Contains(lower, "bear") || strings.Contains(lower, "sell") || strings.Contains(lower, "puts") || strings.Contains(lower, "crash")
    for _, sm := range redditTicker.FindAllStringSubmatch(txt, -1) {
      sym := sm[1]
      if blacklist[sym] {
        continue
      }
      e := m[sym]
      if e == nil {
        e = &acc{sample: clip(txt, 120)}
        m[sym] = e
      }
      e.count++
      if bull {
        e.bull++
      }
      if bear {
        e.bear++
      }
    }
  }
  out := []Mention{}
  for sym, e := range m {
    if e.count < 2 {
      continue
    }
    net := e.bull - e.bear
    s := "neutral"
    if net > 0 {
      s = "bullish"
    } else if net < 0 {
      s = "bearish"
    }
    out = append(out, Mention{Symbol: sym, Mentions: e.count, Bullish: e.bull, Bearish: e.bear, Sentiment: s, Score: e.count*2 + net, Sample: e.sample})
  }
  sort.Slice(out, func(i, j int) bool {
    if out[i].Score != out[j].Score {
      return out[i].Score > out[j].Score
    }
    return out[i].Symbol < out[j].Symbol
  })
  return out
}

func cmdReddit(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("reddit", &o)
  subsFlag := fs.String("subs", "wallstreetbets,stocks", "comma list of subreddits")
  limit := fs.Int("limit", 30, "hot posts per subreddit (max 100)")
  top := fs.Int("top", 25, "max trending symbols")
  posts := fs.Bool("posts", false, "also emit one record per post")
  parseArgs(fs, args)
  subs := splitList([]string{*subsFlag})
  for _, s := range subs {
    if !subRe.MatchString(s) {
      return fmt.Errorf("reddit: invalid subreddit %q", s)
    }
  }
  if len(subs) == 0 {
    return errors.New("reddit: at least one subreddit required")
  }
  lim := *limit
  if lim < 1 || lim > 100 {
    lim = 30
  }
  f := newFetcher(&o, nil, 0.5) // reddit is heavily rate limited
  f.headers["Accept"] = "application/json"
  d := newDelta(o.all)
  key := strings.Join(subs, ",")
  return loop(ctx, &o, func(ctx context.Context) error {
    var texts []string
    var all []RedditPost
    var errs []string
    for _, sub := range subs {
      var l redditListing
      u := "https://www.reddit.com/r/" + sub + "/hot.json?raw_json=1&limit=" + strconv.Itoa(lim)
      if err := f.getJSON(ctx, u, nil, &l); err != nil {
        errs = append(errs, sub+": "+err.Error())
        continue
      }
      for _, ch := range l.Data.Children {
        p := ch.Data
        if p.Title == "" {
          continue
        }
        texts = append(texts, p.Title+" "+p.Selftext)
        all = append(all, RedditPost{
          Sub: sub, Title: p.Title, Score: p.Score, NumComments: p.NumComments,
          URL: "https://www.reddit.com" + p.Permalink, CreatedUTC: p.CreatedUTC,
          CreatedAt: time.Unix(int64(p.CreatedUTC), 0).UTC().Format(time.RFC3339), Flair: p.Flair,
        })
      }
    }
    if len(all) == 0 {
      d.publish(envFail("reddit-trending", key, fmt.Errorf("no posts fetched: %s", strings.Join(errs, "; "))))
      return nil
    }
    trending := countMentions(texts)
    if len(trending) > *top {
      trending = trending[:*top]
    }
    sample := all
    if len(sample) > 20 {
      sample = sample[:20]
    }
    env := envOK("reddit-trending", key, qLive, map[string]any{
      "subs": subs, "trending": trending, "posts": sample, "postCount": len(all),
    })
    if len(errs) > 0 {
      env.Provenance.Error = "partial: " + strings.Join(errs, "; ")
    }
    d.publish(env)
    if *posts {
      for _, p := range all {
        d.publish(envOK("reddit-post", p.URL, qLive, p))
      }
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// CNN Fear & Greed
// ---------------------------------------------------------------------------

type fgComponent struct {
  Score  *float64 `json:"score"`
  Rating string   `json:"rating"`
}

type fgResponse struct {
  FearAndGreed struct {
    Score          *float64 `json:"score"`
    Rating         string   `json:"rating"`
    Timestamp      string   `json:"timestamp"`
    PreviousClose  *float64 `json:"previous_close"`
    Previous1Week  *float64 `json:"previous_1_week"`
    Previous1Month *float64 `json:"previous_1_month"`
    Previous1Year  *float64 `json:"previous_1_year"`
  } `json:"fear_and_greed"`
  Historical struct {
    Data []struct {
      X      float64 `json:"x"`
      Y      float64 `json:"y"`
      Rating string  `json:"rating"`
    } `json:"data"`
  } `json:"fear_and_greed_historical"`
  MarketMomentum  *fgComponent `json:"market_momentum_sp500"`
  PriceStrength   *fgComponent `json:"stock_price_strength"`
  PriceBreadth    *fgComponent `json:"stock_price_breadth"`
  PutCall         *fgComponent `json:"put_call_options"`
  Volatility      *fgComponent `json:"market_volatility_vix"`
  SafeHaven       *fgComponent `json:"safe_haven_demand"`
  JunkBond        *fgComponent `json:"junk_bond_demand"`
}

type FGPoint struct {
  X      float64 `json:"x"`
  Date   string  `json:"date"`
  Y      int     `json:"y"`
  Rating string  `json:"rating"`
}

type FGIndicator struct {
  Name   string `json:"name"`
  Score  *int   `json:"score"`
  Rating string `json:"rating"`
}

func roundPtr(v *float64) *int {
  if v == nil || math.IsNaN(*v) {
    return nil
  }
  r := int(math.Round(*v))
  return &r
}

func cmdFearGreed(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("feargreed", &o)
  parseArgs(fs, args)
  f := newFetcher(&o, nil, 1)
  f.headers["Accept"] = "application/json"
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    var r fgResponse
    if err := f.getJSON(ctx, "https://production.dataviz.cnn.io/index/fearandgreed/graphdata", nil, &r); err != nil {
      d.publish(envFail("feargreed", "cnn", err))
      return nil
    }
    fg := r.FearAndGreed
    if fg.Score == nil {
      d.publish(envFail("feargreed", "cnn", errors.New("CNN response has no fear_and_greed score")))
      return nil
    }
    rating := firstNonEmpty(fg.Rating, "neutral")
    hist := []FGPoint{}
    for _, h := range r.Historical.Data {
      hist = append(hist, FGPoint{
        X: h.X, Date: time.UnixMilli(int64(h.X)).UTC().Format("2006-01-02"),
        Y: int(math.Round(h.Y)), Rating: h.Rating,
      })
    }
    comp := func(name string, c *fgComponent) FGIndicator {
      if c == nil {
        return FGIndicator{Name: name}
      }
      return FGIndicator{Name: name, Score: roundPtr(c.Score), Rating: c.Rating}
    }
    d.publish(envOK("feargreed", "cnn", qDelayed, map[string]any{
      "score":         roundPtr(fg.Score),
      "rating":        rating,
      "label":         strings.ToUpper(strings.ReplaceAll(rating, "_", " ")),
      "previousClose": roundPtr(fg.PreviousClose),
      "previousWeek":  roundPtr(fg.Previous1Week),
      "previousMonth": roundPtr(fg.Previous1Month),
      "previousYear":  roundPtr(fg.Previous1Year),
      "timestamp":     fg.Timestamp,
      "history":       hist,
      "indicators": []FGIndicator{
        comp("Market Momentum", r.MarketMomentum),
        comp("Stock Price Strength", r.PriceStrength),
        comp("Stock Price Breadth", r.PriceBreadth),
        comp("Put/Call Options", r.PutCall),
        comp("Market Volatility", r.Volatility),
        comp("Safe Haven Demand", r.SafeHaven),
        comp("Junk Bond Demand", r.JunkBond),
      },
    }))
    return nil
  })
}

// ---------------------------------------------------------------------------
// Congress trades (House Stock Watcher)
// ---------------------------------------------------------------------------

var congressSources = []string{
  "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
  "https://house-stock-watcher-data.s3.us-west-2.amazonaws.com/data/all_transactions.json",
}

type CongressTrade struct {
  ID              string `json:"id"`
  Representative  string `json:"representative"`
  Ticker          string `json:"ticker"`
  TransactionDate string `json:"transaction_date"`
  DisclosureDate  string `json:"disclosure_date"`
  Type            string `json:"type"`
  Amount          string `json:"amount"`
  Asset           string `json:"asset"`
  Party           string `json:"party"`
  District        string `json:"district"`
  Owner           string `json:"owner,omitempty"`
  Link            string `json:"ptr_link,omitempty"`
  sortKey         string
}

var nonAlpha = regexp.MustCompile(`[^A-Z]`)

func field(t map[string]any, keys ...string) string {
  for _, k := range keys {
    if v, ok := t[k]; ok && v != nil {
      if s := strings.TrimSpace(str(v)); s != "" && s != "--" {
        return s
      }
    }
  }
  return ""
}

// isoDate normalizes "2024-05-01", "2024-05-01T00:00:00" and "05/01/2024".
func isoDate(s string) string {
  s = strings.TrimSpace(s)
  if len(s) >= 10 {
    if t, err := time.Parse("2006-01-02", s[:10]); err == nil {
      return t.Format("2006-01-02")
    }
  }
  for _, f := range []string{"01/02/2006", "1/2/2006"} {
    if t, err := time.Parse(f, s); err == nil {
      return t.Format("2006-01-02")
    }
  }
  return s
}

func cmdCongress(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("congress", &o)
  limit := fs.Int("limit", 100, "max trades (newest by disclosure date, max 1000)")
  srcURL := fs.String("url", "", "override the all_transactions.json source URL")
  parseArgs(fs, args)
  lim := *limit
  if lim < 1 || lim > 1000 {
    lim = 100
  }
  sources := congressSources
  if *srcURL != "" {
    if !strings.HasPrefix(*srcURL, "https://") {
      return errors.New("congress: -url must be https")
    }
    sources = []string{*srcURL}
  }
  f := newFetcher(&o, nil, 1)
  f.maxBody = 128 << 20
  f.headers["Accept"] = "application/json"
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    var data []map[string]any
    var errs []string
    for _, u := range sources {
      var arr []map[string]any
      if err := f.getJSON(ctx, u, nil, &arr); err != nil {
        errs = append(errs, err.Error())
        continue
      }
      if len(arr) > 0 {
        data = arr
        break
      }
      errs = append(errs, u+": empty dataset")
    }
    if data == nil {
      d.publish(envFail("congress-trade", "house-stock-watcher", fmt.Errorf("source unavailable: %s", strings.Join(errs, "; "))))
      return nil
    }
    trades := make([]CongressTrade, 0, len(data))
    for _, t := range data {
      ct := CongressTrade{
        Representative:  firstNonEmpty(field(t, "representative", "Representative", "senator"), "Unknown"),
        Ticker:          clip(nonAlpha.ReplaceAllString(strings.ToUpper(field(t, "ticker", "Ticker")), ""), 6),
        TransactionDate: isoDate(field(t, "transaction_date", "TransactionDate")),
        DisclosureDate:  isoDate(field(t, "disclosure_date", "DisclosureDate")),
        Type:            field(t, "type", "Type", "transaction"),
        Amount:          field(t, "amount", "Amount"),
        Asset:           field(t, "asset_description", "Asset"),
        Party:           field(t, "party", "Party"),
        District:        field(t, "district", "District"),
        Owner:           field(t, "owner"),
        Link:            field(t, "ptr_link"),
      }
      if ct.Ticker == "" {
        continue
      }
      ct.sortKey = firstNonEmpty(ct.DisclosureDate, ct.TransactionDate)
      sum := sha1.Sum([]byte(strings.Join([]string{ct.Representative, ct.Ticker, ct.TransactionDate, ct.DisclosureDate, ct.Type, ct.Amount, ct.Owner, ct.Link}, "|")))
      ct.ID = hex.EncodeToString(sum[:10])
      trades = append(trades, ct)
    }
    sort.SliceStable(trades, func(i, j int) bool { return trades[i].sortKey > trades[j].sortKey })
    if len(trades) > lim {
      trades = trades[:lim]
    }
    for i := len(trades) - 1; i >= 0; i-- { // oldest first for streaming
      d.publish(envOK("congress-trade", trades[i].ID, qDelayed, trades[i]))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

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
  case bool:
    return strconv.FormatBool(t)
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
  if err := f.limiter.wait(ctx); err != nil {
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
