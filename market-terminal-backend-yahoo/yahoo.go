// qube-yahoo: Yahoo Finance collector.
//
// Ports lib/yahoo.ts and the primitive parts of /api/yfin/* (chart, watchlist,
// search, screener/market-movers, trending, options, fundamentals). Output is a
// JSONL stream of {data, provenance} envelopes on stdout; logs go to stderr.
//
//   go run yahoo.go quotes AAPL MSFT ^GSPC
//   go run yahoo.go quotes -every 5s AAPL,MSFT        # stream changed quotes
//   go run yahoo.go chart -range 1y -interval 1d -events div SPY
//   go run yahoo.go search apple
//   go run yahoo.go screener day_gainers -count 50
//   go run yahoo.go trending -region US
//   go run yahoo.go options -expiries 3 AAPL
//   go run yahoo.go keystats AAPL
//   go run yahoo.go quote-batch AAPL MSFT NVDA ...   # 100 symbols per request
//   go run yahoo.go sector-screen                     # sector + industry of all US equities
//
// Env: QUBE_YAHOO_UA overrides the User-Agent.
//
// Pacing (shared by every qube-yahoo process using the same lock file):
//   QUBE_YAHOO_PACE_FILE  lock file (default <state dir>/qube-yahoo-<uid>.pace)
//   QUBE_YAHOO_MIN_GAP    min gap between request starts (default 500ms);
//                         background jobs use a larger gap, so interactive
//                         requests win the free slots
//   QUBE_YAHOO_WAIT_PAUSE 1 = sleep through a rate-limit pause instead of
//                         failing at once (background jobs)
// A 429 / 999 answer pauses all processes (5 min, doubling up to 1 h, or
// Retry-After when longer); the first success afterwards resets the backoff.
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
  "net/http/cookiejar"
  "net/url"
  "os"
  "os/signal"
  "path/filepath"
  "regexp"
  "strconv"
  "strings"
  "sync"
  "syscall"
  "time"

  common "market-terminal/common/go"
)

const (
  progName   = "qube-yahoo"
  sourceName = "yahoo"
  envPrefix  = "QUBE_YAHOO_"
  defaultUA  = "Mozilla/5.0 (compatible; qube-yahoo/1.0)"
)

func main() {
  runMain([]command{
    {"quotes", "[flags] SYM...", "last price/change per symbol (chart meta)", cmdQuotes},
    {"chart", "[-range] [-interval] [-events div,split] SYM...", "OHLCV bars", cmdChart},
    {"search", "[-count N] QUERY", "symbol search", cmdSearch},
    {"screener", "[-count N] SCRID", "predefined screener (day_gainers, most_actives, ...)", cmdScreener},
    {"trending", "[-region US] [-count N]", "trending symbols", cmdTrending},
    {"options", "[-date YYYY-MM-DD] [-expiries N] SYM", "option chain (v7, needs crumb)", cmdOptions},
    {"keystats", "SYM...", "fundamentals / key statistics", cmdKeystats},
    {"quote-batch", "[-chunk N] SYM...", "many quotes per request (v7, needs crumb), with market cap", cmdQuoteBatch},
    {"sector-screen", "[-sectors k,k] [-industries=false] [-region us]", "sector / industry of every listed equity (screener, needs crumb)", cmdSectorScreen},
    {"profile", "SYM...", "sector, industry, country, market cap per symbol (quoteSummary assetProfile)", cmdProfile},
  })
}

// ---------------------------------------------------------------------------
// Yahoo client
// ---------------------------------------------------------------------------

type yahoo struct {
  f       *fetcher
  mu      sync.Mutex
  crumb   string
  crumbAt time.Time
}

func newYahoo(o *opts) *yahoo {
  jar, _ := cookiejar.New(nil)
  f := newFetcher(o, jar, 0)
  f.pacer = newYahooPacer()
  f.headers["Accept"] = "application/json"
  return &yahoo{f: f}
}

// getCrumb acquires (and caches for 30m) the cookie + crumb pair that the
// v7 options and v10 quoteSummary endpoints require.
func (y *yahoo) getCrumb(ctx context.Context, force bool) (string, error) {
  y.mu.Lock()
  defer y.mu.Unlock()
  if !force && y.crumb != "" && time.Since(y.crumbAt) < 30*time.Minute {
    return y.crumb, nil
  }
  // fc.yahoo.com answers 404 but sets the session cookie; errors are expected.
  _, _, _ = y.f.do(ctx, http.MethodGet, "https://fc.yahoo.com/", nil, map[string]string{"Accept": "text/html"})
  st, body, err := y.f.do(ctx, http.MethodGet, "https://query2.finance.yahoo.com/v1/test/getcrumb", nil, map[string]string{"Accept": "text/plain"})
  if err != nil {
    return "", fmt.Errorf("crumb: %w", err)
  }
  c := strings.TrimSpace(string(body))
  if st != http.StatusOK || c == "" || len(c) > 64 || strings.ContainsAny(c, "<{ \n") {
    return "", fmt.Errorf("crumb: HTTP %d", st)
  }
  y.crumb, y.crumbAt = c, time.Now()
  return c, nil
}

// getJSON fetches u into v. When crumbFirst is set the crumb is attached up
// front; otherwise it is only attached after a 401/403.
func (y *yahoo) getJSON(ctx context.Context, u string, crumbFirst bool, v any) error {
  attempt := func(withCrumb, force bool) error {
    target := u
    if withCrumb {
      c, err := y.getCrumb(ctx, force)
      if err != nil {
        return err
      }
      target = addQuery(u, "crumb", c)
    }
    return y.f.getJSON(ctx, target, nil, v)
  }
  err := attempt(crumbFirst, false)
  if st := httpStatus(err); st == http.StatusUnauthorized || st == http.StatusForbidden {
    err = attempt(true, crumbFirst)
  }
  if httpStatus(err) == http.StatusTooManyRequests {
    return fmt.Errorf("yahoo rate limited: %w", err)
  }
  return err
}

func addQuery(u, k, v string) string {
  sep := "?"
  if strings.Contains(u, "?") {
    sep = "&"
  }
  return u + sep + url.QueryEscape(k) + "=" + url.QueryEscape(v)
}

// ---------------------------------------------------------------------------
// Chart (v8), also the quote source, as in the TS code
// ---------------------------------------------------------------------------

type chartMeta struct {
  Symbol               string   `json:"symbol"`
  Currency             string   `json:"currency"`
  ExchangeName         string   `json:"exchangeName"`
  InstrumentType       string   `json:"instrumentType"`
  LongName             string   `json:"longName"`
  ShortName            string   `json:"shortName"`
  RegularMarketPrice   *float64 `json:"regularMarketPrice"`
  RegularMarketTime    int64    `json:"regularMarketTime"`
  RegularMarketDayHigh *float64 `json:"regularMarketDayHigh"`
  RegularMarketDayLow  *float64 `json:"regularMarketDayLow"`
  RegularMarketVolume  *float64 `json:"regularMarketVolume"`
  ChartPreviousClose   *float64 `json:"chartPreviousClose"`
  PreviousClose        *float64 `json:"previousClose"`
  FiftyTwoWeekHigh     *float64 `json:"fiftyTwoWeekHigh"`
  FiftyTwoWeekLow      *float64 `json:"fiftyTwoWeekLow"`
}

type chartResult struct {
  Meta       chartMeta `json:"meta"`
  Timestamp  []int64   `json:"timestamp"`
  Indicators struct {
    Quote []struct {
      Open   []*float64 `json:"open"`
      High   []*float64 `json:"high"`
      Low    []*float64 `json:"low"`
      Close  []*float64 `json:"close"`
      Volume []*float64 `json:"volume"`
    } `json:"quote"`
    AdjClose []struct {
      AdjClose []*float64 `json:"adjclose"`
    } `json:"adjclose"`
  } `json:"indicators"`
  Events struct {
    Dividends map[string]struct {
      Amount float64 `json:"amount"`
      Date   int64   `json:"date"`
    } `json:"dividends"`
    Splits map[string]struct {
      Date        int64   `json:"date"`
      Numerator   float64 `json:"numerator"`
      Denominator float64 `json:"denominator"`
      SplitRatio  string  `json:"splitRatio"`
    } `json:"splits"`
  } `json:"events"`
}

type yError struct {
  Code        string `json:"code"`
  Description string `json:"description"`
}

type chartResponse struct {
  Chart struct {
    Result []chartResult `json:"result"`
    Error  *yError       `json:"error"`
  } `json:"chart"`
}

var (
  validRanges    = setOf("1d", "5d", "1mo", "3mo", "6mo", "ytd", "1y", "2y", "5y", "10y", "max")
  validIntervals = setOf("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")
)

func (y *yahoo) chart(ctx context.Context, sym, rng, interval, events string) (*chartResult, error) {
  q := url.Values{}
  q.Set("range", rng)
  q.Set("interval", interval)
  q.Set("includePrePost", "false")
  if events != "" {
    q.Set("events", strings.ReplaceAll(events, ",", "|"))
  }
  u := "https://query1.finance.yahoo.com/v8/finance/chart/" + url.PathEscape(sym) + "?" + q.Encode()
  var resp chartResponse
  if err := y.getJSON(ctx, u, false, &resp); err != nil {
    return nil, err
  }
  if resp.Chart.Error != nil {
    return nil, fmt.Errorf("yahoo chart %s: %s %s", sym, resp.Chart.Error.Code, resp.Chart.Error.Description)
  }
  if len(resp.Chart.Result) == 0 {
    return nil, fmt.Errorf("yahoo chart %s: empty result", sym)
  }
  return &resp.Chart.Result[0], nil
}

type Quote struct {
  Symbol        string   `json:"symbol"`
  Name          string   `json:"name,omitempty"`
  Currency      string   `json:"currency,omitempty"`
  Exchange      string   `json:"exchange,omitempty"`
  Type          string   `json:"type,omitempty"`
  Price         *float64 `json:"price"`
  PreviousClose *float64 `json:"previousClose"`
  Change        *float64 `json:"change"`
  ChangePercent *float64 `json:"changePercent"`
  DayHigh       *float64 `json:"dayHigh,omitempty"`
  DayLow        *float64 `json:"dayLow,omitempty"`
  Volume        *float64 `json:"volume,omitempty"`
  High52w       *float64 `json:"high52w"`
  Low52w        *float64 `json:"low52w"`
  MarketTime    string   `json:"marketTime,omitempty"`
}

func quoteFromChart(sym string, r *chartResult) Quote {
  m := r.Meta
  q := Quote{
    Symbol:   firstNonEmpty(m.Symbol, sym),
    Name:     firstNonEmpty(m.LongName, m.ShortName),
    Currency: m.Currency,
    Exchange: m.ExchangeName,
    Type:     m.InstrumentType,
    Price:    m.RegularMarketPrice,
    DayHigh:  m.RegularMarketDayHigh,
    DayLow:   m.RegularMarketDayLow,
    Volume:   m.RegularMarketVolume,
    High52w:  m.FiftyTwoWeekHigh,
    Low52w:   m.FiftyTwoWeekLow,
  }
  // The TS code used chartPreviousClose of a 5d range, which is the close
  // *before the range* (a 5-day change). Prefer the prior session's close.
  prev := m.PreviousClose
  if prev == nil && len(r.Indicators.Quote) > 0 {
    closes := r.Indicators.Quote[0].Close
    n := len(closes)
    if n > 0 && n == len(r.Timestamp) {
      idx := n - 2 // last bar is the current session ...
      if m.RegularMarketTime > 0 && m.RegularMarketTime-r.Timestamp[n-1] > 20*3600 {
        idx = n - 1 // ... unless the current session has no bar yet
      }
      for ; idx >= 0; idx-- {
        if closes[idx] != nil {
          prev = closes[idx]
          break
        }
      }
    }
  }
  if prev == nil {
    prev = m.ChartPreviousClose
  }
  q.PreviousClose = prev
  if q.Price != nil && prev != nil && *prev != 0 {
    ch := *q.Price - *prev
    pct := ch / *prev * 100
    q.Change, q.ChangePercent = &ch, &pct
  }
  if m.RegularMarketTime > 0 {
    q.MarketTime = time.Unix(m.RegularMarketTime, 0).UTC().Format(time.RFC3339)
  }
  return q
}

func cmdQuotes(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("quotes", &o)
  conc := fs.Int("concurrency", 8, "max parallel upstream requests")
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("quotes: at least one symbol required")
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    envs := make([]Envelope, len(syms))
    sem := make(chan struct{}, maxInt(1, *conc))
    var wg sync.WaitGroup
    for i, s := range syms {
      wg.Add(1)
      go func(i int, s string) {
        defer wg.Done()
        sem <- struct{}{}
        defer func() { <-sem }()
        v, age, stale, err := cache.get(s, 4*time.Second, func() (any, error) {
          r, err := y.chart(ctx, s, "5d", "1d", "")
          if err != nil {
            return nil, err
          }
          return quoteFromChart(s, r), nil
        })
        envs[i] = resultEnv("quote", s, qDelayed, v, age, stale, err)
      }(i, s)
    }
    wg.Wait()
    for _, e := range envs {
      d.publish(e)
    }
    return nil
  })
}

type Bar struct {
  Time     int64    `json:"t"`
  Open     *float64 `json:"o"`
  High     *float64 `json:"h"`
  Low      *float64 `json:"l"`
  Close    *float64 `json:"c"`
  AdjClose *float64 `json:"adj,omitempty"`
  Volume   *float64 `json:"v"`
}

type Dividend struct {
  Date   string  `json:"date"`
  Amount float64 `json:"amount"`
}

type Split struct {
  Date        string  `json:"date"`
  Numerator   float64 `json:"numerator"`
  Denominator float64 `json:"denominator"`
  Ratio       string  `json:"ratio"`
}

// Chart keeps the column layout of the TS route (timestamps/open/high/...) so
// t.go can pass it through; missing values stay null instead of becoming 0.
type Chart struct {
  Quote
  Range      string     `json:"range"`
  Interval   string     `json:"interval"`
  Timestamps []int64    `json:"timestamps"`
  Open       []*float64 `json:"open"`
  High       []*float64 `json:"high"`
  Low        []*float64 `json:"low"`
  Close      []*float64 `json:"close"`
  AdjClose   []*float64 `json:"adjClose,omitempty"`
  Volume     []*float64 `json:"volume"`
  Dividends  []Dividend `json:"dividends,omitempty"`
  Splits     []Split    `json:"splits,omitempty"`
}

func buildChart(sym, rng, interval string, r *chartResult) Chart {
  c := Chart{Quote: quoteFromChart(sym, r), Range: rng, Interval: interval, Timestamps: r.Timestamp}
  if c.Timestamps == nil {
    c.Timestamps = []int64{}
  }
  if len(r.Indicators.Quote) > 0 {
    q := r.Indicators.Quote[0]
    c.Open, c.High, c.Low, c.Close, c.Volume = q.Open, q.High, q.Low, q.Close, q.Volume
  }
  if len(r.Indicators.AdjClose) > 0 {
    c.AdjClose = r.Indicators.AdjClose[0].AdjClose
  }
  for _, dv := range r.Events.Dividends {
    c.Dividends = append(c.Dividends, Dividend{Date: unixDate(dv.Date), Amount: dv.Amount})
  }
  sortBy(c.Dividends, func(a, b Dividend) bool { return a.Date < b.Date })
  for _, sp := range r.Events.Splits {
    c.Splits = append(c.Splits, Split{Date: unixDate(sp.Date), Numerator: sp.Numerator, Denominator: sp.Denominator, Ratio: sp.SplitRatio})
  }
  sortBy(c.Splits, func(a, b Split) bool { return a.Date < b.Date })
  return c
}

func cmdChart(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("chart", &o)
  rng := fs.String("range", "6mo", "range: 1d 5d 1mo 3mo 6mo ytd 1y 2y 5y 10y max")
  interval := fs.String("interval", "1d", "interval: 1m 2m 5m 15m 30m 60m 90m 1h 1d 5d 1wk 1mo 3mo")
  events := fs.String("events", "", "comma list of events to include: div,split")
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("chart: at least one symbol required")
  }
  if !validRanges[*rng] {
    return fmt.Errorf("chart: invalid -range %q", *rng)
  }
  if !validIntervals[*interval] {
    return fmt.Errorf("chart: invalid -interval %q", *interval)
  }
  for _, e := range splitList([]string{*events}) {
    if e != "div" && e != "split" {
      return fmt.Errorf("chart: invalid event %q", e)
    }
  }
  ttl := 10 * time.Minute
  if strings.HasSuffix(*interval, "m") || *interval == "1h" {
    ttl = 30 * time.Second
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, s := range syms {
      v, age, stale, err := cache.get(s, ttl, func() (any, error) {
        r, err := y.chart(ctx, s, *rng, *interval, *events)
        if err != nil {
          return nil, err
        }
        return buildChart(s, *rng, *interval, r), nil
      })
      d.publish(resultEnv("chart", s, qDelayed, v, age, stale, err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Search, screener, trending
// ---------------------------------------------------------------------------

type SearchHit struct {
  Symbol   string   `json:"symbol"`
  Name     string   `json:"name"`
  Exchange string   `json:"exchange"`
  Type     string   `json:"type"`
  Score    *float64 `json:"score"`
}

func cmdSearch(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("search", &o)
  count := fs.Int("count", 8, "max results")
  pos := parseArgs(fs, args)
  query := strings.TrimSpace(strings.Join(pos, " "))
  if query == "" {
    return errors.New("search: query required")
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    q := url.Values{}
    q.Set("q", query)
    q.Set("quotesCount", strconv.Itoa(*count))
    q.Set("newsCount", "0")
    q.Set("enableFuzzyQuery", "false")
    q.Set("quotesQueryId", "tss_match_phrase_query")
    var raw struct {
      Quotes []map[string]any `json:"quotes"`
    }
    if err := y.getJSON(ctx, "https://query1.finance.yahoo.com/v1/finance/search?"+q.Encode(), false, &raw); err != nil {
      d.publish(envFail("search", query, err))
      return nil
    }
    hits := []SearchHit{}
    for _, it := range raw.Quotes {
      hits = append(hits, SearchHit{
        Symbol:   str(it["symbol"]),
        Name:     firstNonEmpty(str(it["longname"]), str(it["shortname"])),
        Exchange: str(it["exchange"]),
        Type:     str(it["quoteType"]),
        Score:    num(it["score"]),
      })
    }
    d.publish(envOK("search", query, qDelayed, map[string]any{"query": query, "quotes": hits}))
    return nil
  })
}

type ScreenItem struct {
  Symbol        string   `json:"symbol"`
  Name          string   `json:"name"`
  Exchange      string   `json:"exchange,omitempty"`
  Currency      string   `json:"currency,omitempty"`
  Price         *float64 `json:"price"`
  PreviousClose *float64 `json:"previousClose"`
  Change        *float64 `json:"change"`
  ChangePercent *float64 `json:"changePercent"`
  Volume        *float64 `json:"volume"`
  AvgVolume3m   *float64 `json:"avgVolume"`
  UnusualRatio  *float64 `json:"unusualRatio"`
  MarketCap     *float64 `json:"marketCap"`
  TrailingPE    *float64 `json:"pe"`
  ForwardPE     *float64 `json:"forwardPE"`
  DividendYield *float64 `json:"dividendYield"`
  High52w       *float64 `json:"fiftyTwoWeekHigh"`
  Low52w        *float64 `json:"fiftyTwoWeekLow"`
}

var scrIDRe = regexp.MustCompile(`^[a-z0-9_]{2,64}$`)

func cmdScreener(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("screener", &o)
  count := fs.Int("count", 100, "max results (Yahoo caps at 250)")
  pos := parseArgs(fs, args)
  if len(pos) != 1 || !scrIDRe.MatchString(pos[0]) {
    return errors.New("screener: exactly one screener id required, e.g. day_gainers, day_losers, most_actives, undervalued_large_caps, small_cap_gainers, aggressive_small_caps, growth_technology_stocks")
  }
  scr := pos[0]
  y := newYahoo(&o)
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    q := url.Values{}
    q.Set("scrIds", scr)
    q.Set("count", strconv.Itoa(*count))
    var raw struct {
      Finance struct {
        Result []struct {
          Quotes []map[string]any `json:"quotes"`
        } `json:"result"`
        Error *yError `json:"error"`
      } `json:"finance"`
    }
    if err := y.getJSON(ctx, "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?"+q.Encode(), false, &raw); err != nil {
      d.publish(envFail("screener", scr, err))
      return nil
    }
    if raw.Finance.Error != nil {
      d.publish(envFail("screener", scr, fmt.Errorf("yahoo: %s %s", raw.Finance.Error.Code, raw.Finance.Error.Description)))
      return nil
    }
    items := []ScreenItem{}
    if len(raw.Finance.Result) > 0 {
      for _, it := range raw.Finance.Result[0].Quotes {
        si := ScreenItem{
          Symbol:        str(it["symbol"]),
          Name:          firstNonEmpty(str(it["shortName"]), str(it["longName"])),
          Exchange:      str(it["exchange"]),
          Currency:      str(it["currency"]),
          Price:         num(it["regularMarketPrice"]),
          PreviousClose: num(it["regularMarketPreviousClose"]),
          Change:        num(it["regularMarketChange"]),
          ChangePercent: num(it["regularMarketChangePercent"]),
          Volume:        num(it["regularMarketVolume"]),
          AvgVolume3m:   num(it["averageDailyVolume3Month"]),
          MarketCap:     num(it["marketCap"]),
          TrailingPE:    num(it["trailingPE"]),
          ForwardPE:     num(it["forwardPE"]),
          DividendYield: num(it["dividendYield"]),
          High52w:       num(it["fiftyTwoWeekHigh"]),
          Low52w:        num(it["fiftyTwoWeekLow"]),
        }
        if si.Symbol == "" {
          continue
        }
        if si.Volume != nil && si.AvgVolume3m != nil {
          r := *si.Volume / math.Max(1, *si.AvgVolume3m)
          si.UnusualRatio = &r
        }
        items = append(items, si)
      }
    }
    d.publish(envOK("screener", scr, qDelayed, map[string]any{"scrId": scr, "items": items}))
    return nil
  })
}

type TrendingItem struct {
  Symbol        string   `json:"symbol"`
  Name          string   `json:"name,omitempty"`
  Price         *float64 `json:"price,omitempty"`
  ChangePercent *float64 `json:"changePercent,omitempty"`
}

// The TS route fabricated mentionCount from random numbers and fell back to a
// static list; here only what Yahoo returns is emitted.
func cmdTrending(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("trending", &o)
  region := fs.String("region", "US", "region code")
  count := fs.Int("count", 15, "max results")
  parseArgs(fs, args)
  reg := strings.ToUpper(*region)
  if !regexp.MustCompile(`^[A-Z]{2}$`).MatchString(reg) {
    return fmt.Errorf("trending: invalid region %q", *region)
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    var raw struct {
      Finance struct {
        Result []struct {
          Quotes []map[string]any `json:"quotes"`
        } `json:"result"`
      } `json:"finance"`
    }
    u := "https://query1.finance.yahoo.com/v1/finance/trending/" + reg + "?count=" + strconv.Itoa(*count)
    if err := y.getJSON(ctx, u, false, &raw); err != nil {
      d.publish(envFail("trending", reg, err))
      return nil
    }
    items := []TrendingItem{}
    if len(raw.Finance.Result) > 0 {
      for _, q := range raw.Finance.Result[0].Quotes {
        s := str(q["symbol"])
        if s == "" {
          continue
        }
        items = append(items, TrendingItem{
          Symbol:        s,
          Name:          firstNonEmpty(str(q["shortName"]), str(q["longName"])),
          Price:         num(q["regularMarketPrice"]),
          ChangePercent: num(q["regularMarketChangePercent"]),
        })
      }
    }
    d.publish(envOK("trending", reg, qDelayed, map[string]any{"region": reg, "symbols": items}))
    return nil
  })
}

// ---------------------------------------------------------------------------
// Options (v7, crumb required)
// ---------------------------------------------------------------------------

type yOption struct {
  ContractSymbol    string   `json:"contractSymbol"`
  Strike            float64  `json:"strike"`
  LastPrice         *float64 `json:"lastPrice"`
  Change            *float64 `json:"change"`
  Bid               *float64 `json:"bid"`
  Ask               *float64 `json:"ask"`
  Volume            *float64 `json:"volume"`
  OpenInterest      *float64 `json:"openInterest"`
  ImpliedVolatility *float64 `json:"impliedVolatility"`
  InTheMoney        bool     `json:"inTheMoney"`
  Expiration        int64    `json:"expiration"`
  LastTradeDate     int64    `json:"lastTradeDate"`
}

type optionsResponse struct {
  OptionChain struct {
    Result []struct {
      UnderlyingSymbol string    `json:"underlyingSymbol"`
      ExpirationDates  []int64   `json:"expirationDates"`
      Strikes          []float64 `json:"strikes"`
      Quote            struct {
        RegularMarketPrice *float64 `json:"regularMarketPrice"`
        Currency           string   `json:"currency"`
      } `json:"quote"`
      Options []struct {
        ExpirationDate int64     `json:"expirationDate"`
        Calls          []yOption `json:"calls"`
        Puts           []yOption `json:"puts"`
      } `json:"options"`
    } `json:"result"`
    Error *yError `json:"error"`
  } `json:"optionChain"`
}

type OptionContract struct {
  ContractSymbol    string   `json:"contractSymbol"`
  Expiry            string   `json:"expiry"`
  Type              string   `json:"type"`
  Strike            float64  `json:"strike"`
  Last              *float64 `json:"last"`
  Bid               *float64 `json:"bid"`
  Ask               *float64 `json:"ask"`
  Change            *float64 `json:"change"`
  Volume            *float64 `json:"volume"`
  OpenInterest      *float64 `json:"openInterest"`
  ImpliedVolatility *float64 `json:"impliedVolatility"`
  InTheMoney        bool     `json:"inTheMoney"`
  LastTrade         string   `json:"lastTrade,omitempty"`
}

type OptionChain struct {
  Symbol           string           `json:"symbol"`
  Spot             *float64         `json:"spot"`
  Currency         string           `json:"currency,omitempty"`
  Expirations      []string         `json:"expirations"`
  ExpirationEpochs []int64          `json:"expirationEpochs"`
  Contracts        []OptionContract `json:"contracts"`
}

func (y *yahoo) optionsPage(ctx context.Context, sym string, date int64) (*optionsResponse, error) {
  u := "https://query2.finance.yahoo.com/v7/finance/options/" + url.PathEscape(sym)
  if date > 0 {
    u += "?date=" + strconv.FormatInt(date, 10)
  }
  var resp optionsResponse
  if err := y.getJSON(ctx, u, true, &resp); err != nil {
    return nil, err
  }
  if resp.OptionChain.Error != nil {
    return nil, fmt.Errorf("yahoo options %s: %s %s", sym, resp.OptionChain.Error.Code, resp.OptionChain.Error.Description)
  }
  if len(resp.OptionChain.Result) == 0 {
    return nil, fmt.Errorf("yahoo options %s: empty result", sym)
  }
  return &resp, nil
}

func (y *yahoo) options(ctx context.Context, sym string, date int64, expiries int) (*OptionChain, error) {
  first, err := y.optionsPage(ctx, sym, date)
  if err != nil {
    return nil, err
  }
  r0 := first.OptionChain.Result[0]
  oc := &OptionChain{
    Symbol:           firstNonEmpty(r0.UnderlyingSymbol, sym),
    Spot:             r0.Quote.RegularMarketPrice,
    Currency:         r0.Quote.Currency,
    ExpirationEpochs: r0.ExpirationDates,
    Expirations:      []string{},
    Contracts:        []OptionContract{},
  }
  for _, e := range r0.ExpirationDates {
    oc.Expirations = append(oc.Expirations, unixDate(e))
  }
  add := func(resp *optionsResponse) {
    for _, page := range resp.OptionChain.Result[0].Options {
      exp := unixDate(page.ExpirationDate)
      conv := func(kind string, list []yOption) {
        for _, c := range list {
          oc.Contracts = append(oc.Contracts, OptionContract{
            ContractSymbol: c.ContractSymbol, Expiry: exp, Type: kind, Strike: c.Strike,
            Last: c.LastPrice, Bid: c.Bid, Ask: c.Ask, Change: c.Change,
            Volume: c.Volume, OpenInterest: c.OpenInterest, ImpliedVolatility: c.ImpliedVolatility,
            InTheMoney: c.InTheMoney, LastTrade: unixTime(c.LastTradeDate),
          })
        }
      }
      conv("call", page.Calls)
      conv("put", page.Puts)
    }
  }
  add(first)
  if date == 0 && expiries > 1 {
    for i := 1; i < len(r0.ExpirationDates) && i < expiries; i++ {
      page, err := y.optionsPage(ctx, sym, r0.ExpirationDates[i])
      if err != nil {
        return nil, err
      }
      add(page)
    }
  }
  return oc, nil
}

func cmdOptions(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("options", &o)
  date := fs.String("date", "", "expiry as YYYY-MM-DD or unix seconds (default nearest)")
  expiries := fs.Int("expiries", 1, "number of expirations to fetch starting with the nearest (ignored with -date)")
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("options: symbol required")
  }
  var epoch int64
  if *date != "" {
    if t, err := time.Parse("2006-01-02", *date); err == nil {
      epoch = t.Unix()
    } else if n, err := strconv.ParseInt(*date, 10, 64); err == nil {
      epoch = n
    } else {
      return fmt.Errorf("options: invalid -date %q", *date)
    }
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, s := range syms {
      v, age, stale, err := cache.get(s, 2*time.Minute, func() (any, error) {
        return y.options(ctx, s, epoch, *expiries)
      })
      d.publish(resultEnv("options", s, qDelayed, v, age, stale, err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Key statistics (quoteSummary, falling back to the HTML scrape of the TS code)
// ---------------------------------------------------------------------------

var keystatFields = []struct {
  key string
  src [][2]string
}{
  {"marketCap", [][2]string{{"price", "marketCap"}, {"summaryDetail", "marketCap"}}},
  {"trailingPE", [][2]string{{"summaryDetail", "trailingPE"}}},
  {"forwardPE", [][2]string{{"summaryDetail", "forwardPE"}, {"defaultKeyStatistics", "forwardPE"}}},
  {"epsTrailingTwelveMonths", [][2]string{{"defaultKeyStatistics", "trailingEps"}}},
  {"dividendYield", [][2]string{{"summaryDetail", "dividendYield"}}},
  {"beta", [][2]string{{"summaryDetail", "beta"}, {"defaultKeyStatistics", "beta"}}},
  {"sharesOutstanding", [][2]string{{"defaultKeyStatistics", "sharesOutstanding"}}},
  {"floatShares", [][2]string{{"defaultKeyStatistics", "floatShares"}}},
  {"enterpriseValue", [][2]string{{"defaultKeyStatistics", "enterpriseValue"}}},
  {"priceToBook", [][2]string{{"defaultKeyStatistics", "priceToBook"}}},
  {"priceToSalesTrailing12Months", [][2]string{{"summaryDetail", "priceToSalesTrailing12Months"}}},
  {"profitMargins", [][2]string{{"financialData", "profitMargins"}, {"defaultKeyStatistics", "profitMargins"}}},
  {"returnOnEquity", [][2]string{{"financialData", "returnOnEquity"}}},
  {"totalRevenue", [][2]string{{"financialData", "totalRevenue"}}},
  {"freeCashflow", [][2]string{{"financialData", "freeCashflow"}}},
  {"debtToEquity", [][2]string{{"financialData", "debtToEquity"}}},
  {"currentRatio", [][2]string{{"financialData", "currentRatio"}}},
}

func (y *yahoo) keystats(ctx context.Context, sym string) (map[string]any, error) {
  out := map[string]any{"symbol": sym}
  for _, f := range keystatFields {
    out[f.key] = nil
  }
  // Price/52w from chart meta, as the TS route did.
  if r, err := y.chart(ctx, sym, "5d", "1d", ""); err == nil {
    q := quoteFromChart(sym, r)
    out["price"], out["previousClose"], out["currency"], out["exchange"] = q.Price, q.PreviousClose, q.Currency, q.Exchange
    out["high52w"], out["low52w"] = q.High52w, q.Low52w
  }
  found := 0
  modules := "price,summaryDetail,defaultKeyStatistics,financialData"
  u := "https://query2.finance.yahoo.com/v10/finance/quoteSummary/" + url.PathEscape(sym) + "?modules=" + url.QueryEscape(modules)
  var resp struct {
    QuoteSummary struct {
      Result []map[string]any `json:"result"`
      Error  *yError          `json:"error"`
    } `json:"quoteSummary"`
  }
  qsErr := y.getJSON(ctx, u, true, &resp)
  if qsErr == nil && len(resp.QuoteSummary.Result) > 0 {
    mods := resp.QuoteSummary.Result[0]
    for _, f := range keystatFields {
      for _, s := range f.src {
        if m, ok := mods[s[0]].(map[string]any); ok {
          if v := num(m[s[1]]); v != nil {
            out[f.key] = v
            found++
            break
          }
        }
      }
    }
  }
  if found == 0 {
    // Fallback: scrape the key-statistics page for embedded {"raw":...} values.
    body, err := y.f.get(ctx, "https://finance.yahoo.com/quote/"+url.PathEscape(sym)+"/key-statistics/", map[string]string{"Accept": "text/html"})
    if err == nil {
      html := string(body)
      for _, f := range keystatFields {
        re := regexp.MustCompile(`\\?"` + regexp.QuoteMeta(f.key) + `\\?":\{[^}]*?\\?"raw\\?":(-?[\d.]+(?:[eE][-+]?\d+)?)`)
        if m := re.FindStringSubmatch(html); m != nil {
          if v, err := strconv.ParseFloat(m[1], 64); err == nil {
            out[f.key] = &v
            found++
          }
        }
      }
    } else if qsErr == nil {
      qsErr = err
    }
  }
  if found == 0 && out["price"] == nil {
    if qsErr == nil {
      qsErr = errors.New("no key statistics found")
    }
    return nil, qsErr
  }
  return out, nil
}

var plainSymRe = regexp.MustCompile(`^[A-Z][A-Z0-9.\-]{0,9}$`)

func cmdKeystats(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("keystats", &o)
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("keystats: symbol required")
  }
  for _, s := range syms {
    if !plainSymRe.MatchString(s) {
      return fmt.Errorf("keystats: %q is not an equity symbol", s)
    }
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, s := range syms {
      v, age, stale, err := cache.get(s, time.Hour, func() (any, error) { return y.keystats(ctx, s) })
      d.publish(resultEnv("keystats", s, qDelayed, v, age, stale, err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Bulk endpoints: v7 batch quotes and the screener (sector / industry)
// ---------------------------------------------------------------------------

// postJSON POSTs body (JSON) to u with the crumb attached and decodes the
// answer into v; a 401/403 refreshes the crumb once.
func (y *yahoo) postJSON(ctx context.Context, u string, body any, v any) error {
  b, err := json.Marshal(body)
  if err != nil {
    return err
  }
  attempt := func(force bool) error {
    c, err := y.getCrumb(ctx, force)
    if err != nil {
      return err
    }
    rb, err := y.f.fetch(ctx, http.MethodPost, addQuery(u, "crumb", c), b,
      map[string]string{"Content-Type": "application/json"})
    if err != nil {
      return err
    }
    if err := json.Unmarshal(rb, v); err != nil {
      return fmt.Errorf("decode %s: %w", u, err)
    }
    return nil
  }
  err = attempt(false)
  if st := httpStatus(err); st == http.StatusUnauthorized || st == http.StatusForbidden {
    err = attempt(true)
  }
  if httpStatus(err) == http.StatusTooManyRequests {
    return fmt.Errorf("yahoo rate limited: %w", err)
  }
  return err
}

// BatchQuote is a Quote (same fields as `quotes`) plus what only v7 has.
type BatchQuote struct {
  Quote
  MarketCap         *float64 `json:"marketCap"`
  SharesOutstanding *float64 `json:"sharesOutstanding,omitempty"`
  AvgVolume3m       *float64 `json:"avgVolume,omitempty"`
  TrailingPE        *float64 `json:"pe,omitempty"`
  ForwardPE         *float64 `json:"forwardPE,omitempty"`
  DividendYield     *float64 `json:"dividendYield,omitempty"`
  MarketState       string   `json:"marketState,omitempty"`
  FullExchange      string   `json:"fullExchangeName,omitempty"`
}

const (
  batchQuoteChunk    = 100 // symbols per v7 request (URL length stays sane)
  batchQuoteChunkMax = 200
)

func (y *yahoo) quoteBatch(ctx context.Context, syms []string) (map[string]BatchQuote, error) {
  q := url.Values{}
  q.Set("symbols", strings.Join(syms, ","))
  q.Set("formatted", "false")
  q.Set("lang", "en-US")
  q.Set("region", "US")
  var raw struct {
    QuoteResponse struct {
      Result []map[string]any `json:"result"`
      Error  *yError          `json:"error"`
    } `json:"quoteResponse"`
  }
  if err := y.getJSON(ctx, "https://query1.finance.yahoo.com/v7/finance/quote?"+q.Encode(), true, &raw); err != nil {
    return nil, err
  }
  if e := raw.QuoteResponse.Error; e != nil {
    return nil, fmt.Errorf("yahoo quote: %s %s", e.Code, e.Description)
  }
  out := map[string]BatchQuote{}
  for _, it := range raw.QuoteResponse.Result {
    sym := strings.ToUpper(str(it["symbol"]))
    if sym == "" {
      continue
    }
    bq := BatchQuote{
      Quote: Quote{
        Symbol:        sym,
        Name:          firstNonEmpty(str(it["longName"]), str(it["shortName"])),
        Currency:      str(it["currency"]),
        Exchange:      str(it["exchange"]),
        Type:          str(it["quoteType"]),
        Price:         num(it["regularMarketPrice"]),
        PreviousClose: num(it["regularMarketPreviousClose"]),
        Change:        num(it["regularMarketChange"]),
        ChangePercent: num(it["regularMarketChangePercent"]),
        DayHigh:       num(it["regularMarketDayHigh"]),
        DayLow:        num(it["regularMarketDayLow"]),
        Volume:        num(it["regularMarketVolume"]),
        High52w:       num(it["fiftyTwoWeekHigh"]),
        Low52w:        num(it["fiftyTwoWeekLow"]),
      },
      MarketCap:         num(it["marketCap"]),
      SharesOutstanding: num(it["sharesOutstanding"]),
      AvgVolume3m:       num(it["averageDailyVolume3Month"]),
      TrailingPE:        num(it["trailingPE"]),
      ForwardPE:         num(it["forwardPE"]),
      DividendYield:     num(it["dividendYield"]),
      MarketState:       str(it["marketState"]),
      FullExchange:      str(it["fullExchangeName"]),
    }
    if t := num(it["regularMarketTime"]); t != nil {
      bq.MarketTime = unixTime(int64(*t))
    }
    out[sym] = bq
  }
  return out, nil
}

// quote-batch: one envelope per symbol (endpoint "quote", like `quotes`), but
// up to -chunk symbols per upstream request.
func cmdQuoteBatch(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("quote-batch", &o)
  chunk := fs.Int("chunk", batchQuoteChunk, "symbols per request (max 200)")
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("quote-batch: at least one symbol required")
  }
  n := *chunk
  if n < 1 {
    n = 1
  }
  if n > batchQuoteChunkMax {
    n = batchQuoteChunkMax
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for start := 0; start < len(syms); start += n {
      end := start + n
      if end > len(syms) {
        end = len(syms)
      }
      part := syms[start:end]
      got, err := y.quoteBatch(ctx, part)
      for _, s := range part {
        switch q, ok := got[s]; {
        case err != nil:
          d.publish(envFail("quote", s, err))
        case !ok:
          d.publish(envFail("quote", s, errors.New("not returned by yahoo")))
        default:
          d.publish(envOK("quote", s, qDelayed, q))
        }
      }
    }
    return nil
  })
}

// Yahoo's sectors (Morningstar-style names, close to but not exactly GICS).
var yahooSectors = []struct{ key, name string }{
  {"basic-materials", "Basic Materials"},
  {"communication-services", "Communication Services"},
  {"consumer-cyclical", "Consumer Cyclical"},
  {"consumer-defensive", "Consumer Defensive"},
  {"energy", "Energy"},
  {"financial-services", "Financial Services"},
  {"healthcare", "Healthcare"},
  {"industrials", "Industrials"},
  {"real-estate", "Real Estate"},
  {"technology", "Technology"},
  {"utilities", "Utilities"},
}

const (
  screenPageSize = 250 // screener maximum
  screenMaxPages = 40  // 10k rows per query
)

// Classification is one `sector-screen` record.
type Classification struct {
  Symbol            string   `json:"symbol"`
  Name              string   `json:"name,omitempty"`
  Sector            string   `json:"sector"`
  SectorKey         string   `json:"sectorKey"`
  Industry          string   `json:"industry,omitempty"`
  Exchange          string   `json:"exchange,omitempty"`
  FullExchange      string   `json:"fullExchangeName,omitempty"`
  QuoteType         string   `json:"quoteType,omitempty"`
  Currency          string   `json:"currency,omitempty"`
  MarketCap         *float64 `json:"marketCap"`
  Price             *float64 `json:"price,omitempty"`
  ChangePercent     *float64 `json:"changePercent,omitempty"`
  SharesOutstanding *float64 `json:"sharesOutstanding,omitempty"`
}

func screenEq(field, value string) map[string]any {
  return map[string]any{"operator": "eq", "operands": []any{field, value}}
}

// screenAll pages through a custom screener query (AND of operands).
func (y *yahoo) screenAll(ctx context.Context, operands []any, fn func(map[string]any)) (int, error) {
  got := 0
  for page := 0; page < screenMaxPages; page++ {
    body := map[string]any{
      "offset":     page * screenPageSize,
      "size":       screenPageSize,
      "sortField":  "intradaymarketcap",
      "sortType":   "DESC",
      "quoteType":  "EQUITY",
      "query":      map[string]any{"operator": "AND", "operands": operands},
      "userId":     "",
      "userIdType": "guid",
    }
    var raw struct {
      Finance struct {
        Result []struct {
          Total  int              `json:"total"`
          Quotes []map[string]any `json:"quotes"`
        } `json:"result"`
        Error *yError `json:"error"`
      } `json:"finance"`
    }
    q := url.Values{}
    q.Set("formatted", "false")
    q.Set("lang", "en-US")
    q.Set("region", "US")
    if err := y.postJSON(ctx, "https://query1.finance.yahoo.com/v1/finance/screener?"+q.Encode(), body, &raw); err != nil {
      return got, err
    }
    if e := raw.Finance.Error; e != nil {
      return got, fmt.Errorf("yahoo screener: %s %s", e.Code, e.Description)
    }
    if len(raw.Finance.Result) == 0 {
      break
    }
    res := raw.Finance.Result[0]
    for _, it := range res.Quotes {
      fn(it)
      got++
    }
    if len(res.Quotes) < screenPageSize || (page+1)*screenPageSize >= res.Total {
      break
    }
  }
  return got, nil
}

// sectorIndustries lists the industry names of one sector.
func (y *yahoo) sectorIndustries(ctx context.Context, key string) ([]string, error) {
  var raw struct {
    Data struct {
      Industries []struct {
        Key  string `json:"key"`
        Name string `json:"name"`
      } `json:"industries"`
    } `json:"data"`
  }
  u := "https://query1.finance.yahoo.com/v1/finance/sectors/" + url.PathEscape(key) + "?formatted=true&withReturns=false&lang=en-US&region=US"
  if err := y.getJSON(ctx, u, true, &raw); err != nil {
    return nil, err
  }
  var out []string
  for _, ind := range raw.Data.Industries {
    if n := strings.TrimSpace(ind.Name); n != "" && !strings.EqualFold(n, "All Industries") {
      out = append(out, n)
    }
  }
  if len(out) == 0 {
    return nil, fmt.Errorf("yahoo sector %s: no industries", key)
  }
  return out, nil
}

func classificationOf(it map[string]any, sectorKey, sector, industry string) Classification {
  return Classification{
    Symbol:            strings.ToUpper(str(it["symbol"])),
    Name:              firstNonEmpty(str(it["longName"]), str(it["shortName"])),
    Sector:            sector,
    SectorKey:         sectorKey,
    Industry:          industry,
    Exchange:          str(it["exchange"]),
    FullExchange:      str(it["fullExchangeName"]),
    QuoteType:         str(it["quoteType"]),
    Currency:          str(it["currency"]),
    MarketCap:         num(it["marketCap"]),
    Price:             num(it["regularMarketPrice"]),
    ChangePercent:     num(it["regularMarketChangePercent"]),
    SharesOutstanding: num(it["sharesOutstanding"]),
  }
}

// sector-screen: one "classification" envelope per equity. Per sector the
// industries are screened one by one (sector + industry); a final
// sector-only pass adds whatever the industry queries missed (or everything,
// when the industry list is unavailable). Request count is roughly
// industries + sectors x pages, i.e. a few hundred instead of one per symbol.
func cmdSectorScreen(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("sector-screen", &o)
  region := fs.String("region", "us", "screener region")
  only := fs.String("sectors", "", "comma list of sector keys (default all): "+func() string {
    var ks []string
    for _, s := range yahooSectors {
      ks = append(ks, s.key)
    }
    return strings.Join(ks, ",")
  }())
  withInd := fs.Bool("industries", true, "also classify by industry (one query per industry)")
  parseArgs(fs, args)
  want := map[string]bool{}
  for _, k := range splitList([]string{*only}) {
    want[strings.ToLower(k)] = true
  }
  reg := strings.ToLower(strings.TrimSpace(*region))
  if !regexp.MustCompile(`^[a-z]{2}$`).MatchString(reg) {
    return fmt.Errorf("sector-screen: invalid -region %q", *region)
  }
  y := newYahoo(&o)
  return loop(ctx, &o, func(ctx context.Context) error {
    seen := map[string]bool{}
    var failed []string
    industries := 0
    publish := func(c Classification) {
      if c.Symbol == "" || seen[c.Symbol] {
        return
      }
      seen[c.Symbol] = true
      emit(envOK("classification", c.Symbol, qDelayed, c))
    }
    for _, sec := range yahooSectors {
      if len(want) > 0 && !want[sec.key] {
        continue
      }
      if *withInd {
        inds, err := y.sectorIndustries(ctx, sec.key)
        if err != nil {
          logger.Warn("industry list unavailable, sector-only", "sector", sec.key, "err", err)
          failed = append(failed, sec.key+": "+err.Error())
        }
        for _, ind := range inds {
          industries++
          ops := []any{screenEq("region", reg), screenEq("sector", sec.name), screenEq("industry", ind)}
          if _, err := y.screenAll(ctx, ops, func(it map[string]any) {
            publish(classificationOf(it, sec.key, sec.name, ind))
          }); err != nil {
            if ctx.Err() != nil {
              return ctx.Err()
            }
            failed = append(failed, sec.key+"/"+ind+": "+err.Error())
          }
        }
      }
      ops := []any{screenEq("region", reg), screenEq("sector", sec.name)}
      if _, err := y.screenAll(ctx, ops, func(it map[string]any) {
        publish(classificationOf(it, sec.key, sec.name, ""))
      }); err != nil {
        if ctx.Err() != nil {
          return ctx.Err()
        }
        failed = append(failed, sec.key+": "+err.Error())
        emit(envFail("classification-sector", sec.key, err))
      }
    }
    emit(envOK("classification-summary", reg, qDelayed, map[string]any{
      "region": reg, "symbols": len(seen), "industries": industries, "errors": failed,
    }))
    if len(seen) == 0 {
      return errors.New("sector-screen: no symbols classified")
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Profile: sector / industry of one symbol (quoteSummary assetProfile+price)
// ---------------------------------------------------------------------------

// Profile is one `profile` record. NotFound marks symbols Yahoo does not
// know (so callers can cache the negative answer).
type Profile struct {
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
}

func (y *yahoo) profile(ctx context.Context, sym string) (*Profile, error) {
  u := "https://query2.finance.yahoo.com/v10/finance/quoteSummary/" + url.PathEscape(sym) + "?modules=" + url.QueryEscape("assetProfile,price")
  var resp struct {
    QuoteSummary struct {
      Result []map[string]any `json:"result"`
      Error  *yError          `json:"error"`
    } `json:"quoteSummary"`
  }
  err := y.getJSON(ctx, u, true, &resp)
  if httpStatus(err) == http.StatusNotFound ||
    (err == nil && resp.QuoteSummary.Error != nil && strings.EqualFold(resp.QuoteSummary.Error.Code, "Not Found")) {
    return &Profile{Symbol: sym, NotFound: true}, nil
  }
  if err != nil {
    return nil, err
  }
  if e := resp.QuoteSummary.Error; e != nil {
    return nil, fmt.Errorf("yahoo quoteSummary %s: %s %s", sym, e.Code, e.Description)
  }
  if len(resp.QuoteSummary.Result) == 0 {
    return &Profile{Symbol: sym, NotFound: true}, nil
  }
  mods := resp.QuoteSummary.Result[0]
  ap, _ := mods["assetProfile"].(map[string]any)
  pr, _ := mods["price"].(map[string]any)
  return &Profile{
    Symbol:      sym,
    Name:        firstNonEmpty(str(pr["longName"]), str(pr["shortName"])),
    QuoteType:   str(pr["quoteType"]),
    Exchange:    str(pr["exchangeName"]),
    Currency:    str(pr["currency"]),
    Sector:      str(ap["sector"]),
    SectorKey:   str(ap["sectorKey"]),
    Industry:    str(ap["industry"]),
    IndustryKey: str(ap["industryKey"]),
    Country:     str(ap["country"]),
    Website:     str(ap["website"]),
    Employees:   num(ap["fullTimeEmployees"]),
    MarketCap:   num(pr["marketCap"]),
  }, nil
}

// profile: one envelope per symbol, requests one after another (paced).
func cmdProfile(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("profile", &o)
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("profile: at least one symbol required")
  }
  y := newYahoo(&o)
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, s := range syms {
      if ctx.Err() != nil {
        return ctx.Err()
      }
      p, err := y.profile(ctx, s)
      if err != nil {
        d.publish(envFail("profile", s, err))
        continue
      }
      d.publish(envOK("profile", s, qDelayed, p))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Cross-process pacing with rate-limit backoff
// ---------------------------------------------------------------------------

const (
  defaultYahooGap = 500 * time.Millisecond
  backoffMin      = 5 * time.Minute
  backoffMax      = time.Hour
  yahooDenied     = 999 // Yahoo's "request denied" status
)

func isRateLimitStatus(st int) bool {
  return st == http.StatusTooManyRequests || st == yahooDenied
}

// pausedError: all Yahoo requests are paused after a rate-limit answer.
type pausedError struct{ until time.Time }

func (e *pausedError) Error() string {
  return "yahoo rate limited: requests paused until " + e.until.Local().Format("15:04:05")
}

// paceState is the lock file content: "<last start> <paused until> <backoff>"
// (UnixNano, UnixNano, nanoseconds).
type paceState struct {
  last, pausedUntil int64
  backoff           time.Duration
}

// filePacer spaces request starts by gap across every process that uses
// the same lock file (flock) and shares one rate-limit pause between them.
// Processes may use different gaps: a slower background job only gets a
// slot when the last request is at least its own gap old.
type filePacer struct {
  path      string
  gap       time.Duration
  waitPause bool
  mu        sync.Mutex
  local     *limiter
  warnOnce  sync.Once
  inBackoff bool // seen a non-zero backoff: reset it after the next success
}

func newYahooPacer() *filePacer {
  gap := defaultYahooGap
  if v := strings.TrimSpace(os.Getenv(envPrefix + "MIN_GAP")); v != "" {
    if d, err := time.ParseDuration(v); err == nil && d >= 0 {
      gap = d
    }
  }
  path := envOr(envPrefix+"PACE_FILE", filepath.Join(common.StateDir("market-terminal"), fmt.Sprintf("qube-yahoo-%d.pace", os.Getuid())))
  return &filePacer{
    path: path, gap: gap, waitPause: os.Getenv(envPrefix+"WAIT_PAUSE") == "1",
    local: &limiter{gap: gap},
  }
}

// locked runs fn on the shared state with the file lock held; fn may change
// the state, which is written back.
func (p *filePacer) locked(fn func(st *paceState)) error {
  f, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0o600)
  if err != nil {
    return err
  }
  defer f.Close()
  if err := common.LockFile(f); err != nil {
    return err
  }
  defer common.UnlockFile(f)
  buf := make([]byte, 96)
  n, _ := f.ReadAt(buf, 0)
  var st paceState
  if fields := strings.Fields(string(buf[:n])); len(fields) >= 1 {
    st.last, _ = strconv.ParseInt(fields[0], 10, 64)
    if len(fields) >= 3 {
      st.pausedUntil, _ = strconv.ParseInt(fields[1], 10, 64)
      b, _ := strconv.ParseInt(fields[2], 10, 64)
      st.backoff = time.Duration(b)
    }
  }
  fn(&st)
  out := fmt.Sprintf("%d %d %d", st.last, st.pausedUntil, int64(st.backoff))
  if err := f.Truncate(0); err == nil {
    f.WriteAt([]byte(out), 0)
  }
  return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
  if d <= 0 {
    return nil
  }
  t := time.NewTimer(d)
  defer t.Stop()
  select {
  case <-ctx.Done():
    return ctx.Err()
  case <-t.C:
    return nil
  }
}

func (p *filePacer) wait(ctx context.Context) error {
  for {
    p.mu.Lock()
    var until time.Time
    var sleepErr error
    err := p.locked(func(st *paceState) {
      now := time.Now()
      if st.pausedUntil > now.UnixNano() {
        until = time.Unix(0, st.pausedUntil)
        return
      }
      p.inBackoff = st.backoff > 0
      d := time.Until(time.Unix(0, st.last).Add(p.gap))
      if d > p.gap {
        d = p.gap // clock moved backwards
      }
      // Sleeping with the lock held queues the other processes behind us.
      if sleepErr = sleepCtx(ctx, d); sleepErr == nil {
        st.last = time.Now().UnixNano()
      }
    })
    p.mu.Unlock()
    if err != nil {
      p.warnOnce.Do(func() {
        logger.Warn("shared Yahoo pacing unavailable, pacing this process only", "file", p.path, "err", err)
      })
      return p.local.wait(ctx)
    }
    if sleepErr != nil {
      return sleepErr
    }
    if until.IsZero() {
      return nil
    }
    if !p.waitPause {
      return &pausedError{until: until}
    }
    logger.Warn("yahoo rate limited, waiting", "until", until.Format(time.RFC3339))
    if err := sleepCtx(ctx, time.Until(until)); err != nil {
      return err
    }
  }
}

// report feeds an answer status back: rate limits start / extend the shared
// pause, a success after a pause resets the backoff.
func (p *filePacer) report(status int, retryAfter string) {
  switch {
  case isRateLimitStatus(status):
    p.mu.Lock()
    defer p.mu.Unlock()
    p.locked(func(st *paceState) {
      now := time.Now()
      if st.pausedUntil > now.UnixNano() {
        return // a parallel request already started this pause
      }
      b := st.backoff * 2
      if b < backoffMin {
        b = backoffMin
      }
      if b > backoffMax {
        b = backoffMax
      }
      if secs, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && time.Duration(secs)*time.Second > b {
        b = time.Duration(secs) * time.Second
      }
      st.backoff = b
      st.pausedUntil = now.Add(b).UnixNano()
      p.inBackoff = true
      logger.Warn("yahoo rate limit, pausing all yahoo requests", "status", status, "for", b.String())
    })
  case status >= 200 && status < 300 && p.inBackoff:
    p.mu.Lock()
    defer p.mu.Unlock()
    p.locked(func(st *paceState) {
      if st.pausedUntil <= time.Now().UnixNano() {
        st.backoff = 0
      }
    })
    p.inBackoff = false
  }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

var symRe = regexp.MustCompile(`^[A-Z0-9^=.\-]{1,24}$`)

func cleanSymbols(in []string) ([]string, error) {
  var out []string
  for _, s := range splitList(in) {
    s = strings.ToUpper(s)
    if !symRe.MatchString(s) {
      return nil, fmt.Errorf("invalid symbol %q", s)
    }
    out = append(out, s)
  }
  return out, nil
}

func setOf(xs ...string) map[string]bool {
  m := make(map[string]bool, len(xs))
  for _, x := range xs {
    m[x] = true
  }
  return m
}

func unixDate(sec int64) string {
  if sec <= 0 {
    return ""
  }
  return time.Unix(sec, 0).UTC().Format("2006-01-02")
}

func unixTime(sec int64) string {
  if sec <= 0 {
    return ""
  }
  return time.Unix(sec, 0).UTC().Format(time.RFC3339)
}

func sortBy[T any](xs []T, less func(a, b T) bool) {
  // insertion sort: inputs here are small (dividends/splits per chart)
  for i := 1; i < len(xs); i++ {
    for j := i; j > 0 && less(xs[j], xs[j-1]); j-- {
      xs[j], xs[j-1] = xs[j-1], xs[j]
    }
  }
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
  case bool:
    return strconv.FormatBool(t)
  case map[string]any:
    if f, ok := t["fmt"]; ok {
      return str(f)
    }
  }
  return fmt.Sprint(v)
}

// num accepts numbers, numeric strings ("1,234", "$5.10", "12%") and Yahoo's
// {"raw":..,"fmt":..} objects.
func num(v any) *float64 {
  switch t := v.(type) {
  case float64:
    if math.IsNaN(t) || math.IsInf(t, 0) {
      return nil
    }
    return &t
  case string:
    s := strings.TrimSpace(strings.NewReplacer(",", "", "$", "", "%", "").Replace(t))
    if s == "" || s == "--" || strings.EqualFold(s, "N/A") {
      return nil
    }
    f, err := strconv.ParseFloat(s, 64)
    if err != nil {
      return nil
    }
    return &f
  case map[string]any:
    if r, ok := t["raw"]; ok {
      return num(r)
    }
  }
  return nil
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
  pacer   *filePacer // cross-process pacing + rate-limit backoff; replaces limiter when set
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
  if f.pacer != nil {
    f.pacer.report(resp.StatusCode, resp.Header.Get("Retry-After"))
  }
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
    var paused *pausedError
    switch {
    case err != nil:
      if ctx.Err() != nil {
        return nil, ctx.Err()
      }
      if errors.As(err, &paused) {
        return nil, err // retrying cannot help while the pause lasts
      }
      last = err
    case isRateLimitStatus(st):
      return nil, &httpError{st, u} // never retried; the pacer pauses
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
