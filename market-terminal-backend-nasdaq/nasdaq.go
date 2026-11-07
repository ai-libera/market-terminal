// qube-nasdaq: Nasdaq public endpoints collector.
//
// Ports /api/yfin/options-chain (Nasdaq option chain), /api/v2/ipos and
// /api/v2/halts. Output is a JSONL stream of {data, provenance} envelopes on
// stdout; logs go to stderr.
//
//   go run nasdaq.go options-chain AAPL
//   go run nasdaq.go options-chain -assetclass etf SPY
//   go run nasdaq.go ipos -month 2026-10 -market all
//   go run nasdaq.go halts -every 30s                 # stream new/updated halts
//
// Env: QUBE_NASDAQ_UA overrides the User-Agent.
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
  "regexp"
  "strconv"
  "strings"
  "sync"
  "syscall"
  "time"
)

const (
  progName   = "qube-nasdaq"
  sourceName = "nasdaq"
  envPrefix  = "QUBE_NASDAQ_"
  // api.nasdaq.com stalls requests without a browser-like UA; this is the
  // string the TS route used. Override with -ua / QUBE_NASDAQ_UA.
  defaultUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
)

func main() {
  runMain([]command{
    {"options-chain", "[-assetclass stocks|etf|index] SYM...", "full option chain (primary chain source)", cmdOptionsChain},
    {"ipos", "[-month YYYY-MM] [-market upcoming|priced|filed|withdrawn|all]", "IPO calendar, one record per deal", cmdIPOs},
    {"halts", "", "NasdaqTrader trade halts, one record per halt", cmdHalts},
  })
}

func newNasdaq(o *opts) *fetcher {
  f := newFetcher(o, nil, 3)
  f.headers["Accept"] = "application/json, text/plain, */*"
  f.headers["Accept-Language"] = "en-US,en;q=0.9"
  return f
}

// ---------------------------------------------------------------------------
// Option chain
// ---------------------------------------------------------------------------

type OptionContract struct {
  Expiry       string   `json:"expiry"`
  Type         string   `json:"type"`
  Strike       float64  `json:"strike"`
  Last         *float64 `json:"last"`
  Change       *float64 `json:"change"`
  Bid          *float64 `json:"bid"`
  Ask          *float64 `json:"ask"`
  Volume       *float64 `json:"volume"`
  OpenInterest *float64 `json:"openInterest"`
}

type OptionChain struct {
  Symbol      string           `json:"symbol"`
  Spot        *float64         `json:"spot"`
  LastTrade   string           `json:"lastTrade,omitempty"`
  Expirations []string         `json:"expirations"`
  Contracts   []OptionContract `json:"contracts"`
}

var (
  months = map[string]time.Month{
    "jan": time.January, "feb": time.February, "mar": time.March, "apr": time.April,
    "may": time.May, "jun": time.June, "jul": time.July, "aug": time.August,
    "sep": time.September, "oct": time.October, "nov": time.November, "dec": time.December,
  }
  expiryRe = regexp.MustCompile(`^([A-Za-z]{3})[a-z]*\.?\s+(\d{1,2}),?\s*(\d{4})?$`)
  yearRe   = regexp.MustCompile(`(\d{4})`)
  spotRe   = regexp.MustCompile(`\$\s*([\d,]+(?:\.\d+)?)`)
)

// normalizeExpiry turns "May 18" / "May 18, 2026" into 2026-05-18. When the
// label has no year the expiry group's year is used, else the next future
// occurrence (the TS code assumed the current year).
func normalizeExpiry(label, groupYear string, now time.Time) string {
  m := expiryRe.FindStringSubmatch(strings.TrimSpace(label))
  if m == nil {
    return strings.TrimSpace(label)
  }
  mon, ok := months[strings.ToLower(m[1])]
  if !ok {
    return strings.TrimSpace(label)
  }
  day, _ := strconv.Atoi(m[2])
  year := now.Year()
  if m[3] != "" {
    year, _ = strconv.Atoi(m[3])
  } else if groupYear != "" {
    year, _ = strconv.Atoi(groupYear)
  } else if time.Date(year, mon, day, 23, 59, 0, 0, time.UTC).Before(now.AddDate(0, 0, -1)) {
    year++
  }
  return fmt.Sprintf("%04d-%02d-%02d", year, int(mon), day)
}

func parseNum(s string) *float64 {
  s = strings.TrimSpace(strings.NewReplacer(",", "", "$", "", "%", "").Replace(s))
  if s == "" || s == "--" || strings.EqualFold(s, "N/A") {
    return nil
  }
  f, err := strconv.ParseFloat(s, 64)
  if err != nil {
    return nil
  }
  return &f
}

func fetchOptionChain(ctx context.Context, f *fetcher, sym, assetClass string, limit int) (*OptionChain, error) {
  q := url.Values{}
  q.Set("assetclass", assetClass)
  q.Set("callput", "callput")
  q.Set("money", "all")
  q.Set("type", "all")
  q.Set("fromdate", "all")
  if limit > 0 {
    q.Set("limit", strconv.Itoa(limit))
  }
  u := "https://api.nasdaq.com/api/quote/" + url.PathEscape(sym) + "/option-chain?" + q.Encode()
  var raw struct {
    Data *struct {
      LastTrade string `json:"lastTrade"`
      Table     struct {
        Rows []map[string]any `json:"rows"`
      } `json:"table"`
    } `json:"data"`
    Status struct {
      RCode            int `json:"rCode"`
      BCodeMessage     any `json:"bCodeMessage"`
      DeveloperMessage any `json:"developerMessage"`
    } `json:"status"`
  }
  if err := f.getJSON(ctx, u, nil, &raw); err != nil {
    return nil, err
  }
  if raw.Data == nil {
    return nil, fmt.Errorf("nasdaq: no option data for %s (rCode %d: %v)", sym, raw.Status.RCode, raw.Status.BCodeMessage)
  }
  oc := &OptionChain{Symbol: sym, LastTrade: raw.Data.LastTrade, Expirations: []string{}, Contracts: []OptionContract{}}
  if m := spotRe.FindStringSubmatch(raw.Data.LastTrade); m != nil {
    oc.Spot = parseNum(m[1])
  }
  now := time.Now().UTC()
  lastGroup, groupYear := "", ""
  for _, r := range raw.Data.Table.Rows {
    s := func(k string) string {
      if v, ok := r[k].(string); ok {
        return v
      }
      return ""
    }
    if s("strike") == "" {
      if g := s("expirygroup"); g != "" {
        lastGroup = g
        groupYear = ""
        if m := yearRe.FindStringSubmatch(g); m != nil {
          groupYear = m[1]
        }
      }
      continue
    }
    strike := parseNum(s("strike"))
    if strike == nil {
      continue
    }
    exp := normalizeExpiry(firstNonEmpty(s("expiryDate"), lastGroup), groupYear, now)
    if len(oc.Expirations) == 0 || oc.Expirations[len(oc.Expirations)-1] != exp {
      oc.Expirations = append(oc.Expirations, exp)
    }
    for _, side := range []struct{ prefix, kind string }{{"c_", "call"}, {"p_", "put"}} {
      c := OptionContract{
        Expiry: exp, Type: side.kind, Strike: *strike,
        Last:         parseNum(s(side.prefix + "Last")),
        Change:       parseNum(s(side.prefix + "Change")),
        Bid:          parseNum(s(side.prefix + "Bid")),
        Ask:          parseNum(s(side.prefix + "Ask")),
        Volume:       parseNum(s(side.prefix + "Volume")),
        OpenInterest: parseNum(s(side.prefix + "Openinterest")),
      }
      if c.Last == nil && c.Bid == nil && c.Ask == nil && c.OpenInterest == nil {
        continue
      }
      oc.Contracts = append(oc.Contracts, c)
    }
  }
  return oc, nil
}

var symRe = regexp.MustCompile(`^[A-Z][A-Z0-9.\-]{0,9}$`)

func cmdOptionsChain(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("options-chain", &o)
  assetClass := fs.String("assetclass", "stocks", "stocks | etf | index")
  limit := fs.Int("limit", 0, "row limit passed to Nasdaq (0 = upstream default)")
  var syms []string
  for _, s := range splitList(parseArgs(fs, args)) {
    s = strings.ToUpper(s)
    if !symRe.MatchString(s) {
      return fmt.Errorf("options-chain: invalid symbol %q", s)
    }
    syms = append(syms, s)
  }
  if len(syms) == 0 {
    return errors.New("options-chain: symbol required")
  }
  switch *assetClass {
  case "stocks", "etf", "index":
  default:
    return fmt.Errorf("options-chain: invalid -assetclass %q", *assetClass)
  }
  f := newNasdaq(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, s := range syms {
      v, age, stale, err := cache.get(s, 2*time.Minute, func() (any, error) {
        return fetchOptionChain(ctx, f, s, *assetClass, *limit)
      })
      d.publish(resultEnv("options-chain", s, qDelayed, v, age, stale, err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// IPO calendar
// ---------------------------------------------------------------------------

type IPO struct {
  ID                  string   `json:"id"`
  Symbol              string   `json:"symbol"`
  CompanyName         string   `json:"companyName"`
  Status              string   `json:"status"`
  Exchange            *string  `json:"exchange"`
  ExpectedPricingDate *string  `json:"expectedPricingDate"`
  PricedDate          *string  `json:"pricedDate,omitempty"`
  FiledDate           *string  `json:"filedDate,omitempty"`
  WithdrawnDate       *string  `json:"withdrawnDate,omitempty"`
  OfferShares         *float64 `json:"offerShares"`
  PriceRangeLow       *float64 `json:"priceRangeLow"`
  PriceRangeHigh      *float64 `json:"priceRangeHigh"`
  DealSize            *float64 `json:"dealSize,omitempty"`
  LeadUnderwriters    *string  `json:"leadUnderwriters"`
}

var ipoSections = []struct{ name, status string }{
  {"upcoming", "EXPECTED"},
  {"priced", "PRICED"},
  {"filed", "FILED"},
  {"withdrawn", "WITHDRAWN"},
}

// findRows returns the "rows" array of a section, looking one level deeper
// for wrappers such as upcoming.upcomingTable.rows.
func findRows(v any) []map[string]any {
  m, ok := v.(map[string]any)
  if !ok {
    if arr, ok := v.([]any); ok {
      return toMaps(arr)
    }
    return nil
  }
  if arr, ok := m["rows"].([]any); ok {
    return toMaps(arr)
  }
  for _, sub := range m {
    if sm, ok := sub.(map[string]any); ok {
      if arr, ok := sm["rows"].([]any); ok {
        return toMaps(arr)
      }
    }
  }
  return nil
}

func toMaps(arr []any) []map[string]any {
  out := make([]map[string]any, 0, len(arr))
  for _, x := range arr {
    if m, ok := x.(map[string]any); ok {
      out = append(out, m)
    }
  }
  return out
}

func strField(r map[string]any, keys ...string) string {
  for _, k := range keys {
    if v, ok := r[k]; ok && v != nil {
      if s := strings.TrimSpace(str(v)); s != "" {
        return s
      }
    }
  }
  return ""
}

func optStr(s string) *string {
  if s == "" {
    return nil
  }
  return &s
}

func parseIPORow(r map[string]any, status string) IPO {
  e := IPO{
    Symbol:              strings.ToUpper(strField(r, "proposedTickerSymbol", "symbol", "ticker")),
    CompanyName:         strField(r, "companyName", "company"),
    Status:              status,
    Exchange:            optStr(strField(r, "proposedExchange", "exchange", "market")),
    ExpectedPricingDate: optStr(strField(r, "expectedPriceDate", "pricingDate", "expectedDate", "pricedDate")),
    PricedDate:          optStr(strField(r, "pricedDate")),
    FiledDate:           optStr(strField(r, "filedDate")),
    WithdrawnDate:       optStr(strField(r, "withdrawDate", "withdrawnDate")),
    OfferShares:         parseNum(strField(r, "sharesOffered", "shares")),
    DealSize:            parseNum(strField(r, "dollarValueOfSharesOffered")),
    LeadUnderwriters:    optStr(strField(r, "leadUnderwriters", "underwriter")),
  }
  if lo := parseNum(strField(r, "priceRangeLow")); lo != nil {
    e.PriceRangeLow = lo
    e.PriceRangeHigh = parseNum(strField(r, "priceRangeHigh"))
  } else if pr := strField(r, "proposedSharePrice", "priceRange", "price"); pr != "" {
    parts := strings.SplitN(pr, "-", 2)
    e.PriceRangeLow = parseNum(parts[0])
    if len(parts) == 2 {
      e.PriceRangeHigh = parseNum(parts[1])
    } else {
      e.PriceRangeHigh = e.PriceRangeLow
    }
  }
  e.ID = strField(r, "dealID", "dealId")
  if e.ID == "" {
    e.ID = status + ":" + e.Symbol + ":" + e.CompanyName
  }
  return e
}

var monthRe = regexp.MustCompile(`^\d{4}-\d{2}$`)

func cmdIPOs(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("ipos", &o)
  month := fs.String("month", "", "calendar month YYYY-MM (default current)")
  market := fs.String("market", "all", "upcoming | priced | filed | withdrawn | all")
  parseArgs(fs, args)
  if *month != "" && !monthRe.MatchString(*month) {
    return fmt.Errorf("ipos: invalid -month %q", *month)
  }
  want := map[string]bool{}
  for _, m := range splitList([]string{strings.ToLower(*market)}) {
    if m == "all" {
      for _, s := range ipoSections {
        want[s.name] = true
      }
      continue
    }
    ok := false
    for _, s := range ipoSections {
      ok = ok || s.name == m
    }
    if !ok {
      return fmt.Errorf("ipos: invalid -market %q", m)
    }
    want[m] = true
  }
  f := newNasdaq(&o)
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    mon := *month
    if mon == "" {
      mon = time.Now().UTC().Format("2006-01")
    }
    var raw struct {
      Data map[string]any `json:"data"`
    }
    if err := f.getJSON(ctx, "https://api.nasdaq.com/api/ipo/calendar?date="+mon, nil, &raw); err != nil {
      d.publish(envFail("ipo", mon, err))
      return nil
    }
    n := 0
    for _, sec := range ipoSections {
      if !want[sec.name] {
        continue
      }
      for _, r := range findRows(raw.Data[sec.name]) {
        e := parseIPORow(r, sec.status)
        if e.Symbol == "" && e.CompanyName == "" {
          continue
        }
        n++
        d.publish(envOK("ipo", e.ID, qDelayed, e))
      }
    }
    if n == 0 {
      d.publish(envFail("ipo", mon, errors.New("Nasdaq IPO calendar returned no entries")))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Trade halts (NasdaqTrader RSS)
// ---------------------------------------------------------------------------

// Halt reason codes per NasdaqTrader's published table (the TS route's table
// had several codes mislabeled).
var reasonCodes = map[string]string{
  "T1":   "News Pending",
  "T2":   "News Released",
  "T5":   "Single Stock Trading Pause in Effect",
  "T6":   "Extraordinary Market Activity",
  "T7":   "Single Stock Trading Pause / Quotation-Only Period",
  "T8":   "Exchange-Traded Fund (ETF)",
  "T12":  "Additional Information Requested by Nasdaq",
  "H4":   "Non-compliance",
  "H9":   "Not Current",
  "H10":  "SEC Trading Suspension",
  "H11":  "Regulatory Concern",
  "O1":   "Operations Halt",
  "IPO1": "IPO Issue Not Yet Trading",
  "IPOQ": "IPO Security Released for Quotation",
  "IPOE": "IPO Security - Positioning Window Extension",
  "M1":   "Corporate Action",
  "M2":   "Quotation Not Available",
  "LUDP": "Volatility Trading Pause (LULD)",
  "LUDS": "Volatility Trading Pause - Straddle Condition",
  "MWC0": "Market-Wide Circuit Breaker Halt - Carry over from previous day",
  "MWC1": "Market-Wide Circuit Breaker Halt - Level 1",
  "MWC2": "Market-Wide Circuit Breaker Halt - Level 2",
  "MWC3": "Market-Wide Circuit Breaker Halt - Level 3",
  "MWCQ": "Market-Wide Circuit Breaker Resumption",
  "R1":   "New Issue Available",
  "R2":   "Issue Available",
  "R4":   "Qualifications Issues Reviewed/Resolved; Quotations/Trading to Resume",
  "R9":   "Filing Requirements Satisfied/Resolved; Quotations/Trading to Resume",
  "C3":   "Issuer News Not Forthcoming; Quotations/Trading to Resume",
  "C4":   "Qualifications Halt Ended; Maintenance Requirements Met; Resume",
  "C9":   "Qualifications Halt Concluded; Filings Met; Quotes/Trades to Resume",
  "C11":  "Trade Halt Concluded By Other Regulatory Authority; Quotes/Trades Resume",
  "D":    "Security Deletion",
}

func decodeReason(code string) string {
  if s, ok := reasonCodes[strings.ToUpper(code)]; ok {
    return s
  }
  return "Halt - see reason code"
}

type Halt struct {
  ID                  string `json:"id"`
  Symbol              string `json:"symbol"`
  IssueName           string `json:"issueName,omitempty"`
  MarketCenter        string `json:"marketCenter"`
  HaltDate            string `json:"haltDate"`
  HaltTime            string `json:"haltTime"`
  ReasonCode          string `json:"reasonCode"`
  ReasonLabel         string `json:"reasonLabel"`
  PauseThresholdPrice string `json:"pauseThresholdPrice,omitempty"`
  ResumptionDate      string `json:"resumptionDate,omitempty"`
  ResumptionQuoteTime string `json:"resumptionQuoteTime,omitempty"`
  ResumptionTime      string `json:"resumptionTime,omitempty"`
  QuoteOnly           bool   `json:"quoteOnly"`
}

// parseItems decodes RSS <item> elements into maps of lower-cased child
// element local names (namespace prefixes such as ndaq: are dropped).
func parseItems(b []byte) ([]map[string]string, error) {
  dec := xml.NewDecoder(bytes.NewReader(b))
  dec.Strict = false
  dec.AutoClose = xml.HTMLAutoClose
  dec.Entity = xml.HTMLEntity
  dec.CharsetReader = charsetReader
  var items []map[string]string
  var cur map[string]string
  var field string
  var buf strings.Builder
  depth := 0
  for {
    tok, err := dec.Token()
    if err == io.EOF {
      return items, nil
    }
    if err != nil {
      if len(items) > 0 {
        return items, nil
      }
      return nil, err
    }
    switch t := tok.(type) {
    case xml.StartElement:
      name := strings.ToLower(t.Name.Local)
      if cur == nil {
        if name == "item" {
          cur, depth = map[string]string{}, 0
        }
        continue
      }
      depth++
      if depth == 1 {
        field = name
        buf.Reset()
      }
    case xml.CharData:
      if cur != nil && depth >= 1 {
        buf.Write(t)
      }
    case xml.EndElement:
      if cur == nil {
        continue
      }
      if depth == 0 {
        items = append(items, cur)
        cur = nil
        continue
      }
      if depth == 1 {
        cur[field] = strings.TrimSpace(buf.String())
      }
      depth--
    }
  }
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

func pick(m map[string]string, keys ...string) string {
  for _, k := range keys {
    if v := strings.TrimSpace(m[k]); v != "" {
      return v
    }
  }
  return ""
}

func cmdHalts(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("halts", &o)
  parseArgs(fs, args)
  f := newNasdaq(&o)
  f.headers["Accept"] = "application/rss+xml, application/xml, text/xml, */*"
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    b, err := f.get(ctx, "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts", nil)
    if err != nil {
      d.publish(envFail("halt", "feed", fmt.Errorf("halt feed unreachable: %w", err)))
      return nil
    }
    items, err := parseItems(b)
    if err != nil {
      d.publish(envFail("halt", "feed", fmt.Errorf("halt feed: %w", err)))
      return nil
    }
    for _, it := range items {
      sym := pick(it, "issuesymbol", "tsymbol", "symbol", "title")
      if sym == "" {
        continue
      }
      code := strings.ToUpper(pick(it, "reasoncode"))
      h := Halt{
        Symbol:              sym,
        IssueName:           pick(it, "issuename"),
        MarketCenter:        pick(it, "market", "marketcenter"),
        HaltDate:            pick(it, "haltdate"),
        HaltTime:            pick(it, "halttime"),
        ReasonCode:          code,
        ReasonLabel:         decodeReason(code),
        PauseThresholdPrice: pick(it, "pausethresholdprice"),
        ResumptionDate:      pick(it, "resumptiondate"),
        ResumptionQuoteTime: pick(it, "resumptionquotetime"),
        ResumptionTime:      pick(it, "resumptiontradetime", "resumptiontime"),
      }
      h.QuoteOnly = strings.Contains(strings.ToLower(pick(it, "pausetype")+pick(it, "halttype")), "q")
      h.ID = h.Symbol + "-" + h.HaltDate + "-" + h.HaltTime + "-" + code
      // Feed order is oldest first, which suits a stream; t.go reverses for display.
      d.publish(envOK("halt", h.ID, qLive, h))
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
