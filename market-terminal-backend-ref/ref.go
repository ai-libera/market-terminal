// qube-ref: reference / calendar scrapes from sites without an API.
//
// Ports /api/yfin/analyst-ratings (stockanalysis.com), /api/v2/transcripts
// (fool.com) and /api/yfin/economic-calendar (faireconomy.media). These are
// the most fragile scrapers: when a page no longer yields data the record is
// UNAVAILABLE rather than guessed. Check each site's terms before production
// use. Output is a JSONL stream of {data, provenance} envelopes on stdout.
//
//   go run ref.go analyst AAPL MSFT
//   go run ref.go transcripts AAPL                      # one record per call
//   go run ref.go transcript mf:/earnings/call-transcripts/2026/07/31/apple-aapl-q3-2026-earnings-call-transcript/
//   go run ref.go econ-calendar -week thisweek
//   go run ref.go sp500                                 # IVV holdings, Wikipedia fallback
//   go run ref.go sp500 -source wikipedia
//
// Env: QUBE_REF_UA overrides the User-Agent; QUBE_REF_IVV_URL pins the IVV
// holdings CSV URL (otherwise it is discovered on the iShares product page).
package main

import (
  "bufio"
  "bytes"
  "context"
  "crypto/sha1"
  "encoding/csv"
  "encoding/hex"
  "encoding/json"
  "errors"
  "flag"
  "fmt"
  "html"
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
  progName   = "qube-ref"
  sourceName = "ref"
  envPrefix  = "QUBE_REF_"
  defaultUA  = "Mozilla/5.0 (compatible; qube-ref/1.0)"
)

func main() {
  runMain([]command{
    {"analyst", "SYM...", "analyst consensus, targets and ratings (stockanalysis.com)", cmdAnalyst},
    {"transcripts", "SYM...", "earnings-call transcript index (fool.com)", cmdTranscripts},
    {"transcript", "[-symbol S] ID|PATH|URL", "one transcript segmented by speaker/section", cmdTranscript},
    {"econ-calendar", "[-week thisweek|nextweek|lastweek]", "economic calendar, one record per event", cmdEconCalendar},
    {"sp500", "[-source auto|ivv|wikipedia] [-ivv-url URL]", "S&P 500 members (iShares IVV holdings, Wikipedia fallback), one record", cmdSP500},
  })
}

var symRe = regexp.MustCompile(`^[A-Z][A-Z0-9.\-]{0,9}$`)

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

func newRef(o *opts) *fetcher {
  f := newFetcher(o, nil, 1)
  f.headers["Accept"] = "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8"
  f.headers["Accept-Language"] = "en-US,en;q=0.9"
  return f
}

var (
  tagRe = regexp.MustCompile(`(?s)<[^>]*>`)
  wsRe  = regexp.MustCompile(`[ \t]+`)
)

func stripHTML(s string) string {
  for _, re := range []*regexp.Regexp{scriptRe, styleRe} {
    s = re.ReplaceAllString(s, "")
  }
  s = brRe.ReplaceAllString(s, "\n")
  s = blockEndRe.ReplaceAllString(s, "\n")
  s = tagRe.ReplaceAllString(s, "")
  s = html.UnescapeString(s)
  s = strings.ReplaceAll(s, "\r", "")
  s = wsRe.ReplaceAllString(s, " ")
  s = multiNLRe.ReplaceAllString(s, "\n\n")
  return strings.TrimSpace(s)
}

var (
  scriptRe   = regexp.MustCompile(`(?is)<script.*?</script\s*>`)
  styleRe    = regexp.MustCompile(`(?is)<style.*?</style\s*>`)
  brRe       = regexp.MustCompile(`(?i)<br\s*/?>`)
  blockEndRe = regexp.MustCompile(`(?i)</(?:p|div|li|h[1-6])>`)
  multiNLRe  = regexp.MustCompile(`\n\s*\n+`)
)

// ---------------------------------------------------------------------------
// Analyst ratings (stockanalysis.com forecast page)
// ---------------------------------------------------------------------------

type AnalystRating struct {
  Firm        string   `json:"firm"`
  Rating      string   `json:"rating"`
  PriceTarget *float64 `json:"priceTarget"`
  Date        string   `json:"date"`
}

type AnalystSummary struct {
  Symbol       string          `json:"symbol"`
  Consensus    string          `json:"consensus"`
  StrongBuy    *int            `json:"strongBuy"`
  Buy          *int            `json:"buy"`
  Hold         *int            `json:"hold"`
  Sell         *int            `json:"sell"`
  StrongSell   *int            `json:"strongSell"`
  TargetMean   *float64        `json:"targetMean"`
  TargetLow    *float64        `json:"targetLow"`
  TargetHigh   *float64        `json:"targetHigh"`
  TargetMedian *float64        `json:"targetMedian"`
  Ratings      []AnalystRating `json:"ratings"`
  SourceURL    string          `json:"sourceUrl"`
}

var (
  consensusRe = regexp.MustCompile(`consensus rating of "([^"]+)"`)
  avgRe       = regexp.MustCompile(`average price target of \$?([\d,.]+)`)
  lowRe       = regexp.MustCompile(`lowest target is \$?([\d,.]+)`)
  highRe      = regexp.MustCompile(`highest is \$?([\d,.]+)`)
  medianRe    = regexp.MustCompile(`(?i)median[^$\d<]{0,60}\$?([\d,]+\.?\d*)`)
  spanNumRe   = regexp.MustCompile(`>(\d+)</span>`)
  analystRe   = regexp.MustCompile(`analyst-name[^>]*title="([^"]+)"[\s\S]*?<td[^>]*class="rating[^"]*"[^>]*>([\s\S]*?)</td>`)
  ratingWord  = regexp.MustCompile(`>(Strong Buy|Strong Sell|Buy|Hold|Sell|Outperform|Underperform|Neutral|Overweight|Underweight|Equal-Weight|Sector Perform|Market Perform|Mixed)<|^\s*(Strong Buy|Strong Sell|Buy|Hold|Sell|Outperform|Underperform|Neutral)`)
  ptRe        = regexp.MustCompile(`class="[^"]*pt[^"]*"[^>]*>\$?([\d,.]+)`)
  dateCellRe  = regexp.MustCompile(`class="[^"]*date[^"]*"[^>]*>([^<]+)`)
)

func parseF(s string) *float64 {
  s = strings.TrimRight(strings.ReplaceAll(s, ",", ""), ".")
  f, err := strconv.ParseFloat(s, 64)
  if err != nil {
    return nil
  }
  return &f
}

func findF(re *regexp.Regexp, s string) *float64 {
  if m := re.FindStringSubmatch(s); m != nil {
    return parseF(m[1])
  }
  return nil
}

func parseAnalystPage(page, sym, src string) (*AnalystSummary, error) {
  a := &AnalystSummary{Symbol: sym, Ratings: []AnalystRating{}, SourceURL: src}
  if m := consensusRe.FindStringSubmatch(page); m != nil {
    a.Consensus = html.UnescapeString(m[1])
  }
  a.TargetMean = findF(avgRe, page)
  a.TargetLow = findF(lowRe, page)
  a.TargetHigh = findF(highRe, page)
  a.TargetMedian = findF(medianRe, page) // no longer defaults to the mean

  // Rating distribution: numbers in spans between "Strong Buy" and "Strong Sell".
  if i := strings.Index(page, "Strong Buy"); i >= 0 {
    end := i + len("Strong Buy") + 2000
    if end > len(page) {
      end = len(page)
    }
    window := page[i+len("Strong Buy") : end]
    if j := strings.Index(window, "Strong Sell"); j >= 0 {
      nums := spanNumRe.FindAllStringSubmatch(window[:j], -1)
      if len(nums) >= 5 {
        vals := make([]*int, 5)
        for k := 0; k < 5; k++ {
          n, _ := strconv.Atoi(nums[k][1])
          vals[k] = &n
        }
        a.StrongBuy, a.Buy, a.Hold, a.Sell, a.StrongSell = vals[0], vals[1], vals[2], vals[3], vals[4]
      }
    }
  }

  for _, idx := range analystRe.FindAllStringSubmatchIndex(page, -1) {
    if len(a.Ratings) >= 30 {
      break
    }
    firm := html.UnescapeString(page[idx[2]:idx[3]])
    cell := strings.TrimSpace(page[idx[4]:idx[5]])
    rating := strings.TrimSpace(tagRe.ReplaceAllString(cell, ""))
    if m := ratingWord.FindStringSubmatch(cell); m != nil {
      rating = firstNonEmpty(m[1], m[2])
    }
    after := page[idx[0]:minInt(len(page), idx[0]+1500)]
    r := AnalystRating{Firm: firm, Rating: rating, PriceTarget: findF(ptRe, after)}
    if m := dateCellRe.FindStringSubmatch(after); m != nil {
      r.Date = strings.TrimSpace(m[1])
    }
    a.Ratings = append(a.Ratings, r)
  }
  if a.Consensus == "" && a.TargetMean == nil && a.StrongBuy == nil && len(a.Ratings) == 0 {
    return nil, errors.New("no analyst data found (page structure may have changed)")
  }
  return a, nil
}

func cmdAnalyst(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("analyst", &o)
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("analyst: symbol required")
  }
  f := newRef(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, s := range syms {
      v, age, stale, err := cache.get(s, 6*time.Hour, func() (any, error) {
        src := "https://stockanalysis.com/stocks/" + url.PathEscape(strings.ToLower(s)) + "/forecast/"
        b, err := f.get(ctx, src, nil)
        if err != nil {
          return nil, err
        }
        return parseAnalystPage(string(b), s, src)
      })
      d.publish(resultEnv("analyst", s, qDelayed, v, age, stale, err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// Earnings-call transcripts (fool.com)
// ---------------------------------------------------------------------------

type TranscriptMeta struct {
  ID               string `json:"id"`
  Symbol           string `json:"symbol"`
  QuarterLabel     string `json:"quarterLabel"`
  CallDate         string `json:"callDate"`
  Title            string `json:"title"`
  SourceURL        string `json:"sourceUrl"`
  ParticipantCount int    `json:"participantCount,omitempty"`
  WordCount        int    `json:"wordCount,omitempty"`
}

type TranscriptSegment struct {
  ID             string `json:"id"`
  SpeakerName    string `json:"speakerName"`
  SpeakerRole    string `json:"speakerRole"`
  SpeakerTitle   string `json:"speakerTitle,omitempty"`
  Section        string `json:"section"`
  Text           string `json:"text"`
  ParagraphIndex int    `json:"paragraphIndex"`
}

type Transcript struct {
  Metadata TranscriptMeta      `json:"metadata"`
  Segments []TranscriptSegment `json:"segments"`
}

var (
  transcriptLinkRe = regexp.MustCompile(`href="(?:https://www\.fool\.com)?(/earnings/call-transcripts/\d{4}/\d{2}/\d{2}/[^"#?]+)"`)
  slugQuarterRe    = regexp.MustCompile(`(?i)q([1-4])-(\d{4})`)
  pathDateRe       = regexp.MustCompile(`(\d{4})/(\d{2})/(\d{2})`)
  transcriptPathRe = regexp.MustCompile(`^/earnings/call-transcripts/\d{4}/\d{2}/\d{2}/[A-Za-z0-9._\-]+/?$`)
)

func quarterFromSlug(slug string) string {
  if m := slugQuarterRe.FindStringSubmatch(slug); m != nil {
    return "Q" + m[1] + " " + m[2]
  }
  return ""
}

func cmdTranscripts(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("transcripts", &o)
  maxN := fs.Int("max", 12, "max transcripts per symbol")
  syms, err := cleanSymbols(parseArgs(fs, args))
  if err != nil {
    return err
  }
  if len(syms) == 0 {
    return errors.New("transcripts: symbol required")
  }
  f := newRef(&o)
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    for _, s := range syms {
      var page []byte
      var err error
      // The quote page lives under the listing exchange; try the common ones.
      for _, ex := range []string{"nasdaq", "nyse", "nysemkt"} {
        page, err = f.get(ctx, "https://www.fool.com/quote/"+ex+"/"+url.PathEscape(strings.ToLower(s))+"/", nil)
        if err == nil || httpStatus(err) != http.StatusNotFound {
          break
        }
      }
      if err != nil {
        d.publish(envFail("transcripts", s, fmt.Errorf("transcript source unreachable: %w", err)))
        continue
      }
      seen := map[string]bool{}
      n := 0
      var metas []TranscriptMeta
      for _, m := range transcriptLinkRe.FindAllStringSubmatch(string(page), -1) {
        p := m[1]
        if seen[p] || n >= *maxN {
          continue
        }
        seen[p] = true
        n++
        slug := strings.Trim(p[strings.LastIndex(strings.TrimRight(p, "/"), "/")+1:], "/")
        meta := TranscriptMeta{
          ID: "mf:" + p, Symbol: s, QuarterLabel: firstNonEmpty(quarterFromSlug(slug), fmt.Sprintf("T-%d", n-1)),
          Title: strings.ReplaceAll(slug, "-", " "), SourceURL: "https://www.fool.com" + p,
        }
        if dm := pathDateRe.FindStringSubmatch(p); dm != nil {
          meta.CallDate = dm[1] + "-" + dm[2] + "-" + dm[3]
        }
        metas = append(metas, meta)
      }
      if len(metas) == 0 {
        d.publish(envFail("transcripts", s, errors.New("no transcripts listed for symbol")))
        continue
      }
      for i := len(metas) - 1; i >= 0; i-- { // oldest first
        d.publish(envOK("transcript-meta", metas[i].ID, qDelayed, metas[i]))
      }
    }
    return nil
  })
}

var (
  executiveRe = regexp.MustCompile(`(?i)\b(CEO|CFO|COO|CTO|Chief|President|Chairman|VP|Vice President|Director|Head of|Founder|Treasurer|Investor Relations)\b`)
  analystTRe  = regexp.MustCompile(`(?i)\b(Analyst|Research|Equity Research)\b`)
  operatorRe  = regexp.MustCompile(`(?i)operator`)
  // <p><strong>Name</strong> -- <em>Title</em></p> (MF transcript speaker line)
  speakerRe = regexp.MustCompile(`<p[^>]*>\s*(?:<strong>|<b>)\s*([A-Z][A-Za-z .'\-]{1,40}?)\s*(?:</strong>|</b>)\s*(?:(?:--|\x{2013}|\x{2014}|-)\s*(?:<em>|<i>)?\s*([^<]{0,80}?)\s*(?:</em>|</i>)?)?\s*</p>`)
  skipNameRe    = regexp.MustCompile(`(?i)^(summary|call participants|full conference call transcript|advertisement|copyright|prepared remarks|questions (and|&) answers|duration)`)
  qaRe          = regexp.MustCompile(`(?i)question.{0,10}answer|q\s*&\s*a`)
  bodyStartRe   = regexp.MustCompile(`(?i)Prepared Remarks:|prepared-remarks`)
  bodyEndRe     = regexp.MustCompile(`(?i)Call participants:|Duration:\s*\d`)
  titleRe       = regexp.MustCompile(`(?i)<title>([^<]+)</title>`)
  datePubRe     = regexp.MustCompile(`"datePublished"\s*:\s*"([^"]+)"`)
  dateTextRe    = regexp.MustCompile(`([A-Z][a-z]+ \d{1,2}, \d{4})`)
  titleQuarter  = regexp.MustCompile(`(?i)(Q[1-4]\s*\d{4})`)
  titleQuarter2 = regexp.MustCompile(`(?i)(Q[1-4])`)
  urlSymbolRe   = regexp.MustCompile(`(?i)-([a-z]{1,6})-q[1-4]-\d{4}|([a-z]{1,6})-\d{4}-\d{2}-\d{2}\.aspx`)
)

func classifySpeaker(name, role string) string {
  switch {
  case analystTRe.MatchString(role) || analystTRe.MatchString(name):
    return "ANALYST"
  case operatorRe.MatchString(name) || operatorRe.MatchString(role):
    return "OPERATOR"
  case executiveRe.MatchString(role) || executiveRe.MatchString(name):
    return "EXECUTIVE"
  }
  return "EXECUTIVE"
}

// parseTranscriptHTML segments a Motley Fool transcript. Unlike the TS port
// it keeps every paragraph up to the next speaker line, not only the first.
func parseTranscriptHTML(page, symbol, src string) (*Transcript, error) {
  title := symbol + " Earnings Call Transcript"
  if m := titleRe.FindStringSubmatch(page); m != nil {
    title = strings.TrimSpace(stripHTML(m[1]))
  }
  callDate := ""
  if m := datePubRe.FindStringSubmatch(page); m != nil {
    callDate = m[1]
  } else if m := dateTextRe.FindStringSubmatch(page); m != nil {
    callDate = m[1]
  }
  quarter := "Latest"
  if m := titleQuarter.FindStringSubmatch(title); m != nil {
    quarter = strings.ToUpper(strings.Join(strings.Fields(m[1]), " "))
  } else if m := titleQuarter2.FindStringSubmatch(title); m != nil {
    quarter = strings.ToUpper(m[1])
  }
  body := page
  if loc := bodyStartRe.FindStringIndex(body); loc != nil {
    body = body[loc[0]:]
  }
  if loc := bodyEndRe.FindStringIndex(body); loc != nil && loc[0] > 0 {
    body = body[:loc[0]]
  }
  heads := speakerRe.FindAllStringSubmatchIndex(body, -1)
  segs := []TranscriptSegment{}
  inQA := false
  speakers := map[string]bool{}
  words := 0
  for i, h := range heads {
    name := strings.TrimSpace(body[h[2]:h[3]])
    role := ""
    if h[4] >= 0 {
      role = strings.TrimSpace(html.UnescapeString(body[h[4]:h[5]]))
    }
    end := len(body)
    if i+1 < len(heads) {
      end = heads[i+1][0]
    }
    text := stripHTML(body[h[1]:end])
    if qaRe.MatchString(name + " " + role + " " + clip(text, 60)) {
      inQA = true
    }
    if skipNameRe.MatchString(name) || len(text) < 20 {
      continue
    }
    section := "PREPARED_REMARKS"
    if inQA {
      section = "Q_AND_A"
    }
    segs = append(segs, TranscriptSegment{
      ID: fmt.Sprintf("seg-%d", len(segs)), SpeakerName: name, SpeakerRole: classifySpeaker(name, role),
      SpeakerTitle: role, Section: section, Text: text, ParagraphIndex: len(segs),
    })
    speakers[name] = true
    words += len(strings.Fields(text))
  }
  if len(segs) < 3 {
    return nil, errors.New("transcript UNAVAILABLE: source structure changed or no transcript (transcripts are never synthesized)")
  }
  if callDate == "" {
    if m := pathDateRe.FindStringSubmatch(src); m != nil {
      callDate = m[1] + "-" + m[2] + "-" + m[3]
    }
  }
  return &Transcript{
    Metadata: TranscriptMeta{
      ID: "mf:" + strings.TrimPrefix(src, "https://www.fool.com"), Symbol: symbol, QuarterLabel: quarter,
      CallDate: callDate, Title: title, SourceURL: src, ParticipantCount: len(speakers), WordCount: words,
    },
    Segments: segs,
  }, nil
}

// transcriptURL maps an id (mf:/path), a path or a full fool.com URL to a
// fool.com URL; other hosts are rejected.
func transcriptURL(id string) (string, error) {
  s := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(id), "mf:"))
  if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
    u, err := url.Parse(s)
    if err != nil {
      return "", err
    }
    if h := strings.ToLower(u.Hostname()); h != "www.fool.com" && h != "fool.com" {
      return "", fmt.Errorf("host %s not allowed (fool.com only)", u.Hostname())
    }
    s = u.EscapedPath()
  }
  if !transcriptPathRe.MatchString(s) {
    return "", fmt.Errorf("not a fool.com transcript path: %q", s)
  }
  return "https://www.fool.com" + s, nil
}

func cmdTranscript(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("transcript", &o)
  symFlag := fs.String("symbol", "", "symbol to tag the transcript with (default: derived from URL)")
  pos := parseArgs(fs, args)
  if len(pos) != 1 {
    return errors.New("transcript: exactly one id, path or URL required")
  }
  src, err := transcriptURL(pos[0])
  if err != nil {
    return fmt.Errorf("transcript: %w", err)
  }
  sym := strings.ToUpper(*symFlag)
  if sym == "" {
    if m := urlSymbolRe.FindStringSubmatch(src); m != nil {
      sym = strings.ToUpper(firstNonEmpty(m[1], m[2]))
    }
  }
  if sym == "" {
    sym = "N/A"
  }
  f := newRef(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    v, age, stale, err := cache.get(src, 24*time.Hour, func() (any, error) {
      b, err := f.get(ctx, src, nil)
      if err != nil {
        return nil, fmt.Errorf("transcript source unreachable: %w", err)
      }
      return parseTranscriptHTML(string(b), sym, src)
    })
    d.publish(resultEnv("transcript", "mf:"+strings.TrimPrefix(src, "https://www.fool.com"), qDelayed, v, age, stale, err))
    return nil
  })
}

// ---------------------------------------------------------------------------
// Economic calendar (faireconomy.media)
// ---------------------------------------------------------------------------

type EconEvent struct {
  ID       string `json:"id"`
  Title    string `json:"title"`
  Country  string `json:"country"`
  Date     string `json:"date"`
  Impact   string `json:"impact"`
  Forecast string `json:"forecast"`
  Previous string `json:"previous"`
  Actual   string `json:"actual,omitempty"`
  URL      string `json:"url,omitempty"`
}

func cmdEconCalendar(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("econ-calendar", &o)
  week := fs.String("week", "thisweek", "thisweek | nextweek | lastweek")
  impact := fs.String("impact", "", "comma list of impacts to keep (High, Medium, Low, Holiday)")
  country := fs.String("country", "", "comma list of currencies/countries to keep (USD, EUR, ...)")
  parseArgs(fs, args)
  switch *week {
  case "thisweek", "nextweek", "lastweek":
  default:
    return fmt.Errorf("econ-calendar: invalid -week %q", *week)
  }
  impacts := map[string]bool{}
  for _, x := range splitList([]string{*impact}) {
    impacts[strings.ToLower(x)] = true
  }
  countries := map[string]bool{}
  for _, x := range splitList([]string{*country}) {
    countries[strings.ToUpper(x)] = true
  }
  if o.every > 0 && o.every < 5*time.Minute {
    logger.Warn("faireconomy.media rate-limits aggressively; consider -every 5m or more")
  }
  f := newFetcher(&o, nil, 0.2)
  f.headers["Accept"] = "application/json"
  d := newDelta(o.all)
  cache := newMemo()
  ttl := time.Hour
  if o.every > 0 && o.every < ttl {
    ttl = o.every * 9 / 10
  }
  return loop(ctx, &o, func(ctx context.Context) error {
    v, age, stale, err := cache.get(*week, ttl, func() (any, error) {
      var raw []map[string]any
      if err := f.getJSON(ctx, "https://nfs.faireconomy.media/ff_calendar_"+*week+".json", nil, &raw); err != nil {
        return nil, err
      }
      return raw, nil
    })
    if err != nil && !stale {
      d.publish(envFail("econ-event", *week, err))
      return nil
    }
    raw, _ := v.([]map[string]any)
    for _, r := range raw {
      e := EconEvent{
        Title: str(r["title"]), Country: str(r["country"]), Date: str(r["date"]), Impact: str(r["impact"]),
        Forecast: str(r["forecast"]), Previous: str(r["previous"]), Actual: str(r["actual"]), URL: str(r["url"]),
      }
      if len(impacts) > 0 && !impacts[strings.ToLower(e.Impact)] {
        continue
      }
      if len(countries) > 0 && !countries[strings.ToUpper(e.Country)] {
        continue
      }
      sum := sha1.Sum([]byte(e.Country + "|" + e.Date + "|" + e.Title))
      e.ID = hex.EncodeToString(sum[:10])
      d.publish(resultEnv("econ-event", e.ID, qDelayed, e, age, stale, err))
    }
    return nil
  })
}

// ---------------------------------------------------------------------------
// S&P 500 membership: iShares Core S&P 500 ETF (IVV) holdings CSV, with the
// Wikipedia constituents table as fallback. One record (endpoint "sp500",
// key "sp500"); symbols in Yahoo form (BRK-B). A list outside
// sp500MinMembers..sp500MaxMembers is rejected rather than published.
// ---------------------------------------------------------------------------

const (
  ivvProductURL   = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf"
  ivvLatestCSVURL = ivvProductURL + "/latest-holdings.csv"
  ivvLegacyCSVURL = ivvProductURL + "/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"
  wikiSP500URL    = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
  sp500MinMembers = 480 // fewer: source broken or truncated
  sp500MaxMembers = 600 // more: not an S&P 500 list
  sp500TTL        = 6 * time.Hour
  utf8BOM         = "\xef\xbb\xbf"
)

type SP500Member struct {
  Symbol      string   `json:"symbol"`
  Name        string   `json:"name,omitempty"`
  Sector      string   `json:"sector,omitempty"`      // GICS sector
  SubIndustry string   `json:"subIndustry,omitempty"` // GICS sub-industry (wikipedia)
  Weight      *float64 `json:"weight,omitempty"`      // % of the fund (ivv)
  Exchange    string   `json:"exchange,omitempty"`    // ivv
  DateAdded   string   `json:"dateAdded,omitempty"`   // wikipedia
  CIK         string   `json:"cik,omitempty"`         // wikipedia
}

type SP500List struct {
  Source          string        `json:"source"` // "ivv" | "wikipedia"
  SourceURL       string        `json:"sourceUrl"`
  AsOf            string        `json:"asOf,omitempty"` // holdings date YYYY-MM-DD (ivv)
  Count           int           `json:"count"`
  Members         []SP500Member `json:"members"`
  FallbackReasons []string      `json:"fallbackReasons,omitempty"` // why earlier sources were skipped
}

// iShares drops the share-class separator of these two.
var sp500TickerFix = map[string]string{"BRKB": "BRK-B", "BFB": "BF-B"}

var sp500SepReplacer = strings.NewReplacer(" ", "-", ".", "-", "/", "-")

// sp500Symbol returns the Yahoo form of a ticker ("BRK.B", "BRK B" -> "BRK-B"),
// "" for cash lines and other non-tickers.
func sp500Symbol(s string) string {
  s = sp500SepReplacer.Replace(strings.ToUpper(strings.TrimSpace(s)))
  if v, ok := sp500TickerFix[s]; ok {
    s = v
  }
  if !symRe.MatchString(s) {
    return ""
  }
  return s
}

// checkSP500 de-duplicates the members and enforces the plausible size.
func checkSP500(l *SP500List) error {
  seen := map[string]bool{}
  out := make([]SP500Member, 0, len(l.Members))
  for _, m := range l.Members {
    if m.Symbol == "" || seen[m.Symbol] {
      continue
    }
    seen[m.Symbol] = true
    out = append(out, m)
  }
  l.Members, l.Count = out, len(out)
  if l.Count < sp500MinMembers || l.Count > sp500MaxMembers {
    return fmt.Errorf("%s: %d members, expected %d..%d (format may have changed)", l.Source, l.Count, sp500MinMembers, sp500MaxMembers)
  }
  return nil
}

func looksHTML(b []byte) bool {
  b = bytes.TrimLeft(bytes.TrimPrefix(b, []byte(utf8BOM)), " \t\r\n")
  return len(b) > 0 && b[0] == '<'
}

var ivvCSVLinkRe = regexp.MustCompile(`(?i)href="([^"]*(?:latest-holdings\.csv|fileType=csv)[^"]*)"`)

// ivvCandidates: holdings CSV links found on the product page (holdings
// links first), then the known URL patterns.
func ivvCandidates(ctx context.Context, f *fetcher) []string {
  var first, rest []string
  seen := map[string]bool{}
  add := func(u string, pri bool) {
    if seen[u] {
      return
    }
    seen[u] = true
    if pri {
      first = append(first, u)
    } else {
      rest = append(rest, u)
    }
  }
  page, err := f.get(ctx, ivvProductURL, nil)
  if err != nil {
    logger.Warn("sp500: iShares product page unavailable", "err", err)
  } else {
    base, _ := url.Parse(ivvProductURL)
    for _, m := range ivvCSVLinkRe.FindAllStringSubmatch(string(page), -1) {
      ref, err := url.Parse(html.UnescapeString(m[1]))
      if err != nil {
        continue
      }
      u := base.ResolveReference(ref).String()
      add(u, strings.Contains(strings.ToLower(u), "holdings"))
    }
  }
  add(ivvLatestCSVURL, false)
  add(ivvLegacyCSVURL, false)
  return append(first, rest...)
}

var ivvDateLayouts = []string{"Jan 02, 2006", "Jan 2, 2006", "01/02/2006", "2006-01-02"}

// parseIVVCSV reads the iShares holdings CSV: a few "key,value" preamble
// lines ("Fund Holdings as of"), the "Ticker,Name,Sector,Asset Class,..."
// header, the holdings, then disclaimer lines. Only equity rows are kept.
func parseIVVCSV(b []byte, src string) (*SP500List, error) {
  if looksHTML(b) {
    return nil, errors.New("got an HTML page instead of the holdings CSV")
  }
  r := csv.NewReader(bytes.NewReader(bytes.TrimPrefix(b, []byte(utf8BOM))))
  r.FieldsPerRecord = -1
  r.LazyQuotes = true
  l := &SP500List{Source: "ivv", SourceURL: src, Members: []SP500Member{}}
  var col map[string]int
  for {
    rec, err := r.Read()
    if err == io.EOF {
      break
    }
    if err != nil {
      if col != nil {
        break // free text after the table
      }
      continue
    }
    if col == nil {
      first := ""
      if len(rec) > 0 {
        first = strings.TrimSpace(strings.TrimPrefix(rec[0], utf8BOM))
      }
      if len(rec) >= 2 && strings.EqualFold(first, "Fund Holdings as of") {
        for _, lay := range ivvDateLayouts {
          if t, err := time.Parse(lay, strings.TrimSpace(rec[1])); err == nil {
            l.AsOf = t.Format("2006-01-02")
            break
          }
        }
      }
      if strings.EqualFold(first, "Ticker") {
        col = map[string]int{}
        for i, h := range rec {
          col[strings.ToLower(strings.TrimSpace(h))] = i
        }
      }
      continue
    }
    if len(rec) < 3 {
      continue // blank / disclaimer line
    }
    get := func(name string) string {
      if i, ok := col[name]; ok && i < len(rec) {
        return strings.TrimSpace(rec[i])
      }
      return ""
    }
    if ac := get("asset class"); ac != "" && !strings.EqualFold(ac, "Equity") {
      continue // cash, futures, money market
    }
    sym := sp500Symbol(get("ticker"))
    if sym == "" {
      continue
    }
    l.Members = append(l.Members, SP500Member{
      Symbol: sym, Name: get("name"), Sector: get("sector"), Exchange: get("exchange"),
      Weight: parseF(get("weight (%)")),
    })
  }
  if col == nil {
    return nil, errors.New("no 'Ticker' header row in the holdings CSV")
  }
  return l, checkSP500(l)
}

func fetchIVV(ctx context.Context, f *fetcher, pinned string) (*SP500List, error) {
  cands := []string{pinned}
  if pinned == "" {
    cands = ivvCandidates(ctx, f)
  }
  var errs []string
  for _, u := range cands {
    b, err := f.get(ctx, u, map[string]string{"Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8"})
    if err == nil {
      var l *SP500List
      if l, err = parseIVVCSV(b, u); err == nil {
        return l, nil
      }
      err = fmt.Errorf("%s: %w", u, err)
    }
    if ctx.Err() != nil {
      return nil, ctx.Err()
    }
    errs = append(errs, err.Error())
  }
  return nil, errors.New(strings.Join(errs, "; "))
}

var (
  wikiTableRe = regexp.MustCompile(`(?is)<table[^>]*\bid="constituents"[^>]*>(.*?)</table>`)
  wikiRowRe   = regexp.MustCompile(`(?is)<tr[^>]*>(.*?)</tr>`)
  wikiCellRe  = regexp.MustCompile(`(?is)<t([hd])\b[^>]*>(.*?)</t[hd]>`)
  supRe       = regexp.MustCompile(`(?is)<sup[^>]*>.*?</sup>`)
)

func wikiCellText(s string) string {
  return strings.Join(strings.Fields(stripHTML(supRe.ReplaceAllString(s, ""))), " ")
}

// parseWikiSP500 reads the "constituents" table (Symbol, Security, GICS
// Sector, GICS Sub-Industry, Headquarters, Date added, CIK, Founded); columns
// are located by their header text.
func parseWikiSP500(page, src string) (*SP500List, error) {
  m := wikiTableRe.FindStringSubmatch(page)
  if m == nil {
    return nil, errors.New("wikipedia: constituents table not found")
  }
  l := &SP500List{Source: "wikipedia", SourceURL: src, Members: []SP500Member{}}
  var col map[string]int
  for _, row := range wikiRowRe.FindAllStringSubmatch(m[1], -1) {
    cells := wikiCellRe.FindAllStringSubmatch(row[1], -1)
    if len(cells) == 0 {
      continue
    }
    if col == nil {
      if strings.EqualFold(cells[0][1], "h") {
        col = map[string]int{}
        for i, c := range cells {
          col[strings.ToLower(wikiCellText(c[2]))] = i
        }
        if _, ok := col["symbol"]; !ok {
          return nil, errors.New("wikipedia: no Symbol column in the constituents table")
        }
      }
      continue
    }
    txt := func(names ...string) string {
      for _, n := range names {
        if i, ok := col[n]; ok && i < len(cells) {
          return wikiCellText(cells[i][2])
        }
      }
      return ""
    }
    sym := sp500Symbol(txt("symbol"))
    if sym == "" {
      continue
    }
    l.Members = append(l.Members, SP500Member{
      Symbol: sym, Name: txt("security"), Sector: txt("gics sector"), SubIndustry: txt("gics sub-industry"),
      DateAdded: txt("date added", "date first added"), CIK: txt("cik"),
    })
  }
  if col == nil {
    return nil, errors.New("wikipedia: no header row in the constituents table")
  }
  return l, checkSP500(l)
}

// fetchSP500: source "auto" tries IVV, then Wikipedia.
func fetchSP500(ctx context.Context, f *fetcher, source, ivvURL string) (*SP500List, error) {
  var reasons []string
  if source != "wikipedia" {
    l, err := fetchIVV(ctx, f, ivvURL)
    if err == nil {
      return l, nil
    }
    if source == "ivv" || ctx.Err() != nil {
      return nil, err
    }
    reasons = append(reasons, "ivv: "+err.Error())
    logger.Warn("sp500: IVV holdings unusable, trying Wikipedia", "err", err)
  }
  var l *SP500List
  b, err := f.get(ctx, wikiSP500URL, nil)
  if err == nil {
    l, err = parseWikiSP500(string(b), wikiSP500URL)
  }
  if err != nil {
    reasons = append(reasons, "wikipedia: "+err.Error())
    return nil, errors.New(strings.Join(reasons, "; "))
  }
  l.FallbackReasons = reasons
  return l, nil
}

func cmdSP500(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("sp500", &o)
  source := fs.String("source", "auto", "auto (IVV, then Wikipedia) | ivv | wikipedia")
  ivvURL := fs.String("ivv-url", envOr(envPrefix+"IVV_URL", ""), "IVV holdings CSV URL; empty = discover on the iShares product page (env "+envPrefix+"IVV_URL)")
  parseArgs(fs, args)
  switch *source {
  case "auto", "ivv", "wikipedia":
  default:
    return fmt.Errorf("sp500: invalid -source %q", *source)
  }
  f := newRef(&o)
  d := newDelta(o.all)
  cache := newMemo()
  return loop(ctx, &o, func(ctx context.Context) error {
    v, age, stale, err := cache.get("sp500", sp500TTL, func() (any, error) {
      return fetchSP500(ctx, f, *source, *ivvURL)
    })
    d.publish(resultEnv("sp500", "sp500", qDelayed, v, age, stale, err))
    return nil
  })
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func clip(s string, n int) string {
  r := []rune(s)
  if len(r) > n {
    return string(r[:n])
  }
  return s
}

func minInt(a, b int) int {
  if a < b {
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
