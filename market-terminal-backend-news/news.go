// qube-news: RSS wire + article collector.
//
// Ports providers/news-provider.ts, /api/rss/news, /api/yfin/news, /api/v2/news
// and /api/yfin/article. Output is a JSONL stream of {data, provenance}
// envelopes on stdout; logs go to stderr.
//
//   go run news.go feed -limit 40
//   go run news.go feed -every 90s                     # stream new headlines
//   go run news.go feed -tickers AAPL,NVDA -q earnings -exclude crypto
//   go run news.go breaking -every 60s                 # high-urgency only
//   go run news.go article https://www.cnbc.com/2026/10/14/some-story.html
//   go run news.go sources
//
// Env: QUBE_NEWS_UA overrides the User-Agent.
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
  "html"
  "io"
  "log/slog"
  "math/rand"
  "net"
  "net/http"
  "net/url"
  "os"
  "os/signal"
  "regexp"
  "sort"
  "strings"
  "sync"
  "syscall"
  "time"
)

const (
  progName   = "qube-news"
  sourceName = "news"
  envPrefix  = "QUBE_NEWS_"
  defaultUA  = "Mozilla/5.0 (compatible; qube-news/1.0)"
)

func main() {
  runMain([]command{
    {"feed", "[-tickers] [-q] [-exclude] [-categories] [-sources] [-limit]", "unified headline feed, one record per article", cmdFeed},
    {"breaking", "[same flags as feed]", "feed restricted to high-urgency items", cmdBreaking},
    {"article", "[-any-host] URL", "readable-text extraction of an article (SSRF-guarded)", cmdArticle},
    {"sources", "", "list configured feeds", cmdSources},
  })
}

// ---------------------------------------------------------------------------
// Feed configuration
// ---------------------------------------------------------------------------

type feedDef struct {
  Source   string `json:"source"`
  Category string `json:"category"`
  URL      string `json:"url"`
}

// Order matters for de-duplication: earlier feeds win (Seeking Alpha first
// because it is ticker-tagged, as in the TS breaking mode).
var feeds = []feedDef{
  {"SeekingAlpha", "markets", "https://seekingalpha.com/market_currents.xml"},
  {"CNBC", "markets", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&Id=100003114"},
  {"CNBC", "earnings", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&Id=15839069"},
  {"CNBC", "economy", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&Id=20910258"},
  {"MarketWatch", "markets", "https://feeds.content.dowjones.io/public/rss/mw_topstories"},
  {"Yahoo Finance", "general", "https://finance.yahoo.com/news/rssindex"},
  {"BBC", "general", "https://feeds.bbci.co.uk/news/business/rss.xml"},
  {"NYT", "general", "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml"},
  {"ZeroHedge", "markets", "https://feeds.feedburner.com/zerohedge/feed"},
}

// Hosts whose articles may be fetched by `article` (suffix match). These are
// the publishers behind the configured feeds.
var articleHosts = []string{
  "seekingalpha.com", "cnbc.com", "marketwatch.com", "wsj.com", "dowjones.io",
  "yahoo.com", "bbc.co.uk", "bbc.com", "nytimes.com", "zerohedge.com",
  "reuters.com", "apnews.com", "barrons.com", "investors.com", "fool.com",
}

func cmdSources(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("sources", &o)
  parseArgs(fs, args)
  for _, f := range feeds {
    emit(envOK("source", f.Source+"/"+f.Category, qLive, f))
  }
  return nil
}

// ---------------------------------------------------------------------------
// RSS / Atom parsing (encoding/xml instead of the TS regexes)
// ---------------------------------------------------------------------------

type rssLink struct {
  Href string `xml:"href,attr"`
  Rel  string `xml:"rel,attr"`
  Text string `xml:",chardata"`
}

type rssCategory struct {
  Domain string `xml:"domain,attr"`
  Term   string `xml:"term,attr"`
  Text   string `xml:",chardata"`
}

type rssItem struct {
  Titles      []string      `xml:"title"`
  Links       []rssLink     `xml:"link"`
  GUID        string        `xml:"guid"`
  ID          string        `xml:"id"`
  PubDate     string        `xml:"pubDate"`
  Date        string        `xml:"date"`
  Published   string        `xml:"published"`
  Updated     string        `xml:"updated"`
  Description []string      `xml:"description"`
  Summary     []string      `xml:"summary"`
  Categories  []rssCategory `xml:"category"`
  Tickers     []string      `xml:"tickers"`
  Ticker      []string      `xml:"ticker"`
}

type rssDoc struct {
  Channel struct {
    Items []rssItem `xml:"item"`
  } `xml:"channel"`
  Items   []rssItem `xml:"item"`  // RSS 1.0 / RDF
  Entries []rssItem `xml:"entry"` // Atom
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
  return in, nil // best effort: treat unknown charsets as UTF-8-compatible
}

// parseFeed decodes RSS 2.0 / RDF / Atom. Note: no xml.HTMLAutoClose here --
// it lists "link", which made the decoder close every RSS <link> before its
// text and then mis-pair </link> with </item>, so no item ever got a URL and
// every feed yielded zero articles. If the XML decoder fails or produces no
// usable item, a regex scan (as in the TS original) is used instead.
func parseFeed(b []byte) ([]rssItem, error) {
  dec := xml.NewDecoder(bytes.NewReader(b))
  dec.Strict = false
  dec.Entity = xml.HTMLEntity
  dec.CharsetReader = charsetReader
  var doc rssDoc
  err := dec.Decode(&doc)
  var items []rssItem
  if err == nil {
    items = append(append(doc.Channel.Items, doc.Items...), doc.Entries...)
  }
  if usable(items) {
    return items, nil
  }
  if fb := regexItems(b); usable(fb) {
    return fb, nil
  }
  if err != nil {
    return nil, fmt.Errorf("%w (body: %s)", err, snippet(b))
  }
  return nil, fmt.Errorf("no usable <item>/<entry> in %d-byte response (body: %s)", len(b), snippet(b))
}

// usable reports whether at least one item has both a title and a link.
func usable(items []rssItem) bool {
  for _, it := range items {
    if firstNonEmpty(it.Titles...) == "" {
      continue
    }
    for _, l := range it.Links {
      if strings.TrimSpace(l.Text) != "" || strings.TrimSpace(l.Href) != "" {
        return true
      }
    }
    if strings.HasPrefix(strings.TrimSpace(it.GUID), "http") {
      return true
    }
  }
  return false
}

func snippet(b []byte) string {
  return clip(strings.TrimSpace(wsRe.ReplaceAllString(string(b), " ")), 120)
}

var (
  rxItemRe  = regexp.MustCompile(`(?is)<(?:[a-z0-9_]+:)?(?:item|entry)(?:\s[^>]*)?>(.*?)</(?:[a-z0-9_]+:)?(?:item|entry)\s*>`)
  rxCDATARe = regexp.MustCompile(`(?s)<!\[CDATA\[(.*?)\]\]>`)
  rxLinkRe  = regexp.MustCompile(`(?is)<(?:[a-z0-9_]+:)?link\b([^>]*?)/?>`)
  rxAttrRe  = regexp.MustCompile(`(?is)\b(href|rel)\s*=\s*["']([^"']*)["']`)
  rxTagMu   sync.Mutex
  rxTagRes  = map[string]*regexp.Regexp{}
)

func rxTag(block, name string) []string {
  rxTagMu.Lock()
  re := rxTagRes[name]
  if re == nil {
    // the open tag must not be self-closing (<atom:link href=".."/>)
    re = regexp.MustCompile(`(?is)<(?:[a-z0-9_]+:)?` + name + `(?:\s[^>]*[^/>])?>(.*?)</(?:[a-z0-9_]+:)?` + name + `\s*>`)
    rxTagRes[name] = re
  }
  rxTagMu.Unlock()
  var out []string
  for _, m := range re.FindAllStringSubmatch(block, -1) {
    out = append(out, strings.TrimSpace(rxCDATARe.ReplaceAllString(m[1], "$1")))
  }
  return out
}

func rxFirst(block, name string) string {
  if v := rxTag(block, name); len(v) > 0 {
    return html.UnescapeString(v[0])
  }
  return ""
}

// regexItems is a tolerant fallback for feeds encoding/xml rejects.
func regexItems(b []byte) []rssItem {
  var out []rssItem
  for _, m := range rxItemRe.FindAllStringSubmatch(string(b), -1) {
    block := m[1]
    var it rssItem
    it.Titles = rxTag(block, "title")
    for _, l := range rxTag(block, "link") {
      it.Links = append(it.Links, rssLink{Text: html.UnescapeString(l)})
    }
    for _, lm := range rxLinkRe.FindAllStringSubmatch(block, -1) {
      var l rssLink
      for _, a := range rxAttrRe.FindAllStringSubmatch(lm[1], -1) {
        if strings.EqualFold(a[1], "href") {
          l.Href = html.UnescapeString(a[2])
        } else {
          l.Rel = a[2]
        }
      }
      if l.Href != "" {
        it.Links = append(it.Links, l)
      }
    }
    it.GUID = rxFirst(block, "guid")
    it.ID = rxFirst(block, "id")
    it.PubDate = rxFirst(block, "pubDate")
    it.Date = rxFirst(block, "date")
    it.Published = rxFirst(block, "published")
    it.Updated = rxFirst(block, "updated")
    it.Description = rxTag(block, "description")
    it.Summary = rxTag(block, "summary")
    it.Tickers = rxTag(block, "tickers")
    it.Ticker = rxTag(block, "ticker")
    for _, c := range rxTag(block, "category") {
      it.Categories = append(it.Categories, rssCategory{Text: html.UnescapeString(c)})
    }
    out = append(out, it)
  }
  return out
}

var (
  tagRe    = regexp.MustCompile(`(?s)<[^>]*>`)
  wsRe     = regexp.MustCompile(`\s+`)
  dateFmts = []string{
    time.RFC1123Z, time.RFC1123, time.RFC3339, time.RFC3339Nano,
    "Mon, 2 Jan 2006 15:04:05 -0700", "Mon, 2 Jan 2006 15:04:05 MST",
    "Mon, 02 Jan 2006 15:04 -0700", "2 Jan 2006 15:04:05 -0700",
    "2006-01-02T15:04:05Z0700", "2006-01-02T15:04:05", "2006-01-02 15:04:05",
  }
)

func cleanText(s string) string {
  s = html.UnescapeString(s)
  s = tagRe.ReplaceAllString(s, " ")
  s = html.UnescapeString(s)
  return strings.TrimSpace(wsRe.ReplaceAllString(s, " "))
}

func parseDate(s string) (time.Time, bool) {
  s = strings.TrimSpace(s)
  if s == "" {
    return time.Time{}, false
  }
  for _, f := range dateFmts {
    if t, err := time.Parse(f, s); err == nil {
      return t.UTC(), true
    }
  }
  return time.Time{}, false
}

func firstNonEmpty(xs ...string) string {
  for _, x := range xs {
    if strings.TrimSpace(x) != "" {
      return strings.TrimSpace(x)
    }
  }
  return ""
}

func clip(s string, n int) string {
  r := []rune(s)
  if len(r) > n {
    return string(r[:n])
  }
  return s
}

// ---------------------------------------------------------------------------
// Tickers, urgency, category
// ---------------------------------------------------------------------------

var (
  feedTickerRe     = regexp.MustCompile(`^[A-Z]{1,6}(?:[:.][A-Z]{1,2})?$`)
  headlineTickerRe = regexp.MustCompile(`\b[A-Z]{2,5}\b`)
  cashtagRe        = regexp.MustCompile(`\$([A-Z]{1,5})\b`)
  exchTickerRe     = regexp.MustCompile(`\((?:NYSE|NASDAQ|Nasdaq|NYSEAMERICAN|AMEX|OTC)\s*:\s*([A-Z][A-Z.]{0,5})\)`)
  nonTickers       = map[string]bool{}
)

func init() {
  for _, w := range strings.Fields(`US UK EU UN IPO CEO CFO COO CTO AI ETF ETFS GDP CPI PPI PCE USA NYSE FOMC AM PM ET EST EDT
    THE AND FOR NEW HOW WHY WHAT WHO NOT BUT ARE WAS HAS CAN ALL OUR ITS YOU SEC FTC DOJ FDA FBI IRS IMF ECB BOE BOJ OPEC
    NATO TV CNBC BBC NYT WSJ API EV EVS LLC INC CO LTD PLC Q1 Q2 Q3 Q4 FY YTD YOY QOQ EPS ESG IT HR PR VS AT OF IN ON TO
    BY UP OR AS IS IF NO SO DO GO WE BE MY ME HE OK II III IV LIVE NEWS UPDATE WATCH BREAKING JUST`) {
    nonTickers[w] = true
  }
}

func headlineTickers(text string) []string {
  seen := map[string]bool{}
  var out []string
  add := func(s string) {
    if !seen[s] && len(out) < 5 {
      seen[s] = true
      out = append(out, s)
    }
  }
  for _, m := range exchTickerRe.FindAllStringSubmatch(text, -1) {
    add(m[1])
  }
  for _, m := range cashtagRe.FindAllStringSubmatch(text, -1) {
    add(m[1])
  }
  for _, c := range headlineTickerRe.FindAllString(text, -1) {
    if !nonTickers[c] {
      add(c)
    }
  }
  return out
}

var (
  highUrgencyRe   = regexp.MustCompile(`(?i)\b(BREAKING|JUST IN|FLASH|ALERT)\b`)
  mediumUrgencyRe = regexp.MustCompile(`(?i)(earnings|fed\b|inflation|rate (cut|hike)|rates|recession|crash|surge|plunge|tumble|soar|halt|bankrupt|default)`)
  catEarningsRe   = regexp.MustCompile(`(?i)\b(earnings|quarterly results|revenue|guidance|EPS|beats|misses)\b`)
  catEconomyRe    = regexp.MustCompile(`(?i)\b(fed|fomc|inflation|cpi|ppi|payrolls|jobs report|unemployment|gdp|treasury yields?|rate (cut|hike))\b`)
  catCryptoRe     = regexp.MustCompile(`(?i)\b(bitcoin|crypto|ethereum|stablecoin|btc)\b`)
  catDealsRe      = regexp.MustCompile(`(?i)\b(acquire|acquisition|merger|buyout|takeover|ipo)\b`)
)

func urgencyOf(title string) string {
  switch {
  case highUrgencyRe.MatchString(title):
    return "high"
  case mediumUrgencyRe.MatchString(title):
    return "medium"
  }
  return "low"
}

func categoryOf(title, feedCategory string) string {
  switch {
  case catEarningsRe.MatchString(title):
    return "earnings"
  case catEconomyRe.MatchString(title):
    return "economy"
  case catCryptoRe.MatchString(title):
    return "crypto"
  case catDealsRe.MatchString(title):
    return "deals"
  }
  return feedCategory
}

// ---------------------------------------------------------------------------
// Unified feed
// ---------------------------------------------------------------------------

type Article struct {
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

func toArticle(f feedDef, it rssItem) (Article, bool) {
  title := cleanText(firstNonEmpty(it.Titles...))
  link := ""
  for _, l := range it.Links {
    if t := strings.TrimSpace(l.Text); t != "" {
      link = t
      break
    }
    if l.Href != "" && (l.Rel == "" || l.Rel == "alternate") {
      link = strings.TrimSpace(l.Href)
      break
    }
  }
  if link == "" && strings.HasPrefix(it.GUID, "http") {
    link = strings.TrimSpace(it.GUID)
  }
  if title == "" || link == "" {
    return Article{}, false
  }
  a := Article{Title: title, URL: link, Source: f.Source, Tickers: []string{}}
  rawDate := firstNonEmpty(it.PubDate, it.Published, it.Date, it.Updated)
  if t, ok := parseDate(rawDate); ok {
    a.published = t
    a.PublishedAt = t.Format(time.RFC3339)
  } else {
    a.PublishedAt = rawDate
  }
  a.Summary = clip(cleanText(firstNonEmpty(append(it.Description, it.Summary...)...)), 320)
  seen := map[string]bool{}
  addT := func(s string) {
    s = strings.ToUpper(strings.TrimSpace(s))
    if feedTickerRe.MatchString(s) && !seen[s] {
      seen[s] = true
      a.Tickers = append(a.Tickers, s)
    }
  }
  for _, c := range it.Categories {
    if strings.Contains(strings.ToLower(c.Domain), "symbol") {
      addT(c.Text)
    }
  }
  for _, t := range append(it.Tickers, it.Ticker...) {
    for _, p := range strings.Split(t, ",") {
      addT(p)
    }
  }
  if len(a.Tickers) > 0 {
    a.TickerSource = "feed"
  } else if ts := headlineTickers(title); len(ts) > 0 {
    a.Tickers, a.TickerSource = ts, "headline"
  }
  a.Urgency = urgencyOf(title)
  a.Category = categoryOf(title, f.Category)
  sum := sha1.Sum([]byte(link))
  a.ID = strings.ToLower(strings.ReplaceAll(f.Source, " ", "")) + ":" + hex.EncodeToString(sum[:8])
  return a, true
}

var titleKeyRe = regexp.MustCompile(`[^a-z0-9]`)

func titleKey(t string) string {
  return clip(titleKeyRe.ReplaceAllString(strings.ToLower(t), ""), 80)
}

type feedFilter struct {
  tickers    map[string]bool
  keyword    string
  exclude    []string
  categories map[string]bool
  sources    map[string]bool
  urgency    map[string]bool
  since      time.Duration
  limit      int
}

func setFrom(list string, upper bool) map[string]bool {
  m := map[string]bool{}
  for _, x := range splitList([]string{list}) {
    if upper {
      x = strings.ToUpper(x)
    } else {
      x = strings.ToLower(x)
    }
    m[x] = true
  }
  return m
}

func (ff *feedFilter) keep(a Article) bool {
  if len(ff.tickers) > 0 && !ff.tickers["ALL"] {
    hit := false
    for _, t := range a.Tickers {
      hit = hit || ff.tickers[t]
    }
    if !hit {
      return false
    }
  }
  lt, ls := strings.ToLower(a.Title), strings.ToLower(a.Summary)
  if ff.keyword != "" && !strings.Contains(lt, ff.keyword) && !strings.Contains(ls, ff.keyword) {
    return false
  }
  for _, x := range ff.exclude {
    if strings.Contains(lt, x) {
      return false
    }
  }
  if len(ff.categories) > 0 && !ff.categories["all"] && !ff.categories[a.Category] {
    return false
  }
  if len(ff.sources) > 0 && !ff.sources["all"] && !ff.sources[strings.ToLower(a.Source)] {
    return false
  }
  if len(ff.urgency) > 0 && !ff.urgency[a.Urgency] {
    return false
  }
  if ff.since > 0 && !a.published.IsZero() && time.Since(a.published) > ff.since {
    return false
  }
  return true
}

func fetchAll(ctx context.Context, f *fetcher, d *deltaFilter) ([]Article, error) {
  type res struct {
    items []Article
    err   error
  }
  out := make([]res, len(feeds))
  var wg sync.WaitGroup
  for i, fd := range feeds {
    wg.Add(1)
    go func(i int, fd feedDef) {
      defer wg.Done()
      b, err := f.get(ctx, fd.URL, nil)
      if err != nil {
        out[i].err = err
        return
      }
      items, err := parseFeed(b)
      if err != nil {
        out[i].err = fmt.Errorf("parse: %w", err)
        return
      }
      for _, it := range items {
        if a, ok := toArticle(fd, it); ok {
          out[i].items = append(out[i].items, a)
        }
      }
    }(i, fd)
  }
  wg.Wait()
  ok := 0
  for _, r := range out {
    if r.err == nil && len(r.items) > 0 {
      ok++
    }
  }
  var all []Article
  var errs []string
  for i, r := range out {
    key := feeds[i].Source + "/" + feeds[i].Category
    if r.err == nil && len(r.items) == 0 {
      r.err = errors.New("feed parsed but contained no usable articles")
    }
    if r.err != nil {
      logger.Warn("feed failed", "feed", key, "err", r.err)
      errs = append(errs, key+": "+r.err.Error())
      env := envFail("feed-status", key, r.err)
      env.soft = ok > 0 // one dead feed must not fail the whole run
      d.publish(env)
      continue
    }
    d.publish(envOK("feed-status", key, qLive, map[string]any{"source": feeds[i].Source, "category": feeds[i].Category, "ok": true, "items": len(r.items)}))
    all = append(all, r.items...)
  }
  if ok == 0 {
    return nil, fmt.Errorf("no articles from any feed: %s", strings.Join(errs, "; "))
  }
  return all, nil
}

func runFeed(ctx context.Context, name string, args []string, defaultUrgency string) error {
  var o opts
  fs := newFlags(name, &o)
  tickers := fs.String("tickers", "", "comma list of tickers to keep (ALL = any)")
  keyword := fs.String("q", "", "keyword that must appear in title or summary")
  exclude := fs.String("exclude", "", "comma list of title keywords to drop")
  categories := fs.String("categories", "", "comma list: markets, earnings, economy, crypto, deals, general")
  sources := fs.String("sources", "", "comma list of sources (see `sources`)")
  urgency := fs.String("urgency", defaultUrgency, "comma list: high, medium, low")
  since := fs.Duration("since", 0, "only items published within this window (e.g. 6h)")
  limit := fs.Int("limit", 40, "max items per poll (newest)")
  parseArgs(fs, args)
  ff := &feedFilter{
    tickers:    setFrom(*tickers, true),
    keyword:    strings.ToLower(strings.TrimSpace(*keyword)),
    categories: setFrom(*categories, false),
    sources:    setFrom(*sources, false),
    urgency:    setFrom(*urgency, false),
    since:      *since,
    limit:      *limit,
  }
  for x := range setFrom(*exclude, false) {
    ff.exclude = append(ff.exclude, x)
  }
  f := newFetcher(&o, nil, 0)
  f.headers["Accept"] = "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"
  f.maxBody = 8 << 20
  d := newDelta(o.all)
  status := newDelta(false) // feed-status records only on change, even with -all
  titles := map[string]string{} // title key -> article id, kept across polls
  return loop(ctx, &o, func(ctx context.Context) error {
    all, err := fetchAll(ctx, f, status)
    if err != nil {
      return err
    }
    var items []Article
    for _, a := range all {
      k := titleKey(a.Title)
      if id, ok := titles[k]; ok && id != a.ID {
        continue // same story from another feed
      }
      titles[k] = a.ID
      if ff.keep(a) {
        items = append(items, a)
      }
    }
    if len(titles) > 50000 {
      titles = map[string]string{}
    }
    sort.SliceStable(items, func(i, j int) bool {
      ti, tj := items[i].published, items[j].published
      if !ti.Equal(tj) {
        return ti.After(tj)
      }
      return items[i].Source < items[j].Source
    })
    if ff.limit > 0 && len(items) > ff.limit {
      items = items[:ff.limit]
    }
    for i := len(items) - 1; i >= 0; i-- { // oldest first for streaming
      d.publish(envOK("article", items[i].ID, qLive, items[i]))
    }
    return nil
  })
}

func cmdFeed(ctx context.Context, args []string) error { return runFeed(ctx, "feed", args, "") }

func cmdBreaking(ctx context.Context, args []string) error {
  return runFeed(ctx, "breaking", args, "high")
}

// ---------------------------------------------------------------------------
// Article extraction with SSRF guard
// ---------------------------------------------------------------------------

func publicIP(ip net.IP) bool {
  if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsMulticast() ||
    ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
    return false
  }
  if v4 := ip.To4(); v4 != nil {
    switch {
    case v4[0] == 0, v4[0] >= 240: // "this network", reserved/broadcast
      return false
    case v4[0] == 100 && v4[1]&0xc0 == 64: // CGNAT 100.64/10
      return false
    case v4[0] == 192 && v4[1] == 0 && v4[2] == 0: // IETF protocol assignments
      return false
    case v4[0] == 198 && (v4[1] == 18 || v4[1] == 19): // benchmarking
      return false
    }
  }
  return true
}

func hostAllowed(host string) bool {
  host = strings.ToLower(strings.TrimSuffix(host, "."))
  for _, h := range articleHosts {
    if host == h || strings.HasSuffix(host, "."+h) {
      return true
    }
  }
  return false
}

func checkArticleURL(u *url.URL, anyHost bool) error {
  if u.Scheme != "http" && u.Scheme != "https" {
    return fmt.Errorf("scheme %q not allowed", u.Scheme)
  }
  if u.User != nil {
    return errors.New("credentials in URL not allowed")
  }
  if p := u.Port(); p != "" && p != "80" && p != "443" {
    return fmt.Errorf("port %s not allowed", p)
  }
  host := u.Hostname()
  if host == "" {
    return errors.New("missing host")
  }
  if ip := net.ParseIP(host); ip != nil && !publicIP(ip) {
    return fmt.Errorf("address %s not allowed", host)
  }
  if !anyHost && !hostAllowed(host) {
    return fmt.Errorf("host %s is not a configured news publisher (use -any-host to override)", host)
  }
  return nil
}

// guardedClient re-checks every redirect and refuses to connect to
// non-public addresses after DNS resolution.
func guardedClient(timeout time.Duration, anyHost bool) *http.Client {
  dialer := &net.Dialer{
    Timeout: 10 * time.Second,
    Control: func(network, address string, _ syscall.RawConn) error {
      host, _, err := net.SplitHostPort(address)
      if err != nil {
        return err
      }
      if !publicIP(net.ParseIP(host)) {
        return fmt.Errorf("blocked connection to non-public address %s", host)
      }
      return nil
    },
  }
  tr := &http.Transport{
    Proxy:                 nil, // never route through a proxy: the IP check must see the target
    DialContext:           dialer.DialContext,
    ForceAttemptHTTP2:     true,
    MaxIdleConns:          10,
    IdleConnTimeout:       30 * time.Second,
    TLSHandshakeTimeout:   10 * time.Second,
    ResponseHeaderTimeout: 15 * time.Second,
  }
  return &http.Client{
    Timeout:   timeout,
    Transport: tr,
    CheckRedirect: func(req *http.Request, via []*http.Request) error {
      if len(via) >= 5 {
        return errors.New("too many redirects")
      }
      return checkArticleURL(req.URL, anyHost)
    },
  }
}

var (
  removeBlockRes = []*regexp.Regexp{
    regexp.MustCompile(`(?is)<script.*?</script\s*>`),
    regexp.MustCompile(`(?is)<style.*?</style\s*>`),
    regexp.MustCompile(`(?is)<nav.*?</nav\s*>`),
    regexp.MustCompile(`(?is)<header.*?</header\s*>`),
    regexp.MustCompile(`(?is)<footer.*?</footer\s*>`),
    regexp.MustCompile(`(?is)<aside.*?</aside\s*>`),
    regexp.MustCompile(`(?is)<svg.*?</svg\s*>`),
  }
  articleRes = []*regexp.Regexp{
    regexp.MustCompile(`(?is)<article.*?</article>`),
    regexp.MustCompile(`(?is)<div[^>]*class="[^"]*article-body[^"]*".*?</div>`),
    regexp.MustCompile(`(?is)<div[^>]*class="[^"]*article-content[^"]*".*?</div>`),
    regexp.MustCompile(`(?is)<div[^>]*class="[^"]*post-content[^"]*".*?</div>`),
    regexp.MustCompile(`(?is)<div[^>]*class="[^"]*entry-content[^"]*".*?</div>`),
    regexp.MustCompile(`(?is)<div[^>]*class="[^"]*story-body[^"]*".*?</div>`),
    regexp.MustCompile(`(?is)<div[^>]*data-testid="article-body"[^>]*>.*?</div>`),
    regexp.MustCompile(`(?is)<div[^>]*id="article-body"[^>]*>.*?</div>`),
    regexp.MustCompile(`(?is)<main.*?</main>`),
  }
  pageTitleRe = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
  brRe        = regexp.MustCompile(`(?i)<br\s*/?>`)
  paraEndRe   = regexp.MustCompile(`(?i)</p>`)
  blockEndRe  = regexp.MustCompile(`(?i)</(?:div|li)>`)
  headingRe   = regexp.MustCompile(`(?i)</?h[1-6][^>]*>`)
  datePubRes  = []*regexp.Regexp{
    regexp.MustCompile(`"datePublished"\s*:\s*"([^"]+)"`),
    regexp.MustCompile(`"publishedAt"\s*:\s*"([^"]+)"`),
    regexp.MustCompile(`<time[^>]*datetime="([^"]+)"`),
    regexp.MustCompile(`<abbr[^>]*class="[^"]*published[^"]*"[^>]*title="([^"]+)"`),
    regexp.MustCompile(`(\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2})`),
  }
  authorRes = []*regexp.Regexp{
    regexp.MustCompile(`"author"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"`),
    regexp.MustCompile(`"author"\s*:\s*\[\s*\{[^}]*"name"\s*:\s*"([^"]+)"`),
    regexp.MustCompile(`(?i)<meta[^>]*name="author"[^>]*content="([^"]+)"`),
  }
)

type ArticleText struct {
  URL        string   `json:"url"`
  Title      string   `json:"title"`
  Paragraphs []string `json:"paragraphs"`
  PubDate    string   `json:"pubDate"`
  Source     string   `json:"source"`
  Author     string   `json:"author"`
}

func extractArticle(page, pageURL string) ArticleText {
  a := ArticleText{URL: pageURL, Paragraphs: []string{}}
  if m := pageTitleRe.FindStringSubmatch(page); m != nil {
    a.Title = cleanText(m[1])
  }
  clean := page
  for _, re := range removeBlockRes {
    clean = re.ReplaceAllString(clean, "")
  }
  body := ""
  for _, re := range articleRes {
    if m := re.FindString(clean); len(m) > 200 {
      body = m
      break
    }
  }
  if body == "" {
    body = clean
  }
  body = brRe.ReplaceAllString(body, "\n")
  body = paraEndRe.ReplaceAllString(body, "\n\n")
  body = blockEndRe.ReplaceAllString(body, "\n")
  body = headingRe.ReplaceAllString(body, "\n\n")
  body = tagRe.ReplaceAllString(body, "")
  body = html.UnescapeString(body)
  for _, line := range strings.Split(body, "\n") {
    line = strings.TrimSpace(wsRe.ReplaceAllString(line, " "))
    if len(line) > 20 {
      a.Paragraphs = append(a.Paragraphs, line)
    }
  }
  // Structured data first (more reliable than the TS order, which matched any
  // timestamp-looking string before datePublished).
  for _, re := range datePubRes {
    if m := re.FindStringSubmatch(page); m != nil {
      a.PubDate = m[1]
      break
    }
  }
  for _, re := range authorRes {
    if m := re.FindStringSubmatch(page); m != nil {
      a.Author = html.UnescapeString(m[1])
      break
    }
  }
  a.Source = "News"
  if u, err := url.Parse(pageURL); err == nil && u.Hostname() != "" {
    a.Source = strings.TrimPrefix(u.Hostname(), "www.")
  }
  return a
}

func cmdArticle(ctx context.Context, args []string) error {
  var o opts
  fs := newFlags("article", &o)
  anyHost := fs.Bool("any-host", false, "allow hosts outside the configured publishers (private/loopback addresses stay blocked)")
  maxBytes := fs.Int64("max-bytes", 5<<20, "maximum page size")
  pos := parseArgs(fs, args)
  if len(pos) != 1 {
    return errors.New("article: exactly one URL required")
  }
  u, err := url.Parse(strings.TrimSpace(pos[0]))
  if err != nil {
    return fmt.Errorf("article: %w", err)
  }
  if err := checkArticleURL(u, *anyHost); err != nil {
    return fmt.Errorf("article: %w", err)
  }
  f := newFetcher(&o, nil, 0)
  f.client = guardedClient(o.timeout, *anyHost)
  f.maxBody = *maxBytes
  f.retries = 1
  f.headers["Accept"] = "text/html,application/xhtml+xml"
  d := newDelta(o.all)
  return loop(ctx, &o, func(ctx context.Context) error {
    b, err := f.get(ctx, u.String(), nil)
    if err != nil {
      d.publish(envFail("article-text", u.String(), fmt.Errorf("failed to fetch article: %w", err)))
      return nil
    }
    d.publish(envOK("article-text", u.String(), qLive, extractArticle(string(b), u.String())))
    return nil
  })
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
  soft       bool       // UNAVAILABLE but not counted toward the exit status
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
  if env.Provenance.Quality == qUnavailable && !env.soft {
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
