// QUBE Terminal: single-file Qt5 desktop client (no MOC) for the QUBE terminal
// backend (market-terminal-backend, default http://localhost:4032).
//
// Layout (the backend's "DENSE" preset):
//
//   +-------------------+---------------------------+------------------+
//   | MONITOR           | DES / GP (header + chart) | FA  FUNDAMENTALS |
//   | quotes + sparks   |                           |                  |
//   +-------------------+---------------------------+------------------+
//   | N  NEWS           | [EARN | ECO] tabs         | ANR ANALYST RECS |
//   +-------------------+---------------------------+------------------+
//
// Data flow:
//   - GET /api/v2/dense?symbol&watch&range  (NDJSON) fills every panel as the
//     parts arrive ("watchlist", "chart", "spark:SYM", "news", "econ",
//     "fundamentals", "analyst", "earnings"). Refreshed periodically.
//   - GET /api/yfin/watchlist?symbols=       polled for live quotes.
//   - GET /api/yfin/chart/{sym}?range&interval      chart range changes.
//   - GET /api/v2/news, /api/v2/statements, /api/yfin/economic-calendar
//     for the per-panel controls (news filter, statement type/period, week).
//   - GET /healthz                           backend status in the status bar.
//
// EXTRA_DATA_PANELS adds (everything from the backend's free collectors):
//   - symbol tabs next to EARN / ECO: FIL (SEC filings + document reader),
//     INS (Form 4), HDS (13F filers), OMON (option chain), TRAN (transcripts)
//   - workspace tabs (Ctrl+1..5): DENSE, MKT (world indices, FX / commodities,
//     Treasury curve, VIX term structure, sector rotation, breadth), MOV
//     (movers, trending, halts, IPOs), SOC (Fear & Greed, Reddit, congress
//     trades), OWN (13F holdings, market-wide Form 4 feed)
//   - ticker search in the command line, news reader (double-click), Fear &
//     Greed in the status bar
//
// Usage: build/bin/qt-market-terminal [http://host:port]   (or env QUBE_TERMINAL_URL). Type a ticker in
// the command line and press <Enter>. F5 refreshes, Ctrl+L focuses the
// command line, double-click a news headline to open it, click a monitor row
// to load that security.
//
// Build: needs QtNetwork in addition to QtWidgets / QtGui / QtCore
// (-framework QtNetwork -I$QT/include/QtNetwork -DQT_NETWORK_LIB). C++11.

// Light theme: LIGHT toggle in the command bar (Ctrl+T), persisted in settings.
#define LIGHT_THEME
// macOS: use the standard system UI font (SF) at its native size for the GUI
// and the standard fixed-pitch font (Menlo / SF Mono) only for data tables.
#define MACOS_STANDARD_FONTS
// The remaining free data sources of the backend: symbol tabs FIL / INS
// / HDS / OMON / TRAN, workspace pages MKT / MOV / SOC / OWN, ticker search,
// news + filing readers and the Fear & Greed status indicator.
#define EXTRA_DATA_PANELS
// Status bar progress of the backend's ticker universe: how many S&P 500 (then
// all) tickers already have a Yahoo sector / industry profile, EDGAR state,
// rate-limit pauses (GET /api/v2/universe/status).
#define UNIVERSE_PROGRESS
// Keep the main window within the available area of its screen (default
// size, restored geometry and the minimum size of the widgets).
#define FIT_TO_SCREEN
// S&P 500 heatmap: HEATMAP in the command bar (Ctrl+Shift+H) swaps the
// workspace for a treemap (sector -> industry group -> ticker) colored by the
// day's % change, tiles sized by market cap or by the size of the move.
// Members + sectors from GET /api/v2/universe?index=sp500, quotes from
// GET /api/v2/quote-batch (100 per call). Wheel / pinch zooms, drag (or a
// two-finger trackpad scroll) pans, double-click opens the ticker.
#define HEATMAP

#include <QApplication>
#include <QMainWindow>
#include <QWidget>
#include <QFrame>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QComboBox>
#include <QSplitter>
#include <QTabWidget>
#include <QTableWidget>
#include <QTableWidgetItem>
#include <QHeaderView>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QStatusBar>
#include <QShortcut>
#include <QStyleFactory>
#include <QToolTip>
#ifdef EXTRA_DATA_PANELS
#include <QDialog>
#include <QTextBrowser>
#include <QCompleter>
#include <QStringListModel>
#include <QAbstractItemView>
#include <QtCore/QSignalBlocker>
#include <QtCore/QMap>
#endif
#ifdef UNIVERSE_PROGRESS
#include <QProgressBar>
#include <algorithm>
#endif
#ifdef HEATMAP
#include <QStackedWidget>
#include <QProgressBar>
#include <QtGui/QWheelEvent>
#include <QtGui/QCursor>
#include <algorithm>
#include <limits>
#endif

#include <QtGui/QPainter>
#include <QtGui/QPainterPath>
#include <QtGui/QLinearGradient>
#include <QtGui/QPaintEvent>
#include <QtGui/QMouseEvent>
#include <QtGui/QCloseEvent>
#include <QtGui/QFont>
#include <QtGui/QFontDatabase>
#include <QtGui/QFontMetrics>
#include <QtGui/QPixmap>
#include <QtGui/QColor>
#include <QtGui/QDesktopServices>
#include <QtGui/QKeySequence>
#ifdef FIT_TO_SCREEN
#include <QtGui/QScreen>
#include <QtGui/QGuiApplication>
#include <QSizePolicy>
#endif

#include <QtCore/QString>
#include <QtCore/QStringList>
#include <QtCore/QVector>
#include <QtCore/QHash>
#include <QtCore/QSet>
#include <QtCore/QTimer>
#include <QtCore/QPointer>
#include <QtCore/QDateTime>
#include <QtCore/QLocale>
#include <QtCore/QUrl>
#include <QtCore/QUrlQuery>
#include <QtCore/QJsonDocument>
#include <QtCore/QJsonObject>
#include <QtCore/QJsonArray>
#include <QtCore/QJsonValue>
#include <QtCore/QJsonParseError>
#include <QtCore/QSaveFile>
#include <QtCore/QFile>
#include <QtCore/QFileInfo>
#include <QtCore/QDir>
#include <QtCore/QRegExp>
#include <QtCore/QVariant>
#include <QtCore/QStandardPaths>
#include <QtCore/QByteArray>

#include <QtNetwork/QNetworkAccessManager>
#include <QtNetwork/QNetworkRequest>
#include <QtNetwork/QNetworkReply>

#include "paths.h"

#include <cmath>
#include <functional>
#include <memory>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

static const char *APP_NAME = "qube-terminal";
static const char *WINDOW_TITLE = "QUBE Terminal";
static const char *SETTINGS_FILE = "settings.json";
static const char *ENV_BASE_URL = "QUBE_TERMINAL_URL";
static const char *DEFAULT_BASE_URL = "http://localhost:4032";
static const char *DEFAULT_SYMBOL = "AAPL";
static const char *DEFAULT_WATCH =
    "SPY,QQQ,DIA,IWM,^VIX,AAPL,MSFT,NVDA,AMZN,GOOGL";
static const char *DEFAULT_RANGE = "1y";
static const char *DENSE_INTERVAL = "1d";  // interval of the dense "chart" part
static const char *NOT_AVAILABLE = "--";
static const char *NA_STATUS = "N/A";
static const char *LOADING_TEXT = "Loading\xe2\x80\xa6";
static const double PCT_SCALE = 100.0;

// Backend routes.
static const char *ROUTE_DENSE = "/api/v2/dense";
static const char *ROUTE_WATCHLIST = "/api/yfin/watchlist";
static const char *ROUTE_CHART = "/api/yfin/chart/";
static const char *ROUTE_NEWS = "/api/v2/news";
static const char *ROUTE_STATEMENTS = "/api/v2/statements";
static const char *ROUTE_ECON = "/api/yfin/economic-calendar";
static const char *ROUTE_HEALTH = "/healthz";
#ifdef UNIVERSE_PROGRESS
static const char *ROUTE_UNIVERSE_STATUS = "/api/v2/universe/status";
static const int UNIVERSE_POLL_MS = 10000;
static const int UNIVERSE_BAR_W = 260;
static const int UNIVERSE_BAR_H = 16;
static const int SECS_PER_MIN = 60;
static const int SECS_PER_HOUR = 3600;
static const char *UNIVERSE_LABEL_SP500 = "S&P 500 sectors";
static const char *UNIVERSE_LABEL_ALL = "All tickers sectors";
#endif
#ifdef HEATMAP
static const char *ROUTE_UNIVERSE = "/api/v2/universe";
static const char *ROUTE_QUOTE_BATCH = "/api/v2/quote-batch";
static const char *HEATMAP_BTN_TEXT = "HEATMAP";
static const char *HEATMAP_SHORTCUT = "Ctrl+Shift+H";
static const char *HEATMAP_TITLE = "HMAP  S&P 500 HEATMAP";
static const char *SETTINGS_HEATMAP_SIZE_KEY = "heatmapSize";
static const char *HM_SIZE_CAP_KEY = "cap";
static const char *HM_SIZE_MOVE_KEY = "move";
static const int HM_UNIVERSE_LIMIT = 1000;      // more than the member count
static const int HM_QUOTE_CHUNK = 100;          // symbols per quote-batch call
static const int HM_REFRESH_MS = 60000;         // quote refresh while shown
static const double HM_ZOOM_MAX = 40.0;
static const double HM_WHEEL_ZOOM_BASE = 1.0015;  // per 1/8 degree of wheel
static const double HM_KEY_ZOOM = 1.25;           // +/- keys
static const int HM_DRAG_THRESHOLD_PX = 4;
static const double HM_COLOR_RANGE_PCT = 3.0;     // full red / green at +-3%
static const double HM_MIN_WEIGHT_FRAC = 0.002;   // smallest tile vs largest
static const double HM_SECTOR_GAP = 3.0;          // layout px (zoom 1)
static const double HM_GROUP_GAP = 1.5;
static const double HM_SECTOR_HDR_H = 16.0;
static const double HM_GROUP_HDR_H = 12.0;
static const double HM_HDR_MIN_ROWS = 3.0;        // header only if box >= 3x
static const double HM_TILE_GAP_PX = 1.0;         // screen px between tiles
static const int HM_HDR_FONT_MIN_PX = 8;
static const int HM_HDR_FONT_MAX_PX = 26;
static const double HM_HDR_FONT_FRAC = 0.75;      // of the header strip
static const int HM_TILE_FONT_MIN_PX = 8;
static const int HM_TILE_FONT_MAX_PX = 56;
static const double HM_TICKER_H_FRAC = 0.34;      // ticker font vs tile height
static const double HM_CHAR_W_FRAC = 0.62;        // mono glyph width / px
static const int HM_TICKER_MIN_CHARS = 3;
static const double HM_PCT_FONT_FRAC = 0.62;      // % line vs ticker font
static const double HM_NAME_FONT_FRAC = 0.4;      // name line vs ticker font
static const double HM_TWO_LINE_FRAC = 2.3;       // tile h / ticker px
static const double HM_THREE_LINE_FRAC = 3.8;
static const double HM_LINE_SPACING = 1.2;
static const int HM_PCT_DECIMALS = 1;
static const int HM_HOVER_PEN_W = 2;
static const int HM_LEGEND_BOX_W = 34;
static const int HM_LEGEND_H = 16;
static const int HM_LEGEND_STEPS = 7;             // -3 .. +3 %
static const int HM_LEGEND_FONT_PX = 10;
static const int HM_PROGRESS_W = 170;
static const int HM_PROGRESS_H = 16;
static const QColor HM_NEUTRAL(62, 64, 74);
static const QColor HM_UP(46, 156, 94);
static const QColor HM_DOWN(222, 82, 82);
static const QColor HM_NODATA(44, 44, 50);
static const QColor HM_TILE_TEXT(244, 244, 244);
#endif

// Quality markers emitted by the collectors.
static const char *Q_UNAVAILABLE = "UNAVAILABLE";
static const char *Q_STALE = "STALE";

// Timing (ms).
static const int QUOTE_POLL_MS = 5000;
static const int DENSE_REFRESH_MS = 120000;
static const int DENSE_RETRY_MS = 10000;
static const int HEALTH_POLL_MS = 10000;
static const int CLOCK_TICK_MS = 1000;
static const int REQUEST_TIMEOUT_MS = 90000;
static const int SETTINGS_SAVE_DEBOUNCE_MS = 400;
static const int FLASH_MS = 600;

// Limits.
static const int MAX_WATCH = 40;
static const int NEWS_LIMIT = 40;
static const int STATEMENT_LIMIT = 8;
static const int MAX_SYMBOL_LEN = 20;
static const int ERROR_ELIDE_CHARS = 60;

// Window / layout.
static const int WINDOW_W = 1680;
static const int WINDOW_H = 980;
#ifdef FIT_TO_SCREEN
static const int FRAME_ALLOWANCE_W = 16;  // window frame before it is known
static const int FRAME_ALLOWANCE_H = 40;  // (title bar + borders)
static const int EDIT_SHRINK_DIV = 3;     // command-line edits may shrink to 1/3
#endif
static const int FONT_PT = 11;
static const int TICKER_FONT_PT = 16;
static const int HEADER_PRICE_PT = 20;
static const int ROW_H = 20;
static const int PANEL_PAD = 4;
static const int PANEL_SPACING = 4;
static const int SPLITTER_HANDLE = 2;
static const int CMD_EDIT_W = 260;
static const int WATCH_EDIT_W = 420;
static const int BASE_EDIT_W = 200;
static const int NEWS_FILTER_W = 140;
static const int RANGE_BTN_W = 38;

// Splitter initial sizes.
static const int SPLIT_LEFT_W = 560;
static const int SPLIT_CENTER_W = 760;
static const int SPLIT_RIGHT_W = 360;
static const int SPLIT_MONITOR_H = 430;
static const int SPLIT_NEWS_H = 520;
static const int SPLIT_CHART_H = 580;
static const int SPLIT_TABS_H = 370;
static const int SPLIT_FUND_H = 470;
static const int SPLIT_ANALYST_H = 480;

// Sparklines (monitor TREND column).
static const int SPARK_W = 90;
static const int SPARK_H = 16;
static const int SPARK_PAD = 1;

// Chart.
static const int CHART_MIN_H = 220;
static const int CHART_PAD = 8;
static const int CHART_AXIS_W = 74;
static const int CHART_AXIS_H = 20;
static const int CHART_VOL_GAP = 6;
static const int CHART_LABEL_W = 110;
static const int CHART_Y_TICKS = 6;
static const int CHART_X_TICKS = 6;
static const int CHART_FILL_ALPHA = 70;
static const int CHART_VOL_ALPHA = 110;
static const double CHART_VOL_FRAC = 0.2;
static const double CHART_Y_PAD_FRAC = 0.05;
static const double CANDLE_BODY_FRAC = 0.6;
static const double MS_PER_SEC = 1000.0;
static const double MS_TIMESTAMP_MIN = 1e12;  // larger timestamps are already ms

// Analyst distribution bar.
static const int DIST_SLOTS = 5;
static const int DIST_BAR_H = 18;

// Dividend yields above this are already percentages (otherwise fractions).
static const double YIELD_PCT_THRESHOLD = 1.0;

// Colors (the initializers are the dark palette).
#ifdef LIGHT_THEME
// Mutable: applyPalette() swaps them between the dark and light palettes.
#define THEME_COLOR static QColor
#else
#define THEME_COLOR static const QColor
#endif
THEME_COLOR COL_BG(0, 0, 0);
THEME_COLOR COL_PANEL(10, 10, 12);
THEME_COLOR COL_ALT(18, 18, 22);
THEME_COLOR COL_BAR(48, 26, 0);
THEME_COLOR COL_INPUT(17, 17, 17);
THEME_COLOR COL_BUTTON(30, 30, 30);
THEME_COLOR COL_SELECT(50, 50, 72);
THEME_COLOR COL_GRID(40, 40, 44);
THEME_COLOR COL_SCROLL(60, 60, 64);
THEME_COLOR COL_AMBER(255, 160, 40);
THEME_COLOR COL_TEXT(225, 225, 225);
THEME_COLOR COL_DIM(130, 130, 130);
THEME_COLOR COL_UP(0, 210, 106);
THEME_COLOR COL_DOWN(255, 77, 77);
THEME_COLOR COL_WARN(255, 170, 0);
THEME_COLOR COL_HOLD(220, 200, 60);
THEME_COLOR COL_BUY_LIGHT(130, 220, 150);
THEME_COLOR COL_SELL_LIGHT(255, 140, 90);
THEME_COLOR COL_LINK(90, 170, 255);
THEME_COLOR COL_CROSS(160, 160, 160);
THEME_COLOR COL_FLASH_UP(0, 90, 45);
THEME_COLOR COL_FLASH_DOWN(110, 20, 20);
THEME_COLOR COL_BLACK(0, 0, 0);

THEME_COLOR DIST_COLORS[DIST_SLOTS] = {
    COL_UP, COL_BUY_LIGHT, COL_HOLD, COL_SELL_LIGHT, COL_DOWN};
static const char *DIST_KEYS[DIST_SLOTS] = {"strongBuy", "buy", "hold", "sell",
                                            "strongSell"};
static const char *DIST_SHORT[DIST_SLOTS] = {"SB", "B", "H", "S", "SS"};

#ifdef LIGHT_THEME
static const char *SETTINGS_THEME_KEY = "theme";
static const char *THEME_DARK = "dark";
static const char *THEME_LIGHT = "light";
static const char *THEME_BTN_TEXT = "LIGHT";
static const char *THEME_SHORTCUT = "Ctrl+T";

struct ThemePalette {
  QColor bg, panel, alt, bar, input, button, select, grid, scroll, amber, text,
      dim, up, down, warn, hold, buyLight, sellLight, link, cross, flashUp,
      flashDown, onColor;
};

static ThemePalette currentPalette() {
  ThemePalette p = {COL_BG,     COL_PANEL,     COL_ALT,       COL_BAR,
                    COL_INPUT,  COL_BUTTON,    COL_SELECT,    COL_GRID,
                    COL_SCROLL, COL_AMBER,     COL_TEXT,      COL_DIM,
                    COL_UP,     COL_DOWN,      COL_WARN,      COL_HOLD,
                    COL_BUY_LIGHT, COL_SELL_LIGHT, COL_LINK,  COL_CROSS,
                    COL_FLASH_UP, COL_FLASH_DOWN, COL_BLACK};
  return p;
}

// Captured from the initializers above (same TU => initialized after them).
static const ThemePalette DARK_PALETTE = currentPalette();

// Paper-white panels, darker amber / green / red for contrast on white.
static const ThemePalette LIGHT_PALETTE = {
    QColor(236, 236, 232),  // bg
    QColor(255, 255, 255),  // panel
    QColor(244, 245, 248),  // alt
    QColor(255, 226, 184),  // bar
    QColor(255, 255, 255),  // input
    QColor(232, 232, 230),  // button
    QColor(196, 214, 242),  // select
    QColor(200, 200, 206),  // grid
    QColor(176, 176, 182),  // scroll
    QColor(178, 92, 0),     // amber
    QColor(22, 22, 26),     // text
    QColor(108, 108, 112),  // dim
    QColor(0, 145, 72),     // up
    QColor(214, 40, 40),    // down
    QColor(196, 112, 0),    // warn
    QColor(150, 128, 0),    // hold
    QColor(80, 180, 110),   // buyLight
    QColor(232, 120, 60),   // sellLight
    QColor(0, 92, 200),     // link
    QColor(110, 110, 116),  // cross
    QColor(186, 238, 204),  // flashUp
    QColor(250, 200, 200),  // flashDown
    QColor(0, 0, 0),        // onColor (text on colored fills)
};

static void applyPalette(const ThemePalette &p) {
  COL_BG = p.bg;
  COL_PANEL = p.panel;
  COL_ALT = p.alt;
  COL_BAR = p.bar;
  COL_INPUT = p.input;
  COL_BUTTON = p.button;
  COL_SELECT = p.select;
  COL_GRID = p.grid;
  COL_SCROLL = p.scroll;
  COL_AMBER = p.amber;
  COL_TEXT = p.text;
  COL_DIM = p.dim;
  COL_UP = p.up;
  COL_DOWN = p.down;
  COL_WARN = p.warn;
  COL_HOLD = p.hold;
  COL_BUY_LIGHT = p.buyLight;
  COL_SELL_LIGHT = p.sellLight;
  COL_LINK = p.link;
  COL_CROSS = p.cross;
  COL_FLASH_UP = p.flashUp;
  COL_FLASH_DOWN = p.flashDown;
  COL_BLACK = p.onColor;
  const QColor dist[DIST_SLOTS] = {COL_UP, COL_BUY_LIGHT, COL_HOLD,
                                   COL_SELL_LIGHT, COL_DOWN};
  for (int i = 0; i < DIST_SLOTS; ++i) DIST_COLORS[i] = dist[i];
}
#endif

// Chart ranges and the interval used for each.
struct RangeSpec {
  const char *range;
  const char *interval;
  const char *label;
};
static const RangeSpec RANGES[] = {
    {"1d", "5m", "1D"},    {"5d", "15m", "5D"},   {"1mo", "60m", "1M"},
    {"3mo", "1d", "3M"},   {"6mo", "1d", "6M"},   {"ytd", "1d", "YTD"},
    {"1y", "1d", "1Y"},    {"2y", "1wk", "2Y"},   {"5y", "1wk", "5Y"},
    {"10y", "1mo", "10Y"}, {"max", "1mo", "MAX"},
};
static const int RANGE_COUNT = int(sizeof(RANGES) / sizeof(RANGES[0]));

static QString intervalFor(const QString &rng) {
  for (int i = 0; i < RANGE_COUNT; ++i) {
    if (rng == QLatin1String(RANGES[i].range)) return RANGES[i].interval;
  }
  return DENSE_INTERVAL;
}

static bool validRange(const QString &rng) {
  for (int i = 0; i < RANGE_COUNT; ++i) {
    if (rng == QLatin1String(RANGES[i].range)) return true;
  }
  return false;
}

// Global dark "terminal" style sheet; {tokens} are replaced by the colors.
static QString appStyleSheet() {
  QString s = QStringLiteral(
      "QWidget { background-color: {bg}; color: {text}; }"
      "QFrame#panel { border: 1px solid {grid}; background-color: {panel}; }"
      "QWidget#panelBar { background-color: {bar}; }"
      "QWidget#panelBody { background-color: {panel}; }"
      "QLabel#panelTitle { color: {amber}; font-weight: bold;"
      "  background: transparent; }"
      "QLabel#panelStatus { color: {dim}; background: transparent; }"
      "QLabel#logo { color: {bg}; background-color: {amber};"
      "  font-weight: bold; padding: 2px 8px; }"
      "QLabel#dim { color: {dim}; }"
      "QTableWidget { background-color: {panel};"
      "  alternate-background-color: {alt}; gridline-color: {grid};"
      "  selection-background-color: {select}; selection-color: {text};"
      "  border: none; }"
      "QHeaderView { background-color: {panel}; }"
      "QHeaderView::section { background-color: {panel}; color: {amber};"
      "  border: none; border-bottom: 1px solid {grid}; padding: 1px 4px; }"
      "QLineEdit { background-color: {input}; border: 1px solid {grid};"
      "  color: {amber}; padding: 2px 4px;"
      "  selection-background-color: {select}; }"
      "QLineEdit:focus { border-color: {amber}; }"
      "QPushButton { background-color: {button}; border: 1px solid {grid};"
      "  color: {text}; padding: 2px 8px; }"
      "QPushButton:hover { border-color: {amber}; }"
      "QPushButton:checked { background-color: {amber}; color: {bg}; }"
      "QComboBox { background-color: {button}; border: 1px solid {grid};"
      "  color: {text}; padding: 1px 6px; }"
      "QComboBox QAbstractItemView { background-color: {input};"
      "  color: {text}; selection-background-color: {select}; }"
      "QTabWidget::pane { border: none; }"
      "QTabBar::tab { background-color: {button}; color: {dim};"
      "  padding: 3px 12px; border: 1px solid {grid}; }"
      "QTabBar::tab:selected { background-color: {bar}; color: {amber}; }"
      "QSplitter::handle { background-color: {grid}; }"
      "QStatusBar { background-color: {input}; color: {dim}; }"
      "QStatusBar QLabel { background: transparent; }"
      "QScrollBar:vertical { background: {bg}; width: 10px; margin: 0; }"
      "QScrollBar:horizontal { background: {bg}; height: 10px; margin: 0; }"
      "QScrollBar::handle { background: {scroll}; min-height: 20px;"
      "  min-width: 20px; }"
      "QScrollBar::add-line, QScrollBar::sub-line { width: 0; height: 0; }"
      "QToolTip { background-color: {alt}; color: {text};"
      "  border: 1px solid {amber}; }");
  s.replace("{bg}", COL_BG.name());
  s.replace("{panel}", COL_PANEL.name());
  s.replace("{alt}", COL_ALT.name());
  s.replace("{bar}", COL_BAR.name());
  s.replace("{input}", COL_INPUT.name());
  s.replace("{button}", COL_BUTTON.name());
  s.replace("{select}", COL_SELECT.name());
  s.replace("{grid}", COL_GRID.name());
  s.replace("{scroll}", COL_SCROLL.name());
  s.replace("{amber}", COL_AMBER.name());
  s.replace("{text}", COL_TEXT.name());
  s.replace("{dim}", COL_DIM.name());
  return s;
}

// ---------------------------------------------------------------------------
// Formatting / JSON helpers
// ---------------------------------------------------------------------------

static const Qt::Alignment ALIGN_TEXT(Qt::AlignLeft | Qt::AlignVCenter);
static const Qt::Alignment ALIGN_NUM(Qt::AlignRight | Qt::AlignVCenter);

// Reads a JSON number (null / missing / non-number -> false).
static bool jnum(const QJsonValue &v, double &out) {
  if (!v.isDouble()) return false;
  out = v.toDouble();
  return std::isfinite(out);
}

static QString jstr(const QJsonObject &o, const char *key) {
  return o.value(QLatin1String(key)).toString();
}

static QVector<double> jarr(const QJsonValue &v) {
  QVector<double> out;
  const QJsonArray a = v.toArray();
  out.reserve(a.size());
  for (const QJsonValue &x : a) out.append(x.toDouble());
  return out;
}

static const QLocale &enLocale() {
  static const QLocale loc(QLocale::English, QLocale::UnitedStates);
  return loc;
}

static QString fmtNum(double v, int decimals) {
  return enLocale().toString(v, 'f', decimals);
}

static QString fmtPrice(double v) {
  const int smallDecimals = 4, normalDecimals = 2;
  return fmtNum(v, std::fabs(v) < 1.0 ? smallDecimals : normalDecimals);
}

// 1234567 -> "1.23M" etc.
static QString fmtBig(double v) {
  struct Unit { double scale; const char *suffix; };
  static const Unit UNITS[] = {
      {1e12, "T"}, {1e9, "B"}, {1e6, "M"}, {1e3, "K"}};
  const double a = std::fabs(v);
  for (const Unit &u : UNITS) {
    if (a >= u.scale) return fmtNum(v / u.scale, 2) + u.suffix;
  }
  return fmtNum(v, 0);
}

static QString fmtSigned(double v, int decimals) {
  return (v > 0 ? QStringLiteral("+") : QString()) + fmtNum(v, decimals);
}

static QString fmtPctSigned(double pct) { return fmtSigned(pct, 2) + "%"; }

static QColor changeColor(double v) {
  if (v > 0) return COL_UP;
  if (v < 0) return COL_DOWN;
  return COL_TEXT;
}

static QColor qualityColor(const QString &q) {
  if (q.isEmpty()) return COL_DIM;
  if (q == QLatin1String(Q_UNAVAILABLE)) return COL_DOWN;
  if (q == QLatin1String(Q_STALE)) return COL_WARN;
  return COL_UP;
}

// Buy-ish ratings green, sell-ish red, hold yellow.
static QColor ratingColor(const QString &rating) {
  const QString r = rating.toLower();
  if (r.contains("sell") || r.contains("under")) return COL_DOWN;
  if (r.contains("buy") || r.contains("outperform") || r.contains("overweight"))
    return COL_UP;
  if (r.contains("hold") || r.contains("neutral") || r.contains("equal"))
    return COL_HOLD;
  return COL_TEXT;
}

static QDateTime parseIso(const QString &s) {
  QDateTime dt = QDateTime::fromString(s, Qt::ISODate);
  return dt.isValid() ? dt.toLocalTime() : dt;
}

// "14:03" for today, "10/14 14:03" otherwise.
static QString fmtWhen(const QString &iso) {
  const QDateTime dt = parseIso(iso);
  if (!dt.isValid()) return iso;
  if (dt.date() == QDate::currentDate()) return dt.toString("HH:mm");
  return dt.toString("MM/dd HH:mm");
}

static QString elide(const QString &s) {
  if (s.size() <= ERROR_ELIDE_CHARS) return s;
  return s.left(ERROR_ELIDE_CHARS - 1) + QChar(0x2026);
}

static void setLabelColor(QLabel *l, const QColor &c) {
  l->setStyleSheet(QString("color: %1; background: transparent;").arg(c.name()));
}

// Upper-cases and validates a ticker typed by the user ("" when invalid).
static QString normSymbol(const QString &raw) {
  const QString s = raw.trimmed().toUpper();
  if (s.isEmpty() || s.size() > MAX_SYMBOL_LEN) return QString();
  for (const QChar &c : s) {
    if (!(c.isLetterOrNumber() || c == '.' || c == '^' || c == '=' ||
          c == '-')) {
      return QString();
    }
  }
  return s;
}

// "spy, qqq  dia" -> ["SPY","QQQ","DIA"] (unique, valid, capped).
static QStringList parseSymbolList(const QString &text) {
  QStringList out;
  const QStringList parts =
      text.split(QRegExp("[,\\s]+"), QString::SkipEmptyParts);
  for (const QString &p : parts) {
    const QString s = normSymbol(p);
    if (!s.isEmpty() && !out.contains(s)) out << s;
    if (out.size() == MAX_WATCH) break;
  }
  return out;
}

// Error text of a backend body ("" when the call succeeded).
static QString bodyError(int status, const QJsonObject &o, const QString &err) {
  const int httpOk = 200;
  if (status == httpOk && err.isEmpty()) return QString();
  QString msg = o.value("error").toString();
  if (msg.isEmpty()) msg = err;
  if (msg.isEmpty()) msg = QString("HTTP %1").arg(status);
  return msg;
}

// Panel status label from a TS-style provenance object.
static void setProvenance(QLabel *l, const QJsonObject &prov,
                          const QString &prefix = QString()) {
  const QString q = prov.value("quality").toString();
  QStringList parts;
  if (!prefix.isEmpty()) parts << prefix;
  const QString src = prov.value("sourceProvider").toString();
  if (!src.isEmpty()) parts << src.toUpper();
  if (!q.isEmpty()) parts << q;
  const QDateTime t = parseIso(prov.value("retrievalTimestamp").toString());
  if (t.isValid()) parts << t.toString("HH:mm:ss");
  l->setText(parts.join(QStringLiteral("  ")));
  setLabelColor(l, qualityColor(q));
  QStringList tip;
  tip << "source: " + prov.value("source").toString()
      << "endpoint: " + prov.value("endpoint").toString()
      << "quality: " + q
      << QString("cache age: %1 ms")
             .arg(qint64(prov.value("cacheAgeMs").toDouble()));
  const QString err = prov.value("error").toString();
  if (!err.isEmpty()) tip << "error: " + err;
  l->setToolTip(tip.join("\n"));
}

static void setPanelError(QLabel *l, const QString &msg) {
  l->setText("ERR " + elide(msg));
  setLabelColor(l, COL_DOWN);
  l->setToolTip(msg);
}

static void setPanelBusy(QLabel *l, const QString &msg) {
  l->setText(msg);
  setLabelColor(l, COL_DIM);
  l->setToolTip(QString());
}

// ---------------------------------------------------------------------------
// Table helpers
// ---------------------------------------------------------------------------

static QTableWidget *makeTable(const QStringList &headers) {
  QTableWidget *t = new QTableWidget(0, headers.size());
  t->setHorizontalHeaderLabels(headers);
  t->verticalHeader()->setVisible(false);
  t->verticalHeader()->setDefaultSectionSize(ROW_H);
  t->verticalHeader()->setSectionResizeMode(QHeaderView::Fixed);
  t->horizontalHeader()->setHighlightSections(false);
  t->horizontalHeader()->setDefaultAlignment(ALIGN_TEXT);
  t->horizontalHeader()->setStretchLastSection(true);
  t->setEditTriggers(QAbstractItemView::NoEditTriggers);
  t->setSelectionBehavior(QAbstractItemView::SelectRows);
  t->setSelectionMode(QAbstractItemView::SingleSelection);
  t->setAlternatingRowColors(true);
  t->setShowGrid(false);
  t->setWordWrap(false);
  t->setFocusPolicy(Qt::NoFocus);
  return t;
}

static void setColumnWidths(QTableWidget *t, const int *widths, int count) {
  for (int i = 0; i < count; ++i) t->setColumnWidth(i, widths[i]);
}

static QTableWidgetItem *cellAt(QTableWidget *t, int r, int c) {
  QTableWidgetItem *it = t->item(r, c);
  if (!it) {
    it = new QTableWidgetItem;
    it->setFlags(Qt::ItemIsEnabled | Qt::ItemIsSelectable);
    t->setItem(r, c, it);
  }
  return it;
}

static QTableWidgetItem *setCell(QTableWidget *t, int r, int c,
                                 const QString &text,
                                 const QColor &fg = COL_TEXT,
                                 Qt::Alignment align = ALIGN_TEXT) {
  QTableWidgetItem *it = cellAt(t, r, c);
  it->setText(text);
  it->setForeground(fg);
  it->setTextAlignment(align);
  return it;
}

// Shows a single dim message row spanning the whole table.
static void setTableMessage(QTableWidget *t, const QString &msg) {
  t->clearSpans();
  t->setRowCount(1);
  for (int c = 0; c < t->columnCount(); ++c) setCell(t, 0, c, QString());
  setCell(t, 0, 0, msg, COL_DIM);
  if (t->columnCount() > 1) t->setSpan(0, 0, 1, t->columnCount());
}

// Connects QComboBox::currentIndexChanged(int) (overloaded in Qt5) to a lambda.
static void onComboChanged(QComboBox *cb, QObject *ctx,
                           std::function<void(int)> fn) {
  QObject::connect(
      cb, static_cast<void (QComboBox::*)(int)>(&QComboBox::currentIndexChanged),
      ctx, fn);
}

// ---------------------------------------------------------------------------
// Api: tiny QtNetwork client for the backend (JSON bodies and NDJSON streams).
// No MOC: every signal goes to a lambda.
// ---------------------------------------------------------------------------

typedef std::function<void(int status, const QJsonObject &body,
                           const QString &err)> JsonCb;
typedef std::function<void(const QJsonObject &line)> LineCb;
typedef std::function<void(const QString &err)> DoneCb;

class Api {
public:
  explicit Api(QObject *parent) : m_nam(new QNetworkAccessManager(parent)) {}

  void setBase(const QString &b) {
    m_base = b.trimmed();
    while (m_base.endsWith('/')) m_base.chop(1);
  }
  QString base() const { return m_base; }

  // Percent-encodes one path segment (e.g. "^VIX" -> "%5EVIX").
  static QString pathSeg(const QString &s) {
    return QString::fromLatin1(QUrl::toPercentEncoding(s));
  }

  // GET a JSON object. cb(status, body, err): err is "" on HTTP 200 + JSON.
  QNetworkReply *getJson(const QString &path, const QUrlQuery &q, JsonCb cb) {
    QNetworkReply *r = m_nam->get(request(path, q));
    armTimeout(r);
    QObject::connect(r, &QNetworkReply::finished, [r, cb]() {
      const int st =
          r->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();
      const QByteArray body = r->readAll();
      QJsonParseError pe;
      const QJsonDocument doc = QJsonDocument::fromJson(body, &pe);
      const QJsonObject obj = doc.object();
      QString err;
      if (r->error() != QNetworkReply::NoError) {
        err = obj.value("error").toString();
        if (err.isEmpty()) err = r->errorString();
      } else if (pe.error != QJsonParseError::NoError) {
        err = "bad JSON: " + pe.errorString();
      }
      r->deleteLater();
      cb(st, obj, err);
    });
    return r;
  }

  // GET an NDJSON stream; onLine fires for every JSON object line as it
  // arrives, onDone once at the end (err "" on success).
  QNetworkReply *getLines(const QString &path, const QUrlQuery &q,
                          LineCb onLine, DoneCb onDone) {
    QNetworkReply *r = m_nam->get(request(path, q));
    armTimeout(r);
    std::shared_ptr<QByteArray> buf = std::make_shared<QByteArray>();
    auto drain = [buf, onLine](bool final) {
      int nl;
      while ((nl = buf->indexOf('\n')) >= 0) {
        const QByteArray line = buf->left(nl).trimmed();
        buf->remove(0, nl + 1);
        emitLine(line, onLine);
      }
      if (final) {
        emitLine(buf->trimmed(), onLine);
        buf->clear();
      }
    };
    QObject::connect(r, &QNetworkReply::readyRead, [r, buf, drain]() {
      buf->append(r->readAll());
      drain(false);
    });
    QObject::connect(r, &QNetworkReply::finished, [r, buf, drain, onDone]() {
      buf->append(r->readAll());
      drain(true);
      const QString err =
          r->error() == QNetworkReply::NoError ? QString() : r->errorString();
      r->deleteLater();
      onDone(err);
    });
    return r;
  }

private:
  static void emitLine(const QByteArray &line, const LineCb &onLine) {
    if (line.isEmpty()) return;
    const QJsonDocument doc = QJsonDocument::fromJson(line);
    if (doc.isObject()) onLine(doc.object());
  }

  QNetworkRequest request(const QString &path, const QUrlQuery &q) const {
    QUrl u(m_base + path);
    if (!q.isEmpty()) u.setQuery(q);
    QNetworkRequest req(u);
    req.setRawHeader("Accept", "application/json, application/x-ndjson");
    req.setAttribute(QNetworkRequest::CacheLoadControlAttribute,
                     QNetworkRequest::AlwaysNetwork);
    return req;
  }

  static void armTimeout(QNetworkReply *r) {
    // The reply is the context object: the timer dies with the reply.
    QTimer::singleShot(REQUEST_TIMEOUT_MS, r, [r]() {
      if (r->isRunning()) r->abort();
    });
  }

  QNetworkAccessManager *m_nam;
  QString m_base;
};

// ---------------------------------------------------------------------------
// Sparkline (monitor TREND column), rendered to a pixmap for the item icon.
// ---------------------------------------------------------------------------

static QPixmap renderSpark(const QVector<double> &c, bool hasRef, double ref,
                           qreal dpr) {
  QPixmap pm(int(SPARK_W * dpr), int(SPARK_H * dpr));
  pm.setDevicePixelRatio(dpr);
  pm.fill(Qt::transparent);
  if (c.size() < 2) return pm;
  double lo = c[0], hi = c[0];
  for (double v : c) {
    lo = qMin(lo, v);
    hi = qMax(hi, v);
  }
  if (hasRef) {
    lo = qMin(lo, ref);
    hi = qMax(hi, ref);
  }
  if (hi <= lo) hi = lo + 1.0;
  const double base = hasRef ? ref : c.first();
  const QColor col = c.last() >= base ? COL_UP : COL_DOWN;
  const double w = SPARK_W - 2 * SPARK_PAD, h = SPARK_H - 2 * SPARK_PAD;
  QPainter p(&pm);
  p.setRenderHint(QPainter::Antialiasing, true);
  auto yOf = [&](double v) { return SPARK_PAD + h - (v - lo) / (hi - lo) * h; };
  if (hasRef) {
    QPen dash(COL_DIM);
    dash.setStyle(Qt::DotLine);
    p.setPen(dash);
    p.drawLine(QPointF(SPARK_PAD, yOf(ref)), QPointF(SPARK_PAD + w, yOf(ref)));
  }
  QPainterPath path;
  for (int i = 0; i < c.size(); ++i) {
    const QPointF pt(SPARK_PAD + w * i / (c.size() - 1), yOf(c[i]));
    if (i == 0) path.moveTo(pt);
    else path.lineTo(pt);
  }
  p.setPen(QPen(col, 1.2));
  p.drawPath(path);
  return pm;
}

// QFontMetrics::horizontalAdvance only exists since Qt 5.11.
static int textWidth(const QFontMetrics &fm, const QString &s) {
#if QT_VERSION >= QT_VERSION_CHECK(5, 11, 0)
  return fm.horizontalAdvance(s);
#else
  return fm.width(s);
#endif
}

// "Nice" grid step (1, 2, 5 x 10^n) for an axis.
static double niceStep(double raw) {
  if (raw <= 0 || !std::isfinite(raw)) return 1.0;
  const double mag = std::pow(10.0, std::floor(std::log10(raw)));
  const double f = raw / mag;
  double nf = 10.0;
  if (f < 1.5) nf = 1.0;
  else if (f < 3.0) nf = 2.0;
  else if (f < 7.0) nf = 5.0;
  return nf * mag;
}

// ---------------------------------------------------------------------------
// ChartWidget: price graph (line/area or candles) + volume, crosshair on hover.
// ---------------------------------------------------------------------------

class ChartWidget : public QWidget {
public:
  explicit ChartWidget(QWidget *parent = nullptr) : QWidget(parent) {
    setMouseTracking(true);
    setMinimumHeight(CHART_MIN_H);
    setAttribute(Qt::WA_OpaquePaintEvent, true);
    m_msg = LOADING_TEXT;
  }

  void setMessage(const QString &m) {
    m_msg = m;
    update();
  }

  void setCandles(bool on) {
    m_candles = on;
    update();
  }

  // Accepts the /api/yfin/chart body (column arrays + meta).
  void setData(const QJsonObject &o) {
    m_t.clear();
    const QJsonArray ts = o.value("timestamps").toArray();
    m_t.reserve(ts.size());
    for (const QJsonValue &v : ts) {
      const double t = v.toDouble();
      m_t.append(qint64(t >= MS_TIMESTAMP_MIN ? t : t * MS_PER_SEC));
    }
    m_o = jarr(o.value("open"));
    m_h = jarr(o.value("high"));
    m_l = jarr(o.value("low"));
    m_c = jarr(o.value("close"));
    m_v = jarr(o.value("volume"));
    m_symbol = jstr(o, "symbol");
    m_range = jstr(o, "range");
    m_interval = jstr(o, "interval");
    m_intraday = m_interval.endsWith('m') || m_interval.endsWith('h');
    m_hasPrev = jnum(o.value("previousClose"), m_prev);
    m_hover = -1;
    const int n = barCount();
    m_msg = n == 0 ? QStringLiteral("No data") : QString();
    update();
  }

protected:
  void paintEvent(QPaintEvent *) override {
    QPainter p(this);
    p.fillRect(rect(), COL_BG);
    p.setFont(font());
    const QFontMetrics fm(font());
    const int n = barCount();
    if (n == 0 || !m_msg.isEmpty()) {
      p.setPen(COL_DIM);
      p.drawText(rect(), Qt::AlignCenter,
                 m_msg.isEmpty() ? QStringLiteral("No data") : m_msg);
      m_plot = QRect();
      return;
    }

    // Areas: header line, price pane, volume pane, axes.
    const QRect all = rect().adjusted(CHART_PAD, CHART_PAD + fm.height(),
                                      -CHART_AXIS_W, -CHART_AXIS_H);
    const int volH = int(all.height() * CHART_VOL_FRAC);
    const QRect pr(all.left(), all.top(), all.width(),
                   all.height() - volH - CHART_VOL_GAP);
    const QRect vr(all.left(), pr.bottom() + CHART_VOL_GAP, all.width(), volH);
    m_plot = pr;
    m_step = double(pr.width()) / n;

    // Price scale.
    const bool useRef = m_intraday && m_hasPrev && m_range == "1d";
    double lo = m_candles ? m_l[0] : m_c[0];
    double hi = m_candles ? m_h[0] : m_c[0];
    for (int i = 0; i < n; ++i) {
      lo = qMin(lo, m_candles ? m_l[i] : m_c[i]);
      hi = qMax(hi, m_candles ? m_h[i] : m_c[i]);
    }
    if (useRef) {
      lo = qMin(lo, m_prev);
      hi = qMax(hi, m_prev);
    }
    if (hi <= lo) {
      hi += 1.0;
      lo -= 1.0;
    }
    const double pad = (hi - lo) * CHART_Y_PAD_FRAC;
    lo -= pad;
    hi += pad;
    auto yOf = [&](double v) {
      return pr.bottom() - (v - lo) / (hi - lo) * pr.height();
    };
    auto xOf = [&](int i) { return pr.left() + (i + 0.5) * m_step; };

    // Horizontal grid + right-hand price labels.
    const double ys = niceStep((hi - lo) / CHART_Y_TICKS);
    for (double v = std::ceil(lo / ys) * ys; v <= hi; v += ys) {
      const int y = int(yOf(v));
      p.setPen(COL_GRID);
      p.drawLine(pr.left(), y, pr.right(), y);
      p.setPen(COL_DIM);
      p.drawText(QRect(pr.right() + PANEL_PAD, y - fm.height() / 2,
                       CHART_AXIS_W - PANEL_PAD, fm.height()),
                 ALIGN_TEXT, fmtPrice(v));
    }

    // Time labels + vertical grid.
    int lastI = -1;
    for (int k = 0; k < CHART_X_TICKS; ++k) {
      const int i = n == 1 ? 0 : qRound(k * (n - 1.0) / (CHART_X_TICKS - 1));
      if (i == lastI) continue;
      lastI = i;
      const int x = int(xOf(i));
      p.setPen(COL_GRID);
      p.drawLine(x, pr.top(), x, vr.bottom());
      p.setPen(COL_DIM);
      p.drawText(QRect(x - CHART_LABEL_W / 2, vr.bottom() + 2, CHART_LABEL_W,
                       CHART_AXIS_H),
                 Qt::AlignHCenter | Qt::AlignTop, timeLabel(i));
    }

    // Volume bars.
    double maxV = 0;
    for (int i = 0; i < n && i < m_v.size(); ++i) maxV = qMax(maxV, m_v[i]);
    if (maxV > 0) {
      const double bw = qMax(1.0, m_step * CANDLE_BODY_FRAC);
      for (int i = 0; i < n && i < m_v.size(); ++i) {
        const double bh = m_v[i] / maxV * vr.height();
        QColor c = barUp(i) ? COL_UP : COL_DOWN;
        c.setAlpha(CHART_VOL_ALPHA);
        p.fillRect(QRectF(xOf(i) - bw / 2, vr.bottom() - bh, bw, bh), c);
      }
    }

    // Previous close reference (1D intraday).
    if (useRef) {
      QPen dash(COL_DIM);
      dash.setStyle(Qt::DashLine);
      p.setPen(dash);
      const int y = int(yOf(m_prev));
      p.drawLine(pr.left(), y, pr.right(), y);
    }

    // Price series.
    const double ref = useRef ? m_prev : m_c.first();
    const bool up = m_c.last() >= ref;
    const QColor lineCol = up ? COL_UP : COL_DOWN;
    p.setRenderHint(QPainter::Antialiasing, true);
    if (m_candles) {
      const double bw = qMax(1.0, m_step * CANDLE_BODY_FRAC);
      for (int i = 0; i < n; ++i) {
        const QColor c = barUp(i) ? COL_UP : COL_DOWN;
        const double x = xOf(i);
        p.setPen(c);
        p.drawLine(QPointF(x, yOf(m_h[i])), QPointF(x, yOf(m_l[i])));
        const double y1 = yOf(qMax(m_o[i], m_c[i]));
        const double y2 = yOf(qMin(m_o[i], m_c[i]));
        p.fillRect(QRectF(x - bw / 2, y1, bw, qMax(1.0, y2 - y1)), c);
      }
    } else {
      QPainterPath line;
      for (int i = 0; i < n; ++i) {
        const QPointF pt(xOf(i), yOf(m_c[i]));
        if (i == 0) line.moveTo(pt);
        else line.lineTo(pt);
      }
      QPainterPath area = line;
      area.lineTo(xOf(n - 1), pr.bottom());
      area.lineTo(xOf(0), pr.bottom());
      area.closeSubpath();
      QLinearGradient g(0, pr.top(), 0, pr.bottom());
      QColor top = lineCol;
      top.setAlpha(CHART_FILL_ALPHA);
      QColor bottom = lineCol;
      bottom.setAlpha(0);
      g.setColorAt(0, top);
      g.setColorAt(1, bottom);
      p.fillPath(area, g);
      p.setPen(QPen(lineCol, 1.5));
      p.drawPath(line);
    }
    p.setRenderHint(QPainter::Antialiasing, false);

    // Last price tag on the axis.
    {
      const int y = int(yOf(m_c.last()));
      const QRect tag(pr.right() + 1, y - fm.height() / 2,
                      CHART_AXIS_W - 2, fm.height());
      p.fillRect(tag, lineCol);
      p.setPen(COL_BLACK);
      p.drawText(tag.adjusted(PANEL_PAD, 0, 0, 0), ALIGN_TEXT,
                 fmtPrice(m_c.last()));
    }

    // Header line: symbol / range / change over the range.
    const QRect head(CHART_PAD, CHART_PAD / 2, width() - 2 * CHART_PAD,
                     fm.height());
    const double chg = m_c.last() - ref;
    const double pct = ref != 0 ? chg / ref * PCT_SCALE : 0.0;
    p.setPen(COL_AMBER);
    const QString title = QString("%1  %2 \xc2\xb7 %3")
                              .arg(m_symbol, m_range.toUpper(), m_interval);
    p.drawText(head, ALIGN_TEXT, title);
    p.setPen(changeColor(chg));
    p.drawText(head.adjusted(textWidth(fm, title) + CHART_PAD * 2, 0,
                             0, 0),
               ALIGN_TEXT, fmtSigned(chg, 2) + "  " + fmtPctSigned(pct));

    // Crosshair + OHLCV readout.
    if (m_hover >= 0 && m_hover < n) {
      const int i = m_hover;
      const int x = int(xOf(i));
      const int y = int(yOf(m_c[i]));
      QPen cross(COL_CROSS);
      cross.setStyle(Qt::DotLine);
      p.setPen(cross);
      p.drawLine(x, pr.top(), x, vr.bottom());
      p.drawLine(pr.left(), y, pr.right(), y);
      const QString info =
          QString("%1  O %2  H %3  L %4  C %5  V %6")
              .arg(timeLabel(i, true), fmtPrice(m_o[i]), fmtPrice(m_h[i]),
                   fmtPrice(m_l[i]), fmtPrice(m_c[i]),
                   i < m_v.size() ? fmtBig(m_v[i]) : QString(NOT_AVAILABLE));
      p.setPen(COL_TEXT);
      p.drawText(head, Qt::AlignRight | Qt::AlignVCenter, info);
    }
  }

  void mouseMoveEvent(QMouseEvent *e) override {
    int idx = -1;
    if (m_plot.isValid() && m_step > 0 && e->pos().x() >= m_plot.left() &&
        e->pos().x() <= m_plot.right()) {
      idx = qBound(0, int((e->pos().x() - m_plot.left()) / m_step),
                   barCount() - 1);
    }
    if (idx != m_hover) {
      m_hover = idx;
      update();
    }
  }

  void leaveEvent(QEvent *) override {
    m_hover = -1;
    update();
  }

private:
  // Bars usable for drawing (all OHLC columns present).
  int barCount() const {
    return qMin(qMin(m_t.size(), m_c.size()),
                qMin(m_o.size(), qMin(m_h.size(), m_l.size())));
  }

  bool barUp(int i) const {
    if (i > 0 && !m_candles) return m_c[i] >= m_c[i - 1];
    return m_c[i] >= m_o[i];
  }

  QString timeLabel(int i, bool full = false) const {
    const QDateTime dt = QDateTime::fromMSecsSinceEpoch(m_t[i]).toLocalTime();
    QString fmt;
    if (full) fmt = m_intraday ? "ddd MM/dd HH:mm" : "ddd yyyy-MM-dd";
    else if (m_range == "1d") fmt = "HH:mm";
    else if (m_intraday) fmt = "ddd HH:mm";
    else if (m_range == "5y" || m_range == "10y" || m_range == "max")
      fmt = "yyyy";
    else if (m_range == "1y" || m_range == "2y") fmt = "MMM yy";
    else fmt = "MMM dd";
    return enLocale().toString(dt, fmt);
  }

  QVector<qint64> m_t;
  QVector<double> m_o, m_h, m_l, m_c, m_v;
  QString m_symbol, m_range, m_interval, m_msg;
  bool m_intraday = false;
  bool m_hasPrev = false;
  double m_prev = 0;
  bool m_candles = false;
  int m_hover = -1;
  QRect m_plot;       // price pane of the last paint (for hover mapping)
  double m_step = 0;  // bar width of the last paint
};

// ---------------------------------------------------------------------------
// DistBar: stacked strong-buy .. strong-sell bar with a legend row.
// ---------------------------------------------------------------------------

class DistBar : public QWidget {
public:
  explicit DistBar(QWidget *parent = nullptr) : QWidget(parent) {
    setFixedHeight(DIST_BAR_H * 2);
    clear();
  }

  void clear() {
    for (int i = 0; i < DIST_SLOTS; ++i) m_n[i] = 0;
    update();
  }

  void setCounts(const int *n) {
    for (int i = 0; i < DIST_SLOTS; ++i) m_n[i] = qMax(0, n[i]);
    update();
  }

protected:
  void paintEvent(QPaintEvent *) override {
    QPainter p(this);
    p.fillRect(rect(), COL_PANEL);
    const QFontMetrics fm(font());
    int total = 0;
    for (int i = 0; i < DIST_SLOTS; ++i) total += m_n[i];
    if (total == 0) {
      p.setPen(COL_DIM);
      p.drawText(rect(), Qt::AlignCenter, "No rating distribution");
      return;
    }
    // Stacked bar.
    double x = 0;
    for (int i = 0; i < DIST_SLOTS; ++i) {
      if (m_n[i] == 0) continue;
      const double w = double(width()) * m_n[i] / total;
      const QRectF seg(x, 0, w, DIST_BAR_H);
      p.fillRect(seg, DIST_COLORS[i]);
      const QString num = QString::number(m_n[i]);
      if (textWidth(fm, num) + PANEL_PAD < w) {
        p.setPen(COL_BLACK);
        p.drawText(seg, Qt::AlignCenter, num);
      }
      x += w;
    }
    // Legend.
    int lx = 0;
    for (int i = 0; i < DIST_SLOTS; ++i) {
      const QString s = QString("%1 %2").arg(DIST_SHORT[i]).arg(m_n[i]);
      p.setPen(DIST_COLORS[i]);
      p.drawText(QRect(lx, DIST_BAR_H, width() - lx, DIST_BAR_H), ALIGN_TEXT,
                 s);
      lx += textWidth(fm, s) + 2 * PANEL_PAD;
    }
  }

private:
  int m_n[DIST_SLOTS];
};

// ---------------------------------------------------------------------------
// Panel / table definitions
// ---------------------------------------------------------------------------

// A framed panel: amber title bar (title, controls, status) + body layout.
struct Panel {
  QFrame *frame = nullptr;
  QLabel *title = nullptr;
  QLabel *status = nullptr;
  QHBoxLayout *bar = nullptr;   // insert controls with addControl()
  QVBoxLayout *body = nullptr;
  int controls = 0;             // number of controls after the title

  void addControl(QWidget *w) {
    bar->insertWidget(1 + controls, w);
    ++controls;
  }
};

static Panel makePanel(const QString &title) {
  Panel p;
  p.frame = new QFrame;
  p.frame->setObjectName("panel");
  QVBoxLayout *v = new QVBoxLayout(p.frame);
  v->setContentsMargins(0, 0, 0, 0);
  v->setSpacing(0);

  QWidget *bar = new QWidget;
  bar->setObjectName("panelBar");
  p.bar = new QHBoxLayout(bar);
  p.bar->setContentsMargins(PANEL_PAD, 1, PANEL_PAD, 1);
  p.bar->setSpacing(PANEL_SPACING);
  p.title = new QLabel(title);
  p.title->setObjectName("panelTitle");
  p.status = new QLabel;
  p.status->setObjectName("panelStatus");
  p.bar->addWidget(p.title);
  p.bar->addStretch(1);
  p.bar->addWidget(p.status);
  v->addWidget(bar);

  QWidget *body = new QWidget;
  body->setObjectName("panelBody");
  p.body = new QVBoxLayout(body);
  p.body->setContentsMargins(0, 0, 0, 0);
  p.body->setSpacing(PANEL_SPACING);
  v->addWidget(body, 1);
  return p;
}

static QStringList headersOf(const char *const *names, int n) {
  QStringList out;
  for (int i = 0; i < n; ++i) out << QString::fromUtf8(names[i]);
  return out;
}

struct ComboItem {
  const char *label;
  const char *value;
};

static void fillCombo(QComboBox *cb, const ComboItem *items, int n) {
  for (int i = 0; i < n; ++i) cb->addItem(items[i].label, QString(items[i].value));
}

#define COUNT_OF(a) int(sizeof(a) / sizeof((a)[0]))

// MONITOR
enum MonitorCol { MC_SYM, MC_LAST, MC_CHG, MC_PCT, MC_TREND, MC_HIGH, MC_LOW,
                  MC_VOL, MC_COUNT };
static const char *MONITOR_HEADERS[MC_COUNT] = {
    "TICKER", "LAST", "CHG", "%CHG", "TREND 1D", "HIGH", "LOW", "VOLUME"};
static const int MONITOR_COL_W[MC_COUNT] = {72, 82, 70, 70, SPARK_W + 8,
                                            78, 78, 70};

// NEWS
enum NewsCol { NC_TIME, NC_SRC, NC_TICKERS, NC_HEADLINE, NC_COUNT };
static const char *NEWS_HEADERS[NC_COUNT] = {"TIME", "SOURCE", "TICKERS",
                                             "HEADLINE"};
static const int NEWS_COL_W[NC_COUNT - 1] = {84, 100, 90};
static const ComboItem NEWS_SCOPES[] = {{"All news", "all"},
                                        {"Symbol only", "symbol"}};
static const char *NEWS_SCOPE_SYMBOL = "symbol";

// FA (key statistics)
enum FundCol { FC_FIELD, FC_VALUE, FC_COUNT };
static const char *FUND_HEADERS[FC_COUNT] = {"FIELD", "VALUE"};
static const int FUND_COL_W[FC_COUNT - 1] = {170};
enum FundKind { FK_PRICE, FK_BIG, FK_RATIO, FK_FRAC_PCT, FK_YIELD };
struct FundField {
  const char *key;
  const char *label;
  FundKind kind;
};
static const FundField FUND_FIELDS[] = {
    {"price", "Last Price", FK_PRICE},
    {"previousClose", "Prev Close", FK_PRICE},
    {"marketCap", "Market Cap", FK_BIG},
    {"enterpriseValue", "Enterprise Value", FK_BIG},
    {"trailingPE", "P/E (TTM)", FK_RATIO},
    {"forwardPE", "P/E (Fwd)", FK_RATIO},
    {"epsTrailingTwelveMonths", "EPS (TTM)", FK_PRICE},
    {"priceToBook", "Price / Book", FK_RATIO},
    {"priceToSalesTrailing12Months", "Price / Sales", FK_RATIO},
    {"dividendYield", "Dividend Yield", FK_YIELD},
    {"beta", "Beta", FK_RATIO},
    {"high52w", "52W High", FK_PRICE},
    {"low52w", "52W Low", FK_PRICE},
    {"sharesOutstanding", "Shares Out", FK_BIG},
    {"floatShares", "Float", FK_BIG},
    {"totalRevenue", "Revenue (TTM)", FK_BIG},
    {"freeCashflow", "Free Cash Flow", FK_BIG},
    {"profitMargins", "Profit Margin", FK_FRAC_PCT},
    {"returnOnEquity", "Return on Equity", FK_FRAC_PCT},
    {"debtToEquity", "Debt / Equity", FK_RATIO},
    {"currentRatio", "Current Ratio", FK_RATIO},
};

// ANR (analyst ratings table)
enum RatingCol { RC_FIRM, RC_RATING, RC_PT, RC_DATE, RC_COUNT };
static const char *RATING_HEADERS[RC_COUNT] = {"FIRM", "RATING", "PT", "DATE"};
static const int RATING_COL_W[RC_COUNT - 1] = {130, 100, 70};

// EARN (financial statements)
static const ComboItem STATEMENT_TYPES[] = {{"Income", "INCOME_STATEMENT"},
                                            {"Balance", "BALANCE_SHEET"},
                                            {"Cash Flow", "CASH_FLOW"}};
static const ComboItem STATEMENT_PERIODS[] = {{"Quarterly", "QUARTERLY"},
                                              {"Annual", "ANNUAL"}};
static const char *DENSE_STATEMENT_TYPE = "INCOME_STATEMENT";
static const char *DENSE_STATEMENT_PERIOD = "QUARTERLY";
static const char *EPS_KEY_PREFIX = "eps";
static const int EARN_LABEL_W = 210;
static const int EARN_COL_W = 86;
static const int EARN_INDENT_SPACES = 2;

// ECO (economic calendar)
enum EcoCol { EC_TIME, EC_CCY, EC_IMPACT, EC_ACTUAL, EC_FCST, EC_PRIOR,
              EC_EVENT, EC_COUNT };
static const char *ECO_HEADERS[EC_COUNT] = {"TIME", "CCY", "IMPACT", "ACTUAL",
                                            "FCST", "PRIOR", "EVENT"};
static const int ECO_COL_W[EC_COUNT - 1] = {120, 44, 64, 72, 72, 72};
static const ComboItem ECON_WEEKS[] = {{"This week", "thisweek"},
                                       {"Next week", "nextweek"},
                                       {"Last week", "lastweek"}};
static const char *DENSE_ECON_WEEK = "thisweek";
static const ComboItem ECON_IMPACTS[] = {{"All impact", "all"},
                                         {"High", "high"},
                                         {"High + Medium", "highmed"}};

static QColor impactColor(const QString &impact) {
  const QString i = impact.toLower();
  if (i == "high") return COL_DOWN;
  if (i == "medium") return COL_WARN;
  if (i == "low") return COL_HOLD;
  return COL_DIM;
}

#ifdef EXTRA_DATA_PANELS
// ---------------------------------------------------------------------------
// Extra data panels: routes, constants, static universes, helpers, LineChart
// ---------------------------------------------------------------------------

static const char *ROUTE_FILINGS = "/api/sec/filings";
static const char *ROUTE_FILING_DOC = "/api/v2/filing-doc";
static const char *ROUTE_INSIDER = "/api/sec/insider";
static const char *ROUTE_INSTITUTIONAL = "/api/sec/institutional";
static const char *ROUTE_13F = "/api/sec/13f";
static const char *ROUTE_IPOS = "/api/v2/ipos";
static const char *ROUTE_HALTS = "/api/v2/halts";
static const char *ROUTE_OPTIONS = "/api/yfin/options-chain";
static const char *ROUTE_TRANSCRIPTS = "/api/v2/transcripts";
static const char *ROUTE_ARTICLE = "/api/yfin/article";
static const char *ROUTE_FEAR_GREED = "/api/yfin/fear-greed";
static const char *ROUTE_WSB = "/api/wsb/trending";
static const char *ROUTE_CONGRESS = "/api/congress/trades";
static const char *ROUTE_SEARCH = "/api/yfin/search";
static const char *ROUTE_MOVERS = "/api/yfin/market-movers";
static const char *ROUTE_TRENDING = "/api/yfin/trending";

static const int PAGE_FAST_MS = 30000;     // quote boards, movers, halts
static const int PAGE_SLOW_MS = 300000;    // sectors, social, IPOs, 13F
static const int FNG_POLL_MS = 600000;     // status bar Fear & Greed
static const int SEARCH_DEBOUNCE_MS = 300;
static const int SEARCH_MIN_CHARS = 2;
static const int FILINGS_LIMIT = 60;
static const int INSIDER_LIMIT = 25;
static const int MARKETWIDE_INSIDER_LIMIT = 40;
static const int CONGRESS_LIMIT = 150;
static const int HOLDINGS_TOP = 100;
static const int LEADERS_COUNT = 5;        // breadth best / worst rows
static const int READER_W = 900;
static const int READER_H = 760;
static const int CIK_EDIT_W = 110;
static const char *SECTOR_RANGE = "1y";
static const char *SECTOR_INTERVAL = "1d";

static const int LINECHART_MIN_H = 140;
static const int LINECHART_PAD = 6;
static const int LINECHART_AXIS_W = 52;
static const int LINECHART_AXIS_H = 18;
static const int LINECHART_Y_TICKS = 4;
static const int LINECHART_DOT_MAX = 24;   // point markers + values up to this
static const int LINECHART_DOT_R = 3;
static const int LINECHART_LABEL_GAP = 10;

static const int TRADING_DAYS_1W = 5;
static const int TRADING_DAYS_1M = 21;
static const int TRADING_DAYS_3M = 63;
static const int TRADING_DAYS_6M = 126;
static const double BP_PER_PCT = 100.0;
static const double NEAR_HIGH_FRAC = 0.995;
static const double NEAR_LOW_FRAC = 1.005;
static const double UNCHANGED_PCT = 0.01;
static const double VIX_HIGH_FEAR = 30.0;
static const double VIX_ELEVATED = 22.0;
static const double VIX_NORMAL = 15.0;
static const double FNG_FEAR_MAX = 45.0;   // below: fear colors
static const double FNG_GREED_MIN = 55.0;  // above: greed colors

static const char *WORKSPACE_DENSE = "DENSE";
static const char *SETTINGS_WORKSPACE_KEY = "workspace";

struct ColSpec {
  const char *title;
  int width;  // ignored for the last (stretching) column
  bool num;
};

static QTableWidget *makeColTable(const ColSpec *cols, int n) {
  QStringList h;
  for (int i = 0; i < n; ++i) h << QString::fromUtf8(cols[i].title);
  QTableWidget *t = makeTable(h);
  for (int i = 0; i < n; ++i) {
    if (i < n - 1) t->setColumnWidth(i, cols[i].width);
    if (cols[i].num) t->horizontalHeaderItem(i)->setTextAlignment(ALIGN_NUM);
  }
  return t;
}

struct SymName {
  const char *symbol;
  const char *name;
  const char *group;
};

// WEI: world equity indices.
static const SymName WORLD_INDICES[] = {
    {"^GSPC", "S&P 500", "AMER"},        {"^DJI", "Dow Jones", "AMER"},
    {"^IXIC", "Nasdaq Comp", "AMER"},    {"^RUT", "Russell 2000", "AMER"},
    {"^GSPTSE", "S&P/TSX", "AMER"},      {"^MXX", "IPC Mexico", "AMER"},
    {"^BVSP", "Bovespa", "AMER"},        {"^FTSE", "FTSE 100", "EMEA"},
    {"^GDAXI", "DAX", "EMEA"},           {"^FCHI", "CAC 40", "EMEA"},
    {"^STOXX50E", "Euro Stoxx 50", "EMEA"}, {"FTSEMIB.MI", "FTSE MIB", "EMEA"},
    {"^IBEX", "IBEX 35", "EMEA"},        {"^SSMI", "SMI", "EMEA"},
    {"^N225", "Nikkei 225", "APAC"},     {"^HSI", "Hang Seng", "APAC"},
    {"000001.SS", "Shanghai Comp", "APAC"}, {"^BSESN", "Sensex", "APAC"},
    {"^AXJO", "ASX 200", "APAC"},        {"^KS11", "KOSPI", "APAC"},
    {"^TWII", "TAIEX", "APAC"},
};

// FX + key commodities / crypto.
static const SymName FX_BOARD[] = {
    {"EURUSD=X", "EUR/USD", "FX"},   {"GBPUSD=X", "GBP/USD", "FX"},
    {"USDJPY=X", "USD/JPY", "FX"},   {"USDCHF=X", "USD/CHF", "FX"},
    {"AUDUSD=X", "AUD/USD", "FX"},   {"USDCAD=X", "USD/CAD", "FX"},
    {"NZDUSD=X", "NZD/USD", "FX"},   {"USDCNY=X", "USD/CNY", "FX"},
    {"USDMXN=X", "USD/MXN", "FX"},   {"EURGBP=X", "EUR/GBP", "FX"},
    {"EURJPY=X", "EUR/JPY", "FX"},   {"DX-Y.NYB", "Dollar Index", "IDX"},
    {"GC=F", "Gold", "CMDTY"},       {"SI=F", "Silver", "CMDTY"},
    {"CL=F", "WTI Crude", "CMDTY"},  {"NG=F", "Natural Gas", "CMDTY"},
    {"HG=F", "Copper", "CMDTY"},     {"BTC-USD", "Bitcoin", "CRYPTO"},
    {"ETH-USD", "Ether", "CRYPTO"},
};

// Sector ETFs (+ benchmark).
static const SymName SECTORS[] = {
    {"XLK", "Technology", "SECTOR"},     {"XLC", "Communication", "SECTOR"},
    {"XLY", "Discretionary", "SECTOR"},  {"XLF", "Financials", "SECTOR"},
    {"XLV", "Health Care", "SECTOR"},    {"XLI", "Industrials", "SECTOR"},
    {"XLE", "Energy", "SECTOR"},         {"XLB", "Materials", "SECTOR"},
    {"XLP", "Staples", "SECTOR"},        {"XLU", "Utilities", "SECTOR"},
    {"XLRE", "Real Estate", "SECTOR"},   {"SPY", "S&P 500 (bench)", "BENCH"},
};

struct Tenor {
  const char *symbol;
  const char *label;
  double years;
};

// Treasury yields as quoted by Yahoo (percent). 2Y = CBOT 2Y yield future.
static const Tenor TREASURY_TENORS[] = {
    {"^IRX", "3M", 0.25}, {"2YY=F", "2Y", 2.0}, {"^FVX", "5Y", 5.0},
    {"^TNX", "10Y", 10.0}, {"^TYX", "30Y", 30.0},
};

struct Spread {
  const char *name;
  const char *shortLeg;
  const char *longLeg;
};

static const Spread TREASURY_SPREADS[] = {
    {"2s10s", "2Y", "10Y"}, {"3m10y", "3M", "10Y"},
    {"5s30s", "5Y", "30Y"}, {"10s30s", "10Y", "30Y"},
};

// VIX term structure (years field = days to expiry here).
static const Tenor VIX_TENORS[] = {
    {"^VIX9D", "9D", 9}, {"^VIX", "1M", 30}, {"^VIX3M", "3M", 93},
    {"^VIX6M", "6M", 183}, {"^VIX1Y", "1Y", 365},
};
static const char *VVIX_SYMBOL = "^VVIX";
static const char *VIX_SPOT_SYMBOL = "^VIX";
static const char *VIX_3M_SYMBOL = "^VIX3M";

// Breadth universe: S&P 100-style large caps.
static const char *BREADTH_UNIVERSE =
    "AAPL,ABBV,ABT,ACN,ADBE,AIG,AMD,AMGN,AMT,AMZN,AVGO,AXP,BA,BAC,BK,BKNG,"
    "BLK,BMY,BRK-B,C,CAT,CHTR,CL,CMCSA,COF,COP,COST,CRM,CSCO,CVS,CVX,DE,DHR,"
    "DIS,DUK,EMR,F,FDX,GD,GE,GILD,GM,GOOG,GOOGL,GS,HD,HON,IBM,INTC,INTU,ISRG,"
    "JNJ,JPM,KO,LIN,LLY,LMT,LOW,MA,MCD,MDLZ,MDT,MET,META,MMM,MO,MRK,MS,MSFT,"
    "NEE,NFLX,NKE,NOW,NVDA,ORCL,PEP,PFE,PG,PLTR,PM,PYPL,QCOM,RTX,SBUX,SCHW,"
    "SO,SPG,T,TGT,TMO,TMUS,TSLA,TXN,UNH,UNP,UPS,USB,V,VZ,WFC,WMT,XOM";

// 13F managers (SEC CIKs).
static const ComboItem MANAGERS[] = {
    {"Berkshire Hathaway", "1067983"},  {"Pershing Square", "1336528"},
    {"Scion Asset Mgmt", "1649339"},    {"Bridgewater Associates", "1350694"},
    {"Renaissance Technologies", "1037389"}, {"Baupost Group", "1061768"},
};

static const ComboItem FILING_FORMS[] = {
    {"All forms", ""},          {"10-K / 10-Q", "10-K,10-Q"},
    {"8-K", "8-K"},             {"Insider 3/4/5", "3,4,5"},
    {"Registration", "S-1,S-3,424B4"},
};

static const ComboItem MOVER_TYPES[] = {
    {"Gainers", "gainers"},          {"Losers", "losers"},
    {"Most active", "active"},       {"Unusual volume", "unusual"},
    {"Small-cap gainers", "small_gainers"},
    {"Undervalued large caps", "undervalued"},
    {"Growth tech", "growth_tech"},
};
static const char *MOVER_UNUSUAL = "unusual";

static const ComboItem IPO_MARKETS[] = {
    {"All deals", "all"},   {"Upcoming", "upcoming"}, {"Priced", "priced"},
    {"Filed", "filed"},     {"Withdrawn", "withdrawn"},
};
// Month offsets from the current month.
static const ComboItem IPO_MONTHS[] = {
    {"This month", "0"}, {"Next month", "1"}, {"Last month", "-1"},
};

// Form 4 transaction codes.
static QString insiderCodeText(const QString &code) {
  static const ComboItem CODES[] = {
      {"P", "open-market purchase"}, {"S", "open-market sale"},
      {"A", "grant / award"},        {"M", "option exercise"},
      {"F", "tax withholding"},      {"G", "gift"},
      {"C", "conversion"},           {"X", "option exercise (ITM)"},
      {"D", "disposition to issuer"}, {"J", "other"},
  };
  for (const ComboItem &c : CODES) {
    if (code == QLatin1String(c.label)) return c.value;
  }
  return code;
}

static QColor insiderCodeColor(const QString &code, const QString &ad) {
  if (code == "P") return COL_UP;
  if (code == "S") return COL_DOWN;
  if (ad == "D") return COL_SELL_LIGHT;
  if (ad == "A") return COL_BUY_LIGHT;
  return COL_TEXT;
}

static QColor fngColor(double score) {
  if (score < FNG_FEAR_MAX) return COL_DOWN;
  if (score > FNG_GREED_MIN) return COL_UP;
  return COL_HOLD;
}

static QColor sentimentColor(const QString &s) {
  const QString l = s.toLower();
  if (l.contains("bull") || l.contains("greed")) return COL_UP;
  if (l.contains("bear") || l.contains("fear")) return COL_DOWN;
  return COL_HOLD;
}

// Number cell text: "--" when missing.
static QString numOr(const QJsonValue &v, int decimals) {
  double d = 0;
  return jnum(v, d) ? fmtNum(d, decimals) : QString(NOT_AVAILABLE);
}
static QString bigOr(const QJsonValue &v) {
  double d = 0;
  return jnum(v, d) ? fmtBig(d) : QString(NOT_AVAILABLE);
}
static QString priceOr(const QJsonValue &v) {
  double d = 0;
  return jnum(v, d) ? fmtPrice(d) : QString(NOT_AVAILABLE);
}

// % change of the last close vs. the close `back` bars earlier.
static bool backChange(const QVector<double> &c, int back, double &out) {
  if (back < 1 || c.size() <= back) return false;
  const double base = c[c.size() - 1 - back];
  if (base == 0) return false;
  out = (c.last() / base - 1.0) * PCT_SCALE;
  return true;
}

// YTD: last close vs. the last close of the previous calendar year.
static bool ytdChange(const QVector<double> &c, const QVector<double> &ts,
                      double &out) {
  if (c.isEmpty() || c.size() != ts.size()) return false;
  const int year = QDate::currentDate().year();
  double base = 0;
  bool found = false;
  for (int i = 0; i < ts.size(); ++i) {
    const double ms = ts[i] < MS_TIMESTAMP_MIN ? ts[i] * MS_PER_SEC : ts[i];
    if (QDateTime::fromMSecsSinceEpoch(qint64(ms)).date().year() >= year) break;
    base = c[i];
    found = true;
  }
  if (!found || base == 0) return false;
  out = (c.last() / base - 1.0) * PCT_SCALE;
  return true;
}

// Simple line chart (yield curve, VIX term structure, Fear & Greed history).
class LineChart : public QWidget {
public:
  LineChart() { setMinimumHeight(LINECHART_MIN_H); }

  void setSeries(const QVector<double> &ys, const QStringList &labels,
                 int decimals, bool hasRef = false, double ref = 0) {
    m_ys = ys;
    m_labels = labels;
    m_dec = decimals;
    m_hasRef = hasRef;
    m_ref = ref;
    m_msg.clear();
    update();
  }

  void setMessage(const QString &m) {
    m_ys.clear();
    m_labels.clear();
    m_msg = m;
    update();
  }

protected:
  void paintEvent(QPaintEvent *) override {
    QPainter p(this);
    p.fillRect(rect(), COL_PANEL);
    if (m_ys.size() < 2) {
      p.setPen(COL_DIM);
      p.drawText(rect(), Qt::AlignCenter,
                 m_msg.isEmpty() ? QString(NOT_AVAILABLE) : m_msg);
      return;
    }
    const QRectF plot(LINECHART_PAD + LINECHART_AXIS_W, LINECHART_PAD,
                      width() - 2 * LINECHART_PAD - LINECHART_AXIS_W,
                      height() - 2 * LINECHART_PAD - LINECHART_AXIS_H);
    if (plot.width() <= 0 || plot.height() <= 0) return;
    double lo = m_ys[0], hi = m_ys[0];
    for (double v : m_ys) {
      lo = qMin(lo, v);
      hi = qMax(hi, v);
    }
    if (m_hasRef) {
      lo = qMin(lo, m_ref);
      hi = qMax(hi, m_ref);
    }
    if (hi <= lo) hi = lo + 1.0;
    const double pad = (hi - lo) * CHART_Y_PAD_FRAC;
    lo -= pad;
    hi += pad;
    const int n = m_ys.size();
    auto xOf = [&](int i) { return plot.left() + plot.width() * i / (n - 1); };
    auto yOf = [&](double v) {
      return plot.bottom() - (v - lo) / (hi - lo) * plot.height();
    };
    const QFontMetrics fm(font());

    // Horizontal grid + y labels.
    for (int k = 0; k < LINECHART_Y_TICKS; ++k) {
      const double v = lo + (hi - lo) * k / (LINECHART_Y_TICKS - 1);
      const double y = yOf(v);
      p.setPen(COL_GRID);
      p.drawLine(QPointF(plot.left(), y), QPointF(plot.right(), y));
      p.setPen(COL_DIM);
      p.drawText(QRectF(0, y - fm.height() / 2.0,
                        LINECHART_PAD + LINECHART_AXIS_W - LINECHART_PAD,
                        fm.height()),
                 ALIGN_NUM, fmtNum(v, m_dec));
    }
    if (m_hasRef) {
      QPen dash(COL_DIM);
      dash.setStyle(Qt::DotLine);
      p.setPen(dash);
      p.drawLine(QPointF(plot.left(), yOf(m_ref)),
                 QPointF(plot.right(), yOf(m_ref)));
    }

    // X labels: as many as fit without overlapping.
    int labelW = 1;
    for (const QString &l : m_labels) labelW = qMax(labelW, textWidth(fm, l));
    const int every = qMax(
        1, int(std::ceil(n * double(labelW + LINECHART_LABEL_GAP) / plot.width())));
    p.setPen(COL_DIM);
    for (int i = 0; i < n && i < m_labels.size(); i += every) {
      const double x = xOf(i);
      QRectF r(x - labelW / 2.0, plot.bottom() + 2, labelW, LINECHART_AXIS_H);
      if (r.left() < 0) r.moveLeft(0);
      if (r.right() > width()) r.moveRight(width());
      p.drawText(r, Qt::AlignHCenter | Qt::AlignTop, m_labels[i]);
    }

    p.setRenderHint(QPainter::Antialiasing, true);
    QPainterPath path;
    for (int i = 0; i < n; ++i) {
      const QPointF pt(xOf(i), yOf(m_ys[i]));
      if (i == 0) path.moveTo(pt);
      else path.lineTo(pt);
    }
    p.setPen(QPen(COL_AMBER, 1.6));
    p.drawPath(path);
    if (n <= LINECHART_DOT_MAX) {
      p.setBrush(COL_AMBER);
      for (int i = 0; i < n; ++i) {
        const QPointF pt(xOf(i), yOf(m_ys[i]));
        p.setPen(Qt::NoPen);
        p.drawEllipse(pt, LINECHART_DOT_R, LINECHART_DOT_R);
        p.setPen(COL_TEXT);
        const QString t = fmtNum(m_ys[i], m_dec);
        const int tw = textWidth(fm, t);
        p.drawText(QPointF(pt.x() - tw / 2.0, pt.y() - LINECHART_DOT_R - 2), t);
      }
    }
  }

private:
  QVector<double> m_ys;
  QStringList m_labels;
  int m_dec = 2;
  bool m_hasRef = false;
  double m_ref = 0;
  QString m_msg;
};

// One column-spec'd table panel.
struct TablePanel {
  Panel p;
  QTableWidget *table = nullptr;
  int seq = 0;
  QString loadedFor;  // symbol / key the content belongs to
};

// Reader dialog (articles, filings, transcripts).
struct Reader {
  QPointer<QDialog> dlg;
  QPointer<QTextBrowser> text;
  QPointer<QLabel> status;
  QHBoxLayout *bar = nullptr;
};

// Paragraph list -> HTML.
static QString paragraphsHtml(const QJsonArray &paras) {
  QString html;
  for (const QJsonValue &v : paras)
    html += "<p>" + v.toString().toHtmlEscaped() + "</p>";
  return html;
}
#endif

#ifdef HEATMAP
// ---------------------------------------------------------------------------
// Heatmap: S&P 500 treemap (sector -> industry group -> ticker)
// ---------------------------------------------------------------------------

// Sector / group labels in the style of the reference market map. Yahoo's
// ~145 industries are merged into a few groups per sector so that every group
// has enough members for a readable box.
static const char *HM_S_TECH = "TECHNOLOGY";
static const char *HM_S_COMM = "COMMUNICATIONS";
static const char *HM_S_FIN = "FINANCE";
static const char *HM_S_HEALTH = "HEALTHCARE";
static const char *HM_S_CYC = "CONSUMER CYCLICALS";
static const char *HM_S_SVC = "CONSUMER SERVICES";
static const char *HM_S_NONCYC = "CONSUMER NON-CYCLICALS";
static const char *HM_S_IND = "INDUSTRIALS";
static const char *HM_S_ENERGY = "ENERGY";
static const char *HM_S_UTIL = "UTILITIES";
static const char *HM_S_MAT = "NON-ENERGY MATERIALS";
static const char *HM_S_RE = "REAL ESTATE";
static const char *HM_S_OTHER = "OTHER";

static const char *HM_G_SEMI = "SEMICONDUCTORS";
static const char *HM_G_SOFT = "SOFTWARE AND IT SERVICES";
static const char *HM_G_HW = "HARDWARE";
static const char *HM_G_ELEC = "ELECTRONIC COMPONENTS";
static const char *HM_G_INET = "INTERNET AND MEDIA";
static const char *HM_G_TEL = "TELECOM";
static const char *HM_G_BANK = "BANKING";
static const char *HM_G_INV = "INVESTMENT SERVICES";
static const char *HM_G_INS = "INSURANCE";
static const char *HM_G_CREDIT = "CREDIT AND PAYMENTS";
static const char *HM_G_PHARMA = "BIOPHARMACEUTICALS";
static const char *HM_G_HEQ = "HEALTHCARE EQUIPMENT";
static const char *HM_G_HSVC = "HEALTHCARE SERVICES";
static const char *HM_G_RETAIL = "MISCELLANEOUS RETAIL";
static const char *HM_G_AUTO = "AUTOS AND HOUSING";
static const char *HM_G_APPAREL = "APPAREL AND LEISURE";
static const char *HM_G_HOSP = "HOSPITALITY SERVICES";
static const char *HM_G_FOOD = "FOOD, BEVERAGE AND TOBACCO";
static const char *HM_G_HOUSE = "HOUSEHOLD PRODUCTS";
static const char *HM_G_STAPLES = "FOOD AND STAPLES RETAIL";
static const char *HM_G_MFG = "INDUSTRIAL MANUFACTURING";
static const char *HM_G_AERO = "AEROSPACE AND DEFENSE";
static const char *HM_G_TRANS = "TRANSPORTATION";
static const char *HM_G_ISVC = "INDUSTRIAL SERVICES";
static const char *HM_G_UPSTREAM = "UPSTREAM ENERGY";
static const char *HM_G_DOWNSTREAM = "DOWNSTREAM AND INTEGRATED";
static const char *HM_G_ELECU = "ELECTRIC UTILITIES";
static const char *HM_G_MULTIU = "GAS, WATER AND MULTI-UTILITIES";
static const char *HM_G_CHEM = "CHEMICALS";
static const char *HM_G_MINE = "MINING AND METALS";
static const char *HM_G_BMAT = "BUILDING MATERIALS AND PACKAGING";
static const char *HM_G_REIT_SPEC = "SPECIALTY AND INDUSTRIAL REITS";
static const char *HM_G_REIT_PROP = "RESIDENTIAL, RETAIL AND OFFICE REITS";
static const char *HM_G_REIT_HC = "HEALTHCARE REITS AND SERVICES";
static const char *HM_G_OTHER = "OTHER";

// Yahoo industry -> (sector, group).
struct HmIndustry {
  const char *industry;
  const char *sector;
  const char *group;
};
static const HmIndustry HM_INDUSTRIES[] = {
    {"Semiconductors", HM_S_TECH, HM_G_SEMI},
    {"Semiconductor Equipment & Materials", HM_S_TECH, HM_G_SEMI},
    {"Software - Infrastructure", HM_S_TECH, HM_G_SOFT},
    {"Software - Application", HM_S_TECH, HM_G_SOFT},
    {"Information Technology Services", HM_S_TECH, HM_G_SOFT},
    {"Consumer Electronics", HM_S_TECH, HM_G_HW},
    {"Computer Hardware", HM_S_TECH, HM_G_HW},
    {"Communication Equipment", HM_S_TECH, HM_G_HW},
    {"Electronic Components", HM_S_TECH, HM_G_ELEC},
    {"Scientific & Technical Instruments", HM_S_TECH, HM_G_ELEC},
    {"Electronics & Computer Distribution", HM_S_TECH, HM_G_ELEC},
    {"Solar", HM_S_TECH, HM_G_ELEC},
    {"Internet Content & Information", HM_S_COMM, HM_G_INET},
    {"Entertainment", HM_S_COMM, HM_G_INET},
    {"Electronic Gaming & Multimedia", HM_S_COMM, HM_G_INET},
    {"Advertising Agencies", HM_S_COMM, HM_G_INET},
    {"Publishing", HM_S_COMM, HM_G_INET},
    {"Broadcasting", HM_S_COMM, HM_G_INET},
    {"Telecom Services", HM_S_COMM, HM_G_TEL},
    {"Banks - Diversified", HM_S_FIN, HM_G_BANK},
    {"Banks - Regional", HM_S_FIN, HM_G_BANK},
    {"Capital Markets", HM_S_FIN, HM_G_INV},
    {"Asset Management", HM_S_FIN, HM_G_INV},
    {"Financial Data & Stock Exchanges", HM_S_FIN, HM_G_INV},
    {"Insurance - Diversified", HM_S_FIN, HM_G_INS},
    {"Insurance - Property & Casualty", HM_S_FIN, HM_G_INS},
    {"Insurance - Life", HM_S_FIN, HM_G_INS},
    {"Insurance - Specialty", HM_S_FIN, HM_G_INS},
    {"Insurance - Reinsurance", HM_S_FIN, HM_G_INS},
    {"Insurance Brokers", HM_S_FIN, HM_G_INS},
    {"Credit Services", HM_S_FIN, HM_G_CREDIT},
    {"Financial Conglomerates", HM_S_FIN, HM_G_CREDIT},
    {"Mortgage Finance", HM_S_FIN, HM_G_CREDIT},
    {"Drug Manufacturers - General", HM_S_HEALTH, HM_G_PHARMA},
    {"Drug Manufacturers - Specialty & Generic", HM_S_HEALTH, HM_G_PHARMA},
    {"Biotechnology", HM_S_HEALTH, HM_G_PHARMA},
    {"Medical Devices", HM_S_HEALTH, HM_G_HEQ},
    {"Medical Instruments & Supplies", HM_S_HEALTH, HM_G_HEQ},
    {"Diagnostics & Research", HM_S_HEALTH, HM_G_HEQ},
    {"Healthcare Plans", HM_S_HEALTH, HM_G_HSVC},
    {"Medical Care Facilities", HM_S_HEALTH, HM_G_HSVC},
    {"Medical Distribution", HM_S_HEALTH, HM_G_HSVC},
    {"Health Information Services", HM_S_HEALTH, HM_G_HSVC},
    {"Pharmaceutical Retailers", HM_S_HEALTH, HM_G_HSVC},
    {"Internet Retail", HM_S_CYC, HM_G_RETAIL},
    {"Home Improvement Retail", HM_S_CYC, HM_G_RETAIL},
    {"Apparel Retail", HM_S_CYC, HM_G_RETAIL},
    {"Specialty Retail", HM_S_CYC, HM_G_RETAIL},
    {"Department Stores", HM_S_CYC, HM_G_RETAIL},
    {"Auto & Truck Dealerships", HM_S_CYC, HM_G_RETAIL},
    {"Auto Manufacturers", HM_S_CYC, HM_G_AUTO},
    {"Auto Parts", HM_S_CYC, HM_G_AUTO},
    {"Residential Construction", HM_S_CYC, HM_G_AUTO},
    {"Furnishings, Fixtures & Appliances", HM_S_CYC, HM_G_AUTO},
    {"Recreational Vehicles", HM_S_CYC, HM_G_AUTO},
    {"Footwear & Accessories", HM_S_CYC, HM_G_APPAREL},
    {"Apparel Manufacturing", HM_S_CYC, HM_G_APPAREL},
    {"Leisure", HM_S_CYC, HM_G_APPAREL},
    {"Luxury Goods", HM_S_CYC, HM_G_APPAREL},
    {"Textile Manufacturing", HM_S_CYC, HM_G_APPAREL},
    {"Restaurants", HM_S_SVC, HM_G_HOSP},
    {"Lodging", HM_S_SVC, HM_G_HOSP},
    {"Resorts & Casinos", HM_S_SVC, HM_G_HOSP},
    {"Travel Services", HM_S_SVC, HM_G_HOSP},
    {"Gambling", HM_S_SVC, HM_G_HOSP},
    {"Beverages - Non-Alcoholic", HM_S_NONCYC, HM_G_FOOD},
    {"Beverages - Brewers", HM_S_NONCYC, HM_G_FOOD},
    {"Beverages - Wineries & Distilleries", HM_S_NONCYC, HM_G_FOOD},
    {"Confectioners", HM_S_NONCYC, HM_G_FOOD},
    {"Packaged Foods", HM_S_NONCYC, HM_G_FOOD},
    {"Farm Products", HM_S_NONCYC, HM_G_FOOD},
    {"Food Distribution", HM_S_NONCYC, HM_G_FOOD},
    {"Tobacco", HM_S_NONCYC, HM_G_FOOD},
    {"Household & Personal Products", HM_S_NONCYC, HM_G_HOUSE},
    {"Discount Stores", HM_S_NONCYC, HM_G_STAPLES},
    {"Grocery Stores", HM_S_NONCYC, HM_G_STAPLES},
    {"Specialty Industrial Machinery", HM_S_IND, HM_G_MFG},
    {"Farm & Heavy Construction Machinery", HM_S_IND, HM_G_MFG},
    {"Electrical Equipment & Parts", HM_S_IND, HM_G_MFG},
    {"Building Products & Equipment", HM_S_IND, HM_G_MFG},
    {"Tools & Accessories", HM_S_IND, HM_G_MFG},
    {"Conglomerates", HM_S_IND, HM_G_MFG},
    {"Metal Fabrication", HM_S_IND, HM_G_MFG},
    {"Pollution & Treatment Controls", HM_S_IND, HM_G_MFG},
    {"Aerospace & Defense", HM_S_IND, HM_G_AERO},
    {"Railroads", HM_S_IND, HM_G_TRANS},
    {"Integrated Freight & Logistics", HM_S_IND, HM_G_TRANS},
    {"Trucking", HM_S_IND, HM_G_TRANS},
    {"Airlines", HM_S_IND, HM_G_TRANS},
    {"Marine Shipping", HM_S_IND, HM_G_TRANS},
    {"Airports & Air Services", HM_S_IND, HM_G_TRANS},
    {"Industrial Distribution", HM_S_IND, HM_G_ISVC},
    {"Specialty Business Services", HM_S_IND, HM_G_ISVC},
    {"Waste Management", HM_S_IND, HM_G_ISVC},
    {"Rental & Leasing Services", HM_S_IND, HM_G_ISVC},
    {"Consulting Services", HM_S_IND, HM_G_ISVC},
    {"Security & Protection Services", HM_S_IND, HM_G_ISVC},
    {"Business Equipment & Supplies", HM_S_IND, HM_G_ISVC},
    {"Engineering & Construction", HM_S_IND, HM_G_ISVC},
    {"Staffing & Employment Services", HM_S_IND, HM_G_ISVC},
    {"Infrastructure Operations", HM_S_IND, HM_G_ISVC},
    {"Oil & Gas E&P", HM_S_ENERGY, HM_G_UPSTREAM},
    {"Oil & Gas Equipment & Services", HM_S_ENERGY, HM_G_UPSTREAM},
    {"Oil & Gas Drilling", HM_S_ENERGY, HM_G_UPSTREAM},
    {"Oil & Gas Integrated", HM_S_ENERGY, HM_G_DOWNSTREAM},
    {"Oil & Gas Refining & Marketing", HM_S_ENERGY, HM_G_DOWNSTREAM},
    {"Oil & Gas Midstream", HM_S_ENERGY, HM_G_DOWNSTREAM},
    {"Thermal Coal", HM_S_ENERGY, HM_G_DOWNSTREAM},
    {"Uranium", HM_S_ENERGY, HM_G_DOWNSTREAM},
    {"Utilities - Regulated Electric", HM_S_UTIL, HM_G_ELECU},
    {"Utilities - Independent Power Producers", HM_S_UTIL, HM_G_ELECU},
    {"Utilities - Renewable", HM_S_UTIL, HM_G_ELECU},
    {"Utilities - Regulated Gas", HM_S_UTIL, HM_G_MULTIU},
    {"Utilities - Regulated Water", HM_S_UTIL, HM_G_MULTIU},
    {"Utilities - Diversified", HM_S_UTIL, HM_G_MULTIU},
    {"Specialty Chemicals", HM_S_MAT, HM_G_CHEM},
    {"Chemicals", HM_S_MAT, HM_G_CHEM},
    {"Agricultural Inputs", HM_S_MAT, HM_G_CHEM},
    {"Gold", HM_S_MAT, HM_G_MINE},
    {"Silver", HM_S_MAT, HM_G_MINE},
    {"Copper", HM_S_MAT, HM_G_MINE},
    {"Steel", HM_S_MAT, HM_G_MINE},
    {"Aluminum", HM_S_MAT, HM_G_MINE},
    {"Coking Coal", HM_S_MAT, HM_G_MINE},
    {"Other Industrial Metals & Mining", HM_S_MAT, HM_G_MINE},
    {"Other Precious Metals & Mining", HM_S_MAT, HM_G_MINE},
    {"Building Materials", HM_S_MAT, HM_G_BMAT},
    {"Packaging & Containers", HM_S_MAT, HM_G_BMAT},
    {"Paper & Paper Products", HM_S_MAT, HM_G_BMAT},
    {"Lumber & Wood Production", HM_S_MAT, HM_G_BMAT},
    {"REIT - Specialty", HM_S_RE, HM_G_REIT_SPEC},
    {"REIT - Industrial", HM_S_RE, HM_G_REIT_SPEC},
    {"REIT - Diversified", HM_S_RE, HM_G_REIT_SPEC},
    {"REIT - Residential", HM_S_RE, HM_G_REIT_PROP},
    {"REIT - Retail", HM_S_RE, HM_G_REIT_PROP},
    {"REIT - Office", HM_S_RE, HM_G_REIT_PROP},
    {"REIT - Hotel & Motel", HM_S_RE, HM_G_REIT_PROP},
    {"REIT - Mortgage", HM_S_RE, HM_G_REIT_PROP},
    {"REIT - Healthcare Facilities", HM_S_RE, HM_G_REIT_HC},
    {"Real Estate Services", HM_S_RE, HM_G_REIT_HC},
    {"Real Estate - Development", HM_S_RE, HM_G_REIT_HC},
    {"Real Estate - Diversified", HM_S_RE, HM_G_REIT_HC},
};

// Yahoo sector or GICS sector -> sector label, for industries not listed above.
struct HmSectorName {
  const char *name;
  const char *sector;
};
static const HmSectorName HM_SECTOR_NAMES[] = {
    {"Technology", HM_S_TECH},
    {"Information Technology", HM_S_TECH},
    {"Communication Services", HM_S_COMM},
    {"Financial Services", HM_S_FIN},
    {"Financials", HM_S_FIN},
    {"Healthcare", HM_S_HEALTH},
    {"Health Care", HM_S_HEALTH},
    {"Consumer Cyclical", HM_S_CYC},
    {"Consumer Discretionary", HM_S_CYC},
    {"Consumer Defensive", HM_S_NONCYC},
    {"Consumer Staples", HM_S_NONCYC},
    {"Industrials", HM_S_IND},
    {"Energy", HM_S_ENERGY},
    {"Utilities", HM_S_UTIL},
    {"Basic Materials", HM_S_MAT},
    {"Materials", HM_S_MAT},
    {"Real Estate", HM_S_RE},
};

// Sector + group of a ticker from its Yahoo sector / industry, falling back
// to the GICS sector of the S&P 500 list when Yahoo has no profile.
static void hmClassify(const QString &ySector, const QString &yIndustry,
                       const QString &gicsSector, QString &sector,
                       QString &group) {
  static QHash<QString, QPair<QString, QString>> byIndustry;
  static QHash<QString, QString> bySector;
  if (byIndustry.isEmpty()) {
    for (const HmIndustry &m : HM_INDUSTRIES)
      byIndustry.insert(QString::fromLatin1(m.industry).toLower(),
                        qMakePair(QString::fromLatin1(m.sector),
                                  QString::fromLatin1(m.group)));
    for (const HmSectorName &m : HM_SECTOR_NAMES)
      bySector.insert(QString::fromLatin1(m.name).toLower(),
                      QString::fromLatin1(m.sector));
  }
  const QString ind = yIndustry.trimmed();
  const auto it = byIndustry.constFind(ind.toLower());
  if (it != byIndustry.constEnd()) {
    sector = it->first;
    group = it->second;
    return;
  }
  sector = bySector.value(ySector.trimmed().toLower());
  if (sector.isEmpty()) sector = bySector.value(gicsSector.trimmed().toLower());
  if (sector.isEmpty()) sector = HM_S_OTHER;
  group = ind.isEmpty() ? QString(HM_G_OTHER) : ind.toUpper();
}

static QColor hmLerp(const QColor &a, const QColor &b, double f) {
  return QColor(int(a.red() + (b.red() - a.red()) * f + 0.5),
                int(a.green() + (b.green() - a.green()) * f + 0.5),
                int(a.blue() + (b.blue() - a.blue()) * f + 0.5));
}

// Neutral gray at 0 %, full green / red at +-HM_COLOR_RANGE_PCT.
static QColor hmChangeColor(double pct) {
  const double f = qBound(0.0, std::fabs(pct) / HM_COLOR_RANGE_PCT, 1.0);
  return hmLerp(HM_NEUTRAL, pct >= 0 ? HM_UP : HM_DOWN, f);
}

// Squarified treemap: one rectangle per weight (same order), filling rect.
static QVector<QRectF> hmSquarify(const QVector<double> &weights,
                                  const QRectF &rect) {
  const int n = weights.size();
  QVector<QRectF> out(n);
  double total = 0;
  for (double w : weights) total += qMax(0.0, w);
  if (n == 0 || total <= 0 || rect.width() <= 0 || rect.height() <= 0)
    return out;
  QVector<int> order(n);
  for (int i = 0; i < n; ++i) order[i] = i;
  std::sort(order.begin(), order.end(),
            [&weights](int a, int b) { return weights[a] > weights[b]; });
  const double scale = rect.width() * rect.height() / total;
  const double inf = std::numeric_limits<double>::max();
  QRectF r = rect;
  int i = 0;
  while (i < n) {
    const double side = qMin(r.width(), r.height());
    if (side <= 0) break;
    // Grow the row while the worst aspect ratio improves.
    int j = i;
    double sum = 0, best = inf, lo = 0, hi = 0;
    while (j < n) {
      const double a = qMax(0.0, weights[order[j]]) * scale;
      const double s = sum + a;
      const double nlo = j == i ? a : qMin(lo, a);
      const double nhi = j == i ? a : qMax(hi, a);
      const double s2 = s * s, side2 = side * side;
      const double worst =
          nlo > 0 ? qMax(side2 * nhi / s2, s2 / (side2 * nlo)) : inf;
      if (j > i && worst > best) break;
      best = worst;
      sum = s;
      lo = nlo;
      hi = nhi;
      ++j;
    }
    const double thick = sum / side;
    const bool column = r.width() >= r.height();
    double pos = 0;
    for (int k = i; k < j; ++k) {
      const double a = qMax(0.0, weights[order[k]]) * scale;
      const double len = thick > 0 ? a / thick : 0;
      out[order[k]] = column ? QRectF(r.left(), r.top() + pos, thick, len)
                             : QRectF(r.left() + pos, r.top(), len, thick);
      pos += len;
    }
    if (column) r.setLeft(r.left() + thick);
    else r.setTop(r.top() + thick);
    i = j;
  }
  return out;
}

struct HmTile {
  QString sym, name, sector, group;
  double chg = 0, cap = 0, price = 0;
  bool hasChg = false, hasCap = false, hasPrice = false;
  QRectF r;  // layout coordinates (zoom 1 == widget pixels)
};

struct HmGroup {
  QString name;
  QVector<int> tiles;  // indexes into the tile list
  QRectF r, hdr;
};

struct HmSector {
  QString name;
  QVector<HmGroup> groups;
  QRectF r, hdr;
};

// The treemap itself. Layout coordinates equal widget pixels at zoom 1; the
// view transform (m_scale, m_off) maps them to the screen, so zooming keeps
// the layout and only re-renders (labels appear as tiles grow).
class HeatmapView : public QWidget {
public:
  enum SizeMode { SizeCap, SizeMove };

  std::function<void(const QString &)> onOpen;  // double-click on a tile

  explicit HeatmapView(QWidget *parent = nullptr) : QWidget(parent) {
    setMouseTracking(true);
    setFocusPolicy(Qt::StrongFocus);
    setAttribute(Qt::WA_OpaquePaintEvent);
    m_font = QFontDatabase::systemFont(QFontDatabase::FixedFont);
    m_font.setBold(true);
  }

  void setTiles(const QVector<HmTile> &tiles) {
    m_tiles = tiles;
    m_index.clear();
    for (int i = 0; i < m_tiles.size(); ++i) m_index.insert(m_tiles[i].sym, i);
    m_hover = -1;
    regroup();
    relayout();
  }

  // Applies one quote-batch item; false when the symbol is not on the map.
  bool applyQuote(const QJsonObject &q) {
    const auto it = m_index.constFind(jstr(q, "symbol").trimmed().toUpper());
    if (it == m_index.constEnd()) return false;
    HmTile &t = m_tiles[it.value()];
    double v = 0;
    if (jnum(q.value("changePercent"), v)) {
      t.chg = v;
      t.hasChg = true;
    }
    if (jnum(q.value("price"), v)) {
      t.price = v;
      t.hasPrice = true;
    }
    if (jnum(q.value("marketCap"), v) && v > 0) {
      t.cap = v;
      t.hasCap = true;
    }
    return true;
  }

  int tileCount() const { return m_tiles.size(); }

  QStringList symbols() const {
    QStringList out;
    for (const HmTile &t : m_tiles) out << t.sym;
    return out;
  }

  void counts(int &up, int &down, int &quoted) const {
    up = down = quoted = 0;
    for (const HmTile &t : m_tiles) {
      if (!t.hasChg) continue;
      ++quoted;
      if (t.chg > 0) ++up;
      else if (t.chg < 0) ++down;
    }
  }

  SizeMode sizeMode() const { return m_mode; }
  void setSizeMode(SizeMode m) {
    if (m == m_mode) return;
    m_mode = m;
    relayout();
  }

  void setMessage(const QString &m) {
    m_message = m;
    update();
  }

  void fit() {
    m_scale = 1;
    m_off = QPointF();
    update();
  }

  void zoomBy(double factor) { zoomAt(QPointF(width() / 2.0, height() / 2.0), factor); }

  // Recomputes all rectangles for the current size mode and widget size.
  void relayout() {
    const int n = m_tiles.size();
    QVector<double> w(n);
    double maxW = 0;
    for (int i = 0; i < n; ++i) {
      w[i] = tileWeight(m_tiles[i]);
      maxW = qMax(maxW, w[i]);
    }
    if (maxW <= 0) {  // nothing to size by yet (no quotes): equal tiles
      w.fill(1.0);
      maxW = 1.0;
    }
    const double floorW = maxW * HM_MIN_WEIGHT_FRAC;
    for (double &x : w) x = qMax(x, floorW);

    QVector<double> sw(m_sectors.size(), 0.0);
    for (int s = 0; s < m_sectors.size(); ++s)
      for (const HmGroup &g : m_sectors[s].groups)
        for (int t : g.tiles) sw[s] += w[t];
    const QVector<QRectF> sr = hmSquarify(sw, QRectF(0, 0, width(), height()));
    for (int s = 0; s < m_sectors.size(); ++s) {
      HmSector &sec = m_sectors[s];
      sec.r = sr[s];
      QRectF body = sec.r.adjusted(HM_SECTOR_GAP, HM_SECTOR_GAP, -HM_SECTOR_GAP,
                                   -HM_SECTOR_GAP);
      sec.hdr = splitHeader(body, HM_SECTOR_HDR_H);
      QVector<double> gw(sec.groups.size(), 0.0);
      for (int g = 0; g < sec.groups.size(); ++g)
        for (int t : sec.groups[g].tiles) gw[g] += w[t];
      const QVector<QRectF> gr = hmSquarify(gw, body);
      for (int g = 0; g < sec.groups.size(); ++g) {
        HmGroup &grp = sec.groups[g];
        grp.r = gr[g];
        QRectF gbody = grp.r.adjusted(HM_GROUP_GAP, HM_GROUP_GAP, -HM_GROUP_GAP,
                                      -HM_GROUP_GAP);
        grp.hdr = splitHeader(gbody, HM_GROUP_HDR_H);
        QVector<double> tw(grp.tiles.size());
        for (int k = 0; k < grp.tiles.size(); ++k) tw[k] = w[grp.tiles[k]];
        const QVector<QRectF> tr = hmSquarify(tw, gbody);
        for (int k = 0; k < grp.tiles.size(); ++k) m_tiles[grp.tiles[k]].r = tr[k];
      }
    }
    clampView();
    update();
  }

protected:
  void paintEvent(QPaintEvent *) override {
    QPainter p(this);
    p.fillRect(rect(), COL_BG);
    if (m_tiles.isEmpty()) {
      p.setPen(COL_DIM);
      p.drawText(rect(), Qt::AlignCenter | Qt::TextWordWrap, m_message);
      return;
    }
    p.setRenderHint(QPainter::TextAntialiasing);
    const QRectF view = rect();
    for (const HmSector &sec : m_sectors) {
      if (!toScreen(sec.r).intersects(view)) continue;
      for (const HmGroup &grp : sec.groups) {
        if (!toScreen(grp.r).intersects(view)) continue;
        for (int t : grp.tiles) paintTile(p, m_tiles[t], view);
        paintHeader(p, grp.hdr, grp.name, COL_DIM, view);
      }
      paintHeader(p, sec.hdr, sec.name, COL_TEXT, view);
    }
    if (m_hover >= 0 && m_hover < m_tiles.size()) {
      const qreal inset = HM_HOVER_PEN_W / 2.0;
      p.setPen(QPen(COL_AMBER, HM_HOVER_PEN_W));
      p.setBrush(Qt::NoBrush);
      p.drawRect(toScreen(m_tiles[m_hover].r).adjusted(inset, inset, -inset, -inset));
    }
  }

  void resizeEvent(QResizeEvent *e) override {
    QWidget::resizeEvent(e);
    relayout();
  }

  void wheelEvent(QWheelEvent *e) override {
#if QT_VERSION >= QT_VERSION_CHECK(5, 14, 0)
    const QPointF pos = e->position();
#else
    const QPointF pos = e->posF();
#endif
    // Trackpads (and other precise devices) report pixel deltas: two-finger
    // scrolling pans, with Ctrl / Cmd it zooms. Mouse wheels always zoom.
    const QPoint px = e->pixelDelta();
    if (!px.isNull() && !(e->modifiers() & Qt::ControlModifier)) {
      m_off += QPointF(px);
      clampView();
      update();
      updateHover(pos, QCursor::pos());
      e->accept();
      return;
    }
    const int dy = e->angleDelta().y();
    if (dy == 0) {
      e->ignore();
      return;
    }
    zoomAt(pos, std::pow(HM_WHEEL_ZOOM_BASE, dy));
    updateHover(pos, QCursor::pos());
    e->accept();
  }

  bool event(QEvent *e) override {
#ifndef QT_NO_GESTURES
    // macOS trackpad pinch / two-finger double-tap.
    if (e->type() == QEvent::NativeGesture) {
      QNativeGestureEvent *g = static_cast<QNativeGestureEvent *>(e);
      if (g->gestureType() == Qt::ZoomNativeGesture) {
        zoomAt(g->localPos(), 1.0 + g->value());
        return true;
      }
      if (g->gestureType() == Qt::SmartZoomNativeGesture) {
        fit();
        return true;
      }
    }
#endif
    return QWidget::event(e);
  }

  void mousePressEvent(QMouseEvent *e) override {
    if (e->button() != Qt::LeftButton) return;
    m_dragging = true;
    m_dragMoved = false;
    m_pressPos = e->localPos();
    m_pressOff = m_off;
  }

  void mouseMoveEvent(QMouseEvent *e) override {
    if (m_dragging && (e->buttons() & Qt::LeftButton)) {
      const QPointF d = e->localPos() - m_pressPos;
      if (!m_dragMoved && d.manhattanLength() >= HM_DRAG_THRESHOLD_PX) {
        m_dragMoved = true;
        setCursor(Qt::ClosedHandCursor);
        QToolTip::hideText();
      }
      if (m_dragMoved) {
        m_off = m_pressOff + d;
        clampView();
        update();
      }
      return;
    }
    updateHover(e->localPos(), e->globalPos());
  }

  void mouseReleaseEvent(QMouseEvent *e) override {
    if (e->button() != Qt::LeftButton) return;
    m_dragging = false;
    if (m_dragMoved) unsetCursor();
    updateHover(e->localPos(), e->globalPos());
  }

  void mouseDoubleClickEvent(QMouseEvent *e) override {
    if (e->button() != Qt::LeftButton) return;
    const int i = hit(e->localPos());
    if (i >= 0 && onOpen) onOpen(m_tiles[i].sym);
  }

  void leaveEvent(QEvent *) override {
    if (m_hover >= 0) {
      m_hover = -1;
      update();
    }
    QToolTip::hideText();
  }

  void keyPressEvent(QKeyEvent *e) override {
    switch (e->key()) {
    case Qt::Key_0: fit(); return;
    case Qt::Key_Plus:
    case Qt::Key_Equal: zoomBy(HM_KEY_ZOOM); return;
    case Qt::Key_Minus: zoomBy(1.0 / HM_KEY_ZOOM); return;
    default: QWidget::keyPressEvent(e);
    }
  }

private:
  double tileWeight(const HmTile &t) const {
    if (m_mode == SizeMove) return t.hasChg ? std::fabs(t.chg) : 0.0;
    return t.hasCap ? t.cap : 0.0;
  }

  // Takes a header strip off the top of box when the box is tall enough.
  static QRectF splitHeader(QRectF &box, double h) {
    if (box.height() < h * HM_HDR_MIN_ROWS || box.width() <= 0) return QRectF();
    const QRectF hdr(box.left(), box.top(), box.width(), h);
    box.setTop(box.top() + h);
    return hdr;
  }

  // Sectors / groups in first-seen order (the layout sorts them by weight).
  void regroup() {
    m_sectors.clear();
    QHash<QString, int> secIdx, grpIdx;
    for (int i = 0; i < m_tiles.size(); ++i) {
      const HmTile &t = m_tiles[i];
      int s = secIdx.value(t.sector, -1);
      if (s < 0) {
        s = m_sectors.size();
        secIdx.insert(t.sector, s);
        HmSector sec;
        sec.name = t.sector;
        m_sectors << sec;
      }
      const QString gk = t.sector + QChar(0x1f) + t.group;
      int g = grpIdx.value(gk, -1);
      if (g < 0) {
        g = m_sectors[s].groups.size();
        grpIdx.insert(gk, g);
        HmGroup grp;
        grp.name = t.group;
        m_sectors[s].groups << grp;
      }
      m_sectors[s].groups[g].tiles << i;
    }
  }

  QRectF toScreen(const QRectF &r) const {
    return QRectF(r.x() * m_scale + m_off.x(), r.y() * m_scale + m_off.y(),
                  r.width() * m_scale, r.height() * m_scale);
  }
  QPointF toLayout(const QPointF &p) const { return (p - m_off) / m_scale; }

  // Zoom 1 shows everything; the content never leaves the viewport.
  void clampView() {
    m_scale = qBound(1.0, m_scale, HM_ZOOM_MAX);
    const double minX = width() - width() * m_scale;
    const double minY = height() - height() * m_scale;
    m_off.setX(qBound(minX, m_off.x(), 0.0));
    m_off.setY(qBound(minY, m_off.y(), 0.0));
  }

  void zoomAt(const QPointF &pos, double factor) {
    const QPointF anchor = toLayout(pos);
    m_scale = qBound(1.0, m_scale * factor, HM_ZOOM_MAX);
    m_off = pos - anchor * m_scale;
    clampView();
    update();
  }

  int hit(const QPointF &pos) const {
    const QPointF p = toLayout(pos);
    for (int i = 0; i < m_tiles.size(); ++i)
      if (m_tiles[i].r.contains(p)) return i;
    return -1;
  }

  void updateHover(const QPointF &pos, const QPoint &global) {
    const int i = hit(pos);
    if (i != m_hover) {
      m_hover = i;
      update();
    }
    if (i < 0) {
      QToolTip::hideText();
      return;
    }
    const HmTile &t = m_tiles[i];
    QString tip = QString("<b>%1</b> %2<br>%3 / %4")
                      .arg(t.sym.toHtmlEscaped(), t.name.toHtmlEscaped(),
                           t.sector.toHtmlEscaped(), t.group.toHtmlEscaped());
    if (t.hasPrice) tip += "<br>Price " + fmtPrice(t.price);
    if (t.hasChg) tip += "&nbsp;&nbsp;" + fmtPctSigned(t.chg);
    if (t.hasCap) tip += "<br>Market cap " + fmtBig(t.cap);
    tip += "<br><i>double-click to open</i>";
    QToolTip::showText(global, tip, this);
  }

  void paintHeader(QPainter &p, const QRectF &hdr, const QString &name,
                   const QColor &color, const QRectF &view) {
    if (hdr.isEmpty()) return;
    const QRectF s = toScreen(hdr);
    if (!s.intersects(view) || s.height() < HM_HDR_FONT_MIN_PX) return;
    QFont f = m_font;
    f.setPixelSize(qBound(HM_HDR_FONT_MIN_PX, int(s.height() * HM_HDR_FONT_FRAC),
                          HM_HDR_FONT_MAX_PX));
    p.setFont(f);
    p.setPen(color);
    const QString txt =
        QFontMetrics(f).elidedText(name, Qt::ElideRight, int(s.width()));
    p.drawText(s, Qt::AlignLeft | Qt::AlignVCenter, txt);
  }

  void paintTile(QPainter &p, const HmTile &t, const QRectF &view) {
    const qreal g = HM_TILE_GAP_PX / 2.0;
    const QRectF s = toScreen(t.r).adjusted(g, g, -g, -g);
    if (s.width() <= 0 || s.height() <= 0 || !s.intersects(view)) return;
    p.fillRect(s, t.hasChg ? hmChangeColor(t.chg) : HM_NODATA);

    const int len = qMax(t.sym.size(), HM_TICKER_MIN_CHARS);
    int px = int(qMin(s.height() * HM_TICKER_H_FRAC,
                      s.width() / (len * HM_CHAR_W_FRAC)));
    if (px < HM_TILE_FONT_MIN_PX) return;
    px = qMin(px, HM_TILE_FONT_MAX_PX);
    const int pctPx = qMax(HM_TILE_FONT_MIN_PX, int(px * HM_PCT_FONT_FRAC));
    const int namePx = int(px * HM_NAME_FONT_FRAC);
    const bool two = t.hasChg && s.height() >= px * HM_TWO_LINE_FRAC;
    const bool three = two && namePx >= HM_TILE_FONT_MIN_PX &&
                       s.height() >= px * HM_THREE_LINE_FRAC;
    const double hTick = px * HM_LINE_SPACING;
    const double hPct = two ? pctPx * HM_LINE_SPACING : 0;
    const double hName = three ? namePx * HM_LINE_SPACING : 0;
    double y = s.center().y() - (hTick + hPct + hName) / 2.0;

    p.setPen(HM_TILE_TEXT);
    QFont f = m_font;
    f.setPixelSize(px);
    p.setFont(f);
    p.drawText(QRectF(s.left(), y, s.width(), hTick), Qt::AlignCenter, t.sym);
    y += hTick;
    if (two) {
      QFont pf = m_font;
      pf.setBold(false);
      pf.setPixelSize(pctPx);
      p.setFont(pf);
      p.drawText(QRectF(s.left(), y, s.width(), hPct), Qt::AlignCenter,
                 fmtSigned(t.chg, HM_PCT_DECIMALS) + "%");
      y += hPct;
    }
    if (three) {
      QFont nf = m_font;
      nf.setBold(false);
      nf.setPixelSize(namePx);
      p.setFont(nf);
      const QString name = QFontMetrics(nf).elidedText(
          t.name, Qt::ElideRight, int(s.width() - 2 * px * HM_CHAR_W_FRAC));
      p.drawText(QRectF(s.left(), y, s.width(), hName), Qt::AlignCenter, name);
    }
  }

  QVector<HmTile> m_tiles;
  QHash<QString, int> m_index;  // symbol -> tile
  QVector<HmSector> m_sectors;
  SizeMode m_mode = SizeCap;
  QString m_message;
  QFont m_font;
  double m_scale = 1.0;
  QPointF m_off;
  int m_hover = -1;
  bool m_dragging = false;
  bool m_dragMoved = false;
  QPointF m_pressPos;
  QPointF m_pressOff;
};

// Color scale of the map: -3 % .. +3 % in HM_LEGEND_STEPS boxes.
class HeatLegend : public QWidget {
public:
  HeatLegend() {
    setFixedSize(HM_LEGEND_BOX_W * HM_LEGEND_STEPS, HM_LEGEND_H);
    setToolTip("Day change; colors saturate beyond the outer boxes");
  }

protected:
  void paintEvent(QPaintEvent *) override {
    QPainter p(this);
    QFont f = font();
    f.setPixelSize(HM_LEGEND_FONT_PX);
    p.setFont(f);
    const double step = 2.0 * HM_COLOR_RANGE_PCT / (HM_LEGEND_STEPS - 1);
    for (int i = 0; i < HM_LEGEND_STEPS; ++i) {
      const double v = -HM_COLOR_RANGE_PCT + i * step;
      const QRect r(i * HM_LEGEND_BOX_W, 0, HM_LEGEND_BOX_W, HM_LEGEND_H);
      p.fillRect(r.adjusted(0, 0, -1, 0), hmChangeColor(v));
      const int iv = qRound(v);
      p.setPen(HM_TILE_TEXT);
      p.drawText(r, Qt::AlignCenter,
                 (iv > 0 ? QStringLiteral("+") : QString()) + QString::number(iv) + "%");
    }
  }
};

// Heatmap page: control bar (size mode, fit, reload, legend, progress) over
// the map. Loads the S&P 500 members once, then quotes in chunks of
// HM_QUOTE_CHUNK (the map fills in as they arrive), refreshed while shown.
class HeatmapPage : public QWidget {
public:
  std::function<void()> onClose;
  std::function<void(const QString &)> onOpen;
  std::function<void()> onModeChanged;

  explicit HeatmapPage(Api *api) : m_api(api) {
    QVBoxLayout *v = new QVBoxLayout(this);
    v->setContentsMargins(0, 0, 0, 0);
    v->setSpacing(0);

    QWidget *bar = new QWidget;
    bar->setObjectName("panelBar");
    QHBoxLayout *h = new QHBoxLayout(bar);
    h->setContentsMargins(PANEL_PAD, PANEL_PAD, PANEL_PAD, PANEL_PAD);
    h->setSpacing(PANEL_SPACING);
    QLabel *title = new QLabel(HEATMAP_TITLE);
    title->setObjectName("panelTitle");
    h->addWidget(title);
    QLabel *sizeLbl = new QLabel("SIZE");
    sizeLbl->setObjectName("panelStatus");
    h->addWidget(sizeLbl);
    m_capBtn = new QPushButton("MKT CAP");
    m_capBtn->setCheckable(true);
    m_capBtn->setToolTip("Tile size: market capitalization");
    m_moveBtn = new QPushButton("MOVERS");
    m_moveBtn->setCheckable(true);
    m_moveBtn->setToolTip("Tile size: size of today's move (biggest movers largest)");
    QObject::connect(m_capBtn, &QPushButton::clicked, this,
                     [this]() { pickMode(HeatmapView::SizeCap); });
    QObject::connect(m_moveBtn, &QPushButton::clicked, this,
                     [this]() { pickMode(HeatmapView::SizeMove); });
    h->addWidget(m_capBtn);
    h->addWidget(m_moveBtn);
    QPushButton *fit = new QPushButton("FIT");
    fit->setToolTip("Reset zoom (0 / two-finger double-tap)");
    h->addWidget(fit);
    QPushButton *reload = new QPushButton("RELOAD");
    reload->setToolTip("Reload the S&P 500 members and quotes");
    h->addWidget(reload);
    h->addSpacing(PANEL_SPACING);
    h->addWidget(new HeatLegend);
    h->addStretch(1);
    m_progress = new QProgressBar;
    m_progress->setFixedSize(HM_PROGRESS_W, HM_PROGRESS_H);
    m_progress->setTextVisible(true);
    m_progress->setVisible(false);
    h->addWidget(m_progress);
    m_status = new QLabel;
    m_status->setObjectName("panelStatus");
    h->addWidget(m_status);
    QPushButton *close = new QPushButton("CLOSE");
    close->setToolTip("Back to the terminal (Esc)");
    h->addWidget(close);
    v->addWidget(bar);

    m_view = new HeatmapView;
    m_view->setToolTip(QString());
    m_view->setMessage("Wheel / pinch: zoom   drag / two-finger scroll: pan   "
                       "double-click: open ticker");
    m_view->onOpen = [this](const QString &sym) {
      if (onOpen) onOpen(sym);
    };
    v->addWidget(m_view, 1);

    QObject::connect(fit, &QPushButton::clicked, this, [this]() { m_view->fit(); });
    QObject::connect(reload, &QPushButton::clicked, this, [this]() { load(true); });
    QObject::connect(close, &QPushButton::clicked, this, [this]() {
      if (onClose) onClose();
    });
    QShortcut *esc = new QShortcut(QKeySequence(Qt::Key_Escape), this);
    esc->setContext(Qt::WidgetWithChildrenShortcut);
    QObject::connect(esc, &QShortcut::activated, this, [this]() {
      if (onClose) onClose();
    });

    m_timer = new QTimer(this);
    m_timer->setInterval(HM_REFRESH_MS);
    QObject::connect(m_timer, &QTimer::timeout, this, [this]() {
      if (isVisible() && !m_busy) load(false);
    });
    syncModeButtons();
  }

  // Shown: (re)load quotes (members too on first use) and keep refreshing.
  void activate() {
    m_timer->start();
    m_view->setFocus();
    if (!m_busy) load(m_view->tileCount() == 0);
  }
  void deactivate() { m_timer->stop(); }

  HeatmapView::SizeMode sizeMode() const { return m_view->sizeMode(); }
  void setSizeMode(HeatmapView::SizeMode m) {
    m_view->setSizeMode(m);
    syncModeButtons();
  }

  void themeChanged() {
    styleProgress();
    m_view->update();
  }

private:
  void pickMode(HeatmapView::SizeMode m) {
    setSizeMode(m);
    if (onModeChanged) onModeChanged();
  }

  void syncModeButtons() {
    m_capBtn->setChecked(m_view->sizeMode() == HeatmapView::SizeCap);
    m_moveBtn->setChecked(m_view->sizeMode() == HeatmapView::SizeMove);
  }

  void styleProgress() {
    m_progress->setStyleSheet(
        QString("QProgressBar { border: 1px solid %1; background-color: %2;"
                " color: %3; text-align: center; }"
                "QProgressBar::chunk { background-color: %4; }")
            .arg(COL_GRID.name(), COL_INPUT.name(), COL_TEXT.name(),
                 COL_AMBER.name()));
  }

  // withMembers: fetch the member list first (else only quotes).
  void load(bool withMembers) {
    const int seq = ++m_seq;  // replies of an older load are dropped
    m_busy = true;
    if (withMembers || m_view->tileCount() == 0) fetchMembers(seq);
    else fetchQuotes(seq);
  }

  void fail(const QString &msg) {
    m_busy = false;
    m_progress->setVisible(false);
    setPanelError(m_status, msg);
  }

  void fetchMembers(int seq) {
    setPanelBusy(m_status, "Loading S&P 500 members\xe2\x80\xa6");
    QUrlQuery q;
    q.addQueryItem("index", "sp500");
    q.addQueryItem("limit", QString::number(HM_UNIVERSE_LIMIT));
    m_api->getJson(ROUTE_UNIVERSE, q,
                   [this, seq](int st, const QJsonObject &o, const QString &err) {
      if (seq != m_seq) return;
      const QString e = bodyError(st, o, err);
      if (!e.isEmpty()) {
        if (m_view->tileCount() == 0) m_view->setMessage("S&P 500 members unavailable: " + e);
        fail(e);
        return;
      }
      QVector<HmTile> tiles;
      for (const QJsonValue &v : o.value("items").toArray()) {
        const QJsonObject it = v.toObject();
        if (!it.value("sp500").toBool()) continue;
        HmTile t;
        t.sym = jstr(it, "ticker").trimmed().toUpper();
        if (t.sym.isEmpty()) continue;
        t.name = jstr(it, "name");
        hmClassify(jstr(it, "sector"), jstr(it, "industry"),
                   jstr(it, "sp500Sector"), t.sector, t.group);
        double cap = 0;
        if (jnum(it.value("marketCap"), cap) && cap > 0) {
          t.cap = cap;
          t.hasCap = true;
        }
        tiles << t;
      }
      if (tiles.isEmpty()) {
        QString msg = o.value("status").toObject().value("sp500").toObject()
                          .value("error").toString();
        if (msg.isEmpty())
          msg = "No S&P 500 member list yet: the backend fetches it with "
                "`ref sp500` (iShares IVV holdings, Wikipedia fallback).";
        m_view->setMessage(msg);
        fail(msg);
        return;
      }
      m_view->setTiles(tiles);
      fetchQuotes(seq);
    });
  }

  void fetchQuotes(int seq) {
    m_syms = m_view->symbols();
    m_qIdx = 0;
    m_qGot = 0;
    m_qFailed = 0;
    m_qLastErr.clear();
    styleProgress();
    m_progress->setRange(0, qMax(1, m_syms.size()));
    m_progress->setValue(0);
    m_progress->setFormat("quotes %v/%m");
    m_progress->setVisible(true);
    setPanelBusy(m_status, "Fetching quotes\xe2\x80\xa6");
    nextQuotes(seq);
  }

  void nextQuotes(int seq) {
    if (m_qIdx >= m_syms.size()) {
      finishQuotes();
      return;
    }
    const QStringList chunk = m_syms.mid(m_qIdx, HM_QUOTE_CHUNK);
    QUrlQuery q;
    q.addQueryItem("symbols", chunk.join(","));
    m_api->getJson(ROUTE_QUOTE_BATCH, q,
                   [this, seq, chunk](int st, const QJsonObject &o, const QString &err) {
      if (seq != m_seq) return;
      const QString e = bodyError(st, o, err);
      int got = 0;
      if (e.isEmpty()) {
        for (const QJsonValue &v : o.value("items").toArray())
          if (m_view->applyQuote(v.toObject())) ++got;
      } else {
        m_qLastErr = e;
      }
      m_qGot += got;
      m_qFailed += qMax(0, chunk.size() - got);
      m_qIdx += chunk.size();
      m_progress->setValue(m_qIdx);
      m_view->relayout();
      nextQuotes(seq);
    });
  }

  void finishQuotes() {
    m_busy = false;
    m_progress->setVisible(false);
    if (m_qGot == 0) {
      setPanelError(m_status, m_qLastErr.isEmpty() ? QString("no quotes") : m_qLastErr);
      return;
    }
    int up = 0, down = 0, quoted = 0;
    m_view->counts(up, down, quoted);
    QString text = QString("%1 stocks  \xe2\x96\xb2%2  \xe2\x96\xbc%3  %4")
                       .arg(m_view->tileCount())
                       .arg(up)
                       .arg(down)
                       .arg(QTime::currentTime().toString("HH:mm:ss"));
    if (m_qFailed > 0) text += QString("  (%1 without quote)").arg(m_qFailed);
    m_status->setText(text);
    m_status->setToolTip(m_qLastErr.isEmpty() ? QString() : "Last error: " + m_qLastErr);
    setLabelColor(m_status, m_qFailed > 0 ? COL_WARN : COL_DIM);
  }

  Api *m_api;
  HeatmapView *m_view = nullptr;
  QPushButton *m_capBtn = nullptr;
  QPushButton *m_moveBtn = nullptr;
  QProgressBar *m_progress = nullptr;
  QLabel *m_status = nullptr;
  QTimer *m_timer = nullptr;
  int m_seq = 0;
  bool m_busy = false;
  QStringList m_syms;  // quote order of the current load
  int m_qIdx = 0;
  int m_qGot = 0;
  int m_qFailed = 0;
  QString m_qLastErr;
};
#endif

// ---------------------------------------------------------------------------
// TerminalWindow
// ---------------------------------------------------------------------------

class TerminalWindow : public QMainWindow {
public:
  explicit TerminalWindow(const QString &baseOverride)
      : QMainWindow(nullptr), m_api(this) {
    m_symbol = DEFAULT_SYMBOL;
    m_watch = parseSymbolList(DEFAULT_WATCH);
    m_range = DEFAULT_RANGE;
    m_api.setBase(DEFAULT_BASE_URL);
    loadSettings();
    if (!baseOverride.isEmpty()) m_api.setBase(baseOverride);
#ifdef LIGHT_THEME
    // Before buildUi(): widgets pick up the palette when they are created.
    applyPalette(m_lightTheme ? LIGHT_PALETTE : DARK_PALETTE);
    qApp->setStyleSheet(appStyleSheet());
#endif

    buildUi();
    if (!m_savedGeometry.isEmpty()) restoreGeometry(m_savedGeometry);
#ifdef FIT_TO_SCREEN
    relaxFixedWidths();
    fitToScreen();  // before show: client geometry only
#endif
    syncControls();
    rebuildMonitor();
    startTimers();
    QTimer::singleShot(0, this, [this]() {
#ifdef FIT_TO_SCREEN
      fitToScreen();  // shown: now the window frame is known too
#endif
      refreshAll();
      pollHealth();
    });
  }

#ifdef FIT_TO_SCREEN
  // The command-line edits have fixed widths; let them shrink so the
  // window's minimum width fits small screens.
  void relaxFixedWidths() {
    for (QLineEdit *e : {m_cmd, m_watchEdit, m_baseEdit}) {
      if (!e) continue;
      e->setMinimumWidth(e->maximumWidth() / EDIT_SHRINK_DIV);
      e->setSizePolicy(QSizePolicy::Preferred, e->sizePolicy().verticalPolicy());
    }
  }

  QScreen *currentScreen() const {
    QScreen *scr = nullptr;
#if QT_VERSION >= QT_VERSION_CHECK(5, 14, 0)
    scr = screen();
#else
    scr = QGuiApplication::screenAt(geometry().center());
#endif
    return scr ? scr : QGuiApplication::primaryScreen();
  }

  // Shrinks (never grows) the window to the screen's available geometry,
  // frame included, and moves it fully onto that screen.
  void fitToScreen() {
    QScreen *scr = currentScreen();
    if (!scr) return;
    const QRect avail = scr->availableGeometry();
    // Frame size: known once shown; before that assume a typical title bar.
    QSize frame = frameGeometry().size() - geometry().size();
    if (!isVisible() || (frame.width() <= 0 && frame.height() <= 0))
      frame = QSize(FRAME_ALLOWANCE_W, FRAME_ALLOWANCE_H);
    const QSize maxClient(qMax(1, avail.width() - frame.width()),
                          qMax(1, avail.height() - frame.height()));
    // Widgets must not force a larger window than the screen.
    setMinimumSize(minimumSizeHint().boundedTo(maxClient));
    if (isMaximized() || isFullScreen()) return;
    const QSize sz = size().boundedTo(maxClient);
    if (sz != size()) resize(sz);
    const QSize outer = sz + frame;
    QPoint pos = frameGeometry().topLeft();
    pos.setX(qBound(avail.left(), pos.x(), avail.left() + avail.width() - outer.width()));
    pos.setY(qBound(avail.top(), pos.y(), avail.top() + avail.height() - outer.height()));
    if (pos != frameGeometry().topLeft()) move(pos);
  }
#endif

protected:
  void closeEvent(QCloseEvent *e) override {
    saveSettings();
    QMainWindow::closeEvent(e);
  }

private:
  // ---- UI construction ----------------------------------------------------

  void buildUi() {
    setWindowTitle(WINDOW_TITLE);
    resize(WINDOW_W, WINDOW_H);

    QWidget *central = new QWidget(this);
    QVBoxLayout *root = new QVBoxLayout(central);
    root->setContentsMargins(PANEL_SPACING, PANEL_SPACING, PANEL_SPACING,
                             PANEL_SPACING);
    root->setSpacing(PANEL_SPACING);
    root->addLayout(buildCommandBar());

    QSplitter *main = new QSplitter(Qt::Horizontal);
    main->setHandleWidth(SPLITTER_HANDLE);
    main->addWidget(buildLeft());
    main->addWidget(buildCenter());
    main->addWidget(buildRight());
    main->setSizes(QList<int>() << SPLIT_LEFT_W << SPLIT_CENTER_W
                                << SPLIT_RIGHT_W);
#ifdef EXTRA_DATA_PANELS
    m_workspace = new QTabWidget;
    m_workspace->addTab(main, WORKSPACE_DENSE);
    buildExtraPages();
    QWidget *content = m_workspace;
#else
    QWidget *content = main;
#endif
#ifdef HEATMAP
    root->addWidget(buildHeatmapStack(content), 1);
#else
    root->addWidget(content, 1);
#endif
    setCentralWidget(central);

    m_connLabel = new QLabel;
    m_updLabel = new QLabel;
    statusBar()->addWidget(m_connLabel, 1);
#ifdef EXTRA_DATA_PANELS
    m_fngLabel = new QLabel;
    m_fngLabel->setToolTip("CNN Fear & Greed index (SOC page for details)");
    statusBar()->addPermanentWidget(m_fngLabel);
#endif
#ifdef UNIVERSE_PROGRESS
    m_uniBar = new QProgressBar;
    m_uniBar->setFixedSize(UNIVERSE_BAR_W, UNIVERSE_BAR_H);
    m_uniBar->setTextVisible(true);
    m_uniBar->setVisible(false);  // until the backend answers
    statusBar()->addPermanentWidget(m_uniBar);
#endif
    statusBar()->addPermanentWidget(m_updLabel);

    QShortcut *refresh = new QShortcut(QKeySequence(Qt::Key_F5), this);
    QObject::connect(refresh, &QShortcut::activated, this,
                     [this]() { refreshAll(); });
    QShortcut *focusCmd = new QShortcut(QKeySequence("Ctrl+L"), this);
    QObject::connect(focusCmd, &QShortcut::activated, this, [this]() {
      m_cmd->setFocus();
      m_cmd->selectAll();
    });
  }

  QHBoxLayout *buildCommandBar() {
    QHBoxLayout *h = new QHBoxLayout;
    h->setSpacing(PANEL_SPACING);

    QLabel *logo = new QLabel("QUBE");
    logo->setObjectName("logo");
    h->addWidget(logo);

    m_cmd = new QLineEdit;
    m_cmd->setPlaceholderText("Ticker <GO>  e.g. MSFT, ^VIX, BTC-USD");
    m_cmd->setFixedWidth(CMD_EDIT_W);
    QPushButton *go = new QPushButton("GO");
    QObject::connect(m_cmd, &QLineEdit::returnPressed, this,
                     [this]() { runCommand(); });
    QObject::connect(go, &QPushButton::clicked, this,
                     [this]() { runCommand(); });
#ifdef EXTRA_DATA_PANELS
    setupSearch();
#endif
    h->addWidget(m_cmd);
    h->addWidget(go);

    QLabel *wl = new QLabel("WATCH");
    wl->setObjectName("dim");
    h->addWidget(wl);
    m_watchEdit = new QLineEdit;
    m_watchEdit->setFixedWidth(WATCH_EDIT_W);
    m_watchEdit->setToolTip("Monitor symbols, comma separated (max 40)");
    QPushButton *apply = new QPushButton("SET");
    QObject::connect(m_watchEdit, &QLineEdit::returnPressed, this,
                     [this]() { applyWatchEdit(); });
    QObject::connect(apply, &QPushButton::clicked, this,
                     [this]() { applyWatchEdit(); });
    h->addWidget(m_watchEdit);
    h->addWidget(apply);

    QLabel *sl = new QLabel("SERVER");
    sl->setObjectName("dim");
    h->addWidget(sl);
    m_baseEdit = new QLineEdit;
    m_baseEdit->setFixedWidth(BASE_EDIT_W);
    QObject::connect(m_baseEdit, &QLineEdit::returnPressed, this,
                     [this]() { applyBaseEdit(); });
    h->addWidget(m_baseEdit);

    QPushButton *refresh = new QPushButton("REFRESH");
    refresh->setToolTip("Reload every panel (F5)");
    QObject::connect(refresh, &QPushButton::clicked, this,
                     [this]() { refreshAll(); });
    h->addWidget(refresh);

#ifdef HEATMAP
    m_heatmapBtn = new QPushButton(HEATMAP_BTN_TEXT);
    m_heatmapBtn->setCheckable(true);
    m_heatmapBtn->setToolTip(QString("S&P 500 heatmap (%1)").arg(HEATMAP_SHORTCUT));
    QObject::connect(m_heatmapBtn, &QPushButton::toggled, this,
                     [this](bool on) { showHeatmap(on); });
    h->addWidget(m_heatmapBtn);
    QShortcut *heatKey = new QShortcut(QKeySequence(HEATMAP_SHORTCUT), this);
    QObject::connect(heatKey, &QShortcut::activated, this,
                     [this]() { m_heatmapBtn->toggle(); });
#endif

#ifdef LIGHT_THEME
    m_themeBtn = new QPushButton(THEME_BTN_TEXT);
    m_themeBtn->setCheckable(true);
    m_themeBtn->setChecked(m_lightTheme);  // before connect: no re-theme
    m_themeBtn->setToolTip(QString("Light theme (%1)").arg(THEME_SHORTCUT));
    QObject::connect(m_themeBtn, &QPushButton::toggled, this,
                     [this](bool on) { setTheme(on); });
    h->addWidget(m_themeBtn);
    QShortcut *themeKey = new QShortcut(QKeySequence(THEME_SHORTCUT), this);
    QObject::connect(themeKey, &QShortcut::activated, this,
                     [this]() { m_themeBtn->toggle(); });
#endif

    h->addStretch(1);
    m_clock = new QLabel;
    setLabelColor(m_clock, COL_AMBER);
    h->addWidget(m_clock);
    return h;
  }

#ifdef HEATMAP
  // The workspace and the heatmap share one stack below the command bar.
  QWidget *buildHeatmapStack(QWidget *content) {
    m_heatStack = new QStackedWidget;
    m_heatStack->addWidget(content);
    m_heatmap = new HeatmapPage(&m_api);
    m_heatmap->setSizeMode(m_hmSavedMode);
    m_heatmap->onClose = [this]() { m_heatmapBtn->setChecked(false); };
    m_heatmap->onOpen = [this](const QString &sym) {
      m_heatmapBtn->setChecked(false);
#ifdef EXTRA_DATA_PANELS
      m_workspace->setCurrentIndex(0);  // DENSE shows the loaded security
#endif
      loadSymbol(sym);
    };
    m_heatmap->onModeChanged = [this]() { saveSettingsDebounced(); };
    m_heatStack->addWidget(m_heatmap);
    return m_heatStack;
  }

  void showHeatmap(bool on) {
    if (!m_heatStack) return;
    m_heatStack->setCurrentWidget(on ? static_cast<QWidget *>(m_heatmap)
                                     : m_heatStack->widget(0));
    if (on) m_heatmap->activate();
    else m_heatmap->deactivate();
  }
#endif

  QWidget *buildLeft() {
    QSplitter *s = new QSplitter(Qt::Vertical);
    s->setHandleWidth(SPLITTER_HANDLE);

    // MONITOR
    m_monitorPanel = makePanel("MON  MONITOR");
    m_monitor = makeTable(headersOf(MONITOR_HEADERS, MC_COUNT));
    m_monitor->setIconSize(QSize(SPARK_W, SPARK_H));
    m_monitor->horizontalHeader()->setStretchLastSection(false);
    setColumnWidths(m_monitor, MONITOR_COL_W, MC_COUNT);
    for (int c = MC_LAST; c < MC_COUNT; ++c) {
      if (c != MC_TREND)
        m_monitor->horizontalHeaderItem(c)->setTextAlignment(ALIGN_NUM);
    }
    QObject::connect(m_monitor, &QTableWidget::cellClicked, this,
                     [this](int row, int) {
                       if (row >= 0 && row < m_watch.size())
                         loadSymbol(m_watch[row]);
                     });
    m_monitorPanel.body->addWidget(m_monitor);
    s->addWidget(m_monitorPanel.frame);

    // NEWS
    m_newsPanel = makePanel("N  NEWS");
    m_newsScope = new QComboBox;
    fillCombo(m_newsScope, NEWS_SCOPES, COUNT_OF(NEWS_SCOPES));
    onComboChanged(m_newsScope, this, [this](int) { fetchNews(); });
    m_newsPanel.addControl(m_newsScope);
    m_newsQuery = new QLineEdit;
    m_newsQuery->setPlaceholderText("keyword <Enter>");
    m_newsQuery->setFixedWidth(NEWS_FILTER_W);
    QObject::connect(m_newsQuery, &QLineEdit::returnPressed, this,
                     [this]() { fetchNews(); });
    m_newsPanel.addControl(m_newsQuery);
    m_news = makeTable(headersOf(NEWS_HEADERS, NC_COUNT));
    setColumnWidths(m_news, NEWS_COL_W, NC_COUNT - 1);
    QObject::connect(m_news, &QTableWidget::cellDoubleClicked, this,
                     [this](int row, int) { openNewsRow(row); });
    m_newsPanel.body->addWidget(m_news);
    s->addWidget(m_newsPanel.frame);

    s->setSizes(QList<int>() << SPLIT_MONITOR_H << SPLIT_NEWS_H);
    return s;
  }

  QWidget *buildHeader() {
    QWidget *w = new QWidget;
    QHBoxLayout *h = new QHBoxLayout(w);
    h->setContentsMargins(PANEL_PAD, PANEL_PAD, PANEL_PAD, 0);

    QVBoxLayout *left = new QVBoxLayout;
    left->setSpacing(0);
    m_hdrTicker = new QLabel;
    QFont tf = font();
    tf.setPointSize(TICKER_FONT_PT);
    tf.setBold(true);
    m_hdrTicker->setFont(tf);
    setLabelColor(m_hdrTicker, COL_AMBER);
    m_hdrName = new QLabel;
    m_hdrStats = new QLabel;
    setLabelColor(m_hdrStats, COL_DIM);
    left->addWidget(m_hdrTicker);
    left->addWidget(m_hdrName);
    left->addWidget(m_hdrStats);
    h->addLayout(left, 1);

    QVBoxLayout *right = new QVBoxLayout;
    right->setSpacing(0);
    m_hdrPrice = new QLabel;
    QFont pf = font();
    pf.setPointSize(HEADER_PRICE_PT);
    pf.setBold(true);
    m_hdrPrice->setFont(pf);
    m_hdrPrice->setAlignment(ALIGN_NUM);
    m_hdrChange = new QLabel;
    m_hdrChange->setAlignment(ALIGN_NUM);
    m_hdrMeta = new QLabel;
    m_hdrMeta->setAlignment(ALIGN_NUM);
    setLabelColor(m_hdrMeta, COL_DIM);
    right->addWidget(m_hdrPrice);
    right->addWidget(m_hdrChange);
    right->addWidget(m_hdrMeta);
    h->addLayout(right);
    return w;
  }

  QWidget *buildCenter() {
    QSplitter *s = new QSplitter(Qt::Vertical);
    s->setHandleWidth(SPLITTER_HANDLE);

    // DES + GP
    m_chartPanel = makePanel("DES \xc2\xb7 GP");
    for (int i = 0; i < RANGE_COUNT; ++i) {
      QPushButton *b = new QPushButton(RANGES[i].label);
      b->setCheckable(true);
      b->setAutoExclusive(true);
      b->setFixedWidth(RANGE_BTN_W);
      const QString rng = RANGES[i].range;
      QObject::connect(b, &QPushButton::clicked, this,
                       [this, rng]() { setRange(rng); });
      m_rangeButtons.append(b);
      m_chartPanel.addControl(b);
    }
    m_candleBtn = new QPushButton("CNDL");
    m_candleBtn->setCheckable(true);
    m_candleBtn->setToolTip("Candlesticks instead of a line");
    QObject::connect(m_candleBtn, &QPushButton::toggled, this,
                     [this](bool on) {
                       m_candles = on;
                       m_chart->setCandles(on);
                       saveSettingsDebounced();
                     });
    m_chartPanel.addControl(m_candleBtn);
    m_chartPanel.body->addWidget(buildHeader());
    m_chart = new ChartWidget;
    m_chartPanel.body->addWidget(m_chart, 1);
    s->addWidget(m_chartPanel.frame);

    // EARN + ECO tabs
    m_tabs = new QTabWidget;

    m_earnPanel = makePanel("EARN  FINANCIALS");
    m_earnType = new QComboBox;
    fillCombo(m_earnType, STATEMENT_TYPES, COUNT_OF(STATEMENT_TYPES));
    m_earnPeriod = new QComboBox;
    fillCombo(m_earnPeriod, STATEMENT_PERIODS, COUNT_OF(STATEMENT_PERIODS));
    onComboChanged(m_earnType, this, [this](int) { fetchStatements(); });
    onComboChanged(m_earnPeriod, this, [this](int) { fetchStatements(); });
    m_earnPanel.addControl(m_earnType);
    m_earnPanel.addControl(m_earnPeriod);
    m_earn = makeTable(QStringList() << "LINE");
    m_earnPanel.body->addWidget(m_earn);
    m_tabs->addTab(m_earnPanel.frame, "EARN");

    m_ecoPanel = makePanel("ECO  ECONOMIC CALENDAR");
    m_ecoWeek = new QComboBox;
    fillCombo(m_ecoWeek, ECON_WEEKS, COUNT_OF(ECON_WEEKS));
    onComboChanged(m_ecoWeek, this, [this](int) { fetchEcon(); });
    m_ecoImpact = new QComboBox;
    fillCombo(m_ecoImpact, ECON_IMPACTS, COUNT_OF(ECON_IMPACTS));
    onComboChanged(m_ecoImpact, this, [this](int) { renderEcon(); });
    m_ecoPanel.addControl(m_ecoWeek);
    m_ecoPanel.addControl(m_ecoImpact);
    m_eco = makeTable(headersOf(ECO_HEADERS, EC_COUNT));
    setColumnWidths(m_eco, ECO_COL_W, EC_COUNT - 1);
    m_ecoPanel.body->addWidget(m_eco);
    m_tabs->addTab(m_ecoPanel.frame, "ECO");
#ifdef EXTRA_DATA_PANELS
    buildSymbolTabs();
#endif

    s->addWidget(m_tabs);
    s->setSizes(QList<int>() << SPLIT_CHART_H << SPLIT_TABS_H);
    return s;
  }

  QWidget *buildRight() {
    QSplitter *s = new QSplitter(Qt::Vertical);
    s->setHandleWidth(SPLITTER_HANDLE);

    // FA
    m_fundPanel = makePanel("FA  KEY STATS");
    m_fund = makeTable(headersOf(FUND_HEADERS, FC_COUNT));
    setColumnWidths(m_fund, FUND_COL_W, FC_COUNT - 1);
    m_fundPanel.body->addWidget(m_fund);
    s->addWidget(m_fundPanel.frame);

    // ANR
    m_anPanel = makePanel("ANR  ANALYST RECS");
    QWidget *top = new QWidget;
    QVBoxLayout *tv = new QVBoxLayout(top);
    tv->setContentsMargins(PANEL_PAD, PANEL_PAD, PANEL_PAD, 0);
    tv->setSpacing(PANEL_SPACING);
    m_anConsensus = new QLabel;
    QFont cf = font();
    cf.setBold(true);
    cf.setPointSize(TICKER_FONT_PT);
    m_anConsensus->setFont(cf);
    m_anDist = new DistBar;
    m_anTargets = new QLabel;
    m_anTargets->setWordWrap(true);
    tv->addWidget(m_anConsensus);
    tv->addWidget(m_anDist);
    tv->addWidget(m_anTargets);
    m_anPanel.body->addWidget(top);
    m_anTable = makeTable(headersOf(RATING_HEADERS, RC_COUNT));
    setColumnWidths(m_anTable, RATING_COL_W, RC_COUNT - 1);
    m_anPanel.body->addWidget(m_anTable, 1);
    s->addWidget(m_anPanel.frame);

    s->setSizes(QList<int>() << SPLIT_FUND_H << SPLIT_ANALYST_H);
    return s;
  }

  // Push the current state into the editable controls.
  void syncControls() {
    m_watchEdit->setText(m_watch.join(","));
    m_baseEdit->setText(m_api.base());
    m_cmd->clear();
    for (int i = 0; i < RANGE_COUNT && i < m_rangeButtons.size(); ++i) {
      if (m_range == QLatin1String(RANGES[i].range))
        m_rangeButtons[i]->setChecked(true);
    }
    m_candleBtn->setChecked(m_candles);
    m_chart->setCandles(m_candles);
    updateTitle();
  }

  void updateTitle() {
    setWindowTitle(QString("%1 \xe2\x80\x94 %2").arg(WINDOW_TITLE, m_symbol));
  }

  // ---- commands -------------------------------------------------------------

  void startTimers() {
    m_quoteTimer = new QTimer(this);
    m_quoteTimer->setInterval(QUOTE_POLL_MS);
    QObject::connect(m_quoteTimer, &QTimer::timeout, this,
                     [this]() { pollQuotes(); });
    m_quoteTimer->start();

    m_denseTimer = new QTimer(this);
    m_denseTimer->setInterval(DENSE_REFRESH_MS);
    QObject::connect(m_denseTimer, &QTimer::timeout, this,
                     [this]() { fetchDense(); });
    m_denseTimer->start();

    m_healthTimer = new QTimer(this);
    m_healthTimer->setInterval(HEALTH_POLL_MS);
    QObject::connect(m_healthTimer, &QTimer::timeout, this,
                     [this]() { pollHealth(); });
    m_healthTimer->start();

    m_clockTimer = new QTimer(this);
    m_clockTimer->setInterval(CLOCK_TICK_MS);
    QObject::connect(m_clockTimer, &QTimer::timeout, this,
                     [this]() { tickClock(); });
    m_clockTimer->start();
    tickClock();

    m_saveTimer = new QTimer(this);
    m_saveTimer->setSingleShot(true);
    m_saveTimer->setInterval(SETTINGS_SAVE_DEBOUNCE_MS);
    QObject::connect(m_saveTimer, &QTimer::timeout, this,
                     [this]() { saveSettings(); });
#ifdef EXTRA_DATA_PANELS
    startExtraTimers();
#endif
#ifdef UNIVERSE_PROGRESS
    m_uniTimer = new QTimer(this);
    m_uniTimer->setInterval(UNIVERSE_POLL_MS);
    QObject::connect(m_uniTimer, &QTimer::timeout, this,
                     [this]() { pollUniverse(); });
    m_uniTimer->start();
    QTimer::singleShot(0, this, [this]() { pollUniverse(); });
#endif
  }

  void tickClock() {
    m_clock->setText(enLocale().toString(QDateTime::currentDateTime(),
                                         "ddd yyyy-MM-dd  HH:mm:ss"));
  }

  void setConn(const QColor &c, const QString &text) {
    m_connLabel->setText(text);
    setLabelColor(m_connLabel, c);
  }

  void markUpdated(const QString &what) {
    m_updLabel->setText(QString("%1  %2").arg(
        what, QDateTime::currentDateTime().toString("HH:mm:ss")));
  }

  // Command line: first token is the ticker ("msft us equity" -> MSFT).
  void runCommand() {
    const QStringList parts =
        m_cmd->text().split(QRegExp("\\s+"), QString::SkipEmptyParts);
    if (parts.isEmpty()) return;
    const QString sym = normSymbol(parts.first());
    if (sym.isEmpty()) {
      setConn(COL_DOWN, "invalid ticker: " + parts.first());
      return;
    }
    m_cmd->clear();
    loadSymbol(sym);
  }

  void loadSymbol(const QString &raw) {
    const QString sym = normSymbol(raw);
    if (sym.isEmpty()) return;
    m_symbol = sym;
    updateTitle();
    rebuildMonitor();  // re-highlights the active row
    clearSymbolPanels();
    fetchDense();
#ifdef EXTRA_DATA_PANELS
    symbolTabsChanged();
#endif
    saveSettingsDebounced();
  }

  void applyWatchEdit() {
    m_watch = parseSymbolList(m_watchEdit->text());
    m_watchEdit->setText(m_watch.join(","));
    rebuildMonitor();
    fetchDense();
    saveSettingsDebounced();
  }

  void applyBaseEdit() {
    QString b = m_baseEdit->text().trimmed();
    if (b.isEmpty()) b = DEFAULT_BASE_URL;
    if (!b.startsWith("http://") && !b.startsWith("https://"))
      b = "http://" + b;
    m_api.setBase(b);
    m_baseEdit->setText(m_api.base());
    m_quoteBusy = false;
    refreshAll();
    pollHealth();
    saveSettingsDebounced();
  }

  void setRange(const QString &rng) {
    if (!validRange(rng)) return;
    m_range = rng;
    fetchChart();
    saveSettingsDebounced();
  }

#ifdef EXTRA_DATA_PANELS
  void refreshAll() {
    fetchDense();
    refreshExtra();
  }
#else
  void refreshAll() { fetchDense(); }
#endif

#ifdef LIGHT_THEME
  // Swaps the palette and re-colors everything: the style sheet covers the
  // widgets, cached data is re-rendered at once, the rest comes with a refresh.
  void setTheme(bool light) {
    if (light == m_lightTheme) return;
    m_lightTheme = light;
    applyPalette(light ? LIGHT_PALETTE : DARK_PALETTE);
    qApp->setStyleSheet(appStyleSheet());
    setLabelColor(m_clock, COL_AMBER);
    setLabelColor(m_hdrTicker, COL_AMBER);
    setLabelColor(m_hdrStats, COL_DIM);
    const qreal dpr = devicePixelRatioF();
    for (auto it = m_sparkSrc.constBegin(); it != m_sparkSrc.constEnd(); ++it) {
      const SparkSrc &s = it.value();
      m_sparks.insert(it.key(), renderSpark(s.closes, s.hasRef, s.ref, dpr));
    }
    rebuildMonitor();
    if (m_quotes.contains(m_symbol)) applyHeader(m_quotes.value(m_symbol));
    else clearHeader();
    if (!m_econItems.isEmpty()) renderEcon();
    m_chart->update();
    m_anDist->update();
#ifdef HEATMAP
    if (m_heatmap) m_heatmap->themeChanged();
#endif
    refreshAll();
    saveSettingsDebounced();
  }
#endif

  // Panels whose dense snapshot part matches the current controls.
  bool newsIsDense() const {
    return m_newsScope->currentData().toString() != NEWS_SCOPE_SYMBOL &&
           m_newsQuery->text().trimmed().isEmpty();
  }
  bool statementsAreDense() const {
    return m_earnType->currentData().toString() == DENSE_STATEMENT_TYPE &&
           m_earnPeriod->currentData().toString() == DENSE_STATEMENT_PERIOD;
  }
  bool econIsDense() const {
    return m_ecoWeek->currentData().toString() == DENSE_ECON_WEEK;
  }

  // ---- fetching -------------------------------------------------------------

  // NDJSON snapshot of every DENSE panel; parts are applied as they arrive.
  void fetchDense() {
    const int gen = ++m_gen;
    if (m_denseReply) m_denseReply->abort();  // its callbacks see a stale gen
    if (m_denseTimer) m_denseTimer->start();  // restart the refresh period
    m_denseSeen.clear();

    // The snapshot charts with a daily interval; other ranges use the route.
    const bool chartFromDense =
        intervalFor(m_range) == QLatin1String(DENSE_INTERVAL);
    const QString denseRange = chartFromDense ? m_range : QString(DEFAULT_RANGE);
    if (chartFromDense) ++m_chartSeq;  // drop older chart replies
    else fetchChart();
    if (!newsIsDense()) fetchNews();
    if (!statementsAreDense()) fetchStatements();
    if (!econIsDense()) fetchEcon();

    QUrlQuery q;
    q.addQueryItem("symbol", m_symbol);
    if (!m_watch.isEmpty()) q.addQueryItem("watch", m_watch.join(","));
    q.addQueryItem("range", denseRange);
    setConn(COL_DIM, QString("loading %1 snapshot from %2\xe2\x80\xa6")
                         .arg(m_symbol, m_api.base()));
    m_denseReply = m_api.getLines(
        ROUTE_DENSE, q,
        [this, gen, chartFromDense, denseRange](const QJsonObject &line) {
          if (gen != m_gen) return;
          handleDensePart(line, chartFromDense && m_range == denseRange);
        },
        [this, gen](const QString &err) {
          if (gen != m_gen) return;
          finishDense(gen, err);
        });
  }

  void handleDensePart(const QJsonObject &line, bool chartOk) {
    const QString key = jstr(line, "key");
    if (key.isEmpty()) {
      if (jstr(line, "type") == "done") {
        markUpdated(QString("snapshot %1 ms")
                        .arg(qint64(line.value("ms").toDouble())));
      } else if (line.contains("error")) {
        setConn(COL_DOWN, "snapshot: " + jstr(line, "error"));
      }
      return;
    }
    m_denseSeen.insert(key);
    const int st = line.value("status").toInt();
    const QJsonObject data = line.value("data").toObject();
    const QString err = bodyError(st, data, QString());
    const QString sparkPrefix = "spark:";

    if (key == "watchlist") {
      if (err.isEmpty()) applyWatchlist(data);
      else setPanelError(m_monitorPanel.status, err);
    } else if (key == "chart") {
      if (chartOk) applyChart(err, data);
    } else if (key.startsWith(sparkPrefix)) {
      applySpark(key.mid(sparkPrefix.size()), err, data);
    } else if (key == "news") {
      if (newsIsDense()) applyNews(err, data);
    } else if (key == "econ") {
      if (econIsDense()) applyEcon(err, data);
    } else if (key == "fundamentals") {
      applyFund(err, data);
    } else if (key == "analyst") {
      applyAnalyst(err, data);
    } else if (key == "earnings") {
      if (statementsAreDense()) applyStatements(err, data);
    }
  }

  void finishDense(int gen, const QString &err) {
    if (!err.isEmpty()) {
      setConn(COL_DOWN, QString("snapshot from %1 failed: %2")
                            .arg(m_api.base(), err));
      QTimer::singleShot(DENSE_RETRY_MS, this, [this, gen]() {
        if (gen == m_gen) fetchDense();
      });
      return;
    }
    setConn(COL_UP, QString("\xe2\x97\x8f %1  %2 loaded").arg(m_api.base(),
                                                              m_symbol));
    // The backend skips the equity-only parts for indices, FX, crypto, ...
    const QString na = "Not available for " + m_symbol;
    if (!m_denseSeen.contains("fundamentals")) {
      setTableMessage(m_fund, na);
      setPanelBusy(m_fundPanel.status, NA_STATUS);
    }
    if (!m_denseSeen.contains("analyst")) {
      clearAnalyst(na);
      setPanelBusy(m_anPanel.status, NA_STATUS);
    }
    if (!m_denseSeen.contains("earnings") && statementsAreDense()) {
      resetEarnTable(na);
      setPanelBusy(m_earnPanel.status, NA_STATUS);
    }
  }

  void pollQuotes() {
    if (m_quoteBusy) return;
    QStringList syms = m_watch;
    if (!syms.contains(m_symbol)) syms.prepend(m_symbol);
    m_quoteBusy = true;
    QUrlQuery q;
    q.addQueryItem("symbols", syms.join(","));
    m_api.getJson(ROUTE_WATCHLIST, q,
                  [this](int st, const QJsonObject &o, const QString &err) {
                    m_quoteBusy = false;
                    const QString e = bodyError(st, o, err);
                    if (!e.isEmpty()) {
                      setPanelError(m_monitorPanel.status, e);
                      return;
                    }
                    applyWatchlist(o);
                  });
  }

  void fetchChart() {
    const int seq = ++m_chartSeq;
    m_chart->setMessage(QString("Loading %1 %2\xe2\x80\xa6")
                            .arg(m_symbol, m_range.toUpper()));
    QUrlQuery q;
    q.addQueryItem("range", m_range);
    q.addQueryItem("interval", intervalFor(m_range));
    m_api.getJson(QString(ROUTE_CHART) + Api::pathSeg(m_symbol), q,
                  [this, seq](int st, const QJsonObject &o,
                              const QString &err) {
                    if (seq != m_chartSeq) return;
                    applyChart(bodyError(st, o, err), o);
                  });
  }

  void fetchNews() {
    const int seq = ++m_newsSeq;
    QUrlQuery q;
    q.addQueryItem("limit", QString::number(NEWS_LIMIT));
    if (m_newsScope->currentData().toString() == NEWS_SCOPE_SYMBOL)
      q.addQueryItem("tickers", m_symbol);
    const QString kw = m_newsQuery->text().trimmed();
    if (!kw.isEmpty()) q.addQueryItem("q", kw);
    setPanelBusy(m_newsPanel.status, LOADING_TEXT);
    m_api.getJson(ROUTE_NEWS, q,
                  [this, seq](int st, const QJsonObject &o,
                              const QString &err) {
                    if (seq != m_newsSeq) return;
                    applyNews(bodyError(st, o, err), o);
                  });
  }

  void fetchStatements() {
    const int seq = ++m_stmtSeq;
    QUrlQuery q;
    q.addQueryItem("symbol", m_symbol);
    q.addQueryItem("type", m_earnType->currentData().toString());
    q.addQueryItem("period", m_earnPeriod->currentData().toString());
    q.addQueryItem("limit", QString::number(STATEMENT_LIMIT));
    setPanelBusy(m_earnPanel.status, LOADING_TEXT);
    m_api.getJson(ROUTE_STATEMENTS, q,
                  [this, seq](int st, const QJsonObject &o,
                              const QString &err) {
                    if (seq != m_stmtSeq) return;
                    applyStatements(bodyError(st, o, err), o);
                  });
  }

  void fetchEcon() {
    const int seq = ++m_econSeq;
    QUrlQuery q;
    q.addQueryItem("week", m_ecoWeek->currentData().toString());
    setPanelBusy(m_ecoPanel.status, LOADING_TEXT);
    m_api.getJson(ROUTE_ECON, q,
                  [this, seq](int st, const QJsonObject &o,
                              const QString &err) {
                    if (seq != m_econSeq) return;
                    applyEcon(bodyError(st, o, err), o);
                  });
  }

  void pollHealth() {
    m_api.getJson(
        ROUTE_HEALTH, QUrlQuery(),
        [this](int st, const QJsonObject &o, const QString &err) {
          const QString e = bodyError(st, o, err);
          if (!e.isEmpty() || !o.value("ok").toBool()) {
            setConn(COL_DOWN,
                    QString("\xe2\x97\x8f backend %1 unreachable: %2")
                        .arg(m_api.base(), e.isEmpty() ? QString("not ok") : e));
            return;
          }
          // Keep a more specific snapshot message while one is in flight.
          if (m_denseReply) return;
          setConn(COL_UP,
                  QString("\xe2\x97\x8f %1  up %2  clients %3  streams %4")
                      .arg(m_api.base(), jstr(o, "uptime"),
                           o.value("clients").toVariant().toString(),
                           o.value("streams").toVariant().toString()));
        });
  }

  // ---- MONITOR / DES / GP renderers -----------------------------------------

  static QString orNA(bool ok, const QString &s) {
    return ok ? s : QString(NOT_AVAILABLE);
  }

  // "lo - hi" when both ends are known.
  static QString spanText(const QJsonObject &o, const char *loKey,
                          const char *hiKey) {
    double lo = 0, hi = 0;
    if (!jnum(o.value(loKey), lo) || !jnum(o.value(hiKey), hi))
      return QString(NOT_AVAILABLE);
    return fmtPrice(lo) + " \xe2\x80\x93 " + fmtPrice(hi);
  }

  void rebuildMonitor() {
    m_monitor->clearSpans();
    m_monitor->setRowCount(m_watch.size());
    m_monitorRows.clear();
    for (int r = 0; r < m_watch.size(); ++r) {
      const QString &sym = m_watch[r];
      m_monitorRows.insert(sym, r);
      for (int c = 0; c < MC_COUNT; ++c) {
        QTableWidgetItem *it = setCell(m_monitor, r, c, QString(), COL_TEXT,
                                       c == MC_SYM ? ALIGN_TEXT : ALIGN_NUM);
        it->setBackground(QBrush());
        it->setToolTip(QString());
      }
      paintSymbolCell(r, sym);
      if (m_quotes.contains(sym)) applyQuoteRow(r, m_quotes.value(sym), false);
      cellAt(m_monitor, r, MC_TREND)
          ->setData(Qt::DecorationRole,
                    m_sparks.contains(sym)
                        ? QVariant::fromValue(m_sparks.value(sym))
                        : QVariant());
    }
    if (m_watch.isEmpty()) setTableMessage(m_monitor, "Empty watch list");
  }

  void paintSymbolCell(int row, const QString &sym) {
    const bool active = sym == m_symbol;
    QTableWidgetItem *it =
        setCell(m_monitor, row, MC_SYM, sym, active ? COL_AMBER : COL_TEXT);
    QFont f = m_monitor->font();
    f.setBold(active);
    it->setFont(f);
  }

  void applyWatchlist(const QJsonObject &o) {
    const QJsonArray items = o.value("items").toArray();
    int bad = 0;
    for (const QJsonValue &v : items) {
      const QJsonObject it = v.toObject();
      const QString sym = jstr(it, "symbol");
      if (sym.isEmpty()) continue;
      m_quotes.insert(sym, it);
      if (jstr(it, "quality") == QLatin1String(Q_UNAVAILABLE)) ++bad;
      if (m_monitorRows.contains(sym))
        applyQuoteRow(m_monitorRows.value(sym), it, true);
      if (sym == m_symbol) applyHeader(it);
    }
    QString txt = QString("%1 quotes").arg(items.size());
    if (bad > 0) txt += QString("  %1 unavailable").arg(bad);
    txt += "  " + QDateTime::currentDateTime().toString("HH:mm:ss");
    m_monitorPanel.status->setText(txt);
    m_monitorPanel.status->setToolTip(QString());
    setLabelColor(m_monitorPanel.status, bad > 0 ? COL_WARN : COL_UP);
  }

  void applyQuoteRow(int row, const QJsonObject &it, bool flash) {
    const QString sym = jstr(it, "symbol");
    const QString q = jstr(it, "quality");
    const bool degraded =
        q == QLatin1String(Q_UNAVAILABLE) || q == QLatin1String(Q_STALE);
    double price = 0, chg = 0, pct = 0, hi = 0, lo = 0, vol = 0;
    const bool hp = jnum(it.value("price"), price);
    const bool hc = jnum(it.value("change"), chg);
    const bool hpct = jnum(it.value("changePercent"), pct);
    const bool hh = jnum(it.value("dayHigh"), hi);
    const bool hl = jnum(it.value("dayLow"), lo);
    const bool hv = jnum(it.value("volume"), vol);
    const QColor cc = hc ? changeColor(chg) : COL_DIM;

    QTableWidgetItem *last =
        setCell(m_monitor, row, MC_LAST, orNA(hp, fmtPrice(price)),
                degraded ? COL_DIM : COL_TEXT, ALIGN_NUM);
    setCell(m_monitor, row, MC_CHG, orNA(hc, fmtSigned(chg, 2)), cc, ALIGN_NUM);
    setCell(m_monitor, row, MC_PCT, orNA(hpct, fmtPctSigned(pct)), cc,
            ALIGN_NUM);
    setCell(m_monitor, row, MC_HIGH, orNA(hh, fmtPrice(hi)), COL_TEXT,
            ALIGN_NUM);
    setCell(m_monitor, row, MC_LOW, orNA(hl, fmtPrice(lo)), COL_TEXT,
            ALIGN_NUM);
    setCell(m_monitor, row, MC_VOL, orNA(hv, fmtBig(vol)), COL_DIM, ALIGN_NUM);

    QStringList tip;
    const QString name = jstr(it, "name");
    if (!name.isEmpty()) tip << name;
    const QString exch = jstr(it, "exchange"), cur = jstr(it, "currency");
    if (!exch.isEmpty() || !cur.isEmpty()) tip << exch + " " + cur;
    tip << "52W " + spanText(it, "low52w", "high52w");
    if (!q.isEmpty()) tip << "quality: " + q;
    const QString mt = jstr(it, "marketTime");
    if (!mt.isEmpty()) tip << "market time: " + fmtWhen(mt);
    const QString err = jstr(it, "error");
    if (!err.isEmpty()) tip << "error: " + err;
    cellAt(m_monitor, row, MC_SYM)->setToolTip(tip.join("\n"));

    // Flash the LAST cell on a price change.
    if (flash && hp && m_lastPrice.contains(sym) &&
        m_lastPrice.value(sym) != price) {
      last->setBackground(price > m_lastPrice.value(sym) ? COL_FLASH_UP
                                                          : COL_FLASH_DOWN);
      QTimer::singleShot(FLASH_MS, this, [this, sym]() {
        if (!m_monitorRows.contains(sym)) return;
        QTableWidgetItem *c = m_monitor->item(m_monitorRows.value(sym), MC_LAST);
        if (c) c->setBackground(QBrush());
      });
    }
    if (hp) m_lastPrice.insert(sym, price);
  }

  void clearHeader() {
    m_hdrTicker->setText(m_symbol);
    m_hdrName->setText(QString());
    m_hdrStats->setText(QString());
    m_hdrPrice->setText(NOT_AVAILABLE);
    setLabelColor(m_hdrPrice, COL_DIM);
    m_hdrChange->setText(QString());
    m_hdrMeta->setText(QString());
    m_hdrMeta->setToolTip(QString());
  }

  // DES: security header above the chart.
  void applyHeader(const QJsonObject &it) {
    m_hdrTicker->setText(jstr(it, "symbol"));
    m_hdrName->setText(jstr(it, "name"));

    double price = 0, chg = 0, pct = 0, prev = 0, vol = 0;
    const bool hp = jnum(it.value("price"), price);
    const bool hc = jnum(it.value("change"), chg);
    const bool hpct = jnum(it.value("changePercent"), pct);
    m_hdrPrice->setText(orNA(hp, fmtPrice(price)));
    setLabelColor(m_hdrPrice, hp ? COL_TEXT : COL_DIM);
    QString ch = orNA(hc, fmtSigned(chg, 2));
    if (hpct) ch += "  (" + fmtPctSigned(pct) + ")";
    m_hdrChange->setText(ch);
    setLabelColor(m_hdrChange, hc ? changeColor(chg) : COL_DIM);

    QStringList stats;
    stats << "PREV " + orNA(jnum(it.value("previousClose"), prev),
                            fmtPrice(prev))
          << "DAY " + spanText(it, "dayLow", "dayHigh")
          << "52W " + spanText(it, "low52w", "high52w")
          << "VOL " + orNA(jnum(it.value("volume"), vol), fmtBig(vol));
    m_hdrStats->setText(stats.join("   "));

    QStringList meta;
    const QString exch = jstr(it, "exchange"), cur = jstr(it, "currency");
    const QString q = jstr(it, "quality"), mt = jstr(it, "marketTime");
    if (!exch.isEmpty()) meta << exch;
    if (!cur.isEmpty()) meta << cur;
    if (!q.isEmpty()) meta << q;
    if (!mt.isEmpty()) meta << fmtWhen(mt);
    m_hdrMeta->setText(meta.join(" \xc2\xb7 "));
    setLabelColor(m_hdrMeta, qualityColor(q));
    m_hdrMeta->setToolTip(jstr(it, "error"));
  }

  void applyChart(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      m_chart->setMessage("Chart unavailable: " + err);
      setPanelError(m_chartPanel.status, err);
      return;
    }
    m_chart->setData(o);
    setProvenance(m_chartPanel.status, o.value("provenance").toObject());
  }

  void applySpark(const QString &sym, const QString &err,
                  const QJsonObject &o) {
    if (!err.isEmpty()) {
      if (m_monitorRows.contains(sym))
        cellAt(m_monitor, m_monitorRows.value(sym), MC_TREND)
            ->setToolTip("sparkline unavailable: " + err);
      return;
    }
    double prev = 0;
    const bool hasPrev = jnum(o.value("previousClose"), prev);
    const QVector<double> closes = jarr(o.value("close"));
    const QPixmap pm = renderSpark(closes, hasPrev, prev, devicePixelRatioF());
    m_sparks.insert(sym, pm);
#ifdef LIGHT_THEME
    SparkSrc src;
    src.closes = closes;
    src.hasRef = hasPrev;
    src.ref = prev;
    m_sparkSrc.insert(sym, src);
#endif
    if (!m_monitorRows.contains(sym)) return;
    QTableWidgetItem *it = cellAt(m_monitor, m_monitorRows.value(sym), MC_TREND);
    it->setData(Qt::DecorationRole, QVariant::fromValue(pm));
    it->setToolTip(QString("%1 %2 \xc2\xb7 %3 bars")
                       .arg(jstr(o, "range"), jstr(o, "interval"))
                       .arg(closes.size()));
  }

  // Symbol switch: blank the symbol-specific panels until data arrives.
  void clearSymbolPanels() {
    const QString loading = LOADING_TEXT;
    if (m_quotes.contains(m_symbol)) applyHeader(m_quotes.value(m_symbol));
    else clearHeader();
    m_chart->setMessage(QString("Loading %1\xe2\x80\xa6").arg(m_symbol));
    setPanelBusy(m_chartPanel.status, loading);
    setTableMessage(m_fund, loading);
    setPanelBusy(m_fundPanel.status, loading);
    clearAnalyst(loading);
    setPanelBusy(m_anPanel.status, loading);
    resetEarnTable(loading);
    setPanelBusy(m_earnPanel.status, loading);
    if (!newsIsDense()) setTableMessage(m_news, loading);
  }

  // ---- NEWS -----------------------------------------------------------------

  void applyNews(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      setPanelError(m_newsPanel.status, err);
      if (m_news->rowCount() == 0) setTableMessage(m_news, "News unavailable");
      return;
    }
    const QJsonArray items = o.value("items").toArray();
    m_news->clearSpans();
    m_news->setRowCount(items.size());
    for (int r = 0; r < items.size(); ++r) {
      const QJsonObject a = items[r].toObject();
      QStringList tickers;
      for (const QJsonValue &t : a.value("tickers").toArray())
        tickers << t.toString();
      const QString title = jstr(a, "title");
      const QString urgency = jstr(a, "urgency").toLower();
      const bool urgent = urgency == "high";
      const bool mine = tickers.contains(m_symbol);
      const QString published = jstr(a, "publishedAt");

      setCell(m_news, r, NC_TIME, fmtWhen(published), COL_DIM)
          ->setToolTip(published);
      setCell(m_news, r, NC_SRC, jstr(a, "source"), COL_DIM)
          ->setToolTip(jstr(a, "category"));
      setCell(m_news, r, NC_TICKERS, tickers.join(" "), COL_LINK);
      QTableWidgetItem *h = setCell(
          m_news, r, NC_HEADLINE, title,
          urgent ? COL_DOWN : (mine ? COL_AMBER : COL_TEXT));
      h->setData(Qt::UserRole, jstr(a, "url"));
      h->setToolTip(QString("<p><b>%1</b></p><p>%2</p><p><i>%3 \xc2\xb7 %4 "
                            "\xc2\xb7 %5</i></p>")
                        .arg(title.toHtmlEscaped(),
                             jstr(a, "summary").toHtmlEscaped(),
                             jstr(a, "source").toHtmlEscaped(),
                             jstr(a, "category").toHtmlEscaped(),
                             urgency.toHtmlEscaped()));
    }
    if (items.isEmpty()) setTableMessage(m_news, "No headlines");
    const int total = o.value("total").toInt();
    setProvenance(m_newsPanel.status, o.value("provenance").toObject(),
                  QString("%1/%2").arg(items.size()).arg(total));
  }

  void openNewsRow(int row) {
    QTableWidgetItem *it = m_news->item(row, NC_HEADLINE);
    if (!it) return;
    const QString url = it->data(Qt::UserRole).toString();
#ifdef EXTRA_DATA_PANELS
    if (!url.isEmpty()) openArticle(url, it->text());
#else
    if (!url.isEmpty()) QDesktopServices::openUrl(QUrl(url));
#endif
  }

  // ---- FA -------------------------------------------------------------------

  static QString fmtFund(FundKind k, double v) {
    switch (k) {
    case FK_PRICE: return fmtPrice(v);
    case FK_BIG: return fmtBig(v);
    case FK_RATIO: return fmtNum(v, 2);
    case FK_FRAC_PCT: return fmtNum(v * PCT_SCALE, 2) + "%";
    case FK_YIELD:
      return fmtNum(v > YIELD_PCT_THRESHOLD ? v : v * PCT_SCALE, 2) + "%";
    }
    return fmtNum(v, 2);
  }

  void applyFund(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      setTableMessage(m_fund, "Unavailable: " + elide(err));
      setPanelError(m_fundPanel.status, err);
      return;
    }
    const int n = COUNT_OF(FUND_FIELDS);
    m_fund->clearSpans();
    m_fund->setRowCount(n);
    for (int r = 0; r < n; ++r) {
      const FundField &f = FUND_FIELDS[r];
      double v = 0;
      const bool ok = jnum(o.value(f.key), v);
      setCell(m_fund, r, FC_FIELD, f.label, COL_DIM);
      setCell(m_fund, r, FC_VALUE, orNA(ok, fmtFund(f.kind, v)),
              ok ? COL_TEXT : COL_DIM, ALIGN_NUM);
    }
    setProvenance(m_fundPanel.status, o.value("provenance").toObject());
  }

  // ---- ANR ------------------------------------------------------------------

  void clearAnalyst(const QString &msg) {
    m_anConsensus->setText(QString());
    m_anDist->clear();
    m_anTargets->setText(QString());
    setTableMessage(m_anTable, msg);
  }

  static QString span(const QColor &c, const QString &text) {
    return QString("<span style='color:%1'>%2</span>")
        .arg(c.name(), text.toHtmlEscaped());
  }

  void applyAnalyst(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      clearAnalyst("Unavailable: " + elide(err));
      setPanelError(m_anPanel.status, err);
      return;
    }
    const QString consensus = jstr(o, "consensus");
    m_anConsensus->setText(consensus.isEmpty() ? QString("NO CONSENSUS")
                                               : consensus.toUpper());
    setLabelColor(m_anConsensus,
                  consensus.isEmpty() ? COL_DIM : ratingColor(consensus));

    int counts[DIST_SLOTS];
    int total = 0;
    for (int i = 0; i < DIST_SLOTS; ++i) {
      double d = 0;
      counts[i] = jnum(o.value(DIST_KEYS[i]), d) ? int(d) : 0;
      total += counts[i];
    }
    m_anDist->setCounts(counts);

    // Price targets + upside vs. the last quote.
    struct Target { const char *key; const char *label; };
    static const Target TARGETS[] = {{"targetLow", "LOW"},
                                     {"targetMean", "MEAN"},
                                     {"targetMedian", "MED"},
                                     {"targetHigh", "HIGH"}};
    QStringList parts;
    for (const Target &t : TARGETS) {
      double v = 0;
      if (jnum(o.value(t.key), v))
        parts << span(COL_DIM, t.label) + " " + span(COL_TEXT, fmtPrice(v));
    }
    double mean = 0, price = 0;
    if (jnum(o.value("targetMean"), mean) &&
        jnum(m_quotes.value(m_symbol).value("price"), price) && price != 0) {
      const double up = (mean - price) / price * PCT_SCALE;
      parts << span(COL_DIM, "UPSIDE") + " " +
                   span(changeColor(up), fmtPctSigned(up));
    }
    m_anTargets->setText(parts.isEmpty() ? span(COL_DIM, "No price targets")
                                         : parts.join("&nbsp;&nbsp;"));

    const QJsonArray ratings = o.value("ratings").toArray();
    m_anTable->clearSpans();
    m_anTable->setRowCount(ratings.size());
    for (int r = 0; r < ratings.size(); ++r) {
      const QJsonObject x = ratings[r].toObject();
      const QString rating = jstr(x, "rating");
      double pt = 0;
      const bool hasPt = jnum(x.value("priceTarget"), pt);
      setCell(m_anTable, r, RC_FIRM, jstr(x, "firm"));
      setCell(m_anTable, r, RC_RATING, rating, ratingColor(rating));
      setCell(m_anTable, r, RC_PT, orNA(hasPt, fmtPrice(pt)), COL_TEXT,
              ALIGN_NUM);
      setCell(m_anTable, r, RC_DATE, jstr(x, "date"), COL_DIM);
    }
    if (ratings.isEmpty()) setTableMessage(m_anTable, "No individual ratings");
    m_anTable->setToolTip(jstr(o, "sourceUrl"));
    setProvenance(m_anPanel.status, o.value("provenance").toObject(),
                  QString("%1 analysts").arg(total));
  }

  // ---- EARN -----------------------------------------------------------------

  void resetEarnTable(const QString &msg) {
    m_earn->clear();
    m_earn->setColumnCount(1);
    m_earn->setHorizontalHeaderLabels(QStringList() << "LINE");
    setTableMessage(m_earn, msg);
  }

  // Statement table: rows = line items, columns = periods.
  void applyStatements(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      resetEarnTable("Unavailable: " + elide(err));
      setPanelError(m_earnPanel.status, err);
      return;
    }
    const QJsonArray sts = o.value("statements").toArray();
    if (sts.isEmpty()) {
      resetEarnTable("No statements");
      setPanelBusy(m_earnPanel.status, NA_STATUS);
      return;
    }
    const QJsonObject st = sts.first().toObject();
    QStringList periods;
    for (const QJsonValue &p : st.value("periods").toArray())
      periods << p.toString();
    const QJsonArray lines = st.value("lines").toArray();

    m_earn->clear();
    m_earn->clearSpans();
    m_earn->setColumnCount(1 + periods.size());
    m_earn->setHorizontalHeaderLabels(QStringList() << "LINE" << periods);
    m_earn->setColumnWidth(0, EARN_LABEL_W);
    for (int c = 1; c <= periods.size(); ++c) {
      m_earn->setColumnWidth(c, EARN_COL_W);
      m_earn->horizontalHeaderItem(c)->setTextAlignment(ALIGN_NUM);
    }
    m_earn->setRowCount(lines.size());
    for (int r = 0; r < lines.size(); ++r) {
      const QJsonObject ln = lines[r].toObject();
      const QString key = jstr(ln, "key");
      const int indent = ln.value("indent").toInt();
      const QJsonObject values = ln.value("values").toObject();
      const bool perShare = key.startsWith(EPS_KEY_PREFIX);
      setCell(m_earn, r, 0,
              QString(indent * EARN_INDENT_SPACES, ' ') + jstr(ln, "label"),
              indent == 0 ? COL_TEXT : COL_DIM);
      for (int c = 0; c < periods.size(); ++c) {
        double v = 0;
        const bool ok = jnum(values.value(periods[c]), v);
        setCell(m_earn, r, c + 1,
                orNA(ok, perShare ? fmtNum(v, 2) : fmtBig(v)),
                ok ? (v < 0 ? COL_DOWN : COL_TEXT) : COL_DIM, ALIGN_NUM);
      }
    }
    if (lines.isEmpty()) setTableMessage(m_earn, "No line items");
    QJsonObject prov = st.value("provenance").toObject();
    if (prov.isEmpty()) prov = o.value("provenance").toObject();
    setProvenance(m_earnPanel.status, prov, jstr(st, "entityName"));
  }

  // ---- ECO ------------------------------------------------------------------

  void applyEcon(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      setPanelError(m_ecoPanel.status, err);
      if (m_econItems.isEmpty()) setTableMessage(m_eco, "Calendar unavailable");
      return;
    }
    m_econItems = o.value("items").toArray();
    // The route answers 200 + {items: [], error} when the collector failed.
    const QString bodyErr = jstr(o, "error");
    if (!bodyErr.isEmpty()) setPanelError(m_ecoPanel.status, bodyErr);
    else
      setProvenance(m_ecoPanel.status, o.value("provenance").toObject(),
                    QString("%1 events").arg(m_econItems.size()));
    renderEcon();
  }

  // Renders m_econItems through the impact filter; past events are dimmed and
  // the view scrolls to the next upcoming event.
  void renderEcon() {
    const QString mode = m_ecoImpact->currentData().toString();
    QVector<QJsonObject> rows;
    for (const QJsonValue &v : m_econItems) {
      const QJsonObject e = v.toObject();
      const QString imp = jstr(e, "impact").toLower();
      if (mode == "high" && imp != "high") continue;
      if (mode == "highmed" && imp != "high" && imp != "medium") continue;
      rows.append(e);
    }
    m_eco->clearSpans();
    m_eco->setRowCount(rows.size());
    const QDateTime now = QDateTime::currentDateTime();
    int nextRow = -1;
    for (int r = 0; r < rows.size(); ++r) {
      const QJsonObject &e = rows[r];
      const QString date = jstr(e, "date");
      const QDateTime dt = parseIso(date);
      const bool past = dt.isValid() && dt < now;
      if (!past && nextRow < 0) nextRow = r;
      const QColor base = past ? COL_DIM : COL_TEXT;
      const QString impact = jstr(e, "impact");
      setCell(m_eco, r, EC_TIME,
              dt.isValid() ? enLocale().toString(dt, "ddd MM/dd HH:mm") : date,
              base)
          ->setToolTip(date);
      setCell(m_eco, r, EC_CCY, jstr(e, "country"), base);
      setCell(m_eco, r, EC_IMPACT, impact, impactColor(impact));
      setCell(m_eco, r, EC_ACTUAL, jstr(e, "actual"), COL_AMBER, ALIGN_NUM);
      setCell(m_eco, r, EC_FCST, jstr(e, "forecast"), base, ALIGN_NUM);
      setCell(m_eco, r, EC_PRIOR, jstr(e, "previous"), base, ALIGN_NUM);
      setCell(m_eco, r, EC_EVENT, jstr(e, "title"), base)
          ->setToolTip(jstr(e, "url"));
    }
    if (rows.isEmpty()) {
      setTableMessage(m_eco, "No events");
    } else if (nextRow >= 0) {
      m_eco->scrollToItem(m_eco->item(nextRow, EC_TIME),
                          QAbstractItemView::PositionAtTop);
    }
  }

#ifdef EXTRA_DATA_PANELS
  // ==== Extra data panels: infrastructure ===================================

  static const int SYMBOL_ROLE = Qt::UserRole + 1;  // ticker behind a cell
  static const int URL_ROLE = Qt::UserRole;         // link behind a cell

  static bool isEquity(const QString &s) {
    static const QRegExp re("^[A-Z][A-Z0-9.\\-]{0,9}$");
    return re.exactMatch(s);
  }

  // GET + JSON; replies of an older request (same *seq) are dropped.
  void getSeq(const QString &path, const QUrlQuery &q, int *seq,
              std::function<void(const QString &, const QJsonObject &)> fn) {
    const int s = ++*seq;
    m_api.getJson(path, q,
                  [seq, s, fn](int st, const QJsonObject &o, const QString &err) {
                    if (*seq != s) return;
                    fn(bodyError(st, o, err), o);
                  });
  }

  TablePanel makeTablePanel(const QString &title, const ColSpec *cols, int n) {
    TablePanel tp;
    tp.p = makePanel(title);
    tp.table = makeColTable(cols, n);
    tp.p.body->addWidget(tp.table, 1);
    return tp;
  }

  static void beginRows(QTableWidget *t, int n) {
    t->clearSpans();
    t->setRowCount(0);  // fresh items: no stale tooltips / roles
    t->setRowCount(n);
  }

  // Fixed-layout boards: keep the items (and the scroll position).
  static void ensureRows(QTableWidget *t, int n) {
    t->clearSpans();
    t->setRowCount(n);
  }

  static QLabel *makeInfoLabel() {
    QLabel *l = new QLabel;
    l->setWordWrap(true);
    l->setTextFormat(Qt::RichText);
    l->setContentsMargins(PANEL_PAD, PANEL_PAD, PANEL_PAD, 0);
    return l;
  }

  void busyTable(TablePanel &tp) {
    setTableMessage(tp.table, LOADING_TEXT);
    setPanelBusy(tp.p.status, LOADING_TEXT);
  }

  void naTable(TablePanel &tp) {
    setTableMessage(tp.table, "Not available for " + m_symbol);
    setPanelBusy(tp.p.status, NA_STATUS);
  }

  // Error / empty handling of a list body; returns the rows to render (the
  // table already has that many rows).
  QJsonArray listItems(TablePanel &tp, const QString &err, const QJsonObject &o,
                       const QString &emptyMsg, const char *key = "items") {
    if (!err.isEmpty()) {
      setPanelError(tp.p.status, err);
      setTableMessage(tp.table, "Unavailable: " + elide(err));
      return QJsonArray();
    }
    const QJsonArray items = o.value(QLatin1String(key)).toArray();
    if (items.isEmpty()) setTableMessage(tp.table, emptyMsg);
    else beginRows(tp.table, items.size());
    setProvenance(tp.p.status, o.value("provenance").toObject(),
                  QString::number(items.size()));
    return items;
  }

  // Double-click on column `col` loads the ticker stored in SYMBOL_ROLE (or
  // the cell text) and switches to the DENSE workspace.
  void linkSymbolColumn(QTableWidget *t, int col) {
    QObject::connect(t, &QTableWidget::cellDoubleClicked, this,
                     [this, t, col](int row, int c) {
                       if (c != col) return;
                       QTableWidgetItem *it = t->item(row, col);
                       if (!it) return;
                       QString sym = it->data(SYMBOL_ROLE).toString();
                       if (sym.isEmpty()) sym = it->text();
                       sym = normSymbol(sym);
                       if (sym.isEmpty()) return;
                       loadSymbol(sym);
                       m_workspace->setCurrentIndex(0);
                     });
  }

  // Double-click anywhere except `skipCol` opens the URL stored in column 0.
  void linkUrlRows(QTableWidget *t, int skipCol) {
    QObject::connect(t, &QTableWidget::cellDoubleClicked, this,
                     [t, skipCol](int row, int c) {
                       if (c == skipCol) return;
                       QTableWidgetItem *it = t->item(row, 0);
                       const QString url =
                           it ? it->data(URL_ROLE).toString() : QString();
                       if (!url.isEmpty()) QDesktopServices::openUrl(QUrl(url));
                     });
  }

  // Quote cells shared by the boards: LAST, CHG, %CHG at columns c0..c0+2.
  static void setQuoteCells(QTableWidget *t, int r, int c0,
                            const QJsonObject &q) {
    double price = 0, chg = 0, pct = 0;
    const bool hp = jnum(q.value("price"), price);
    const bool hc = jnum(q.value("change"), chg);
    const bool hpct = jnum(q.value("changePercent"), pct);
    const QColor cc = hc ? changeColor(chg) : COL_DIM;
    setCell(t, r, c0, orNA(hp, fmtPrice(price)), hp ? COL_TEXT : COL_DIM,
            ALIGN_NUM);
    setCell(t, r, c0 + 1, orNA(hc, fmtSigned(chg, 2)), cc, ALIGN_NUM);
    setCell(t, r, c0 + 2, orNA(hpct, fmtPctSigned(pct)), cc, ALIGN_NUM);
  }

  static void boardStatus(QLabel *l, int total, int bad) {
    QString txt = QString("%1 quotes").arg(total);
    if (bad > 0) txt += QString("  %1 unavailable").arg(bad);
    txt += "  " + QDateTime::currentDateTime().toString("HH:mm:ss");
    l->setText(txt);
    l->setToolTip(QString());
    setLabelColor(l, bad > 0 ? COL_WARN : COL_UP);
  }

  // Modeless reader window; the QPointers go null once it is closed.
  Reader openReader(const QString &title, const QString &url) {
    Reader rd;
    QDialog *d = new QDialog(this);
    d->setAttribute(Qt::WA_DeleteOnClose);
    d->setWindowTitle(title.isEmpty() ? QString(WINDOW_TITLE) : title);
    d->resize(READER_W, READER_H);
    QVBoxLayout *v = new QVBoxLayout(d);
    v->setContentsMargins(PANEL_PAD, PANEL_PAD, PANEL_PAD, PANEL_PAD);
    v->setSpacing(PANEL_SPACING);
    rd.bar = new QHBoxLayout;
    QLabel *st = new QLabel;
    setPanelBusy(st, LOADING_TEXT);
    rd.bar->addWidget(st, 1);
    if (!url.isEmpty()) {
      QPushButton *open = new QPushButton("BROWSER");
      open->setToolTip(url);
      QObject::connect(open, &QPushButton::clicked, d,
                       [url]() { QDesktopServices::openUrl(QUrl(url)); });
      rd.bar->addWidget(open);
    }
    QPushButton *close = new QPushButton("CLOSE");
    QObject::connect(close, &QPushButton::clicked, d, &QDialog::close);
    rd.bar->addWidget(close);
    v->addLayout(rd.bar);
    QTextBrowser *tb = new QTextBrowser;
    tb->setOpenLinks(false);
    const QUrl base(url);
    QObject::connect(tb, &QTextBrowser::anchorClicked, d,
                     [tb, base](const QUrl &link) {
                       if (link.isRelative() && link.path().isEmpty() &&
                           !link.fragment().isEmpty()) {
                         tb->scrollToAnchor(link.fragment());
                         return;
                       }
                       const QUrl abs = base.isValid() ? base.resolved(link) : link;
                       if (abs.scheme() == "http" || abs.scheme() == "https")
                         QDesktopServices::openUrl(abs);
                     });
    v->addWidget(tb, 1);
    rd.dlg = d;
    rd.text = tb;
    rd.status = st;
    d->show();
    return rd;
  }

  // News reader: readable text through the news collector, the browser as
  // fallback.
  void openArticle(const QString &url, const QString &title) {
    const Reader rd = openReader(title, url);
    const QString head = "<h2>" + title.toHtmlEscaped() + "</h2>";
    rd.text->setHtml(head + "<p>" + QString(LOADING_TEXT) + "</p>");
    QUrlQuery q;
    q.addQueryItem("url", url);
    m_api.getJson(
        ROUTE_ARTICLE, q,
        [rd, head, url](int st, const QJsonObject &o, const QString &err) {
          if (!rd.text) return;
          const QString e = bodyError(st, o, err);
          if (!e.isEmpty()) {
            setPanelError(rd.status, e);
            rd.text->setHtml(head + "<p>Reader text unavailable: " +
                             e.toHtmlEscaped() + "</p><p><a href=\"" +
                             url.toHtmlEscaped() +
                             "\">Open the original article</a></p>");
            return;
          }
          QStringList meta;
          for (const char *k : {"source", "author", "pubDate"}) {
            const QString v = jstr(o, k);
            if (!v.isEmpty()) meta << v.toHtmlEscaped();
          }
          const QString t = jstr(o, "title");
          rd.text->setHtml(
              (t.isEmpty() ? head : "<h2>" + t.toHtmlEscaped() + "</h2>") +
              "<p><i>" + meta.join(" \xc2\xb7 ") + "</i></p>" +
              paragraphsHtml(o.value("paragraphs").toArray()));
          setProvenance(rd.status, o.value("provenance").toObject());
        });
  }

  // SEC filing document (sanitized HTML) with section and exhibit pickers.
  void openFilingDoc(const QString &cik, const QString &acc, const QString &doc,
                     const QString &title, const QString &url) {
    static const char *PAPER_CSS =
        "QTextBrowser { background-color: #ffffff; color: #000000; }";
    const Reader rd = openReader(title, url);
    rd.text->setStyleSheet(PAPER_CSS);
    QUrlQuery q;
    q.addQueryItem("cik", cik);
    q.addQueryItem("accession", acc);
    q.addQueryItem("doc", doc);
    m_api.getJson(
        ROUTE_FILING_DOC, q,
        [rd, url, doc](int st, const QJsonObject &o, const QString &err) {
          if (!rd.text) return;
          const QString e = bodyError(st, o, err);
          if (!e.isEmpty()) {
            setPanelError(rd.status, e);
            rd.text->setHtml("<p>Document unavailable: " + e.toHtmlEscaped() +
                             "</p><p><a href=\"" + url.toHtmlEscaped() +
                             "\">Open on SEC.gov</a></p>");
            return;
          }
          rd.text->setHtml(jstr(o, "html"));
          QDialog *dlg = rd.dlg.data();
          const QJsonArray secs = o.value("sections").toArray();
          if (!secs.isEmpty()) {
            QComboBox *sc = new QComboBox;
            sc->addItem("Sections\xe2\x80\xa6");
            for (const QJsonValue &v : secs)
              sc->addItem(jstr(v.toObject(), "title"),
                          jstr(v.toObject(), "anchor"));
            QPointer<QTextBrowser> tb = rd.text;
            onComboChanged(sc, dlg, [sc, tb](int i) {
              if (i > 0 && tb) tb->scrollToAnchor(sc->itemData(i).toString());
            });
            rd.bar->insertWidget(1, sc);
          }
          const QJsonArray exh = o.value("exhibits").toArray();
          if (!exh.isEmpty()) {
            QComboBox *ec = new QComboBox;
            ec->addItem("Exhibits\xe2\x80\xa6");
            for (const QJsonValue &v : exh)
              ec->addItem(jstr(v.toObject(), "name"), jstr(v.toObject(), "url"));
            onComboChanged(ec, dlg, [ec](int i) {
              if (i > 0) QDesktopServices::openUrl(QUrl(ec->itemData(i).toString()));
            });
            rd.bar->insertWidget(1, ec);
          }
          setProvenance(rd.status, o.value("provenance").toObject(), doc);
        });
  }

  void refreshExtra() {
    for (TablePanel *tp : {&m_fil, &m_ins, &m_hds, &m_om, &m_tran})
      tp->loadedFor.clear();
    ensureSymbolTab();
    m_pagesLoaded.clear();
    refreshPage(false, false);
    fetchFearGreed();
  }

  void startExtraTimers() {
    m_pageFastTimer = new QTimer(this);
    m_pageFastTimer->setInterval(PAGE_FAST_MS);
    QObject::connect(m_pageFastTimer, &QTimer::timeout, this,
                     [this]() { refreshPage(true, false); });
    m_pageFastTimer->start();
    m_pageSlowTimer = new QTimer(this);
    m_pageSlowTimer->setInterval(PAGE_SLOW_MS);
    QObject::connect(m_pageSlowTimer, &QTimer::timeout, this,
                     [this]() { refreshPage(false, false); });
    m_pageSlowTimer->start();
    m_fngTimer = new QTimer(this);
    m_fngTimer->setInterval(FNG_POLL_MS);
    QObject::connect(m_fngTimer, &QTimer::timeout, this,
                     [this]() { fetchFearGreed(); });
    m_fngTimer->start();
  }

  // ---- ticker search (command line completer) ------------------------------

  void setupSearch() {
    m_searchModel = new QStringListModel(this);
    m_completer = new QCompleter(m_searchModel, this);
    m_completer->setCaseSensitivity(Qt::CaseInsensitive);
    m_completer->setCompletionMode(QCompleter::UnfilteredPopupCompletion);
    m_cmd->setCompleter(m_completer);
    QObject::connect(
        m_completer,
        static_cast<void (QCompleter::*)(const QString &)>(&QCompleter::activated),
        this, [this](const QString &text) {
          const QString sym = normSymbol(text.section(' ', 0, 0));
          // After QCompleter has finished writing into the line edit.
          QTimer::singleShot(0, this, [this, sym]() {
            m_cmd->clear();
            if (!sym.isEmpty()) loadSymbol(sym);
          });
        });
    m_searchTimer = new QTimer(this);
    m_searchTimer->setSingleShot(true);
    m_searchTimer->setInterval(SEARCH_DEBOUNCE_MS);
    QObject::connect(m_searchTimer, &QTimer::timeout, this,
                     [this]() { fetchSearch(); });
    QObject::connect(m_cmd, &QLineEdit::textEdited, this,
                     [this](const QString &) { m_searchTimer->start(); });
  }

  void fetchSearch() {
    const QString text = m_cmd->text().trimmed();
    if (text.size() < SEARCH_MIN_CHARS || text.startsWith('-')) return;
    QUrlQuery q;
    q.addQueryItem("q", text);
    getSeq(ROUTE_SEARCH, q, &m_searchSeq,
           [this](const QString &err, const QJsonObject &o) {
             if (!err.isEmpty()) return;  // typing goes on; no popup
             QStringList rows;
             for (const QJsonValue &v : o.value("quotes").toArray()) {
               const QJsonObject x = v.toObject();
               QStringList meta;
               for (const char *k : {"exchange", "type"}) {
                 const QString s = jstr(x, k);
                 if (!s.isEmpty()) meta << s;
               }
               rows << QString("%1  \xe2\x80\x94  %2  (%3)")
                           .arg(jstr(x, "symbol"), jstr(x, "name"),
                                meta.join(" \xc2\xb7 "));
             }
             m_searchModel->setStringList(rows);
             if (!rows.isEmpty() && m_cmd->hasFocus() &&
                 !m_cmd->text().trimmed().isEmpty())
               m_completer->complete();
           });
  }

  // ---- symbol tabs: FIL / INS / HDS / OMON / TRAN --------------------------

  void buildSymbolTabs() {
    static const ColSpec FIL_COLS[] = {{"FILED", 84, false},
                                       {"FORM", 70, false},
                                       {"PERIOD", 84, false},
                                       {"DESCRIPTION", 0, false}};
    m_fil = makeTablePanel("FIL  SEC FILINGS", FIL_COLS, COUNT_OF(FIL_COLS));
    m_filForms = new QComboBox;
    fillCombo(m_filForms, FILING_FORMS, COUNT_OF(FILING_FORMS));
    onComboChanged(m_filForms, this, [this](int) { fetchFilings(); });
    m_fil.p.addControl(m_filForms);
    m_fil.table->setToolTip("Double-click: open the filing document");
    QObject::connect(m_fil.table, &QTableWidget::cellDoubleClicked, this,
                     [this](int row, int) { openFilingRow(row); });
    m_tabs->addTab(m_fil.p.frame, "FIL");

    static const ColSpec INS_COLS[] = {
        {"FILED", 84, false},  {"INSIDER", 170, false}, {"TITLE", 120, false},
        {"CODE", 46, false},   {"SHARES", 80, true},    {"PRICE", 70, true},
        {"VALUE", 80, true},   {"OWNED AFTER", 0, true}};
    m_ins = makeTablePanel("INS  INSIDER TRANSACTIONS (FORM 4)", INS_COLS,
                           COUNT_OF(INS_COLS));
    m_ins.table->setToolTip("Double-click: open the Form 4 on SEC.gov");
    linkUrlRows(m_ins.table, -1);
    m_tabs->addTab(m_ins.p.frame, "INS");

    static const ColSpec HDS_COLS[] = {{"FILER (13F-HR)", 280, false},
                                       {"PERIOD", 90, false},
                                       {"FILED", 90, false},
                                       {"ACCESSION", 0, false}};
    m_hds = makeTablePanel("HDS  INSTITUTIONAL HOLDERS", HDS_COLS,
                           COUNT_OF(HDS_COLS));
    m_hdsSummary = makeInfoLabel();
    m_hds.p.body->insertWidget(0, m_hdsSummary);
    m_hds.table->setToolTip("Double-click: show this manager's 13F holdings");
    QObject::connect(m_hds.table, &QTableWidget::cellDoubleClicked, this,
                     [this](int row, int) {
                       QTableWidgetItem *it = m_hds.table->item(row, 0);
                       const QString cik =
                           it ? it->data(SYMBOL_ROLE).toString() : QString();
                       if (!cik.isEmpty()) openManager(cik);
                     });
    m_tabs->addTab(m_hds.p.frame, "HDS");

    static const ColSpec OM_COLS[] = {
        {"C BID", 62, true}, {"C ASK", 62, true},  {"C LAST", 62, true},
        {"C VOL", 58, true}, {"C OI", 62, true},   {"C IV", 52, true},
        {"STRIKE", 70, true}, {"P IV", 52, true},  {"P OI", 62, true},
        {"P VOL", 58, true}, {"P LAST", 62, true}, {"P BID", 62, true},
        {"P ASK", 0, true}};
    m_om = makeTablePanel("OMON  OPTION MONITOR", OM_COLS, COUNT_OF(OM_COLS));
    m_omExpiry = new QComboBox;
    m_omExpiry->setToolTip("Expiration");
    onComboChanged(m_omExpiry, this, [this](int) {
      fetchOptions(m_omExpiry->currentData().toString());
    });
    m_om.p.addControl(m_omExpiry);
    m_omSummary = makeInfoLabel();
    m_om.p.body->insertWidget(0, m_omSummary);
    m_tabs->addTab(m_om.p.frame, "OMON");

    static const ColSpec TRAN_COLS[] = {{"CALL DATE", 90, false},
                                        {"QUARTER", 90, false},
                                        {"TITLE", 0, false}};
    m_tran.p = makePanel("TRAN  EARNINGS CALL TRANSCRIPTS");
    m_tran.table = makeColTable(TRAN_COLS, COUNT_OF(TRAN_COLS));
    m_tranText = new QTextBrowser;
    m_tranText->setOpenExternalLinks(true);
    QSplitter *ts = new QSplitter(Qt::Vertical);
    ts->setHandleWidth(SPLITTER_HANDLE);
    ts->addWidget(m_tran.table);
    ts->addWidget(m_tranText);
    ts->setStretchFactor(1, 1);
    m_tran.p.body->addWidget(ts, 1);
    QObject::connect(m_tran.table, &QTableWidget::cellClicked, this,
                     [this](int row, int) {
                       QTableWidgetItem *it = m_tran.table->item(row, 0);
                       const QString id =
                           it ? it->data(SYMBOL_ROLE).toString() : QString();
                       if (!id.isEmpty()) fetchTranscript(id);
                     });
    m_tabs->addTab(m_tran.p.frame, "TRAN");

    QObject::connect(m_tabs, &QTabWidget::currentChanged, this,
                     [this](int) { ensureSymbolTab(); });
  }

  // Loads the visible symbol tab when it does not show m_symbol yet.
  void ensureSymbolTab() {
    if (!m_tabs) return;
    QWidget *w = m_tabs->currentWidget();
    if (w == m_fil.p.frame && m_fil.loadedFor != m_symbol) fetchFilings();
    else if (w == m_ins.p.frame && m_ins.loadedFor != m_symbol) fetchInsider();
    else if (w == m_hds.p.frame && m_hds.loadedFor != m_symbol)
      fetchInstitutional();
    else if (w == m_om.p.frame && m_om.loadedFor != m_symbol)
      fetchOptions(QString());
    else if (w == m_tran.p.frame && m_tran.loadedFor != m_symbol)
      fetchTranscripts();
  }

  void symbolTabsChanged() {
    // Drop replies still in flight for the previous symbol.
    for (TablePanel *tp : {&m_fil, &m_ins, &m_hds, &m_om, &m_tran}) ++tp->seq;
    ++m_tranDocSeq;
    ensureSymbolTab();
  }

  // FIL ---------------------------------------------------------------------

  void fetchFilings() {
    m_fil.loadedFor = m_symbol;
    m_filItems = QJsonArray();
    if (!isEquity(m_symbol)) {
      naTable(m_fil);
      return;
    }
    QUrlQuery q;
    q.addQueryItem("ticker", m_symbol);
    q.addQueryItem("limit", QString::number(FILINGS_LIMIT));
    const QString forms = m_filForms->currentData().toString();
    if (!forms.isEmpty()) q.addQueryItem("forms", forms);
    busyTable(m_fil);
    getSeq(ROUTE_FILINGS, q, &m_fil.seq,
           [this](const QString &err, const QJsonObject &o) {
             m_filItems = listItems(m_fil, err, o, "No filings");
             QTableWidget *t = m_fil.table;
             for (int r = 0; r < m_filItems.size(); ++r) {
               const QJsonObject f = m_filItems[r].toObject();
               const QString form = jstr(f, "form");
               const bool major = form.startsWith("10-") || form == "20-F";
               setCell(t, r, 0, jstr(f, "filingDate"), COL_DIM);
               setCell(t, r, 1, form, major ? COL_AMBER : COL_LINK);
               setCell(t, r, 2, jstr(f, "reportDate"), COL_DIM);
               QString desc = jstr(f, "primaryDocDescription");
               if (desc.isEmpty()) desc = jstr(f, "primaryDocument");
               setCell(t, r, 3, desc)
                   ->setToolTip(jstr(f, "accessionNumber") + "\n" +
                                jstr(f, "url"));
             }
             if (!m_filItems.isEmpty())
               setProvenance(m_fil.p.status, o.value("provenance").toObject(),
                             jstr(m_filItems[0].toObject(), "entityName"));
           });
  }

  void openFilingRow(int row) {
    if (row < 0 || row >= m_filItems.size()) return;
    const QJsonObject f = m_filItems[row].toObject();
    const QString doc = jstr(f, "primaryDocument");
    const QString url = jstr(f, "url");
    QString cik = jstr(f, "cik");
    while (cik.startsWith('0') && cik.size() > 1) cik.remove(0, 1);
    const QString title = QString("%1 %2 \xe2\x80\x94 %3")
                              .arg(jstr(f, "entityName"), jstr(f, "form"),
                                   jstr(f, "filingDate"));
    if (doc.isEmpty() || cik.isEmpty()) {
      if (!url.isEmpty()) QDesktopServices::openUrl(QUrl(url));
      return;
    }
    openFilingDoc(cik, jstr(f, "accessionNumber"), doc, title, url);
  }

  // INS ---------------------------------------------------------------------

  void fetchInsider() {
    m_ins.loadedFor = m_symbol;
    if (!isEquity(m_symbol)) {
      naTable(m_ins);
      return;
    }
    QUrlQuery q;
    q.addQueryItem("ticker", m_symbol);
    q.addQueryItem("limit", QString::number(INSIDER_LIMIT));
    busyTable(m_ins);
    getSeq(ROUTE_INSIDER, q, &m_ins.seq,
           [this](const QString &err, const QJsonObject &o) {
             renderInsider(m_ins, err, o, false);
           });
  }

  // One row per Form 4 transaction (filings without parsed transactions get
  // a single row). Market-wide: columns 1/2 are TICKER / ISSUER.
  void renderInsider(TablePanel &tp, const QString &err, const QJsonObject &o,
                     bool marketwide) {
    const QJsonArray filings = listItems(tp, err, o, "No Form 4 filings");
    if (filings.isEmpty()) return;
    struct Row {
      QJsonObject f;
      QJsonObject tx;
    };
    QVector<Row> rows;
    for (const QJsonValue &v : filings) {
      const QJsonObject f = v.toObject();
      const QJsonArray txs = f.value("transactions").toArray();
      if (txs.isEmpty()) rows.append({f, QJsonObject()});
      for (const QJsonValue &t : txs) rows.append({f, t.toObject()});
    }
    QTableWidget *t = tp.table;
    beginRows(t, rows.size());
    for (int r = 0; r < rows.size(); ++r) {
      const QJsonObject &f = rows[r].f;
      const QJsonObject &tx = rows[r].tx;
      QTableWidgetItem *c0 = setCell(t, r, 0, jstr(f, "filingDate"), COL_DIM);
      c0->setData(URL_ROLE, jstr(f, "url"));
      const QString txDate = jstr(tx, "date");
      c0->setToolTip(txDate.isEmpty() ? jstr(f, "accessionNumber")
                                      : "transaction " + txDate);
      if (marketwide) {
        const QString tick = jstr(f, "issuerTicker");
        setCell(t, r, 1, tick, COL_LINK)->setData(SYMBOL_ROLE, tick);
        setCell(t, r, 2, jstr(f, "issuerName"));
      } else {
        setCell(t, r, 1, jstr(f, "reportingName"));
        setCell(t, r, 2, jstr(f, "reportingTitle"), COL_DIM);
      }
      const QString code = jstr(tx, "code");
      const QString ad = jstr(tx, "acquiredDisposed");
      const bool deriv = tx.value("derivative").toBool();
      QString codeTip = code.isEmpty() ? jstr(f, "detailsError")
                                       : insiderCodeText(code);
      if (deriv) codeTip += " (derivative)";
      if (marketwide) codeTip = jstr(f, "reportingName") + ": " + codeTip;
      setCell(t, r, 3, code + (deriv ? "*" : ""), insiderCodeColor(code, ad))
          ->setToolTip(codeTip);
      const QString sign = ad == "D" ? "-" : QString();
      double sh = 0;
      const bool hs = jnum(tx.value("shares"), sh);
      setCell(t, r, 4, hs ? sign + fmtBig(sh) : QString(NOT_AVAILABLE),
              insiderCodeColor(code, ad), ALIGN_NUM);
      setCell(t, r, 5, priceOr(tx.value("price")), COL_TEXT, ALIGN_NUM);
      setCell(t, r, 6, bigOr(tx.value("value")), COL_TEXT, ALIGN_NUM);
      if (!marketwide)
        setCell(t, r, 7, bigOr(tx.value("sharesOwnedAfter")), COL_DIM, ALIGN_NUM);
      else
        setCell(t, r, 7, jstr(f, "reportingName"), COL_DIM);
    }
  }

  // HDS ---------------------------------------------------------------------

  void fetchInstitutional() {
    m_hds.loadedFor = m_symbol;
    m_hdsSummary->clear();
    if (!isEquity(m_symbol)) {
      naTable(m_hds);
      return;
    }
    QUrlQuery q;
    q.addQueryItem("ticker", m_symbol);
    busyTable(m_hds);
    getSeq(ROUTE_INSTITUTIONAL, q, &m_hds.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray filers = listItems(m_hds, err, o,
                                                 "No 13F-HR filers found",
                                                 "filers");
             if (!err.isEmpty()) return;
             QTableWidget *t = m_hds.table;
             for (int r = 0; r < filers.size(); ++r) {
               const QJsonObject f = filers[r].toObject();
               QTableWidgetItem *n = setCell(t, r, 0, jstr(f, "name"));
               n->setData(SYMBOL_ROLE, jstr(f, "cik"));
               n->setToolTip("CIK " + jstr(f, "cik"));
               setCell(t, r, 1, jstr(f, "periodEnding"), COL_DIM);
               setCell(t, r, 2, jstr(f, "filingDate"), COL_DIM);
               setCell(t, r, 3, jstr(f, "accessionNumber"), COL_DIM);
             }
             QStringList parts;
             parts << span(COL_AMBER, jstr(o, "name"));
             parts << span(COL_DIM, "13F-HR FILERS") + " " +
                          span(COL_TEXT, QString::number(o.value("totalFilers").toInt()));
             double so = 0;
             if (jnum(o.value("sharesOutstanding"), so))
               parts << span(COL_DIM, "SHARES OUT") + " " +
                            span(COL_TEXT, fmtBig(so) + " (" +
                                               jstr(o, "sharesOutstandingDate") + ")");
             m_hdsSummary->setText(parts.join("&nbsp;&nbsp;&nbsp;"));
             setProvenance(m_hds.p.status, o.value("provenance").toObject(),
                           QString::number(filers.size()));
           });
  }

  // OMON --------------------------------------------------------------------

  void fetchOptions(const QString &date) {
    m_om.loadedFor = m_symbol;
    if (m_omExpSymbol != m_symbol) {
      const QSignalBlocker block(m_omExpiry);
      m_omExpiry->clear();
    }
    QUrlQuery q;
    q.addQueryItem("symbol", m_symbol);
    if (!date.isEmpty()) q.addQueryItem("date", date);
    busyTable(m_om);
    m_omSummary->setText(span(COL_DIM, LOADING_TEXT));
    const QString sym = m_symbol;
    getSeq(ROUTE_OPTIONS, q, &m_om.seq,
           [this, date, sym](const QString &err, const QJsonObject &o) {
             applyOptions(err, o, date, sym);
           });
  }

  void applyOptions(const QString &err, const QJsonObject &o,
                    const QString &date, const QString &sym) {
    if (!err.isEmpty()) {
      setPanelError(m_om.p.status, err);
      setTableMessage(m_om.table, "Option chain unavailable: " + elide(err));
      m_omSummary->clear();
      return;
    }
    const QJsonArray exps = o.value("expirations").toArray();
    const QString expiry =
        !date.isEmpty() ? date : (exps.isEmpty() ? QString() : exps[0].toString());
    if (m_omExpSymbol != sym || m_omExpiry->count() == 0) {
      const QSignalBlocker block(m_omExpiry);
      m_omExpiry->clear();
      for (const QJsonValue &v : exps) m_omExpiry->addItem(v.toString(), v.toString());
      m_omExpSymbol = sym;
    }
    {
      const QSignalBlocker block(m_omExpiry);
      const int idx = m_omExpiry->findData(expiry);
      if (idx >= 0) m_omExpiry->setCurrentIndex(idx);
    }

    double spot = 0;
    const bool hasSpot = jnum(o.value("spot"), spot);
    struct Pair {
      QJsonObject call, put;
    };
    QMap<double, Pair> byStrike;
    double callOI = 0, putOI = 0, callVol = 0, putVol = 0;
    for (const QJsonValue &v : o.value("contracts").toArray()) {
      const QJsonObject c = v.toObject();
      if (!expiry.isEmpty() && jstr(c, "expiry") != expiry) continue;
      double k = 0, oi = 0, vol = 0;
      if (!jnum(c.value("strike"), k)) continue;
      jnum(c.value("openInterest"), oi);
      jnum(c.value("volume"), vol);
      if (jstr(c, "type") == "call") {
        byStrike[k].call = c;
        callOI += oi;
        callVol += vol;
      } else {
        byStrike[k].put = c;
        putOI += oi;
        putVol += vol;
      }
    }
    QTableWidget *t = m_om.table;
    if (byStrike.isEmpty()) {
      setTableMessage(t, "No contracts for " + sym);
      m_omSummary->clear();
      setProvenance(m_om.p.status, o.value("provenance").toObject());
      return;
    }
    beginRows(t, byStrike.size());
    int r = 0, atmRow = 0;
    double atmDist = -1;
    enum { C_BID, C_ASK, C_LAST, C_VOL, C_OI, C_IV, K, P_IV, P_OI, P_VOL,
           P_LAST, P_BID, P_ASK };
    auto itm = [&](const QJsonObject &c, bool call, double k) {
      if (c.contains("inTheMoney")) return c.value("inTheMoney").toBool();
      return hasSpot && (call ? k < spot : k > spot);
    };
    auto ivText = [](const QJsonObject &c) {
      double iv = 0;
      return jnum(c.value("impliedVolatility"), iv)
                 ? fmtNum(iv * PCT_SCALE, 1)
                 : QString(NOT_AVAILABLE);
    };
    for (auto it = byStrike.constBegin(); it != byStrike.constEnd(); ++it, ++r) {
      const double k = it.key();
      const QJsonObject &c = it.value().call;
      const QJsonObject &p = it.value().put;
      const QColor cc = itm(c, true, k) ? COL_TEXT : COL_DIM;
      const QColor pc = itm(p, false, k) ? COL_TEXT : COL_DIM;
      setCell(t, r, C_BID, priceOr(c.value("bid")), cc, ALIGN_NUM);
      setCell(t, r, C_ASK, priceOr(c.value("ask")), cc, ALIGN_NUM);
      setCell(t, r, C_LAST, priceOr(c.value("last")), cc, ALIGN_NUM);
      setCell(t, r, C_VOL, bigOr(c.value("volume")), cc, ALIGN_NUM);
      setCell(t, r, C_OI, bigOr(c.value("openInterest")), cc, ALIGN_NUM);
      setCell(t, r, C_IV, ivText(c), cc, ALIGN_NUM);
      QTableWidgetItem *ki = setCell(t, r, K, fmtPrice(k), COL_AMBER, ALIGN_NUM);
      ki->setToolTip(jstr(c, "contractSymbol") + "\n" + jstr(p, "contractSymbol"));
      setCell(t, r, P_IV, ivText(p), pc, ALIGN_NUM);
      setCell(t, r, P_OI, bigOr(p.value("openInterest")), pc, ALIGN_NUM);
      setCell(t, r, P_VOL, bigOr(p.value("volume")), pc, ALIGN_NUM);
      setCell(t, r, P_LAST, priceOr(p.value("last")), pc, ALIGN_NUM);
      setCell(t, r, P_BID, priceOr(p.value("bid")), pc, ALIGN_NUM);
      setCell(t, r, P_ASK, priceOr(p.value("ask")), pc, ALIGN_NUM);
      if (hasSpot && (atmDist < 0 || std::fabs(k - spot) < atmDist)) {
        atmDist = std::fabs(k - spot);
        atmRow = r;
      }
    }
    if (hasSpot) {
      QTableWidgetItem *ki = t->item(atmRow, K);
      QFont f = t->font();
      f.setBold(true);
      ki->setFont(f);
      ki->setBackground(COL_BAR);
      t->scrollToItem(ki, QAbstractItemView::PositionAtCenter);
    }
    QStringList parts;
    parts << span(COL_AMBER, sym);
    if (hasSpot) parts << span(COL_DIM, "SPOT") + " " + span(COL_TEXT, fmtPrice(spot));
    const QDate ed = QDate::fromString(expiry, Qt::ISODate);
    if (ed.isValid())
      parts << span(COL_DIM, "EXP") + " " +
                   span(COL_TEXT, QString("%1 (%2d)").arg(expiry).arg(
                                      QDate::currentDate().daysTo(ed)));
    if (callOI > 0)
      parts << span(COL_DIM, "P/C OI") + " " + span(COL_TEXT, fmtNum(putOI / callOI, 2));
    if (callVol > 0)
      parts << span(COL_DIM, "P/C VOL") + " " + span(COL_TEXT, fmtNum(putVol / callVol, 2));
    parts << span(COL_DIM, "CALL OI") + " " + span(COL_UP, fmtBig(callOI))
          << span(COL_DIM, "PUT OI") + " " + span(COL_DOWN, fmtBig(putOI));
    m_omSummary->setText(parts.join("&nbsp;&nbsp;&nbsp;"));
    setProvenance(m_om.p.status, o.value("provenance").toObject(),
                  QString("%1 strikes").arg(byStrike.size()));
  }

  // TRAN --------------------------------------------------------------------

  void fetchTranscripts() {
    m_tran.loadedFor = m_symbol;
    m_tranText->clear();
    if (!isEquity(m_symbol)) {
      naTable(m_tran);
      return;
    }
    QUrlQuery q;
    q.addQueryItem("symbol", m_symbol);
    busyTable(m_tran);
    getSeq(ROUTE_TRANSCRIPTS, q, &m_tran.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray items = listItems(m_tran, err, o, "No transcripts");
             QTableWidget *t = m_tran.table;
             for (int r = 0; r < items.size(); ++r) {
               const QJsonObject x = items[r].toObject();
               setCell(t, r, 0, jstr(x, "callDate"), COL_DIM)
                   ->setData(SYMBOL_ROLE, jstr(x, "id"));
               setCell(t, r, 1, jstr(x, "quarterLabel"), COL_AMBER);
               setCell(t, r, 2, jstr(x, "title"))->setToolTip(jstr(x, "sourceUrl"));
             }
             if (!items.isEmpty()) {
               t->selectRow(0);
               fetchTranscript(jstr(items[0].toObject(), "id"));
             }
           });
  }

  void fetchTranscript(const QString &id) {
    QUrlQuery q;
    q.addQueryItem("id", id);
    m_tranText->setHtml("<p>" + QString(LOADING_TEXT) + "</p>");
    getSeq(ROUTE_TRANSCRIPTS, q, &m_tranDocSeq,
           [this](const QString &err, const QJsonObject &o) {
             if (!err.isEmpty()) {
               m_tranText->setHtml("<p>Transcript unavailable: " +
                                   err.toHtmlEscaped() + "</p>");
               return;
             }
             const QJsonObject meta = o.value("metadata").toObject();
             QString html = "<h3>" + jstr(meta, "title").toHtmlEscaped() +
                            "</h3><p><a href=\"" +
                            jstr(meta, "sourceUrl").toHtmlEscaped() +
                            "\">source</a></p>";
             QString section, speaker;
             for (const QJsonValue &v : o.value("segments").toArray()) {
               const QJsonObject s = v.toObject();
               const QString sec = jstr(s, "section");
               if (sec != section) {
                 section = sec;
                 speaker.clear();
                 html += "<h4 style='color:" + COL_AMBER.name() + "'>" +
                         QString(sec).replace('_', ' ').toHtmlEscaped() + "</h4>";
               }
               const QString who = jstr(s, "speakerName");
               if (who != speaker) {
                 speaker = who;
                 QStringList role;
                 for (const char *k : {"speakerTitle", "speakerRole"}) {
                   const QString x = jstr(s, k);
                   if (!x.isEmpty()) role << x;
                 }
                 const QColor wc =
                     jstr(s, "speakerRole") == "ANALYST" ? COL_LINK : COL_TEXT;
                 html += "<p><b style='color:" + wc.name() + "'>" +
                         who.toHtmlEscaped() + "</b> <span style='color:" +
                         COL_DIM.name() + "'>" +
                         role.join(" \xc2\xb7 ").toHtmlEscaped() + "</span></p>";
               }
               html += "<p>" + jstr(s, "text").toHtmlEscaped() + "</p>";
             }
             m_tranText->setHtml(html);
             setProvenance(m_tran.p.status, o.value("provenance").toObject(),
                           jstr(meta, "quarterLabel"));
           });
  }

  // ==== Workspace pages: MKT / MOV / SOC / OWN ==============================

  static QSplitter *makeSplitter(Qt::Orientation o) {
    QSplitter *s = new QSplitter(o);
    s->setHandleWidth(SPLITTER_HANDLE);
    s->setChildrenCollapsible(false);
    return s;
  }

  void buildExtraPages() {
    m_mktPage = buildMarketsPage();
    m_movPage = buildFlowPage();
    m_socPage = buildSocialPage();
    m_ownPage = buildOwnershipPage();
    struct PageSpec {
      QWidget *w;
      const char *name;
      const char *tip;
    };
    const PageSpec pages[] = {
        {m_mktPage, "MKT", "World indices, FX / commodities, Treasury curve, "
                           "VIX term structure, sectors, breadth"},
        {m_movPage, "MOV", "Market movers, trending, trading halts, IPO calendar"},
        {m_socPage, "SOC", "Fear & Greed, Reddit (WSB) mentions, congress trades"},
        {m_ownPage, "OWN", "13F manager holdings, market-wide Form 4 feed"},
    };
    for (const PageSpec &p : pages) {
      const int i = m_workspace->addTab(p.w, p.name);
      m_workspace->setTabToolTip(i, p.tip);
    }
    m_workspace->setTabToolTip(0, "Quote monitor, chart, news, fundamentals, "
                                  "analysts, symbol tabs");
    for (int i = 0; i < m_workspace->count(); ++i) {
      m_workspace->setTabToolTip(
          i, QString("%1  (Ctrl+%2)").arg(m_workspace->tabToolTip(i)).arg(i + 1));
      QShortcut *sc = new QShortcut(QKeySequence(Qt::CTRL + Qt::Key_1 + i), this);
      QObject::connect(sc, &QShortcut::activated, this,
                       [this, i]() { m_workspace->setCurrentIndex(i); });
    }
    if (m_savedWorkspace > 0 && m_savedWorkspace < m_workspace->count())
      m_workspace->setCurrentIndex(m_savedWorkspace);
    QObject::connect(m_workspace, &QTabWidget::currentChanged, this,
                     [this](int) {
                       refreshPage(false, true);
                       saveSettingsDebounced();
                     });
  }

  // Refreshes the visible page. fast: only the cheap, fast-moving parts;
  // lazy: only when the page has not been loaded since the last refresh.
  void refreshPage(bool fast, bool lazy) {
    if (!m_workspace) return;
    QWidget *w = m_workspace->currentWidget();
    if (lazy && m_pagesLoaded.contains(w)) return;
    if (!lazy && fast && !m_pagesLoaded.contains(w)) fast = false;
    m_pagesLoaded.insert(w);
    if (w == m_mktPage) {
      fetchBoards();
      if (!fast) {
        fetchSectors();
        fetchBreadth();
      }
    } else if (w == m_movPage) {
      fetchMovers();
      fetchHalts();
      if (!fast) {
        fetchTrending();
        fetchIpos();
      }
    } else if (w == m_socPage) {
      if (!fast) {
        fetchFearGreed();
        fetchWsb();
        fetchCongress();
      }
    } else if (w == m_ownPage) {
      if (!fast) {
        fetch13F();
        fetchMarketInsider();
      }
    }
  }

  // ---- MKT ------------------------------------------------------------------

  QWidget *buildMarketsPage() {
    static const ColSpec BOARD_COLS[] = {
        {"GRP", 54, false}, {"NAME", 120, false}, {"LAST", 86, true},
        {"CHG", 74, true},  {"%CHG", 66, true},   {"TIME", 0, false}};
    m_wei = makeTablePanel("WEI  WORLD EQUITY INDICES", BOARD_COLS,
                           COUNT_OF(BOARD_COLS));
    m_fx = makeTablePanel("FX \xc2\xb7 CMDTY \xc2\xb7 CRYPTO", BOARD_COLS,
                          COUNT_OF(BOARD_COLS));
    for (QTableWidget *t : {m_wei.table, m_fx.table}) {
      t->setToolTip("Double-click: load the security");
      linkSymbolColumn(t, 1);
    }

    static const ColSpec CURVE_COLS[] = {{"TENOR", 60, false},
                                         {"YIELD %", 76, true},
                                         {"CHG bp", 70, true},
                                         {"SYMBOL", 0, false}};
    m_curve = makeTablePanel("GC  US TREASURY CURVE", CURVE_COLS,
                             COUNT_OF(CURVE_COLS));
    m_curveChart = new LineChart;
    m_curveSpreads = makeInfoLabel();
    m_curve.p.body->insertWidget(0, m_curveChart, 1);
    m_curve.p.body->insertWidget(1, m_curveSpreads);
    linkSymbolColumn(m_curve.table, 3);

    static const ColSpec VIX_COLS[] = {{"TENOR", 60, false},
                                       {"LAST", 76, true},
                                       {"%CHG", 70, true},
                                       {"SYMBOL", 0, false}};
    m_vix = makeTablePanel("VIX  VOLATILITY TERM STRUCTURE", VIX_COLS,
                           COUNT_OF(VIX_COLS));
    m_vixChart = new LineChart;
    m_vixSummary = makeInfoLabel();
    m_vix.p.body->insertWidget(0, m_vixChart, 1);
    m_vix.p.body->insertWidget(1, m_vixSummary);
    linkSymbolColumn(m_vix.table, 3);

    static const ColSpec SECTOR_COLS[] = {
        {"SECTOR", 120, false}, {"ETF", 48, false}, {"LAST", 70, true},
        {"1D", 58, true},       {"1W", 58, true},   {"1M", 58, true},
        {"3M", 58, true},       {"6M", 58, true},   {"YTD", 58, true},
        {"1Y", 0, true}};
    m_sectors = makeTablePanel("SECTORS  ROTATION (SPDR ETFs)", SECTOR_COLS,
                               COUNT_OF(SECTOR_COLS));
    m_sectors.table->setToolTip("Sorted by 1D change. Double-click: load ETF");
    linkSymbolColumn(m_sectors.table, 1);

    static const ColSpec STAT_COLS[] = {{"BREADTH", 170, false},
                                        {"VALUE", 0, true}};
    m_breadth = makeTablePanel("BRDTH  LARGE-CAP BREADTH", STAT_COLS,
                               COUNT_OF(STAT_COLS));
    static const ColSpec LEAD_COLS[] = {{"LEADERS / LAGGARDS", 170, false},
                                        {"LAST", 80, true},
                                        {"%CHG", 0, true}};
    m_leaders = makeColTable(LEAD_COLS, COUNT_OF(LEAD_COLS));
    m_breadth.p.body->addWidget(m_leaders, 1);
    linkSymbolColumn(m_leaders, 0);

    QSplitter *c1 = makeSplitter(Qt::Vertical);
    c1->addWidget(m_wei.p.frame);
    c1->addWidget(m_fx.p.frame);
    QSplitter *c2 = makeSplitter(Qt::Vertical);
    c2->addWidget(m_curve.p.frame);
    c2->addWidget(m_vix.p.frame);
    QSplitter *c3 = makeSplitter(Qt::Vertical);
    c3->addWidget(m_sectors.p.frame);
    c3->addWidget(m_breadth.p.frame);
    QSplitter *page = makeSplitter(Qt::Horizontal);
    page->addWidget(c1);
    page->addWidget(c2);
    page->addWidget(c3);
    page->setSizes(QList<int>() << SPLIT_LEFT_W << SPLIT_RIGHT_W + SPLIT_RIGHT_W / 2
                                << SPLIT_CENTER_W);
    return page;
  }

  QStringList boardSymbols() const {
    QStringList s;
    for (const SymName &x : WORLD_INDICES) s << x.symbol;
    for (const SymName &x : FX_BOARD) s << x.symbol;
    for (const Tenor &x : TREASURY_TENORS) s << x.symbol;
    for (const Tenor &x : VIX_TENORS) {
      if (!s.contains(x.symbol)) s << x.symbol;
    }
    s << VVIX_SYMBOL;
    return s;
  }

  // One watchlist request for WEI, FX, GC and VIX.
  void fetchBoards() {
    QUrlQuery q;
    q.addQueryItem("symbols", boardSymbols().join(","));
    getSeq(ROUTE_WATCHLIST, q, &m_wei.seq,
           [this](const QString &err, const QJsonObject &o) {
             if (!err.isEmpty()) {
               for (TablePanel *tp : {&m_wei, &m_fx, &m_curve, &m_vix}) {
                 setPanelError(tp->p.status, err);
                 if (tp->table->rowCount() <= 1)
                   setTableMessage(tp->table, "Quotes unavailable");
               }
               return;
             }
             QHash<QString, QJsonObject> qs;
             for (const QJsonValue &v : o.value("items").toArray()) {
               const QJsonObject it = v.toObject();
               qs.insert(jstr(it, "symbol"), it);
             }
             renderBoard(m_wei, WORLD_INDICES, COUNT_OF(WORLD_INDICES), qs);
             renderBoard(m_fx, FX_BOARD, COUNT_OF(FX_BOARD), qs);
             renderCurve(qs);
             renderVix(qs);
           });
  }

  static bool quoteOk(const QJsonObject &q) {
    double p = 0;
    return jnum(q.value("price"), p) &&
           jstr(q, "quality") != QLatin1String(Q_UNAVAILABLE);
  }

  void renderBoard(TablePanel &tp, const SymName *rows, int n,
                   const QHash<QString, QJsonObject> &qs) {
    QTableWidget *t = tp.table;
    ensureRows(t, n);
    int bad = 0;
    for (int r = 0; r < n; ++r) {
      const QJsonObject q = qs.value(rows[r].symbol);
      if (!quoteOk(q)) ++bad;
      setCell(t, r, 0, rows[r].group, COL_DIM);
      QTableWidgetItem *nm = setCell(t, r, 1, rows[r].name, COL_AMBER);
      nm->setData(SYMBOL_ROLE, QString(rows[r].symbol));
      nm->setToolTip(QString(rows[r].symbol) + (jstr(q, "error").isEmpty()
                                                    ? QString()
                                                    : "\n" + jstr(q, "error")));
      setQuoteCells(t, r, 2, q);
      setCell(t, r, 5, fmtWhen(jstr(q, "marketTime")), COL_DIM);
    }
    boardStatus(tp.p.status, n, bad);
  }

  void renderCurve(const QHash<QString, QJsonObject> &qs) {
    QTableWidget *t = m_curve.table;
    const int n = COUNT_OF(TREASURY_TENORS);
    ensureRows(t, n);
    QVector<double> ys;
    QStringList labels;
    QHash<QString, double> byTenor;
    int bad = 0;
    for (int r = 0; r < n; ++r) {
      const Tenor &tn = TREASURY_TENORS[r];
      const QJsonObject q = qs.value(tn.symbol);
      double y = 0, prev = 0;
      const bool hy = jnum(q.value("price"), y) && quoteOk(q);
      const bool hp = jnum(q.value("previousClose"), prev);
      if (!hy) ++bad;
      setCell(t, r, 0, tn.label, COL_AMBER);
      setCell(t, r, 1, orNA(hy, fmtNum(y, 3)), hy ? COL_TEXT : COL_DIM, ALIGN_NUM);
      const double bp = (y - prev) * BP_PER_PCT;
      setCell(t, r, 2, orNA(hy && hp, fmtSigned(bp, 1)),
              hy && hp ? changeColor(bp) : COL_DIM, ALIGN_NUM);
      setCell(t, r, 3, tn.symbol, COL_DIM)->setData(SYMBOL_ROLE, QString(tn.symbol));
      if (hy) {
        ys << y;
        labels << tn.label;
        byTenor.insert(tn.label, y);
      }
    }
    if (ys.size() >= 2) m_curveChart->setSeries(ys, labels, 2);
    else m_curveChart->setMessage("Curve unavailable");
    QStringList sp;
    for (const Spread &s : TREASURY_SPREADS) {
      if (!byTenor.contains(s.shortLeg) || !byTenor.contains(s.longLeg)) continue;
      const double v =
          (byTenor.value(s.longLeg) - byTenor.value(s.shortLeg)) * BP_PER_PCT;
      sp << span(COL_DIM, s.name) + " " +
                span(v < 0 ? COL_DOWN : COL_UP,
                     fmtSigned(v, 0) + "bp" + (v < 0 ? " INV" : ""));
    }
    m_curveSpreads->setText(sp.join("&nbsp;&nbsp;&nbsp;"));
    boardStatus(m_curve.p.status, n, bad);
  }

  void renderVix(const QHash<QString, QJsonObject> &qs) {
    QTableWidget *t = m_vix.table;
    const int n = COUNT_OF(VIX_TENORS);
    ensureRows(t, n);
    QVector<double> ys;
    QStringList labels;
    int bad = 0;
    for (int r = 0; r < n; ++r) {
      const Tenor &tn = VIX_TENORS[r];
      const QJsonObject q = qs.value(tn.symbol);
      double v = 0, pct = 0;
      const bool hv = jnum(q.value("price"), v) && quoteOk(q);
      const bool hpct = jnum(q.value("changePercent"), pct);
      if (!hv) ++bad;
      setCell(t, r, 0, tn.label, COL_AMBER)
          ->setToolTip(QString("%1 days").arg(tn.years));
      setCell(t, r, 1, orNA(hv, fmtNum(v, 2)), hv ? COL_TEXT : COL_DIM, ALIGN_NUM);
      // Rising volatility is bad news: invert the colors.
      setCell(t, r, 2, orNA(hpct, fmtPctSigned(pct)),
              hpct ? changeColor(-pct) : COL_DIM, ALIGN_NUM);
      setCell(t, r, 3, tn.symbol, COL_DIM)->setData(SYMBOL_ROLE, QString(tn.symbol));
      if (hv) {
        ys << v;
        labels << tn.label;
      }
    }
    if (ys.size() >= 2) m_vixChart->setSeries(ys, labels, 2);
    else m_vixChart->setMessage("Term structure unavailable");

    QStringList parts;
    double spot = 0, m3 = 0, vvix = 0;
    const QJsonObject qSpot = qs.value(VIX_SPOT_SYMBOL);
    const bool hSpot = jnum(qSpot.value("price"), spot) && quoteOk(qSpot);
    if (hSpot) {
      QString regime = "COMPLACENT";
      QColor rc = COL_UP;
      if (spot > VIX_HIGH_FEAR) {
        regime = "HIGH FEAR";
        rc = COL_DOWN;
      } else if (spot > VIX_ELEVATED) {
        regime = "ELEVATED";
        rc = COL_WARN;
      } else if (spot > VIX_NORMAL) {
        regime = "NORMAL";
        rc = COL_TEXT;
      }
      parts << span(COL_DIM, "SPOT") + " " + span(COL_TEXT, fmtNum(spot, 2))
            << span(COL_DIM, "REGIME") + " " + span(rc, regime);
    }
    const QJsonObject q3 = qs.value(VIX_3M_SYMBOL);
    if (hSpot && spot > 0 && jnum(q3.value("price"), m3) && quoteOk(q3)) {
      const double c = (m3 / spot - 1.0) * PCT_SCALE;
      parts << span(COL_DIM, c >= 0 ? "CONTANGO 3M/1M" : "BACKWARDATION 3M/1M") +
                   " " + span(c >= 0 ? COL_UP : COL_DOWN, fmtPctSigned(c));
    }
    const QJsonObject qv = qs.value(VVIX_SYMBOL);
    if (jnum(qv.value("price"), vvix) && quoteOk(qv))
      parts << span(COL_DIM, "VVIX") + " " + span(COL_TEXT, fmtNum(vvix, 2));
    m_vixSummary->setText(parts.join("&nbsp;&nbsp;&nbsp;"));
    boardStatus(m_vix.p.status, n, bad);
  }

  // Sectors: 1y daily chart per ETF; returns computed as the charts arrive.
  enum SectorPeriod { SP_1D, SP_1W, SP_1M, SP_3M, SP_6M, SP_YTD, SP_1Y, SP_COUNT };
  struct SectorPerf {
    bool ok = false;
    QString err;
    double last = 0;
    bool has[SP_COUNT] = {};
    double chg[SP_COUNT] = {};
  };

  void fetchSectors() {
    const int gen = ++m_sectors.seq;
    m_sectorPerf.clear();
    setPanelBusy(m_sectors.p.status, LOADING_TEXT);
    QUrlQuery q;
    q.addQueryItem("range", SECTOR_RANGE);
    q.addQueryItem("interval", SECTOR_INTERVAL);
    for (const SymName &s : SECTORS) {
      const QString sym = s.symbol;
      m_api.getJson(QString(ROUTE_CHART) + Api::pathSeg(sym), q,
                    [this, gen, sym](int st, const QJsonObject &o,
                                     const QString &err) {
                      if (gen != m_sectors.seq) return;
                      m_sectorPerf.insert(sym, sectorPerf(bodyError(st, o, err), o));
                      renderSectors();
                    });
    }
  }

  static SectorPerf sectorPerf(const QString &err, const QJsonObject &o) {
    SectorPerf p;
    p.err = err;
    if (!err.isEmpty()) return p;
    const QVector<double> c = jarr(o.value("close"));
    const QVector<double> ts = jarr(o.value("timestamps"));
    if (c.size() < 2) {
      p.err = "no bars";
      return p;
    }
    p.ok = true;
    p.last = c.last();
    const int backs[] = {1, TRADING_DAYS_1W, TRADING_DAYS_1M, TRADING_DAYS_3M,
                         TRADING_DAYS_6M};
    const SectorPeriod ids[] = {SP_1D, SP_1W, SP_1M, SP_3M, SP_6M};
    for (int i = 0; i < COUNT_OF(backs); ++i)
      p.has[ids[i]] = backChange(c, backs[i], p.chg[ids[i]]);
    p.has[SP_YTD] = ytdChange(c, ts, p.chg[SP_YTD]);
    p.has[SP_1Y] = backChange(c, c.size() - 1, p.chg[SP_1Y]);
    return p;
  }

  void renderSectors() {
    struct Row {
      const SymName *s;
      SectorPerf p;
    };
    QVector<Row> rows;
    for (const SymName &s : SECTORS) {
      if (m_sectorPerf.contains(s.symbol)) rows.append({&s, m_sectorPerf.value(s.symbol)});
    }
    std::stable_sort(rows.begin(), rows.end(), [](const Row &a, const Row &b) {
      const double x = a.p.has[SP_1D] ? a.p.chg[SP_1D] : -1e300;
      const double y = b.p.has[SP_1D] ? b.p.chg[SP_1D] : -1e300;
      return x > y;
    });
    QTableWidget *t = m_sectors.table;
    beginRows(t, rows.size());
    const int firstPeriodCol = 3;
    int bad = 0;
    for (int r = 0; r < rows.size(); ++r) {
      const Row &x = rows[r];
      const bool bench = QLatin1String(x.s->group) == QLatin1String("BENCH");
      setCell(t, r, 0, x.s->name, bench ? COL_DIM : COL_TEXT)
          ->setToolTip(x.p.err);
      setCell(t, r, 1, x.s->symbol, COL_AMBER);
      if (!x.p.ok) ++bad;
      setCell(t, r, 2, orNA(x.p.ok, fmtPrice(x.p.last)), COL_TEXT, ALIGN_NUM);
      for (int k = 0; k < SP_COUNT; ++k) {
        const bool h = x.p.ok && x.p.has[k];
        setCell(t, r, firstPeriodCol + k, orNA(h, fmtPctSigned(x.p.chg[k])),
                h ? changeColor(x.p.chg[k]) : COL_DIM, ALIGN_NUM);
      }
    }
    boardStatus(m_sectors.p.status, rows.size(), bad);
  }

  void fetchBreadth() {
    QUrlQuery q;
    q.addQueryItem("symbols", BREADTH_UNIVERSE);
    setPanelBusy(m_breadth.p.status, LOADING_TEXT);
    getSeq(ROUTE_WATCHLIST, q, &m_breadth.seq,
           [this](const QString &err, const QJsonObject &o) {
             if (!err.isEmpty()) {
               setPanelError(m_breadth.p.status, err);
               setTableMessage(m_breadth.table, "Breadth unavailable");
               return;
             }
             applyBreadth(o.value("items").toArray());
           });
  }

  void applyBreadth(const QJsonArray &items) {
    int adv = 0, dec = 0, unch = 0, nearHi = 0, nearLo = 0, bad = 0;
    double upVol = 0, downVol = 0, sum = 0;
    QVector<double> pcts;
    struct Mover {
      QString sym;
      double price, pct;
    };
    QVector<Mover> movers;
    for (const QJsonValue &v : items) {
      const QJsonObject q = v.toObject();
      double price = 0, pct = 0, vol = 0, hi = 0, lo = 0;
      if (!quoteOk(q) || !jnum(q.value("changePercent"), pct)) {
        ++bad;
        continue;
      }
      jnum(q.value("price"), price);
      jnum(q.value("volume"), vol);
      if (std::fabs(pct) < UNCHANGED_PCT) ++unch;
      else if (pct > 0) {
        ++adv;
        upVol += vol;
      } else {
        ++dec;
        downVol += vol;
      }
      if (jnum(q.value("high52w"), hi) && price >= hi * NEAR_HIGH_FRAC) ++nearHi;
      if (jnum(q.value("low52w"), lo) && price <= lo * NEAR_LOW_FRAC) ++nearLo;
      sum += pct;
      pcts << pct;
      movers.append({jstr(q, "symbol"), price, pct});
    }
    const int n = pcts.size();
    if (n == 0) {
      setTableMessage(m_breadth.table, "No quotes");
      setPanelError(m_breadth.p.status, "no usable quotes");
      return;
    }
    std::sort(pcts.begin(), pcts.end());
    const double median =
        n % 2 ? pcts[n / 2] : (pcts[n / 2 - 1] + pcts[n / 2]) / 2.0;
    const double totalVol = upVol + downVol;
    struct Stat {
      QString label, value;
      QColor color;
    };
    const QVector<Stat> stats = {
        {"Advancers", QString::number(adv), COL_UP},
        {"Decliners", QString::number(dec), COL_DOWN},
        {"Unchanged", QString::number(unch), COL_DIM},
        {"A/D ratio", dec > 0 ? fmtNum(double(adv) / dec, 2) : QString(NOT_AVAILABLE),
         adv >= dec ? COL_UP : COL_DOWN},
        {"Net advancers", fmtSigned(adv - dec, 0), changeColor(adv - dec)},
        {"Average % chg", fmtPctSigned(sum / n), changeColor(sum)},
        {"Median % chg", fmtPctSigned(median), changeColor(median)},
        {"Up volume share",
         totalVol > 0 ? fmtNum(upVol / totalVol * PCT_SCALE, 1) + "%"
                      : QString(NOT_AVAILABLE),
         upVol >= downVol ? COL_UP : COL_DOWN},
        {"Near 52W high", QString::number(nearHi), COL_UP},
        {"Near 52W low", QString::number(nearLo), COL_DOWN},
    };
    QTableWidget *t = m_breadth.table;
    beginRows(t, stats.size());
    for (int r = 0; r < stats.size(); ++r) {
      setCell(t, r, 0, stats[r].label, COL_DIM);
      setCell(t, r, 1, stats[r].value, stats[r].color, ALIGN_NUM);
    }
    std::sort(movers.begin(), movers.end(),
              [](const Mover &a, const Mover &b) { return a.pct > b.pct; });
    QVector<Mover> lead;
    for (int i = 0; i < movers.size() && i < LEADERS_COUNT; ++i) lead << movers[i];
    for (int i = qMax(LEADERS_COUNT, movers.size() - LEADERS_COUNT);
         i < movers.size(); ++i)
      lead << movers[i];
    beginRows(m_leaders, lead.size());
    for (int r = 0; r < lead.size(); ++r) {
      setCell(m_leaders, r, 0, lead[r].sym, COL_AMBER);
      setCell(m_leaders, r, 1, fmtPrice(lead[r].price), COL_TEXT, ALIGN_NUM);
      setCell(m_leaders, r, 2, fmtPctSigned(lead[r].pct), changeColor(lead[r].pct),
              ALIGN_NUM);
    }
    boardStatus(m_breadth.p.status, items.size(), bad);
  }

  // ---- MOV ------------------------------------------------------------------

  QWidget *buildFlowPage() {
    static const ColSpec MOVER_COLS[] = {
        {"TICKER", 64, false}, {"NAME", 180, false}, {"LAST", 74, true},
        {"%CHG", 66, true},    {"VOLUME", 74, true}, {"AVG VOL", 74, true},
        {"REL VOL", 62, true}, {"MKT CAP", 76, true}, {"P/E", 0, true}};
    m_movers = makeTablePanel("MOV  MARKET MOVERS", MOVER_COLS,
                              COUNT_OF(MOVER_COLS));
    m_moverType = new QComboBox;
    fillCombo(m_moverType, MOVER_TYPES, COUNT_OF(MOVER_TYPES));
    onComboChanged(m_moverType, this, [this](int) { fetchMovers(); });
    m_movers.p.addControl(m_moverType);
    m_movers.table->setToolTip("Double-click: load the security");
    linkSymbolColumn(m_movers.table, 0);

    static const ColSpec TREND_COLS[] = {{"TICKER", 70, false},
                                         {"NAME", 200, false},
                                         {"LAST", 80, true},
                                         {"%CHG", 0, true}};
    m_trending = makeTablePanel("TRND  TRENDING (YAHOO)", TREND_COLS,
                                COUNT_OF(TREND_COLS));
    linkSymbolColumn(m_trending.table, 0);

    static const ColSpec HALT_COLS[] = {
        {"DATE", 84, false},    {"HALTED", 70, false}, {"TICKER", 64, false},
        {"NAME", 170, false},   {"REASON", 170, false}, {"MKT", 44, false},
        {"RESUMED", 0, false}};
    m_halts = makeTablePanel("HALT  TRADING HALTS (NASDAQTRADER)", HALT_COLS,
                             COUNT_OF(HALT_COLS));
    linkSymbolColumn(m_halts.table, 2);

    static const ColSpec IPO_COLS[] = {
        {"TICKER", 64, false}, {"COMPANY", 200, false}, {"STATUS", 80, false},
        {"EXCH", 70, false},   {"DATE", 84, false},     {"PRICE RANGE", 90, true},
        {"SHARES", 74, true},  {"DEAL SIZE", 0, true}};
    m_ipos = makeTablePanel("IPO  IPO CALENDAR (NASDAQ)", IPO_COLS,
                            COUNT_OF(IPO_COLS));
    m_ipoMarket = new QComboBox;
    fillCombo(m_ipoMarket, IPO_MARKETS, COUNT_OF(IPO_MARKETS));
    m_ipoMonth = new QComboBox;
    fillCombo(m_ipoMonth, IPO_MONTHS, COUNT_OF(IPO_MONTHS));
    onComboChanged(m_ipoMarket, this, [this](int) { fetchIpos(); });
    onComboChanged(m_ipoMonth, this, [this](int) { fetchIpos(); });
    m_ipos.p.addControl(m_ipoMarket);
    m_ipos.p.addControl(m_ipoMonth);
    linkSymbolColumn(m_ipos.table, 0);

    QSplitter *right = makeSplitter(Qt::Vertical);
    right->addWidget(m_trending.p.frame);
    right->addWidget(m_halts.p.frame);
    QSplitter *top = makeSplitter(Qt::Horizontal);
    top->addWidget(m_movers.p.frame);
    top->addWidget(right);
    top->setSizes(QList<int>() << SPLIT_CENTER_W << SPLIT_CENTER_W);
    QSplitter *page = makeSplitter(Qt::Vertical);
    page->addWidget(top);
    page->addWidget(m_ipos.p.frame);
    page->setSizes(QList<int>() << SPLIT_CHART_H << SPLIT_TABS_H);
    return page;
  }

  void fetchMovers() {
    const QString type = m_moverType->currentData().toString();
    QUrlQuery q;
    q.addQueryItem("type", type);
    setPanelBusy(m_movers.p.status, LOADING_TEXT);
    getSeq(ROUTE_MOVERS, q, &m_movers.seq,
           [this, type](const QString &err, const QJsonObject &o) {
             QJsonArray items = listItems(m_movers, err, o, "No movers");
             if (items.isEmpty()) return;
             QVector<QJsonObject> rows;
             for (const QJsonValue &v : items) rows << v.toObject();
             if (type == QLatin1String(MOVER_UNUSUAL)) {
               std::stable_sort(rows.begin(), rows.end(),
                                [](const QJsonObject &a, const QJsonObject &b) {
                                  return a.value("unusualRatio").toDouble() >
                                         b.value("unusualRatio").toDouble();
                                });
             }
             QTableWidget *t = m_movers.table;
             for (int r = 0; r < rows.size(); ++r) {
               const QJsonObject &x = rows[r];
               double pct = 0, rel = 0;
               const bool hp = jnum(x.value("changePercent"), pct);
               const bool hr = jnum(x.value("unusualRatio"), rel);
               setCell(t, r, 0, jstr(x, "symbol"), COL_AMBER);
               setCell(t, r, 1, jstr(x, "name"));
               setCell(t, r, 2, priceOr(x.value("price")), COL_TEXT, ALIGN_NUM);
               setCell(t, r, 3, orNA(hp, fmtPctSigned(pct)),
                       hp ? changeColor(pct) : COL_DIM, ALIGN_NUM);
               setCell(t, r, 4, bigOr(x.value("volume")), COL_TEXT, ALIGN_NUM);
               setCell(t, r, 5, bigOr(x.value("avgVolume")), COL_DIM, ALIGN_NUM);
               setCell(t, r, 6, orNA(hr, fmtNum(rel, 2) + "x"),
                       hr && rel > 1.0 ? COL_WARN : COL_DIM, ALIGN_NUM);
               setCell(t, r, 7, bigOr(x.value("marketCap")), COL_TEXT, ALIGN_NUM);
               setCell(t, r, 8, numOr(x.value("pe"), 1), COL_DIM, ALIGN_NUM);
             }
           });
  }

  void fetchTrending() {
    setPanelBusy(m_trending.p.status, LOADING_TEXT);
    getSeq(ROUTE_TRENDING, QUrlQuery(), &m_trending.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray items =
                 listItems(m_trending, err, o, "Nothing trending", "symbols");
             QTableWidget *t = m_trending.table;
             for (int r = 0; r < items.size(); ++r) {
               const QJsonObject x = items[r].toObject();
               double pct = 0;
               const bool hp = jnum(x.value("changePercent"), pct);
               setCell(t, r, 0, jstr(x, "symbol"), COL_AMBER);
               setCell(t, r, 1, jstr(x, "name"));
               setCell(t, r, 2, priceOr(x.value("price")), COL_TEXT, ALIGN_NUM);
               setCell(t, r, 3, orNA(hp, fmtPctSigned(pct)),
                       hp ? changeColor(pct) : COL_DIM, ALIGN_NUM);
             }
           });
  }

  void fetchHalts() {
    getSeq(ROUTE_HALTS, QUrlQuery(), &m_halts.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray items =
                 listItems(m_halts, err, o, "No trading halts today");
             QVector<QJsonObject> rows;
             for (const QJsonValue &v : items) rows << v.toObject();
             std::stable_sort(rows.begin(), rows.end(),
                              [](const QJsonObject &a, const QJsonObject &b) {
                                return jstr(a, "haltDate") + jstr(a, "haltTime") >
                                       jstr(b, "haltDate") + jstr(b, "haltTime");
                              });
             QTableWidget *t = m_halts.table;
             int active = 0;
             for (int r = 0; r < rows.size(); ++r) {
               const QJsonObject &x = rows[r];
               const QString resumed = jstr(x, "resumptionTime");
               const bool halted = resumed.isEmpty();
               if (halted) ++active;
               setCell(t, r, 0, jstr(x, "haltDate"), COL_DIM);
               setCell(t, r, 1, jstr(x, "haltTime"), halted ? COL_DOWN : COL_TEXT);
               setCell(t, r, 2, jstr(x, "symbol"), COL_AMBER);
               setCell(t, r, 3, jstr(x, "issueName"));
               setCell(t, r, 4, jstr(x, "reasonLabel"), COL_DIM)
                   ->setToolTip(jstr(x, "reasonCode"));
               setCell(t, r, 5, jstr(x, "marketCenter"), COL_DIM);
               setCell(t, r, 6,
                       halted ? QString("HALTED")
                              : jstr(x, "resumptionDate") + " " + resumed,
                       halted ? COL_DOWN : COL_UP);
             }
             if (!rows.isEmpty())
               setProvenance(m_halts.p.status, o.value("provenance").toObject(),
                             QString("%1 halts, %2 active")
                                 .arg(rows.size())
                                 .arg(active));
           });
  }

  static QColor ipoStatusColor(const QString &s) {
    const QString u = s.toUpper();
    if (u == "PRICED") return COL_UP;
    if (u == "EXPECTED") return COL_AMBER;
    if (u == "WITHDRAWN") return COL_DIM;
    return COL_TEXT;
  }

  void fetchIpos() {
    QUrlQuery q;
    q.addQueryItem("market", m_ipoMarket->currentData().toString());
    const int off = m_ipoMonth->currentData().toString().toInt();
    q.addQueryItem("month", QDate::currentDate().addMonths(off).toString("yyyy-MM"));
    busyTable(m_ipos);
    getSeq(ROUTE_IPOS, q, &m_ipos.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray items = listItems(m_ipos, err, o, "No deals");
             QTableWidget *t = m_ipos.table;
             for (int r = 0; r < items.size(); ++r) {
               const QJsonObject x = items[r].toObject();
               QString date;
               for (const char *k : {"pricedDate", "expectedPricingDate",
                                     "filedDate", "withdrawnDate"}) {
                 date = jstr(x, k);
                 if (!date.isEmpty()) break;
               }
               double lo = 0, hi = 0;
               const bool hl = jnum(x.value("priceRangeLow"), lo);
               const bool hh = jnum(x.value("priceRangeHigh"), hi);
               QString range = NOT_AVAILABLE;
               if (hl && hh && lo != hi) range = fmtPrice(lo) + "\xe2\x80\x93" + fmtPrice(hi);
               else if (hl || hh) range = fmtPrice(hl ? lo : hi);
               const QString status = jstr(x, "status");
               setCell(t, r, 0, jstr(x, "symbol"), COL_AMBER);
               setCell(t, r, 1, jstr(x, "companyName"))
                   ->setToolTip(jstr(x, "leadUnderwriters"));
               setCell(t, r, 2, status, ipoStatusColor(status));
               setCell(t, r, 3, jstr(x, "exchange"), COL_DIM);
               setCell(t, r, 4, date, COL_DIM);
               setCell(t, r, 5, range, COL_TEXT, ALIGN_NUM);
               setCell(t, r, 6, bigOr(x.value("offerShares")), COL_TEXT, ALIGN_NUM);
               setCell(t, r, 7, bigOr(x.value("dealSize")), COL_TEXT, ALIGN_NUM);
             }
           });
  }

  // ---- SOC ------------------------------------------------------------------

  QWidget *buildSocialPage() {
    // Fear & Greed
    m_fngPanel = makePanel("F&G  CNN FEAR & GREED");
    m_fngScore = new QLabel;
    QFont sf = font();
    sf.setPointSize(HEADER_PRICE_PT);
    sf.setBold(true);
    m_fngScore->setFont(sf);
    m_fngScore->setContentsMargins(PANEL_PAD, PANEL_PAD, PANEL_PAD, 0);
    m_fngDetail = makeInfoLabel();
    m_fngChart = new LineChart;
    static const ColSpec FNG_COLS[] = {{"INDICATOR", 200, false},
                                       {"SCORE", 60, true},
                                       {"RATING", 0, false}};
    m_fngTable = makeColTable(FNG_COLS, COUNT_OF(FNG_COLS));
    m_fngPanel.body->addWidget(m_fngScore);
    m_fngPanel.body->addWidget(m_fngDetail);
    m_fngPanel.body->addWidget(m_fngChart, 1);
    m_fngPanel.body->addWidget(m_fngTable, 1);

    // Reddit
    static const ColSpec WSB_COLS[] = {
        {"TICKER", 64, false}, {"MENTIONS", 70, true}, {"BULL", 48, true},
        {"BEAR", 48, true},    {"SENTIMENT", 80, false}, {"SCORE", 60, true},
        {"SAMPLE", 0, false}};
    m_wsb = makeTablePanel("WSB  REDDIT MENTIONS", WSB_COLS, COUNT_OF(WSB_COLS));
    static const ComboItem SUBS[] = {{"WSB + r/stocks", ""},
                                     {"r/wallstreetbets", "wallstreetbets"},
                                     {"r/stocks", "stocks"},
                                     {"r/investing", "investing"},
                                     {"r/options", "options"}};
    m_wsbSubs = new QComboBox;
    fillCombo(m_wsbSubs, SUBS, COUNT_OF(SUBS));
    onComboChanged(m_wsbSubs, this, [this](int) { fetchWsb(); });
    m_wsb.p.addControl(m_wsbSubs);
    linkSymbolColumn(m_wsb.table, 0);
    static const ColSpec POST_COLS[] = {{"SUB", 110, false},
                                        {"SCORE", 60, true},
                                        {"CMTS", 56, true},
                                        {"POST", 0, false}};
    m_wsbPosts = makeColTable(POST_COLS, COUNT_OF(POST_COLS));
    m_wsbPosts->setToolTip("Double-click: open the post");
    linkUrlRows(m_wsbPosts, -1);
    QSplitter *ws = makeSplitter(Qt::Vertical);
    ws->addWidget(m_wsb.table);
    ws->addWidget(m_wsbPosts);
    m_wsb.p.body->addWidget(ws, 1);

    // Congress
    static const ColSpec CONG_COLS[] = {
        {"DISCLOSED", 84, false}, {"TRADED", 84, false}, {"MEMBER", 170, false},
        {"PTY", 36, false},       {"TICKER", 60, false}, {"TYPE", 90, false},
        {"AMOUNT", 0, false}};
    m_congress = makeTablePanel("CONG  CONGRESS STOCK TRADES", CONG_COLS,
                                COUNT_OF(CONG_COLS));
    m_congress.table->setToolTip(
        "Double-click the ticker: load it; elsewhere: open the disclosure");
    linkSymbolColumn(m_congress.table, 4);
    linkUrlRows(m_congress.table, 4);

    QSplitter *page = makeSplitter(Qt::Horizontal);
    page->addWidget(m_fngPanel.frame);
    page->addWidget(m_wsb.p.frame);
    page->addWidget(m_congress.p.frame);
    page->setSizes(QList<int>() << SPLIT_LEFT_W << SPLIT_CENTER_W << SPLIT_CENTER_W);
    return page;
  }

  // Fear & Greed: status bar indicator + SOC panel.
  void fetchFearGreed() {
    getSeq(ROUTE_FEAR_GREED, QUrlQuery(), &m_fngSeq,
           [this](const QString &err, const QJsonObject &o) {
             applyFearGreed(err, o);
           });
  }

  void applyFearGreed(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      m_fngLabel->setText("F&G " + QString(NOT_AVAILABLE));
      setLabelColor(m_fngLabel, COL_DIM);
      m_fngLabel->setToolTip(err);
      setPanelError(m_fngPanel.status, err);
      m_fngScore->setText(NOT_AVAILABLE);
      setLabelColor(m_fngScore, COL_DIM);
      m_fngChart->setMessage("Unavailable");
      setTableMessage(m_fngTable, "Unavailable: " + elide(err));
      return;
    }
    double score = 0;
    const bool hs = jnum(o.value("score"), score);
    const QString label = jstr(o, "label").isEmpty()
                              ? jstr(o, "rating").toUpper().replace('_', ' ')
                              : jstr(o, "label").toUpper();
    const QColor c = hs ? fngColor(score) : COL_DIM;
    m_fngLabel->setText(QString("F&G %1 %2").arg(orNA(hs, fmtNum(score, 0)), label));
    setLabelColor(m_fngLabel, c);
    m_fngScore->setText(orNA(hs, fmtNum(score, 0)) + "  " + label);
    setLabelColor(m_fngScore, c);

    struct Prev {
      const char *key;
      const char *label;
    };
    static const Prev PREVS[] = {{"previousClose", "PREV CLOSE"},
                                 {"previousWeek", "1W AGO"},
                                 {"previousMonth", "1M AGO"},
                                 {"previousYear", "1Y AGO"}};
    QStringList parts;
    for (const Prev &p : PREVS) {
      double v = 0;
      if (jnum(o.value(p.key), v))
        parts << span(COL_DIM, p.label) + " " + span(fngColor(v), fmtNum(v, 0));
    }
    m_fngDetail->setText(parts.join("&nbsp;&nbsp;&nbsp;"));

    QVector<double> ys;
    QStringList labels;
    for (const QJsonValue &v : o.value("history").toArray()) {
      const QJsonObject h = v.toObject();
      ys << h.value("y").toDouble();
      const QDate d = QDate::fromString(jstr(h, "date"), Qt::ISODate);
      labels << (d.isValid() ? d.toString("MM/dd") : jstr(h, "date"));
    }
    const double neutral = (FNG_FEAR_MAX + FNG_GREED_MIN) / 2.0;
    if (ys.size() >= 2) m_fngChart->setSeries(ys, labels, 0, true, neutral);
    else m_fngChart->setMessage("No history");

    const QJsonArray inds = o.value("indicators").toArray();
    beginRows(m_fngTable, inds.size());
    for (int r = 0; r < inds.size(); ++r) {
      const QJsonObject x = inds[r].toObject();
      double v = 0;
      const bool hv = jnum(x.value("score"), v);
      const QString rating = jstr(x, "rating").replace('_', ' ');
      setCell(m_fngTable, r, 0, jstr(x, "name"));
      setCell(m_fngTable, r, 1, orNA(hv, fmtNum(v, 0)), hv ? fngColor(v) : COL_DIM,
              ALIGN_NUM);
      setCell(m_fngTable, r, 2, rating, sentimentColor(rating));
    }
    if (inds.isEmpty()) setTableMessage(m_fngTable, "No components");
    const QJsonObject prov = o.value("provenance").toObject();
    setProvenance(m_fngPanel.status, prov);
    m_fngLabel->setToolTip("CNN Fear & Greed  " +
                           prov.value("quality").toString() + "  " +
                           fmtWhen(jstr(o, "timestamp")));
  }

  void fetchWsb() {
    QUrlQuery q;
    const QString subs = m_wsbSubs->currentData().toString();
    if (!subs.isEmpty()) q.addQueryItem("subs", subs);
    busyTable(m_wsb);
    getSeq(ROUTE_WSB, q, &m_wsb.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray items =
                 listItems(m_wsb, err, o, "No ticker mentions", "trending");
             QTableWidget *t = m_wsb.table;
             for (int r = 0; r < items.size(); ++r) {
               const QJsonObject x = items[r].toObject();
               const QString sent = jstr(x, "sentiment");
               setCell(t, r, 0, jstr(x, "symbol"), COL_AMBER);
               setCell(t, r, 1, numOr(x.value("mentions"), 0), COL_TEXT, ALIGN_NUM);
               setCell(t, r, 2, numOr(x.value("bullish"), 0), COL_UP, ALIGN_NUM);
               setCell(t, r, 3, numOr(x.value("bearish"), 0), COL_DOWN, ALIGN_NUM);
               setCell(t, r, 4, sent.toUpper(), sentimentColor(sent));
               setCell(t, r, 5, numOr(x.value("score"), 0), COL_DIM, ALIGN_NUM);
               setCell(t, r, 6, jstr(x, "sample"), COL_DIM)
                   ->setToolTip(jstr(x, "sample"));
             }
             const QJsonArray posts = err.isEmpty() ? o.value("posts").toArray()
                                                    : QJsonArray();
             beginRows(m_wsbPosts, posts.size());
             for (int r = 0; r < posts.size(); ++r) {
               const QJsonObject x = posts[r].toObject();
               setCell(m_wsbPosts, r, 0, "r/" + jstr(x, "sub"), COL_DIM)
                   ->setData(URL_ROLE, jstr(x, "url"));
               setCell(m_wsbPosts, r, 1, numOr(x.value("score"), 0), COL_TEXT,
                       ALIGN_NUM);
               setCell(m_wsbPosts, r, 2, numOr(x.value("num_comments"), 0),
                       COL_DIM, ALIGN_NUM);
               setCell(m_wsbPosts, r, 3, jstr(x, "title"))
                   ->setToolTip(jstr(x, "flair") + "  " + fmtWhen(jstr(x, "createdAt")));
             }
             if (posts.isEmpty()) setTableMessage(m_wsbPosts, "No posts");
           });
  }

  void fetchCongress() {
    QUrlQuery q;
    q.addQueryItem("limit", QString::number(CONGRESS_LIMIT));
    busyTable(m_congress);
    getSeq(ROUTE_CONGRESS, q, &m_congress.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray items =
                 listItems(m_congress, err, o, "No disclosures");
             QTableWidget *t = m_congress.table;
             for (int r = 0; r < items.size(); ++r) {
               const QJsonObject x = items[r].toObject();
               const QString party = jstr(x, "party");
               const QString type = jstr(x, "type");
               const QString tl = type.toLower();
               const QColor tc = tl.contains("purchase") ? COL_UP
                                 : tl.contains("sale")   ? COL_DOWN
                                                         : COL_TEXT;
               QTableWidgetItem *c0 =
                   setCell(t, r, 0, jstr(x, "disclosure_date"), COL_DIM);
               c0->setData(URL_ROLE, jstr(x, "ptr_link"));
               setCell(t, r, 1, jstr(x, "transaction_date"), COL_DIM);
               setCell(t, r, 2, jstr(x, "representative"))
                   ->setToolTip(jstr(x, "district") + "  " + jstr(x, "owner"));
               setCell(t, r, 3, party,
                       party.startsWith('D') ? COL_LINK
                       : party.startsWith('R') ? COL_DOWN
                                               : COL_DIM);
               setCell(t, r, 4, jstr(x, "ticker"), COL_AMBER)
                   ->setToolTip(jstr(x, "asset"));
               setCell(t, r, 5, type, tc);
               setCell(t, r, 6, jstr(x, "amount"));
             }
           });
  }

  // ---- OWN ------------------------------------------------------------------

  QWidget *buildOwnershipPage() {
    static const ColSpec H_COLS[] = {
        {"ISSUER", 220, false}, {"CLASS", 70, false}, {"CUSIP", 90, false},
        {"VALUE", 84, true},    {"SHARES", 84, true}, {"% PORT", 64, true},
        {"PUT/CALL", 0, false}};
    m_13f = makeTablePanel("13F  MANAGER HOLDINGS", H_COLS, COUNT_OF(H_COLS));
    m_mgrCombo = new QComboBox;
    fillCombo(m_mgrCombo, MANAGERS, COUNT_OF(MANAGERS));
    onComboChanged(m_mgrCombo, this, [this](int) {
      m_mgrCik->setText(m_mgrCombo->currentData().toString());
      fetch13F();
    });
    m_mgrCik = new QLineEdit(m_mgrCombo->currentData().toString());
    m_mgrCik->setFixedWidth(CIK_EDIT_W);
    m_mgrCik->setPlaceholderText("CIK <Enter>");
    m_mgrCik->setToolTip("SEC CIK of any 13F-HR filer");
    QObject::connect(m_mgrCik, &QLineEdit::returnPressed, this,
                     [this]() { fetch13F(); });
    m_13f.p.addControl(m_mgrCombo);
    m_13f.p.addControl(m_mgrCik);
    m_13fSummary = makeInfoLabel();
    m_13f.p.body->insertWidget(0, m_13fSummary);

    static const ColSpec MW_COLS[] = {
        {"FILED", 84, false}, {"TICKER", 60, false}, {"ISSUER", 170, false},
        {"CODE", 46, false},  {"SHARES", 80, true},  {"PRICE", 70, true},
        {"VALUE", 80, true},  {"INSIDER", 0, false}};
    m_mwIns = makeTablePanel("FORM 4  MARKET-WIDE INSIDER FEED", MW_COLS,
                             COUNT_OF(MW_COLS));
    m_mwIns.table->setToolTip(
        "Double-click the ticker: load it; elsewhere: open the Form 4");
    linkSymbolColumn(m_mwIns.table, 1);
    linkUrlRows(m_mwIns.table, 1);

    QSplitter *page = makeSplitter(Qt::Horizontal);
    page->addWidget(m_13f.p.frame);
    page->addWidget(m_mwIns.p.frame);
    page->setSizes(QList<int>() << SPLIT_CENTER_W << SPLIT_CENTER_W);
    return page;
  }

  // HDS double-click: show that manager's 13F on the OWN page.
  void openManager(const QString &cik) {
    m_mgrCik->setText(cik);
    m_workspace->setCurrentWidget(m_ownPage);  // lazy page load may run first
    fetch13F();
  }

  void fetch13F() {
    QString cik = m_mgrCik->text().trimmed();
    while (cik.startsWith('0') && cik.size() > 1) cik.remove(0, 1);
    if (cik.isEmpty() || !QRegExp("^[0-9]{1,10}$").exactMatch(cik)) {
      setPanelError(m_13f.p.status, "numeric CIK required");
      return;
    }
    QUrlQuery q;
    q.addQueryItem("cik", cik);
    q.addQueryItem("top", QString::number(HOLDINGS_TOP));
    busyTable(m_13f);
    m_13fSummary->clear();
    getSeq(ROUTE_13F, q, &m_13f.seq,
           [this](const QString &err, const QJsonObject &o) {
             const QJsonArray items =
                 listItems(m_13f, err, o, "No holdings", "holdings");
             if (!err.isEmpty()) return;
             QTableWidget *t = m_13f.table;
             for (int r = 0; r < items.size(); ++r) {
               const QJsonObject x = items[r].toObject();
               setCell(t, r, 0, jstr(x, "issuer"), r == 0 ? COL_AMBER : COL_TEXT);
               setCell(t, r, 1, jstr(x, "class"), COL_DIM);
               setCell(t, r, 2, jstr(x, "cusip"), COL_DIM);
               setCell(t, r, 3, bigOr(x.value("value")), COL_TEXT, ALIGN_NUM);
               setCell(t, r, 4, bigOr(x.value("shares")), COL_TEXT, ALIGN_NUM)
                   ->setToolTip(jstr(x, "shareType"));
               double pct = 0;
               const bool hp = jnum(x.value("pctPortfolio"), pct);
               setCell(t, r, 5, orNA(hp, fmtNum(pct, 2) + "%"), COL_TEXT, ALIGN_NUM);
               const QString pc = jstr(x, "putCall");
               setCell(t, r, 6, pc, pc.toLower() == "put" ? COL_DOWN : COL_UP);
             }
             QStringList parts;
             parts << span(COL_AMBER, jstr(o, "filer"));
             parts << span(COL_DIM, jstr(o, "form")) + " " +
                          span(COL_TEXT, jstr(o, "reportDate"));
             parts << span(COL_DIM, "FILED") + " " +
                          span(COL_TEXT, jstr(o, "filingDate"));
             parts << span(COL_DIM, "VALUE") + " " +
                          span(COL_TEXT, bigOr(o.value("totalValue")));
             parts << span(COL_DIM, "POSITIONS") + " " +
                          span(COL_TEXT, numOr(o.value("positions"), 0));
             m_13fSummary->setText(parts.join("&nbsp;&nbsp;&nbsp;"));
             m_13fSummary->setToolTip(jstr(o, "url"));
           });
  }

  void fetchMarketInsider() {
    QUrlQuery q;
    q.addQueryItem("marketwide", "1");
    q.addQueryItem("limit", QString::number(MARKETWIDE_INSIDER_LIMIT));
    busyTable(m_mwIns);
    getSeq(ROUTE_INSIDER, q, &m_mwIns.seq,
           [this](const QString &err, const QJsonObject &o) {
             renderInsider(m_mwIns, err, o, true);
           });
  }
#endif

#ifdef UNIVERSE_PROGRESS
  // ---- universe progress (status bar) --------------------------------------

  static QString fmtEta(qint64 secs) {
    if (secs <= 0) return QString();
    if (secs >= SECS_PER_HOUR)
      return QString("%1h %2m").arg(secs / SECS_PER_HOUR)
          .arg((secs % SECS_PER_HOUR) / SECS_PER_MIN);
    if (secs >= SECS_PER_MIN) return QString("%1m").arg(secs / SECS_PER_MIN);
    return QString("%1s").arg(secs);
  }

  static QString coverageLine(const char *label, const QJsonObject &c) {
    QString s = QString("%1: %2/%3 profiled, %4 with sector")
                    .arg(label)
                    .arg(c.value("have").toInt())
                    .arg(c.value("total").toInt())
                    .arg(c.value("classified").toInt());
    const QString eta = fmtEta(qint64(c.value("etaSeconds").toDouble()));
    if (c.value("remaining").toInt() > 0 && !eta.isEmpty())
      s += "  (ETA ~" + eta + ")";
    return s;
  }

  void pollUniverse() {
    if (m_uniBusy) return;
    m_uniBusy = true;
    m_api.getJson(ROUTE_UNIVERSE_STATUS, QUrlQuery(),
                  [this](int st, const QJsonObject &o, const QString &err) {
                    m_uniBusy = false;
                    applyUniverseStatus(bodyError(st, o, err), o);
                  });
  }

  // S&P 500 coverage until it is complete, then the whole universe.
  void applyUniverseStatus(const QString &err, const QJsonObject &o) {
    if (!err.isEmpty()) {
      m_uniBar->setVisible(false);  // backend down or without the route
      return;
    }
    const QJsonObject prof = o.value("profiles").toObject();
    const QJsonObject edgar = o.value("edgar").toObject();
    const QJsonObject sp = prof.value("sp500").toObject();
    const QJsonObject all = prof.value("all").toObject();
    const bool spDone = sp.value("total").toInt() > 0 &&
                        sp.value("remaining").toInt() <= 0;
    const QJsonObject cur = spDone ? all : sp;
    const int total = cur.value("total").toInt();
    const int have = qMin(cur.value("have").toInt(), total);
    const bool paused = prof.contains("pausedUntil");
    const bool done = total > 0 && have >= total;

    m_uniBar->setRange(0, qMax(1, total));
    m_uniBar->setValue(have);
    // QProgressBar formats expand %v / %m / %p only: plain text is safe.
    QString text = QString("%1 %2/%3")
                       .arg(spDone ? UNIVERSE_LABEL_ALL : UNIVERSE_LABEL_SP500)
                       .arg(have)
                       .arg(total);
    if (paused) text += "  PAUSED";
    m_uniBar->setFormat(text);
    const QColor chunk = paused ? COL_WARN : (done ? COL_UP : COL_AMBER);
    m_uniBar->setStyleSheet(
        QString("QProgressBar { border: 1px solid %1; background-color: %2;"
                " color: %3; text-align: center; }"
                "QProgressBar::chunk { background-color: %4; }")
            .arg(COL_GRID.name(), COL_INPUT.name(), COL_TEXT.name(),
                 chunk.name()));

    QStringList tip;
    tip << coverageLine("S&P 500", sp) << coverageLine("All tickers", all);
    const QJsonArray curSyms = prof.value("current").toArray();
    if (!curSyms.isEmpty()) {
      QStringList syms;
      for (const QJsonValue &v : curSyms) syms << v.toString();
      tip << "Fetching: " + syms.join(" ");
    } else if (!prof.value("enabled").toBool()) {
      tip << "Background profile fetching is off (-enrich-auto=false)";
    }
    double rate = 0;
    if (jnum(prof.value("perMinute"), rate) && rate > 0)
      tip << QString("Rate: %1 profiles/min (one request every %2 ms)")
                 .arg(fmtNum(rate, 1))
                 .arg(qint64(prof.value("gapMs").toDouble()));
    if (paused)
      tip << "Yahoo rate limit: all Yahoo requests paused until " +
                 parseIso(jstr(prof, "pausedUntil")).toString("HH:mm:ss");
    if (!jstr(prof, "error").isEmpty()) tip << "Last error: " + jstr(prof, "error");
    QString ed = "EDGAR submissions.zip: ";
    ed += jstr(edgar, "date").isEmpty() ? QString("none yet")
                                        : jstr(edgar, "date") +
                                              QString(", %1 tickers")
                                                  .arg(edgar.value("tickers").toInt());
    if (edgar.value("building").toBool()) ed += "  [" + jstr(edgar, "phase") + "]";
    tip << ed;
    if (!jstr(edgar, "error").isEmpty()) tip << "EDGAR error: " + jstr(edgar, "error");
    if (!edgar.value("secContactConfigured").toBool())
      tip << "Set \"email\" in " + jstr(edgar, "settingsFile") +
                 " (SEC requires a contact address)";
    m_uniBar->setToolTip(tip.join("\n"));
    m_uniBar->setVisible(true);
  }
#endif

  // ---- settings (atomic JSON via QSaveFile) ---------------------------------

  static QString settingsPath() {
    return common::configDir("market-terminal") + "/" + SETTINGS_FILE;
  }

  void saveSettingsDebounced() {
    if (m_saveTimer) m_saveTimer->start();  // restarts if already running
  }

  void saveSettings() {
    QJsonObject o;
    o["baseUrl"] = m_api.base();
    o["symbol"] = m_symbol;
    o["watch"] = QJsonArray::fromStringList(m_watch);
    o["range"] = m_range;
    o["candles"] = m_candles;
#ifdef LIGHT_THEME
    o[SETTINGS_THEME_KEY] = QString(m_lightTheme ? THEME_LIGHT : THEME_DARK);
#endif
#ifdef EXTRA_DATA_PANELS
    if (m_workspace) o[SETTINGS_WORKSPACE_KEY] = m_workspace->currentIndex();
#endif
#ifdef HEATMAP
    if (m_heatmap)
      o[SETTINGS_HEATMAP_SIZE_KEY] = QString(
          m_heatmap->sizeMode() == HeatmapView::SizeMove ? HM_SIZE_MOVE_KEY
                                                         : HM_SIZE_CAP_KEY);
#endif
    o["geometry"] = QString::fromLatin1(saveGeometry().toBase64());

    const QString fn = settingsPath();
    QDir().mkpath(QFileInfo(fn).absolutePath());
    QSaveFile f(fn);
    if (f.open(QIODevice::WriteOnly)) {
      f.write(QJsonDocument(o).toJson(QJsonDocument::Indented));
      f.commit();
    }
  }

  void loadSettings() {
    QFile f(settingsPath());
    if (!f.open(QIODevice::ReadOnly)) return;
    const QJsonObject o = QJsonDocument::fromJson(f.readAll()).object();
    f.close();
    const QString base = jstr(o, "baseUrl");
    if (!base.isEmpty()) m_api.setBase(base);
    const QString sym = normSymbol(jstr(o, "symbol"));
    if (!sym.isEmpty()) m_symbol = sym;
    if (o.contains("watch")) {
      QStringList w;
      for (const QJsonValue &v : o.value("watch").toArray()) w << v.toString();
      m_watch = parseSymbolList(w.join(","));
    }
    const QString rng = jstr(o, "range");
    if (validRange(rng)) m_range = rng;
    m_candles = o.value("candles").toBool();
#ifdef LIGHT_THEME
    m_lightTheme = jstr(o, SETTINGS_THEME_KEY) == QLatin1String(THEME_LIGHT);
#endif
#ifdef EXTRA_DATA_PANELS
    m_savedWorkspace = o.value(SETTINGS_WORKSPACE_KEY).toInt();
#endif
#ifdef HEATMAP
    m_hmSavedMode = jstr(o, SETTINGS_HEATMAP_SIZE_KEY) == QLatin1String(HM_SIZE_MOVE_KEY)
                        ? HeatmapView::SizeMove
                        : HeatmapView::SizeCap;
#endif
    m_savedGeometry =
        QByteArray::fromBase64(jstr(o, "geometry").toLatin1());
  }

  // ---- state ----------------------------------------------------------------

  Api m_api;
  QString m_symbol;
  QStringList m_watch;
  QString m_range;
  bool m_candles = false;
  QByteArray m_savedGeometry;
#ifdef LIGHT_THEME
  bool m_lightTheme = false;
  QPushButton *m_themeBtn = nullptr;
  // Sparkline inputs, kept to re-render the pixmaps on a theme switch.
  struct SparkSrc {
    QVector<double> closes;
    bool hasRef = false;
    double ref = 0;
  };
  QHash<QString, SparkSrc> m_sparkSrc;
#endif

  int m_gen = 0;         // dense snapshot generation (symbol / refresh)
  int m_chartSeq = 0;    // drops stale chart replies
  int m_newsSeq = 0;
  int m_stmtSeq = 0;
  int m_econSeq = 0;
  bool m_quoteBusy = false;
  QPointer<QNetworkReply> m_denseReply;
  QSet<QString> m_denseSeen;  // part keys received in the current snapshot

  QHash<QString, QJsonObject> m_quotes;  // last watchlist item per symbol
  QHash<QString, QPixmap> m_sparks;      // rendered sparkline per symbol
  QHash<QString, double> m_lastPrice;    // for up/down flashes
  QHash<QString, int> m_monitorRows;     // symbol -> monitor row
  QJsonArray m_econItems;                // last calendar (filtered on render)

  QTimer *m_quoteTimer = nullptr;
  QTimer *m_denseTimer = nullptr;
  QTimer *m_healthTimer = nullptr;
  QTimer *m_clockTimer = nullptr;
  QTimer *m_saveTimer = nullptr;

  // ---- widgets --------------------------------------------------------------

  QLineEdit *m_cmd = nullptr;
  QLineEdit *m_watchEdit = nullptr;
  QLineEdit *m_baseEdit = nullptr;
  QLabel *m_clock = nullptr;
  QLabel *m_connLabel = nullptr;
  QLabel *m_updLabel = nullptr;

  Panel m_monitorPanel;
  QTableWidget *m_monitor = nullptr;

  Panel m_newsPanel;
  QComboBox *m_newsScope = nullptr;
  QLineEdit *m_newsQuery = nullptr;
  QTableWidget *m_news = nullptr;

  Panel m_chartPanel;
  QList<QPushButton *> m_rangeButtons;
  QPushButton *m_candleBtn = nullptr;
  ChartWidget *m_chart = nullptr;
  QLabel *m_hdrTicker = nullptr;
  QLabel *m_hdrName = nullptr;
  QLabel *m_hdrStats = nullptr;
  QLabel *m_hdrPrice = nullptr;
  QLabel *m_hdrChange = nullptr;
  QLabel *m_hdrMeta = nullptr;

  QTabWidget *m_tabs = nullptr;
  Panel m_earnPanel;
  QComboBox *m_earnType = nullptr;
  QComboBox *m_earnPeriod = nullptr;
  QTableWidget *m_earn = nullptr;

  Panel m_ecoPanel;
  QComboBox *m_ecoWeek = nullptr;
  QComboBox *m_ecoImpact = nullptr;
  QTableWidget *m_eco = nullptr;

  Panel m_fundPanel;
  QTableWidget *m_fund = nullptr;

  Panel m_anPanel;
  QLabel *m_anConsensus = nullptr;
  DistBar *m_anDist = nullptr;
  QLabel *m_anTargets = nullptr;
  QTableWidget *m_anTable = nullptr;

#ifdef EXTRA_DATA_PANELS
  // Workspace pages + shared state.
  QTabWidget *m_workspace = nullptr;
  int m_savedWorkspace = 0;
  QSet<QWidget *> m_pagesLoaded;  // pages loaded since the last refresh
  QWidget *m_mktPage = nullptr;
  QWidget *m_movPage = nullptr;
  QWidget *m_socPage = nullptr;
  QWidget *m_ownPage = nullptr;
  QTimer *m_pageFastTimer = nullptr;
  QTimer *m_pageSlowTimer = nullptr;
  QTimer *m_fngTimer = nullptr;
  QTimer *m_searchTimer = nullptr;
  QLabel *m_fngLabel = nullptr;  // status bar Fear & Greed
  QStringListModel *m_searchModel = nullptr;
  QCompleter *m_completer = nullptr;
  int m_searchSeq = 0;

  // Symbol tabs.
  TablePanel m_fil;
  QComboBox *m_filForms = nullptr;
  QJsonArray m_filItems;
  TablePanel m_ins;
  TablePanel m_hds;
  QLabel *m_hdsSummary = nullptr;
  TablePanel m_om;
  QComboBox *m_omExpiry = nullptr;
  QLabel *m_omSummary = nullptr;
  QString m_omExpSymbol;  // symbol the expiry list belongs to
  TablePanel m_tran;
  QTextBrowser *m_tranText = nullptr;
  int m_tranDocSeq = 0;

  // MKT
  TablePanel m_wei;  // its seq also guards the shared board request
  TablePanel m_fx;
  TablePanel m_curve;
  LineChart *m_curveChart = nullptr;
  QLabel *m_curveSpreads = nullptr;
  TablePanel m_vix;
  LineChart *m_vixChart = nullptr;
  QLabel *m_vixSummary = nullptr;
  TablePanel m_sectors;
  QHash<QString, SectorPerf> m_sectorPerf;
  TablePanel m_breadth;
  QTableWidget *m_leaders = nullptr;

  // MOV
  TablePanel m_movers;
  QComboBox *m_moverType = nullptr;
  TablePanel m_trending;
  TablePanel m_halts;
  TablePanel m_ipos;
  QComboBox *m_ipoMarket = nullptr;
  QComboBox *m_ipoMonth = nullptr;

  // SOC
  Panel m_fngPanel;
  QLabel *m_fngScore = nullptr;
  QLabel *m_fngDetail = nullptr;
  LineChart *m_fngChart = nullptr;
  QTableWidget *m_fngTable = nullptr;
  int m_fngSeq = 0;
  TablePanel m_wsb;
  QComboBox *m_wsbSubs = nullptr;
  QTableWidget *m_wsbPosts = nullptr;
  TablePanel m_congress;

  // OWN
  TablePanel m_13f;
  QComboBox *m_mgrCombo = nullptr;
  QLineEdit *m_mgrCik = nullptr;
  QLabel *m_13fSummary = nullptr;
  TablePanel m_mwIns;
#endif

#ifdef UNIVERSE_PROGRESS
  QProgressBar *m_uniBar = nullptr;
  QTimer *m_uniTimer = nullptr;
  bool m_uniBusy = false;  // a status request is in flight
#endif

#ifdef HEATMAP
  QStackedWidget *m_heatStack = nullptr;
  HeatmapPage *m_heatmap = nullptr;
  QPushButton *m_heatmapBtn = nullptr;
  HeatmapView::SizeMode m_hmSavedMode = HeatmapView::SizeCap;
#endif
};

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char **argv) {
  QApplication app(argc, argv);
  QApplication::setApplicationName(APP_NAME);
  QApplication::setStyle(QStyleFactory::create("Fusion"));

#if defined(MACOS_STANDARD_FONTS) && defined(Q_OS_MACOS)
  {
    // Standard macOS UI font (system font, native size) for all widgets.
    const QFont ui = QFontDatabase::systemFont(QFontDatabase::GeneralFont);
    QApplication::setFont(ui);
    // Standard macOS monospaced font for tabular data so digits align; keep
    // it the same size as the UI font. Table headers stay on the UI font
    // (exact class-name matches take precedence over inherited ones).
    QFont mono = QFontDatabase::systemFont(QFontDatabase::FixedFont);
    if (ui.pointSizeF() > 0) mono.setPointSizeF(ui.pointSizeF());
    QApplication::setFont(mono, "QAbstractItemView");
    QApplication::setFont(ui, "QHeaderView");
  }
#else
  QFont f = QFontDatabase::systemFont(QFontDatabase::FixedFont);
  f.setPointSize(FONT_PT);
  QApplication::setFont(f);
#endif
  app.setStyleSheet(appStyleSheet());

  // Backend URL: argv[1] > $QUBE_TERMINAL_URL > saved settings > default.
  QString base;
  const QStringList args = QApplication::arguments();
  // (Finder may pass "-psn_..." on older macOS; ignore option-like args.)
  if (args.size() > 1 && !args.at(1).startsWith('-')) base = args.at(1);
  else base = QString::fromLocal8Bit(qgetenv(ENV_BASE_URL)).trimmed();
  if (!base.isEmpty() && !base.startsWith("http://") &&
      !base.startsWith("https://")) {
    base = "http://" + base;
  }

  TerminalWindow w(base);
  w.show();
  return app.exec();
}
