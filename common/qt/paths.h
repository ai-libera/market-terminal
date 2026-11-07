// Per-platform config / data / cache / state dirs, the same as common/go.
// Header-only; no QStandardPaths, so the Qt app and the Go programs agree.
#pragma once

#include <QtCore/QDir>
#include <QtCore/QString>
#include <QtCore/QtGlobal>

namespace common {

inline QString xdgDir(const char *env) {
  const QString v = qEnvironmentVariable(env);
  return (!v.isEmpty() && QDir::isAbsolutePath(v)) ? v : QString();
}

inline QString ensureDir(const QString &dir) {
  QDir().mkpath(dir);
  return dir;
}

inline QString platformDir(const QString &app, const char *xdgEnv,
                           const QString &linuxDefault,
                           const QString &windows, const QString &macos) {
#ifdef Q_OS_WIN
  Q_UNUSED(xdgEnv);
  Q_UNUSED(linuxDefault);
  Q_UNUSED(macos);
  return windows + "/" + app;
#else
  Q_UNUSED(windows);
  const QString x = xdgDir(xdgEnv);
  if (!x.isEmpty()) return x + "/" + app;
#ifdef Q_OS_MACOS
  Q_UNUSED(linuxDefault);
  return QDir::homePath() + "/" + macos + "/" + app;
#else
  Q_UNUSED(macos);
  return QDir::homePath() + "/" + linuxDefault + "/" + app;
#endif
#endif
}

inline QString localAppData() {
  const QString v = qEnvironmentVariable("LOCALAPPDATA");
  return v.isEmpty() ? QDir::homePath() + "/AppData/Local" : v;
}

inline QString roamingAppData() {
  const QString v = qEnvironmentVariable("APPDATA");
  return v.isEmpty() ? QDir::homePath() + "/AppData/Roaming" : v;
}

// Settings: ~/.config/<app>, %APPDATA%\<app>, ~/Library/Preferences/<app>.
inline QString configDir(const QString &app) {
  return ensureDir(platformDir(app, "XDG_CONFIG_HOME", ".config",
                               roamingAppData(), "Library/Preferences"));
}

// Storage: ~/.local/share/<app>, %LOCALAPPDATA%\<app>,
// ~/Library/Application Support/<app>.
inline QString dataDir(const QString &app) {
  return ensureDir(platformDir(app, "XDG_DATA_HOME", ".local/share",
                               localAppData(), "Library/Application Support"));
}

// Re-downloadable data: ~/.cache/<app>, %LOCALAPPDATA%\<app>\Cache,
// ~/Library/Caches/<app>.
inline QString cacheDir(const QString &app) {
#ifdef Q_OS_WIN
  return ensureDir(localAppData() + "/" + app + "/Cache");
#else
  return ensureDir(platformDir(app, "XDG_CACHE_HOME", ".cache", QString(),
                               "Library/Caches"));
#endif
}

// Logs, lock files, "last run": ~/.local/state/<app>, %LOCALAPPDATA%\<app>,
// ~/Library/Application Support/<app>.
inline QString stateDir(const QString &app) {
  return ensureDir(platformDir(app, "XDG_STATE_HOME", ".local/state",
                               localAppData(), "Library/Application Support"));
}

}  // namespace common
