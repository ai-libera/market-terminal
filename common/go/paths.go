// Package common holds the code shared by the market-terminal Go programs.
package common

import (
  "os"
  "path/filepath"
  "runtime"
)

// xdgDir returns $<env> if it holds an absolute path, else "".
func xdgDir(env string) string {
  if v := os.Getenv(env); v != "" && filepath.IsAbs(v) {
    return v
  }
  return ""
}

func homeDir() string {
  if h, err := os.UserHomeDir(); err == nil {
    return h
  }
  return "."
}

// localAppData is %LOCALAPPDATA% (Windows), with a fallback below the home dir.
func localAppData() string {
  if v := os.Getenv("LOCALAPPDATA"); v != "" {
    return v
  }
  return filepath.Join(homeDir(), "AppData", "Local")
}

func roamingAppData() string {
  if v := os.Getenv("APPDATA"); v != "" {
    return v
  }
  return filepath.Join(homeDir(), "AppData", "Roaming")
}

// platformDir resolves one row of the directory table:
// XDG variable (any Unix, if set and absolute), then the native dir.
func platformDir(app, xdgEnv, linuxDefault, windows, macos string) string {
  if runtime.GOOS == "windows" {
    return filepath.Join(windows, app)
  }
  if d := xdgDir(xdgEnv); d != "" {
    return filepath.Join(d, app)
  }
  if runtime.GOOS == "darwin" {
    return filepath.Join(homeDir(), macos, app)
  }
  return filepath.Join(homeDir(), linuxDefault, app)
}

// EnsureDir creates dir (and its parents) and returns it.
func EnsureDir(dir string) string {
  _ = os.MkdirAll(dir, 0o755)
  return dir
}

// ConfigDir is the settings dir of app (created):
// Linux $XDG_CONFIG_HOME/<app> or ~/.config/<app>, Windows %APPDATA%\<app>,
// macOS ~/Library/Preferences/<app>.
func ConfigDir(app string) string {
  return EnsureDir(platformDir(app, "XDG_CONFIG_HOME", ".config",
    roamingAppData(), filepath.Join("Library", "Preferences")))
}

// DataDir is the storage dir of app for data that can't be re-created
// (created): Linux $XDG_DATA_HOME/<app> or ~/.local/share/<app>, Windows
// %LOCALAPPDATA%\<app>, macOS ~/Library/Application Support/<app>.
func DataDir(app string) string {
  return EnsureDir(platformDir(app, "XDG_DATA_HOME", filepath.Join(".local", "share"),
    localAppData(), filepath.Join("Library", "Application Support")))
}

// CacheDir is the dir of app for data that can be downloaded again
// (created): Linux $XDG_CACHE_HOME/<app> or ~/.cache/<app>, Windows
// %LOCALAPPDATA%\<app>\Cache, macOS ~/Library/Caches/<app>.
func CacheDir(app string) string {
  if runtime.GOOS == "windows" {
    return EnsureDir(filepath.Join(localAppData(), app, "Cache"))
  }
  return EnsureDir(platformDir(app, "XDG_CACHE_HOME", ".cache",
    "", filepath.Join("Library", "Caches")))
}

// StateDir is the dir of app for logs, lock files and "last run" info
// (created): Linux $XDG_STATE_HOME/<app> or ~/.local/state/<app>, Windows
// %LOCALAPPDATA%\<app>, macOS ~/Library/Application Support/<app>.
func StateDir(app string) string {
  return EnsureDir(platformDir(app, "XDG_STATE_HOME", filepath.Join(".local", "state"),
    localAppData(), filepath.Join("Library", "Application Support")))
}

// FileExists reports whether fn exists and is not a directory.
func FileExists(fn string) bool {
  st, err := os.Stat(fn)
  return err == nil && !st.IsDir()
}
