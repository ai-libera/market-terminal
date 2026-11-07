package common

import (
  "os"
  "path/filepath"
  "runtime"
  "strings"
)

// ExeSuffix is ".exe" on Windows and "" elsewhere.
var ExeSuffix = func() string {
  if runtime.GOOS == "windows" {
    return ".exe"
  }
  return ""
}()

// ExeDir is the directory of the running executable (symlinks resolved).
// All programs of this repo are built into the same dir (build/bin).
func ExeDir() string {
  exe, err := os.Executable()
  if err != nil {
    return "."
  }
  if r, err := filepath.EvalSymlinks(exe); err == nil {
    exe = r
  }
  return filepath.Dir(exe)
}

// SiblingPath is the path of the program name next to the running executable,
// with ".exe" on Windows.
func SiblingPath(name string) string {
  return WithExeSuffix(filepath.Join(ExeDir(), name))
}

// WithExeSuffix adds ExeSuffix to path unless it already has an extension.
func WithExeSuffix(path string) string {
  if ExeSuffix == "" || filepath.Ext(path) != "" {
    return path
  }
  if strings.HasSuffix(strings.ToLower(path), ExeSuffix) {
    return path
  }
  return path + ExeSuffix
}
