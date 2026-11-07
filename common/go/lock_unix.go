//go:build !windows

package common

import (
  "os"
  "syscall"
)

// LockFile takes an exclusive lock on f and blocks until it gets it.
func LockFile(f *os.File) error {
  return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// UnlockFile releases the lock taken by LockFile.
func UnlockFile(f *os.File) error {
  return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
