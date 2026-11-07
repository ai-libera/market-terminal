//go:build windows

package common

import (
  "os"
  "syscall"
  "unsafe"
)

var (
  kernel32         = syscall.NewLazyDLL("kernel32.dll")
  procLockFileEx   = kernel32.NewProc("LockFileEx")
  procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const lockfileExclusiveLock = 0x2

// LockFile takes an exclusive lock on f and blocks until it gets it.
func LockFile(f *os.File) error {
  var ol syscall.Overlapped
  r, _, err := procLockFileEx.Call(f.Fd(), lockfileExclusiveLock, 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
  if r == 0 {
    return err
  }
  return nil
}

// UnlockFile releases the lock taken by LockFile.
func UnlockFile(f *os.File) error {
  var ol syscall.Overlapped
  r, _, err := procUnlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
  if r == 0 {
    return err
  }
  return nil
}
