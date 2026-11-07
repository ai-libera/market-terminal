//go:build windows

package common

import (
  "os/exec"
  "syscall"
)

// SetProcessGroup starts cmd in a new process group.
func SetProcessGroup(cmd *exec.Cmd) {
  cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// TerminateProcessGroup ends cmd. Windows has no SIGTERM, so the process
// is killed.
func TerminateProcessGroup(cmd *exec.Cmd) error {
  return cmd.Process.Kill()
}
