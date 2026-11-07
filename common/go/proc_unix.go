//go:build !windows

package common

import (
  "os/exec"
  "syscall"
)

// SetProcessGroup starts cmd in its own process group, so that
// TerminateProcessGroup also reaches the children it starts.
func SetProcessGroup(cmd *exec.Cmd) {
  cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// TerminateProcessGroup sends SIGTERM to the process group of cmd.
func TerminateProcessGroup(cmd *exec.Cmd) error {
  return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
}
