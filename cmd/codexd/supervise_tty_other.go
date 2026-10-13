//go:build !darwin

package main

import "os"

func supervisorStdinIsTerminal(*os.File) bool { return false }
