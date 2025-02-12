package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Returns the smaller integer between the given two.
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Returns the greater integer between the given two.
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
