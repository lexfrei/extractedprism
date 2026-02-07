package main

import (
	"fmt"
	"os"
)

// Version and Revision are set via ldflags at build time.
var (
	Version  = "development"
	Revision = "unknown"
)

func main() {
	fmt.Fprintf(os.Stderr, "extractedprism %s (%s)\n", Version, Revision)
	os.Exit(1)
}
