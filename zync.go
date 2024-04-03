package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"
)

func main() {
	subcommands.Register(&snapshotCmd{}, "")
	subcommands.Register(&syncCmd{}, "")
	flag.Parse()
	os.Exit(int(subcommands.Execute(context.Background())))
}
