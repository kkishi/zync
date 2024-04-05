package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/google/subcommands"
)

type snapshotCmd struct {
	dataset zfsDataset
}

func (*snapshotCmd) Name() string     { return "snapshot" }
func (*snapshotCmd) Synopsis() string { return "Create a new snapshot." }
func (*snapshotCmd) Usage() string {
	return ""
}
func (s *snapshotCmd) SetFlags(f *flag.FlagSet) {
	f.TextVar(&s.dataset, "dataset", &zfsDataset{}, "")
}

func (s *snapshotCmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...any) subcommands.ExitStatus {
	cmd := exec.CommandContext(ctx, "zfs", "snapshot", s.dataset.path+"@"+time.Now().Format("2006-01-02-15:04:05"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Printf("command: %s\n", cmd)
	if err := cmd.Run(); err != nil {
		fmt.Printf("Failed to create a snapshot: %v\n", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
