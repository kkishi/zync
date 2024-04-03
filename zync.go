package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/google/subcommands"
)

type snapshotCmd struct {
	dataset string
}

func (*snapshotCmd) Name() string     { return "snapshot" }
func (*snapshotCmd) Synopsis() string { return "Create a new snapshot." }
func (*snapshotCmd) Usage() string {
	return ""
}
func (s *snapshotCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&s.dataset, "dataset", "", "")
}

func (s *snapshotCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...any) subcommands.ExitStatus {
	cmd := exec.CommandContext(ctx, "sudo", "zfs", "snapshot", s.dataset+"@"+time.Now().Format("2006-01-02-15:04:05"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("Failed to create a snapshot: %v\n", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

type syncCmd struct {
	src, dst string
}

func (*syncCmd) Name() string     { return "sync" }
func (*syncCmd) Synopsis() string { return "Sync two datasets." }
func (*syncCmd) Usage() string {
	return ""
}
func (s *syncCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&s.src, "src", "", "")
	f.StringVar(&s.dst, "dst", "", "")
}

func (s *syncCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...any) subcommands.ExitStatus {
	snapshots := func(dataset string) ([]string, error) {
		var buf bytes.Buffer
		cmd := exec.CommandContext(ctx, "zfs", "list", "-t", "snapshot", dataset, "-o", "name", "-H")
		cmd.Stdout = &buf
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("FIXME: %w", err)
		}
		var ss []string
		for _, s := range strings.Fields(strings.TrimSpace(buf.String())) {
			ss = append(ss, s[len(dataset)+1:])
		}
		return ss, nil
	}

	src, err := snapshots(s.src)
	if err != nil {
		fmt.Printf("Error listing src snapshots: %v\n", err)
		return subcommands.ExitFailure
	}
	if len(src) == 0 {
		return subcommands.ExitSuccess
	}
	srcLatest := s.src + "@" + src[len(src)-1]

	dst, err := snapshots(s.dst)
	if err != nil {
		fmt.Printf("Error listing dst snapshots: %v\n", err)
		return subcommands.ExitFailure
	}
	var commonLatest string
	for i := len(dst) - 1; i >= 0; i-- {
		if slices.Index(src, dst[i]) != -1 {
			commonLatest = s.src + "@" + dst[i]
		}
	}
	if commonLatest == srcLatest {
		return subcommands.ExitSuccess
	}

	receiveCMD := exec.CommandContext(ctx, "sudo", "zfs", "receive", "-F", "-v", s.dst)
	receiveCMD.Stdout = os.Stdout
	receiveCMD.Stderr = os.Stderr
	receiveStdin, err := receiveCMD.StdinPipe()
	if err != nil {
		fmt.Printf("Error creating stdinpipe for receiveCMD: %v\n", err)
		return subcommands.ExitFailure
	}

	var sendCMD *exec.Cmd
	if commonLatest == "" {
		sendCMD = exec.CommandContext(ctx, "sudo", "zfs", "send", "-v", srcLatest)
	} else {
		sendCMD = exec.CommandContext(ctx, "sudo", "zfs", "send", "-v", "-i", commonLatest, srcLatest)
	}
	sendCMD.Stdout = receiveStdin
	sendCMD.Stderr = os.Stderr
	log.Printf("command: %s\n", sendCMD)
	go func() {
		defer receiveStdin.Close()
		err := sendCMD.Run()
		if err != nil {
			log.Printf("sendCMD error: %v", err)
		} else {
			log.Println("sendCMD success")
		}
	}()
	log.Printf("command: %s\n", receiveCMD)
	if err := receiveCMD.Run(); err != nil {
		fmt.Printf("receiveCMD error: %v\n", err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func main() {
	subcommands.Register(&snapshotCmd{}, "")
	subcommands.Register(&syncCmd{}, "")
	flag.Parse()
	os.Exit(int(subcommands.Execute(context.Background())))
}
