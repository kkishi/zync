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

	"github.com/google/subcommands"
)

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

func snapshots(ctx context.Context, dataset string) ([]string, error) {
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, "zfs", "list", "-t", "snapshot", dataset, "-o", "name", "-H")
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed running zfs list command: %w", err)
	}
	var ss []string
	for _, s := range strings.Fields(strings.TrimSpace(buf.String())) {
		ss = append(ss, s[len(dataset)+1:])
	}
	return ss, nil
}

func (s *syncCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...any) subcommands.ExitStatus {
	src, err := snapshots(ctx, s.src)
	if err != nil {
		fmt.Printf("Error listing src snapshots: %v\n", err)
		return subcommands.ExitFailure
	}
	if len(src) == 0 {
		return subcommands.ExitSuccess
	}
	srcLatest := s.src + "@" + src[len(src)-1]

	dst, err := snapshots(ctx, s.dst)
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
		log.Printf("receiveCMD error: %v\n", err)
		return subcommands.ExitFailure
	} else {
		log.Println("receiveCMD success")
	}

	return subcommands.ExitSuccess
}
