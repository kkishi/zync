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
	src, dst zfsDataset
}

func (*syncCmd) Name() string     { return "sync" }
func (*syncCmd) Synopsis() string { return "Sync two datasets." }
func (*syncCmd) Usage() string {
	return ""
}
func (s *syncCmd) SetFlags(f *flag.FlagSet) {
	f.TextVar(&s.src, "src", &zfsDataset{}, "")
	f.TextVar(&s.dst, "dst", &zfsDataset{}, "")
}

func snapshots(ctx context.Context, dataset zfsDataset) ([]string, error) {
	com := []string{"zfs", "list", "-t", "snapshot", dataset.path, "-o", "name", "-H"}
	if dataset.host != "" {
		com = []string{"ssh", dataset.host, strings.Join(com, " ")}
	}
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, com[0], com[1:]...)
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed running zfs list command: %w", err)
	}
	var ss []string
	for _, s := range strings.Fields(strings.TrimSpace(buf.String())) {
		ss = append(ss, s[len(dataset.path)+1:])
	}
	return ss, nil
}

func (s *syncCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...any) subcommands.ExitStatus {
	src, err := snapshots(ctx, s.src)
	if err != nil {
		fmt.Printf("Error listing src snapshots: %v\n", err)
		return subcommands.ExitFailure
	}
	dst, err := snapshots(ctx, s.dst)
	if err != nil {
		fmt.Printf("Error listing dst snapshots: %v\n", err)
		return subcommands.ExitFailure
	}
	commonLatest := -1
	for i := len(dst) - 1; i >= 0; i-- {
		if j := slices.Index(src, dst[i]); j != -1 {
			commonLatest = j
			break
		}
	}

	for i := commonLatest; i+1 < len(src); i++ {
		receiveCMD := func() *exec.Cmd {
			com := []string{"zfs", "receive", "-F", "-v", s.dst.path}
			if s.dst.host != "" {
				com = []string{"ssh", s.dst.host, strings.Join(com, " ")}
			}
			return exec.CommandContext(ctx, com[0], com[1:]...)
		}()
		receiveCMD.Stdout = os.Stdout
		receiveCMD.Stderr = os.Stderr
		receiveStdin, err := receiveCMD.StdinPipe()
		if err != nil {
			fmt.Printf("Error creating stdinpipe for receiveCMD: %v\n", err)
			return subcommands.ExitFailure
		}

		sendCMD := func() *exec.Cmd {
			com := []string{"zfs", "send", "-v"}
			if i >= 0 {
				com = append(com, "-i", s.src.path+"@"+src[i])
			}
			com = append(com, s.src.path+"@"+src[i+1])
			if s.src.host != "" {
				com = []string{"ssh", s.src.host, strings.Join(com, " ")}
			}
			return exec.CommandContext(ctx, com[0], com[1:]...)
		}()
		sendCMD.Stdout = receiveStdin
		sendCMD.Stderr = os.Stderr
		log.Printf("command: %s\n", sendCMD)
		go func() {
			defer receiveStdin.Close()
			if err := sendCMD.Run(); err != nil {
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
	}

	return subcommands.ExitSuccess
}
