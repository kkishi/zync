package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/subcommands"
)

func createZpool(t *testing.T, tmpDir, dataset string) func() {
	t.Helper()
	{
		cmd := exec.Command("truncate", "-s", "100M", filepath.Join(tmpDir, dataset))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("error running truncate command: %v", err)
		}
	}
	{
		cmd := exec.Command("zpool", "create", dataset, filepath.Join(tmpDir, dataset))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("error running zpool create command: %v", err)
		}
	}
	return func() {
		cmd := exec.Command("zpool", "destroy", dataset)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("error running zpool destroy command: %v", err)
		}
	}
}

func TestSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	dataset := zfsDataset{path: "test_src"}
	defer createZpool(t, tmpDir, dataset.path)()

	ctx := context.Background()
	before, err := snapshots(ctx, dataset)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 0 {
		t.Fatalf("Unexpected number of snapshots: %d", len(before))
	}

	if s := (&snapshotCmd{dataset: dataset}).Execute(ctx, nil); s != subcommands.ExitSuccess {
		t.Fatalf("Unexpected status: %v", s)
	}

	after, err := snapshots(ctx, dataset)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 {
		t.Fatalf("Unexpected number of snapshots: %d", len(after))
	}
}

func TestSync(t *testing.T) {
	for numSnapshots := 0; numSnapshots <= 2; numSnapshots++ {
		t.Run(fmt.Sprintf("%d_snapshot(s)", numSnapshots), func(t *testing.T) {
			tmpDir := t.TempDir()
			src := zfsDataset{path: "test_src"}
			dst := zfsDataset{path: "test_dst"}
			defer createZpool(t, tmpDir, src.path)()
			defer createZpool(t, tmpDir, dst.path)()

			ctx := context.Background()

			srcBefore, err := snapshots(ctx, src)
			if err != nil {
				t.Fatal(err)
			}
			if len(srcBefore) != 0 {
				t.Fatalf("Unexpected number of snapshots: %d", len(srcBefore))
			}

			dstBefore, err := snapshots(ctx, dst)
			if err != nil {
				t.Fatal(err)
			}
			if len(dstBefore) != 0 {
				t.Fatalf("Unexpected number of snapshots: %d", len(dstBefore))
			}

			for i := 0; i < numSnapshots; i++ {
				if i > 0 {
					time.Sleep(time.Second) // FIXME
				}
				if s := (&snapshotCmd{dataset: src}).Execute(ctx, nil); s != subcommands.ExitSuccess {
					t.Fatalf("Unexpected status: %v", s)
				}
			}
			if s := (&syncCmd{src: src, dst: dst}).Execute(ctx, nil); s != subcommands.ExitSuccess {
				t.Fatalf("Unexpected status: %v", s)
			}

			srcAfter, err := snapshots(ctx, src)
			if err != nil {
				t.Fatal(err)
			}
			if len(srcAfter) != numSnapshots {
				t.Fatalf("Unexpected number of snapshots: %d, want %d", len(srcAfter), numSnapshots)
			}

			dstAfter, err := snapshots(ctx, dst)
			if err != nil {
				t.Fatal(err)
			}
			if len(dstAfter) != numSnapshots {
				t.Fatalf("Unexpected number of snapshots: %d, want %d", len(dstAfter), numSnapshots)
			}

			if diff := cmp.Diff(srcAfter, dstAfter); diff != "" {
				t.Fatalf("Unexpected diff of snapshot values(-src,+dst):\n%s", diff)
			}
		})
	}
}
