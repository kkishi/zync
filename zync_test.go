package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
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

	(&snapshotCmd{dataset: dataset}).Execute(ctx, nil)

	after, err := snapshots(ctx, dataset)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 {
		t.Fatalf("Unexpected number of snapshots: %d", len(after))
	}
}

func TestSync(t *testing.T) {
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

	(&snapshotCmd{dataset: src}).Execute(ctx, nil)
	(&syncCmd{src: src, dst: dst}).Execute(ctx, nil)

	srcAfter, err := snapshots(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(srcAfter) != 1 {
		t.Fatalf("Unexpected number of snapshots: %d", len(srcAfter))
	}

	dstAfter, err := snapshots(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(dstAfter) != 1 {
		t.Fatalf("Unexpected number of snapshots: %d", len(dstAfter))
	}

	if srcAfter[0] != dstAfter[0] {
		t.Fatalf("Unexpected snapshot values: %s v.s. %s", srcAfter[0], dstAfter[0])
	}
}
