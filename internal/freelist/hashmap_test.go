package freelist

import (
	"testing"

	"go.etcd.io/bbolt/internal/common"
)

func TestFreelistHashmap_allocate(t *testing.T) {
	f := NewHashMapFreelist()

	ids := []common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18}
	f.Init(ids)

	f.Allocate(1, 3)
	if x := f.FreeCount(); x != 6 {
		t.Fatalf("exp=6; got=%v", x)
	}

	f.Allocate(1, 2)
	if x := f.FreeCount(); x != 4 {
		t.Fatalf("exp=4; got=%v", x)
	}
	f.Allocate(1, 1)
	if x := f.FreeCount(); x != 3 {
		t.Fatalf("exp=3; got=%v", x)
	}

	f.Allocate(1, 0)
	if x := f.FreeCount(); x != 3 {
		t.Fatalf("exp=3; got=%v", x)
	}
}
