package freelist

import (
	"reflect"
	"testing"
	"unsafe"

	"go.etcd.io/bbolt/internal/common"
)

// Ensure that a freelist can deserialize from a freelist page.
func TestFreelist_read(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*common.Page)(unsafe.Pointer(&buf[0]))
	page.SetFlags(common.FreelistPageFlag)
	page.SetCount(2)

	// Insert 2 page ids.
	ids := (*[3]common.Pgid)(unsafe.Pointer(uintptr(unsafe.Pointer(page)) + unsafe.Sizeof(*page)))
	ids[0] = 23
	ids[1] = 50

	// Deserialize page into a freelist.
	f := newTestFreelist()
	Serializer{}.Read(f, page)

	// Ensure that there are two page ids in the freelist.
	if exp := common.Pgids([]common.Pgid{23, 50}); !reflect.DeepEqual(exp, f.FreePageIds()) {
		t.Fatalf("exp=%v; got=%v", exp, f.FreePageIds())
	}
}

// Ensure that a freelist can serialize into a freelist page.
func TestFreelist_write(t *testing.T) {
	// Create a freelist and write it to a page.
	var buf [4096]byte
	f := newTestFreelist()

	f.Init([]common.Pgid{12, 39})
	f.pendingPageIds()[100] = &txPending{ids: []common.Pgid{28, 11}}
	f.pendingPageIds()[101] = &txPending{ids: []common.Pgid{3}}
	p := (*common.Page)(unsafe.Pointer(&buf[0]))
	Serializer{}.Write(f, p)

	// Read the page back out.
	f2 := newTestFreelist()
	Serializer{}.Read(f2, p)

	// Ensure that the freelist is correct.
	// All pages should be present and in reverse order.
	if exp := common.Pgids([]common.Pgid{3, 11, 12, 28, 39}); !reflect.DeepEqual(exp, f2.FreePageIds()) {
		t.Fatalf("exp=%v; got=%v", exp, f2.FreePageIds())
	}
}
