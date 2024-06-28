package freelist

import (
	"fmt"
	"sort"
	"unsafe"

	"go.etcd.io/bbolt/internal/common"
)

type Serializable interface {
	// Read calls Init with the page ids stored in te given page.
	Read(f Interface, page *common.Page)

	// Write writes the freelist into the given page.
	Write(f Interface, page *common.Page)

	// EstimatedWritePageSize returns the size of the freelist after serialization in Write.
	// This should never underestimate the size.
	EstimatedWritePageSize(f Interface) int
}

type Serializer struct {
}

func (s Serializer) Read(f Interface, p *common.Page) {
	if !p.IsFreelistPage() {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.Id(), p.Typ()))
	}

	ids := p.FreelistPageIds()

	// Copy the list of page ids from the freelist.
	if len(ids) == 0 {
		f.Init(nil)
	} else {
		// copy the ids, so we don't modify on the freelist page directly
		idsCopy := make([]common.Pgid, len(ids))
		copy(idsCopy, ids)
		// Make sure they're sorted.
		sort.Sort(common.Pgids(idsCopy))

		f.Init(idsCopy)
	}
}

func (s Serializer) EstimatedWritePageSize(f Interface) int {
	n := f.Count()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return int(common.PageHeaderSize) + (int(unsafe.Sizeof(common.Pgid(0))) * n)
}

func (s Serializer) Write(f Interface, p *common.Page) {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.SetFlags(common.FreelistPageFlag)

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	l := f.Count()
	if l == 0 {
		p.SetCount(uint16(l))
	} else if l < 0xFFFF {
		p.SetCount(uint16(l))
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l)
		Copyall(f, ids)
	} else {
		p.SetCount(0xFFFF)
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l+1)
		ids[0] = common.Pgid(l)
		Copyall(f, ids[1:])
	}
}
