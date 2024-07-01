package freelist

import (
	"fmt"
	"sort"
	"unsafe"

	"go.etcd.io/bbolt/internal/common"
)

type sharedInterface interface {
	// Init initializes this freelist with the given list of pages.
	Init(ids common.Pgids)
	// Count returns the number of free and pending pages.
	Count() int
	// FreePageIds returns all free pages.
	FreePageIds() common.Pgids
	// mergeSpans is merging the given pages into the freelist
	mergeSpans(ids common.Pgids)
}

type txPending struct {
	ids              []common.Pgid
	alloctx          []common.Txid // txids allocating the ids
	lastReleaseBegin common.Txid   // beginning txid of last matching releaseRange
}

type shared struct {
	sharedInterface

	allocs  map[common.Pgid]common.Txid // mapping of Txid that allocated a pgid.
	cache   map[common.Pgid]struct{}    // fast lookup of all free and pending page ids.
	pending map[common.Txid]*txPending  // mapping of soon-to-be free page ids by tx.
}

func (t *shared) pendingPageIds() map[common.Txid]*txPending {
	return t.pending
}

func (t *shared) PendingCount() int {
	var count int
	for _, txp := range t.pending {
		count += len(txp.ids)
	}
	return count
}

func (t *shared) Freed(pgId common.Pgid) bool {
	_, ok := t.cache[pgId]
	return ok
}

func (t *shared) Free(txid common.Txid, p *common.Page) {
	if p.Id() <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.Id()))
	}

	// Free page and all its overflow pages.
	txp := t.pending[txid]
	if txp == nil {
		txp = &txPending{}
		t.pending[txid] = txp
	}
	allocTxid, ok := t.allocs[p.Id()]
	if ok {
		delete(t.allocs, p.Id())
	} else if p.IsFreelistPage() {
		// Freelist is always allocated by prior tx.
		allocTxid = txid - 1
	}

	for id := p.Id(); id <= p.Id()+common.Pgid(p.Overflow()); id++ {
		// Verify that page is not already free.
		if _, ok := t.cache[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		t.cache[id] = struct{}{}
	}
}

func (t *shared) Rollback(txid common.Txid) {
	// Remove page ids from cache.
	txp := t.pending[txid]
	if txp == nil {
		return
	}
	var m common.Pgids
	for i, pgid := range txp.ids {
		delete(t.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			t.allocs[pgid] = tx
		} else {
			// Freed page was allocated by this txn; OK to throw away.
			m = append(m, pgid)
		}
	}
	// Remove pages from pending list and mark as free if allocated by txid.
	delete(t.pending, txid)
	t.sharedInterface.mergeSpans(m)
}

func (t *shared) Release(txid common.Txid) {
	m := make(common.Pgids, 0)
	for tid, txp := range t.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(t.pending, tid)
		}
	}
	t.mergeSpans(m)
}

func (t *shared) ReleaseRange(begin, end common.Txid) {
	if begin > end {
		return
	}
	var m common.Pgids
	for tid, txp := range t.pending {
		if tid < begin || tid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(t.pending, tid)
		}
	}
	t.mergeSpans(m)
}

func (t *shared) Read(p *common.Page) {
	if !p.IsFreelistPage() {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.Id(), p.Typ()))
	}

	ids := p.FreelistPageIds()

	// Copy the list of page ids from the freelist.
	if len(ids) == 0 {
		t.Init(nil)
	} else {
		// copy the ids, so we don't modify on the freelist page directly
		idsCopy := make([]common.Pgid, len(ids))
		copy(idsCopy, ids)
		// Make sure they're sorted.
		sort.Sort(common.Pgids(idsCopy))

		t.Init(idsCopy)
	}
}

func (t *shared) EstimatedWritePageSize() int {
	n := t.Count()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return int(common.PageHeaderSize) + (int(unsafe.Sizeof(common.Pgid(0))) * n)
}

func (t *shared) Write(p *common.Page) {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.SetFlags(common.FreelistPageFlag)

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	l := t.Count()
	if l == 0 {
		p.SetCount(uint16(l))
	} else if l < 0xFFFF {
		p.SetCount(uint16(l))
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l)
		t.copyall(ids)
	} else {
		p.SetCount(0xFFFF)
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l+1)
		ids[0] = common.Pgid(l)
		t.copyall(ids[1:])
	}
}

// Copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func (t *shared) copyall(dst []common.Pgid) {
	m := make(common.Pgids, 0, t.PendingCount())
	for _, txp := range t.pendingPageIds() {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	common.Mergepgids(dst, t.FreePageIds(), m)
}

// reindex rebuilds the free cache based on available and pending free lists.
func (t *shared) reindex(free common.Pgids, pending map[common.Txid]*txPending) {
	t.cache = make(map[common.Pgid]struct{}, len(free))
	for _, id := range free {
		t.cache[id] = struct{}{}
	}
	for _, txp := range pending {
		for _, pendingID := range txp.ids {
			t.cache[pendingID] = struct{}{}
		}
	}
}
