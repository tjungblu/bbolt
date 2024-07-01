package freelist

import (
	"sort"

	"go.etcd.io/bbolt/internal/common"
)

type ReadWriter interface {
	// Read calls Init with the page ids stored in te given page.
	Read(page *common.Page)

	// Write writes the freelist into the given page.
	Write(page *common.Page)

	// EstimatedWritePageSize returns the size of the freelist after serialization in Write.
	// This should never underestimate the size.
	EstimatedWritePageSize() int
}

type Interface interface {
	ReadWriter

	// Init initializes this freelist with the given list of pages.
	Init(ids common.Pgids)

	// Allocate returns the starting page id of a contiguous block of a given size in number of pages.
	// If a contiguous block cannot be found then 0 is returned.
	Allocate(txid common.Txid, numPages int) common.Pgid

	// Count returns the number of free and pending pages.
	Count() int

	// FreeCount returns the number of free pages.
	FreeCount() int

	// FreePageIds returns all free pages.
	FreePageIds() common.Pgids

	// PendingCount returns the number of pending pages.
	PendingCount() int

	// Release moves all page ids for a transaction id (or older) to the freelist.
	Release(txId common.Txid)

	// ReleaseRange moves pending pages allocated within an extent [begin,end] to the free list.
	ReleaseRange(begin, end common.Txid)

	// Free releases a page and its overflow for a given transaction id.
	// If the page is already free then a panic will occur.
	Free(txId common.Txid, p *common.Page)

	// Freed returns whether a given page is in the free list.
	Freed(pgId common.Pgid) bool

	// Rollback removes the pages from a given pending tx.
	Rollback(txId common.Txid)

	// pendingPageIds returns all pending pages by transaction id.
	pendingPageIds() map[common.Txid]*txPending
}

// Copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func Copyall(f Interface, dst []common.Pgid) {
	m := make(common.Pgids, 0, f.PendingCount())
	for _, txp := range f.pendingPageIds() {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	common.Mergepgids(dst, f.FreePageIds(), m)
}

// Reload reads the freelist from a page and filters out pending items.
func Reload(f Interface, p *common.Page) {
	f.Read(p)
	NoSyncReload(f, p.FreelistPageIds())
}

// NoSyncReload reads the freelist from Pgids and filters out pending items.
func NoSyncReload(f Interface, pgIds common.Pgids) {
	// Build a cache of only pending pages.
	pcache := make(map[common.Pgid]bool)
	for _, txp := range f.pendingPageIds() {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []common.Pgid
	for _, id := range pgIds {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.Init(a)
}
