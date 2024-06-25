package freelist

import (
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"

	"go.etcd.io/bbolt/internal/common"
)

// TestFreelistType is used as a env variable for test to indicate the backend type
const TestFreelistType = "TEST_FREELIST_TYPE"

// Ensure that a page is added to a transaction's freelist.
func TestFreelist_free(t *testing.T) {
	f := newTestFreelist()
	f.Free(100, common.NewPage(12, 0, 0, 0))
	if !reflect.DeepEqual([]common.Pgid{12}, f.pendingPageIds()[100].ids) {
		t.Fatalf("exp=%v; got=%v", []common.Pgid{12}, f.pendingPageIds()[100].ids)
	}
}

// Ensure that a page and its overflow is added to a transaction's freelist.
func TestFreelist_free_overflow(t *testing.T) {
	f := newTestFreelist()
	f.Free(100, common.NewPage(12, 0, 0, 3))
	if exp := []common.Pgid{12, 13, 14, 15}; !reflect.DeepEqual(exp, f.pendingPageIds()[100].ids) {
		t.Fatalf("exp=%v; got=%v", exp, f.pendingPageIds()[100].ids)
	}
}

// Ensure that a transaction's free pages can be released.
func TestFreelist_release(t *testing.T) {
	f := newTestFreelist()
	f.Free(100, common.NewPage(12, 0, 0, 1))
	f.Free(100, common.NewPage(9, 0, 0, 0))
	f.Free(102, common.NewPage(39, 0, 0, 0))
	f.Release(100)
	f.Release(101)
	if exp := common.Pgids([]common.Pgid{9, 12, 13}); !reflect.DeepEqual(exp, f.FreePageIds()) {
		t.Fatalf("exp=%v; got=%v", exp, f.FreePageIds())
	}

	f.Release(102)
	if exp := common.Pgids([]common.Pgid{9, 12, 13, 39}); !reflect.DeepEqual(exp, f.FreePageIds()) {
		t.Fatalf("exp=%v; got=%v", exp, f.FreePageIds())
	}
}

// Ensure that releaseRange handles boundary conditions correctly
func TestFreelist_releaseRange(t *testing.T) {
	type testRange struct {
		begin, end common.Txid
	}

	type testPage struct {
		id       common.Pgid
		n        int
		allocTxn common.Txid
		freeTxn  common.Txid
	}

	var releaseRangeTests = []struct {
		title         string
		pagesIn       []testPage
		releaseRanges []testRange
		wantFree      []common.Pgid
	}{
		{
			title:         "Single pending in range",
			pagesIn:       []testPage{{id: 3, n: 1, allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{1, 300}},
			wantFree:      []common.Pgid{3},
		},
		{
			title:         "Single pending with minimum end range",
			pagesIn:       []testPage{{id: 3, n: 1, allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{1, 200}},
			wantFree:      []common.Pgid{3},
		},
		{
			title:         "Single pending outsize minimum end range",
			pagesIn:       []testPage{{id: 3, n: 1, allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{1, 199}},
			wantFree:      nil,
		},
		{
			title:         "Single pending with minimum begin range",
			pagesIn:       []testPage{{id: 3, n: 1, allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{100, 300}},
			wantFree:      []common.Pgid{3},
		},
		{
			title:         "Single pending outside minimum begin range",
			pagesIn:       []testPage{{id: 3, n: 1, allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{101, 300}},
			wantFree:      nil,
		},
		{
			title:         "Single pending in minimum range",
			pagesIn:       []testPage{{id: 3, n: 1, allocTxn: 199, freeTxn: 200}},
			releaseRanges: []testRange{{199, 200}},
			wantFree:      []common.Pgid{3},
		},
		{
			title:         "Single pending and read transaction at 199",
			pagesIn:       []testPage{{id: 3, n: 1, allocTxn: 199, freeTxn: 200}},
			releaseRanges: []testRange{{100, 198}, {200, 300}},
			wantFree:      nil,
		},
		{
			title: "Adjacent pending and read transactions at 199, 200",
			pagesIn: []testPage{
				{id: 3, n: 1, allocTxn: 199, freeTxn: 200},
				{id: 4, n: 1, allocTxn: 200, freeTxn: 201},
			},
			releaseRanges: []testRange{
				{100, 198},
				{200, 199}, // Simulate the ranges db.freePages might produce.
				{201, 300},
			},
			wantFree: nil,
		},
		{
			title: "Out of order ranges",
			pagesIn: []testPage{
				{id: 3, n: 1, allocTxn: 199, freeTxn: 200},
				{id: 4, n: 1, allocTxn: 200, freeTxn: 201},
			},
			releaseRanges: []testRange{
				{201, 199},
				{201, 200},
				{200, 200},
			},
			wantFree: nil,
		},
		{
			title: "Multiple pending, read transaction at 150",
			pagesIn: []testPage{
				{id: 3, n: 1, allocTxn: 100, freeTxn: 200},
				{id: 4, n: 1, allocTxn: 100, freeTxn: 125},
				{id: 5, n: 1, allocTxn: 125, freeTxn: 150},
				{id: 6, n: 1, allocTxn: 125, freeTxn: 175},
				{id: 7, n: 2, allocTxn: 150, freeTxn: 175},
				{id: 9, n: 2, allocTxn: 175, freeTxn: 200},
			},
			releaseRanges: []testRange{{50, 149}, {151, 300}},
			wantFree:      []common.Pgid{4, 9, 10},
		},
	}

	for _, c := range releaseRangeTests {
		f := newTestFreelist()
		var ids []common.Pgid
		for _, p := range c.pagesIn {
			for i := uint64(0); i < uint64(p.n); i++ {
				ids = append(ids, common.Pgid(uint64(p.id)+i))
			}
		}
		f.Init(ids)
		for _, p := range c.pagesIn {
			f.Allocate(p.allocTxn, p.n)
		}

		for _, p := range c.pagesIn {
			f.Free(p.freeTxn, common.NewPage(p.id, 0, 0, uint32(p.n-1)))
		}

		for _, r := range c.releaseRanges {
			f.ReleaseRange(r.begin, r.end)
		}

		if exp := common.Pgids(c.wantFree); !reflect.DeepEqual(exp, f.FreePageIds()) {
			t.Errorf("exp=%v; got=%v for %s", exp, f.FreePageIds(), c.title)
		}
	}
}

func Benchmark_FreelistRelease10K(b *testing.B)    { benchmark_FreelistRelease(b, 10000) }
func Benchmark_FreelistRelease100K(b *testing.B)   { benchmark_FreelistRelease(b, 100000) }
func Benchmark_FreelistRelease1000K(b *testing.B)  { benchmark_FreelistRelease(b, 1000000) }
func Benchmark_FreelistRelease10000K(b *testing.B) { benchmark_FreelistRelease(b, 10000000) }

func benchmark_FreelistRelease(b *testing.B, size int) {
	ids := randomPgids(size)
	pending := randomPgids(len(ids) / 400)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txp := &txPending{ids: pending}
		f := newTestFreelist()
		f.pendingPageIds()[1] = txp
		f.Init(ids)
		f.Release(1)
	}
}

func randomPgids(n int) []common.Pgid {
	pgids := make(common.Pgids, n)
	for i := range pgids {
		pgids[i] = common.Pgid(rand.Int63())
	}
	sort.Sort(pgids)
	return pgids
}

func Test_freelist_ReadIDs_and_getFreePageIDs(t *testing.T) {
	f := newTestFreelist()
	exp := common.Pgids([]common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18})

	f.Init(exp)

	if got := f.FreePageIds(); !reflect.DeepEqual(exp, got) {
		t.Fatalf("exp=%v; got=%v", exp, got)
	}

	f2 := newTestFreelist()
	var exp2 []common.Pgid
	f2.Init(exp2)

	if got2 := f2.FreePageIds(); !reflect.DeepEqual(got2, common.Pgids(exp2)) {
		t.Fatalf("exp2=%#v; got2=%#v", exp2, got2)
	}

}

// newTestFreelist get the freelist type from env and initial the freelist
func newTestFreelist() Freelist {
	if env := os.Getenv(TestFreelistType); env == "map" {
		return NewHashMap()
	}

	return NewArray()
}
