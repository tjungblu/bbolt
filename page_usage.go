package bbolt

import (
	"github.com/bits-and-blooms/bitset"
)

func PageUsageOverview(tx *Tx) *bitset.BitSet {
	bset := bitset.New(uint(tx.meta.Pgid()))
	var id int
	for {
		p, err := tx.Page(id)
		if err != nil {
			panic(err)
		} else if p == nil {
			break
		}

		if p.Type == "free" {
			bset.Set(uint(p.ID))
		} else {
			id += p.OverflowCount
		}

		id += 1
	}

	return bset
}
