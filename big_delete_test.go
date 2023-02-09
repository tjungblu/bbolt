package bbolt

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestDeleteBucketRunIO(t *testing.T) {
	db, err := Open("database.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.MmapFlags = syscall.MAP_POPULATE
	bucketName := []byte("testbucket_0")

	timedPut(t, db, bucketName)

	err = db.Update(func(tx *Tx) error {
		return tx.DeleteBucket(bucketName)
	})
	if err != nil {
		t.Fatal(err)
	}

	otherBucketName := []byte("testbucket_2")
	timedPut(t, db, otherBucketName)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func timedPut(t *testing.T, db *DB, bucketName []byte) {
	start := time.Now()
	err := db.Update(func(tx *Tx) error {
		return tx.Bucket(bucketName).Put([]byte{1, 2, 3}, []byte{})
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("PUT took %s\n", time.Now().Sub(start).String())
}

func TestCreateAndFill(t *testing.T) {
	keygen := make(chan [32]byte, 100000)

	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			rnd := rand.New(rand.NewSource(rand.Int63()))
			for {
				var key [32]byte
				rnd.Read(key[:])
				keygen <- key
			}
		}()
	}

	db, err := Open("database.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.MmapFlags = syscall.MAP_POPULATE
	for b := 0; b < 5; b++ {
		bucketName := []byte(fmt.Sprintf("testbucket_%d", b))
		for i := 0; i < 500; i++ {
			fmt.Printf("[%s] writing batch %d/500 with 100k keys...\n", bucketName, i)
			err = db.Batch(func(tx *Tx) error {
				b, err := tx.CreateBucketIfNotExists(bucketName)
				if err != nil {
					return err
				}

				for i := 0; i < 100000; i++ {
					key := <-keygen
					var val [4]byte
					binary.BigEndian.PutUint32(val[:], uint32(i))
					err = b.Put(key[:], val[:])
					if err != nil {
						return err
					}
				}

				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	close(keygen)

}
