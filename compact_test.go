package bbolt_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// TestCompactWithChanges tests basic compaction functionality
func TestCompactWithChanges(t *testing.T) {
	// Create source database with some data
	srcDB := btesting.MustCreateDB(t)
	defer func() {
		require.NoError(t, srcDB.Close())
	}()
	// Fill source database
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 10000; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Create destination database
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dstDB, err := bolt.Open(dstPath, 0600, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dstDB.Close())
	}()
	// Perform compaction
	err = bolt.CompactWithChanges(dstDB, srcDB.DB, 0, nil)
	require.NoError(t, err)

	// Verify data in destination
	err = dstDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		require.NotNil(t, b)
		for i := 0; i < 10000; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			expectedValue := []byte(fmt.Sprintf("value%d", i))
			value := b.Get(key)
			require.Equal(t, expectedValue, value, "key %d mismatch", i)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompactAndSwap tests the atomic swap functionality
func TestCompactAndSwap(t *testing.T) {
	// Create source database with some data
	srcDB := btesting.MustCreateDB(t)

	// Fill source database
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 5000; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Perform compaction and swap
	newDB, err := bolt.CompactAndSwap(srcDB.DB, 0)
	require.NoError(t, err)
	// ensure that the cleanup takes care of the swapped database
	srcDB.DB = newDB

	// Verify data is still there after swap
	err = newDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		require.NotNil(t, b)
		for i := 0; i < 5000; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			expectedValue := []byte(fmt.Sprintf("value%d", i))
			value := b.Get(key)
			require.Equal(t, expectedValue, value, "key %d mismatch", i)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompactionTransaction tests the compaction transaction functionality
func TestCompactionTransaction(t *testing.T) {
	// Create source database
	srcDB := btesting.MustCreateDB(t)
	defer func() {
		require.NoError(t, srcDB.Close())
	}()

	// Fill with data
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 2000; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Start compaction transaction
	compactionTx, err := srcDB.DB.BeginCompaction()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compactionTx.Rollback())
	}()

	// Verify we can read from compaction transaction
	err = compactionTx.ForEach(func(name []byte, b *bolt.Bucket) error {
		require.Equal(t, []byte("test"), name)
		count := 0
		return b.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
	})
	require.NoError(t, err)

	// Verify it's a compaction transaction
	require.True(t, compactionTx.IsCompaction())
	require.False(t, compactionTx.Writable())

	// Get change log (should be empty initially)
	changes := compactionTx.GetChangeLog()
	require.NotNil(t, changes)
	require.Equal(t, 0, len(changes))
}

// TestCompactMultipleBuckets tests compaction with multiple buckets
func TestCompactMultipleBuckets(t *testing.T) {
	// Create source database
	srcDB := btesting.MustCreateDB(t)
	defer func() {
		require.NoError(t, srcDB.Close())
	}()

	// Create multiple buckets with data
	err := srcDB.Update(func(tx *bolt.Tx) error {
		for bucketNum := 0; bucketNum < 5; bucketNum++ {
			bucketName := []byte(fmt.Sprintf("bucket%d", bucketNum))
			b, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}
			for i := 0; i < 1000; i++ {
				key := []byte(fmt.Sprintf("key%d", i))
				value := []byte(fmt.Sprintf("value%d", i))
				if err := b.Put(key, value); err != nil {
					return err
				}
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Create destination database
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dstDB, err := bolt.Open(dstPath, 0600, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dstDB.Close())
	}()
	// Perform compaction
	err = bolt.CompactWithChanges(dstDB, srcDB.DB, 0, nil)
	require.NoError(t, err)

	// Verify all buckets and data
	err = dstDB.View(func(tx *bolt.Tx) error {
		for bucketNum := 0; bucketNum < 5; bucketNum++ {
			bucketName := []byte(fmt.Sprintf("bucket%d", bucketNum))
			b := tx.Bucket(bucketName)
			require.NotNil(t, b, "bucket %d should exist", bucketNum)
			for i := 0; i < 1000; i++ {
				key := []byte(fmt.Sprintf("key%d", i))
				expectedValue := []byte(fmt.Sprintf("value%d", i))
				value := b.Get(key)
				require.Equal(t, expectedValue, value, "bucket %d, key %d mismatch", bucketNum, i)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompactNestedBuckets tests compaction with nested buckets
func TestCompactNestedBuckets(t *testing.T) {
	// Create source database
	srcDB := btesting.MustCreateDB(t)
	defer func() {
		require.NoError(t, srcDB.Close())
	}()

	// Create nested buckets
	err := srcDB.Update(func(tx *bolt.Tx) error {
		parent, err := tx.CreateBucketIfNotExists([]byte("parent"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			child, err := parent.CreateBucketIfNotExists([]byte(fmt.Sprintf("child-%d", i)))
			if err != nil {
				return err
			}
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			if err := child.Put(key, value); err != nil {
				return err
			}
			parent = child
		}
		return nil
	})
	require.NoError(t, err)

	// Create destination database
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dstDB, err := bolt.Open(dstPath, 0600, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dstDB.Close())
	}()

	// Perform compaction
	err = bolt.CompactWithChanges(dstDB, srcDB.DB, 0, nil)
	require.NoError(t, err)

	// Verify nested structure
	err = dstDB.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket([]byte("parent"))
		require.NotNil(t, parent)
		for i := 0; i < 10; i++ {
			child := parent.Bucket([]byte(fmt.Sprintf("child-%d", i)))
			require.NotNil(t, child)
			key := []byte(fmt.Sprintf("key%d", i))
			expectedValue := []byte(fmt.Sprintf("value%d", i))
			value := child.Get(key)
			require.Equal(t, expectedValue, value, "key %d mismatch", i)
			parent = child
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompactConcurrentWrites runs writers under distinct prefixes while compaction
// is in progress, then stops writers gracefully and asserts the source DB contains
// every prefix and all integers written up to the stop signal.
func TestCompactConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	const numWriters = 5
	const runDuration = 5 * time.Second
	const throttle = 1 * time.Millisecond

	srcDB := btesting.MustCreateDB(t)
	defer func() {
		require.NoError(t, srcDB.Close())
	}()

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dstDB, err := bolt.Open(dstPath, 0600, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dstDB.Close())
	}()

	// Ensure "data" bucket exists so writers can use it
	err = srcDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("data"))
		return err
	})
	require.NoError(t, err)

	stopCh := make(chan struct{})
	writerCounts := make([]int, numWriters)
	var writerCountsMu sync.Mutex
	var wg sync.WaitGroup

	for id := 0; id < numWriters; id++ {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			prefix := fmt.Sprintf("writer%d", id)
			count := 0
			for {
				select {
				case <-stopCh:
					writerCountsMu.Lock()
					writerCounts[id] = count
					writerCountsMu.Unlock()
					return
				default:
				}
				err := srcDB.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("data"))
					key := []byte(fmt.Sprintf("%s-%d", prefix, count))
					value := []byte(fmt.Sprintf("%d", count))
					return b.Put(key, value)
				})
				if err != nil {
					writerCountsMu.Lock()
					writerCounts[id] = count
					writerCountsMu.Unlock()
					return
				}
				count++
				time.Sleep(throttle)
			}
		}()
	}

	// Start compaction after a short delay so some writes happen first
	time.Sleep(30 * time.Millisecond)
	compactionDone := make(chan struct{})
	var compactionErr error
	go func() {
		compactionErr = bolt.CompactWithChanges(dstDB, srcDB.DB, 0, nil)
		close(compactionDone)
	}()

	// Let writers and compaction run together
	time.Sleep(runDuration)
	close(stopCh)
	wg.Wait()
	<-compactionDone
	require.NoError(t, compactionErr)

	// Assert source DB has each writer's prefix and all integers 0..count-1
	err = srcDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		require.NotNil(t, b)
		for id := 0; id < numWriters; id++ {
			prefix := fmt.Sprintf("writer%d", id)
			expectedCount := writerCounts[id]
			for i := 0; i < expectedCount; i++ {
				key := []byte(fmt.Sprintf("%s-%d", prefix, i))
				value := b.Get(key)
				expectedValue := []byte(fmt.Sprintf("%d", i))
				require.Equal(t, expectedValue, value, "prefix %s key %d", prefix, i)
			}
		}
		return nil
	})
	require.NoError(t, err)
}
