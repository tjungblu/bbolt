package bbolt_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// TestCompactWithChanges tests basic compaction functionality
func TestCompactWithChanges(t *testing.T) {
	// Create source database with some data
	srcDB := btesting.MustCreateDB(t)
	defer srcDB.Close()

	// Fill source database
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
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
	defer dstDB.Close()

	// Perform compaction
	err = bolt.CompactWithChanges(dstDB, srcDB.DB, 0, nil)
	require.NoError(t, err)

	// Verify data in destination
	err = dstDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		require.NotNil(t, b)
		for i := 0; i < 100; i++ {
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
		for i := 0; i < 50; i++ {
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
		for i := 0; i < 50; i++ {
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
	defer srcDB.Close()

	// Fill with data
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 20; i++ {
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
	defer compactionTx.Rollback()

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
	defer srcDB.Close()

	// Create multiple buckets with data
	err := srcDB.Update(func(tx *bolt.Tx) error {
		for bucketNum := 0; bucketNum < 5; bucketNum++ {
			bucketName := []byte(fmt.Sprintf("bucket%d", bucketNum))
			b, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}
			for i := 0; i < 10; i++ {
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
	defer dstDB.Close()

	// Perform compaction
	err = bolt.CompactWithChanges(dstDB, srcDB.DB, 0, nil)
	require.NoError(t, err)

	// Verify all buckets and data
	err = dstDB.View(func(tx *bolt.Tx) error {
		for bucketNum := 0; bucketNum < 5; bucketNum++ {
			bucketName := []byte(fmt.Sprintf("bucket%d", bucketNum))
			b := tx.Bucket(bucketName)
			require.NotNil(t, b, "bucket %d should exist", bucketNum)
			for i := 0; i < 10; i++ {
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
	defer srcDB.Close()

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
	defer dstDB.Close()

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
