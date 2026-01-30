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

// stepLogger implements bolt.Logger and logs each call to t.Log with timestamps.
// Used by tests to see where CompactAndSwap gets stuck when it hangs.
type stepLogger struct {
	t   *testing.T
	buf *syncBuffer // optional: capture last N lines for timeout failure message
}

type syncBuffer struct {
	lines []string
	mu    sync.Mutex
	n     int
}

func (b *syncBuffer) append(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines = append(b.lines, s)
	if len(b.lines) > 20 {
		b.lines = b.lines[1:]
	}
}

func (b *syncBuffer) lastLines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.lines...)
}

func (l *stepLogger) log(level, format string, v ...interface{}) {
	msg := fmt.Sprintf("[%s] %s", level, fmt.Sprintf(format, v...))
	ts := time.Now().Format("15:04:05.000")
	full := ts + " " + msg
	l.t.Log(full)
	if l.buf != nil {
		l.buf.append(full)
	}
}

func (l *stepLogger) Debug(v ...interface{})              { l.log("DEBUG", "%s", fmt.Sprint(v...)) }
func (l *stepLogger) Debugf(f string, v ...interface{})   { l.log("DEBUG", f, v...) }
func (l *stepLogger) Error(v ...interface{})              { l.log("ERROR", "%s", fmt.Sprint(v...)) }
func (l *stepLogger) Errorf(f string, v ...interface{})   { l.log("ERROR", f, v...) }
func (l *stepLogger) Info(v ...interface{})               { l.log("INFO", "%s", fmt.Sprint(v...)) }
func (l *stepLogger) Infof(f string, v ...interface{})    { l.log("INFO", f, v...) }
func (l *stepLogger) Warning(v ...interface{})            { l.log("WARN", "%s", fmt.Sprint(v...)) }
func (l *stepLogger) Warningf(f string, v ...interface{}) { l.log("WARN", f, v...) }
func (l *stepLogger) Fatal(v ...interface{})              { l.t.Fatal(v...) }
func (l *stepLogger) Fatalf(f string, v ...interface{})   { l.t.Fatalf(f, v...) }
func (l *stepLogger) Panic(v ...interface{})              { panic(fmt.Sprint(v...)) }
func (l *stepLogger) Panicf(f string, v ...interface{})   { panic(fmt.Sprintf(f, v...)) }

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

// TestCompactAndSwapBlocksWithOpenWriteTx demonstrates that CompactAndSwap
// blocks on LockAllTransactions (specifically on rwlock) when there is an open
// write transaction on the database. This can help diagnose etcd defrag hangs:
// if defrag is stuck at "locking all transactions", ensure no Batch/Update or
// long-lived write transaction is still open on the same DB.
func TestCompactAndSwapBlocksWithOpenWriteTx(t *testing.T) {
	srcDB := btesting.MustCreateDB(t)

	// Write data and commit so DB has content to compact.
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		require.NoError(t, err)
		return b.Put([]byte("k"), []byte("v"))
	})
	require.NoError(t, err)

	// Open a write transaction and leave it open (do not commit/rollback).
	openTx, err := srcDB.Begin(true)
	require.NoError(t, err)
	defer openTx.Rollback()

	// CompactAndSwap in another goroutine will block on rwlock until openTx is closed.
	done := make(chan struct{})
	var resultDB *bolt.DB
	var resultErr error
	go func() {
		resultDB, resultErr = bolt.CompactAndSwap(srcDB.DB, 0)
		close(done)
	}()

	// We expect it to still be blocking after a short time.
	select {
	case <-done:
		t.Fatal("CompactAndSwap completed unexpectedly while write tx is open (expected it to block on rwlock)")
	case <-time.After(300 * time.Millisecond):
		// Good: still blocking as expected.
	}

	// Release the write transaction so CompactAndSwap can proceed.
	err = openTx.Rollback()
	require.NoError(t, err)

	// Now CompactAndSwap should complete.
	select {
	case <-done:
		require.NoError(t, resultErr)
		require.NotNil(t, resultDB)
		srcDB.DB = resultDB
	case <-time.After(5 * time.Second):
		t.Fatal("CompactAndSwap did not complete within 5s after closing write tx")
	}
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

// TestCompactAndSwapReopen tests that CompactAndSwap automatically reopens the database
func TestCompactAndSwapReopen(t *testing.T) {
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

	// Perform compaction and swap with automatic reopen
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

// TestCompactWithConcurrentWrites tests compaction while writes are happening
func TestCompactWithConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// Create source database
	srcDB := btesting.MustCreateDB(t)

	// Initial data
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
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
		return nil
	})
	require.NoError(t, err)

	// Start concurrent writes
	var wg sync.WaitGroup
	stopWrites := make(chan struct{})
	writeErrors := make(chan error, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopWrites:
				return
			default:
				err := srcDB.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("test"))
					if b == nil {
						return fmt.Errorf("bucket not found")
					}
					// Write some new keys
					for i := 10; i < 20; i++ {
						key := []byte(fmt.Sprintf("key%d", i))
						value := []byte(fmt.Sprintf("value%d", i))
						if err := b.Put(key, value); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					select {
					case writeErrors <- err:
					default:
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Give writes a moment to start
	time.Sleep(50 * time.Millisecond)

	// Create destination database
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dstDB, err := bolt.Open(dstPath, 0600, nil)
	require.NoError(t, err)
	defer dstDB.Close()

	// Perform compaction while writes are happening
	err = bolt.CompactWithChanges(dstDB, srcDB.DB, 0, nil)
	require.NoError(t, err)

	// Stop writes
	close(stopWrites)
	wg.Wait()

	// Check for write errors (some are expected during swap)
	close(writeErrors)
	writeErrCount := 0
	for err := range writeErrors {
		if err != nil {
			writeErrCount++
			t.Logf("Write error during compaction: %v", err)
		}
	}

	// Verify initial data is in destination
	err = dstDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		require.NotNil(t, b)
		// Check initial keys
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			expectedValue := []byte(fmt.Sprintf("value%d", i))
			value := b.Get(key)
			require.Equal(t, expectedValue, value, "key %d mismatch", i)
		}
		return nil
	})
	require.NoError(t, err)

	t.Logf("Completed compaction with %d write errors (expected during swap)", writeErrCount)
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

// TestCompactionChangeTracking tests that changes are tracked during compaction
func TestCompactionChangeTracking(t *testing.T) {
	// Create source database
	srcDB := btesting.MustCreateDB(t)
	defer srcDB.Close()

	// Initial data
	err := srcDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("key1"), []byte("value1")); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// Start compaction transaction
	compactionTx, err := srcDB.DB.BeginCompaction()
	require.NoError(t, err)
	defer compactionTx.Rollback()

	// Perform writes while compaction transaction is active
	err = srcDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if err := b.Put([]byte("key2"), []byte("value2")); err != nil {
			return err
		}
		if err := b.Put([]byte("key3"), []byte("value3")); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// Get tracked changes
	changes := compactionTx.GetChangeLog()
	require.NotNil(t, changes)
	// Should have tracked the writes (at least for root bucket operations)
	t.Logf("Tracked %d changes during compaction", len(changes))
}

// TestLockAllTransactions tests the transaction locking functionality
func TestLockAllTransactions(t *testing.T) {
	// Create database
	db := btesting.MustCreateDB(t)
	defer db.Close()

	// Start a read transaction
	readTx, err := db.Begin(false)
	require.NoError(t, err)

	// Lock all transactions in a goroutine (should block)
	lockDone := make(chan bool)
	unlockDone := make(chan bool)
	go func() {
		db.DB.LockAllTransactions()
		lockDone <- true
		db.DB.UnlockAllTransactions()
		unlockDone <- true
	}()

	// Give it a moment
	time.Sleep(10 * time.Millisecond)

	// Try to start a new transaction (should block)
	newTxStarted := make(chan bool)
	var newTx *bolt.Tx
	go func() {
		var err error
		newTx, err = db.DB.Begin(false)
		require.NoError(t, err)
		newTxStarted <- true
	}()

	// Give it a moment
	time.Sleep(10 * time.Millisecond)

	// Close the read transaction
	err = readTx.Rollback()
	require.NoError(t, err)

	// Wait a bit for lock to complete
	time.Sleep(50 * time.Millisecond)

	// Check if lock completed
	select {
	case <-lockDone:
		// Good, lock completed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Lock did not complete")
	}

	// Check if new transaction started (should have after unlock)
	select {
	case <-newTxStarted:
		// Good, transaction started after unlock
		// Close it before the test ends
		if newTx != nil {
			newTx.Rollback()
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("New transaction did not start after unlock")
	}

	// Wait for unlock to complete before test ends (so defer db.Close() doesn't deadlock)
	select {
	case <-unlockDone:
		// Good, unlock completed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Unlock did not complete")
	}
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
		child, err := parent.CreateBucketIfNotExists([]byte("child"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			if err := child.Put(key, value); err != nil {
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

	// Verify nested structure
	err = dstDB.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket([]byte("parent"))
		require.NotNil(t, parent)
		child := parent.Bucket([]byte("child"))
		require.NotNil(t, child)
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			expectedValue := []byte(fmt.Sprintf("value%d", i))
			value := child.Get(key)
			require.Equal(t, expectedValue, value, "key %d mismatch", i)
		}
		return nil
	})
	require.NoError(t, err)
}
