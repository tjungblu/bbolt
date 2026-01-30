package bbolt

import (
	"errors"
	"fmt"
	"os"

	berrors "go.etcd.io/bbolt/errors"
)

// Compact will create a copy of the source DB and in the destination DB. This may
// reclaim space that the source database no longer has use for. txMaxSize can be
// used to limit the transactions size of this process and may trigger intermittent
// commits. A value of zero will ignore transaction sizes.
// TODO: merge with: https://github.com/etcd-io/etcd/blob/b7f0f52a16dbf83f18ca1d803f7892d750366a94/mvcc/backend/backend.go#L349
func Compact(dst, src *DB, txMaxSize int64) error {
	return CompactWithChanges(dst, src, txMaxSize, nil)
}

// CompactWithChanges performs compaction similar to Compact, but also applies
// a list of changes that occurred during compaction. This allows compaction to
// proceed concurrently with write operations.
//
// The changes parameter is a list of ChangeOp operations that represent
// modifications made to the source database during compaction. These changes
// will be applied to the compacted result after the initial compaction is complete.
//
// If changes is nil or empty, this function behaves identically to Compact().
func CompactWithChanges(dst, src *DB, txMaxSize int64, changes []ChangeOp) error {
	// commit regularly, or we'll run out of memory for large datasets if using one transaction.
	var size int64
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	// Note: We don't use defer for tx because it gets reassigned during the loop.
	// We'll manually rollback if there's an error, or commit at the end.

	// Use compaction transaction for source database to allow concurrent writes.
	compactionTx, err := src.BeginCompaction()
	if err != nil {
		tx.Rollback()
		return err
	}
	defer compactionTx.Rollback()

	// Walk the source database using the compaction transaction.
	if err := walkWithTx(compactionTx, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		if size+sz > txMaxSize && txMaxSize != 0 {
			// Commit previous transaction.
			if err := tx.Commit(); err != nil {
				return err
			}

			// Start new transaction.
			tx, err = dst.Begin(true)
			if err != nil {
				return err
			}
			size = 0
		}
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if b == nil {
			// Bucket doesn't exist, create it.
			var err error
			b, err = tx.CreateBucket(keys[0])
			if err != nil {
				return err
			}
		}
		if nk > 1 {
			for _, k := range keys[1:] {
				child := b.Bucket(k)
				if child == nil {
					// Bucket doesn't exist, create it.
					var err error
					child, err = b.CreateBucket(k)
					if err != nil {
						return err
					}
				}
				b = child
			}
		}

		// Fill the entire page for best compaction.
		b.FillPercent = 1.0

		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Otherwise treat it as a key/value pair.
		return b.Put(k, v)
	}); err != nil {
		// Rollback transaction on error.
		tx.Rollback()
		return err
	}

	// Get any changes that were tracked during compaction.
	// Note: For now, we use the provided changes parameter. In the future,
	// we could automatically collect changes from write transactions.
	trackedChanges := compactionTx.GetChangeLog()
	if len(trackedChanges) > 0 {
		// Merge provided changes with tracked changes.
		allChanges := make([]ChangeOp, 0, len(changes)+len(trackedChanges))
		allChanges = append(allChanges, changes...)
		allChanges = append(allChanges, trackedChanges...)
		changes = allChanges
	}

	// Commit the destination transaction.
	if err := tx.Commit(); err != nil {
		return err
	}

	// Apply any changes that occurred during compaction.
	if len(changes) > 0 {
		if err := applyChanges(dst, changes); err != nil {
			return err
		}
	}

	return nil
}

// CompactAndSwap performs compaction and atomically swaps the source database
// with the compacted version using filesystem rename operations. This is an
// atomic operation at the filesystem level.
//
// The function:
// 1. Compacts the source database to a temporary file
// 2. Closes the source database
// 3. Briefly locks all transactions to prevent new ones
// 4. Atomically renames the temp file to replace the source (atomic operation)
// 5. Reopens the database with the compacted file
// 6. Unlocks transactions
//
// This approach is more efficient than locking all transactions during the
// entire compaction process, as it only locks briefly during the atomic rename.
//
// The source database must be writable. The function will preserve the original
// file mode and options when reopening.
//
// Returns the reopened database and any error that occurred.
// If an error occurs, the original database file is preserved.
func CompactAndSwap(src *DB, txMaxSize int64) (*DB, error) {
	if src.IsReadOnly() {
		return nil, errors.New("cannot swap read-only database")
	}

	lg := src.Logger()
	if lg != discardLogger {
		lg.Infof("Starting compaction and swap for database at %s", src.path)
	}

	// Get source database path and options for reopening
	// We must save the path BEFORE closing the database, as close() clears it
	srcPath := src.path
	if srcPath == "" {
		return nil, fmt.Errorf("database path is empty")
	}
	srcMode := os.FileMode(0600) // Default mode
	if info, err := os.Stat(srcPath); err == nil {
		srcMode = info.Mode()
	}

	// Save options for reopening
	options := &Options{
		NoSync:         src.NoSync,
		NoGrowSync:     src.NoGrowSync,
		MmapFlags:      src.MmapFlags,
		NoFreelistSync: src.NoFreelistSync,
		FreelistType:   src.FreelistType,
		Mlock:          src.Mlock,
		MaxSize:        src.MaxSize,
		PageSize:       src.pageSize,
		Logger:         src.logger,
	}

	// Create temporary file path for compaction
	tempPath := srcPath + ".tmp.compact"

	// Remove temp file if it exists
	if _, err := os.Stat(tempPath); err == nil {
		if err := os.Remove(tempPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing temp file: %w", err)
		}
	}

	// Open temporary database for compaction
	tempDB, err := Open(tempPath, srcMode, &Options{
		NoSync:         src.NoSync,
		NoGrowSync:     src.NoGrowSync,
		MmapFlags:      src.MmapFlags,
		NoFreelistSync: src.NoFreelistSync,
		FreelistType:   src.FreelistType,
		Mlock:          src.Mlock,
		MaxSize:        src.MaxSize,
		PageSize:       src.pageSize,
		Logger:         src.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open temp database: %w", err)
	}

	// Perform compaction to temp file
	if err := CompactWithChanges(tempDB, src, txMaxSize, nil); err != nil {
		tempDB.Close()
		os.Remove(tempPath)
		return nil, fmt.Errorf("compaction failed: %w", err)
	}

	// Close temp database - this will sync the file to disk
	if err := tempDB.Close(); err != nil {
		os.Remove(tempPath)
		return nil, fmt.Errorf("failed to close temp database: %w", err)
	}

	// At this point, CompactWithChanges has returned, which means its deferred
	// compaction transaction rollback should have executed. However, to be safe,
	// we lock all transactions to ensure no new ones can start and any remaining
	// cleanup can complete.
	// Lock all transactions on source database to prevent new ones during swap.
	// This must be done BEFORE closing the database.
	if lg != discardLogger {
		lg.Debugf("CompactAndSwap: about to LockAllTransactions")
	}
	src.LockAllTransactions()
	if lg != discardLogger {
		lg.Debugf("CompactAndSwap: LockAllTransactions done, closing source")
	}

	// Close source database while locked.
	// We use the internal close() method since we already hold all necessary locks.
	// This ensures no new transactions can start and existing ones are blocked.
	// Note: close() will unmount the mmap and unlock/close the file.
	closeErr := src.close()

	// On some systems (especially Windows), we need to ensure the file is fully
	// closed and unlocked before we can rename it. The close() above should handle
	// this, but we check for errors first.
	if closeErr != nil {
		src.UnlockAllTransactions()
		return nil, fmt.Errorf("failed to close source database: %w", closeErr)
	}

	// Atomically rename temp file to source (atomic filesystem operation)
	// This is the critical atomic step - the rename is atomic at the filesystem level
	// The source file should now be fully closed and unlocked, so rename should work.
	renameErr := os.Rename(tempPath, srcPath)

	// Unlock transactions (safe even after closing)
	src.UnlockAllTransactions()

	// Check for rename errors
	if renameErr != nil {
		// If rename failed, try to reopen original database
		_, _ = Open(srcPath, srcMode, nil)
		return nil, fmt.Errorf("failed to rename temp file to source: %w", renameErr)
	}

	// Reopen the database with the compacted file
	newDB, err := Open(srcPath, srcMode, options)
	if err != nil {
		return nil, fmt.Errorf("failed to reopen database after swap: %w", err)
	}

	if lg != discardLogger {
		lg.Infof("Successfully swapped and reopened compacted database at %s", srcPath)
	}

	return newDB, nil
}

// walkWithTx walks the database using a specific transaction.
func walkWithTx(tx *Tx, walkFn walkFunc) error {
	return tx.ForEach(func(name []byte, b *Bucket) error {
		return walkBucket(b, nil, name, nil, b.Sequence(), walkFn)
	})
}

// applyChanges applies a list of changes to the destination database.
func applyChanges(dst *DB, changes []ChangeOp) error {
	return dst.Update(func(tx *Tx) error {
		for _, change := range changes {
			// Navigate to the target bucket.
			b := &tx.root
			for _, bucketKey := range change.KeyPath {
				child := b.Bucket(bucketKey)
				if child == nil {
					// Bucket doesn't exist, create it.
					var err error
					child, err = b.CreateBucket(bucketKey)
					if err != nil {
						return err
					}
				}
				b = child
			}

			// Apply the change operation.
			switch change.OpType {
			case ChangeOpPut:
				if err := b.Put(change.Key, change.Value); err != nil {
					return err
				}

			case ChangeOpDelete:
				if err := b.Delete(change.Key); err != nil {
					return err
				}

			case ChangeOpCreateBucket:
				bkt, err := b.CreateBucket(change.Key)
				if err != nil {
					// Bucket might already exist, try to get it.
					bkt = b.Bucket(change.Key)
					if bkt == nil {
						return err
					}
				}
				if change.Sequence != 0 {
					if err := bkt.SetSequence(change.Sequence); err != nil {
						return err
					}
				}

			case ChangeOpDeleteBucket:
				if err := b.DeleteBucket(change.Key); err != nil {
					// Bucket might not exist, which is fine.
					// Check if it's a different error.
					if err != berrors.ErrBucketNotFound {
						return err
					}
				}

			default:
				return fmt.Errorf("unknown change operation type: %d", change.OpType)
			}
		}
		return nil
	})
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walk(db *DB, walkFn walkFunc) error {
	return db.View(func(tx *Tx) error {
		return tx.ForEach(func(name []byte, b *Bucket) error {
			return walkBucket(b, nil, name, nil, b.Sequence(), walkFn)
		})
	})
}

func walkBucket(b *Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc) error {
	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}

	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}

	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := b.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn)
		}
		return walkBucket(b, keypath, k, v, b.Sequence(), fn)
	})
}
