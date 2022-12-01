//go:build !windows && !plan9 && !solaris && !aix
// +build !windows,!plan9,!solaris,!aix

package bbolt

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// pwritev writes the given pages using vectorized io. The pages must be sorted by their id indicating their sequence.
// The logic identifies runs of consecutive pages that are written with vectorized io using the pwritev syscall.
func pwritev(db *DB, pages []*page) error {
	if len(pages) == 0 {
		return nil
	}

	offsets, iovecs := pagesToIovec2(db, pages)
	for i := 0; i < len(offsets); i++ {
		if _, err := unix.Pwritev(int(db.file.Fd()), iovecs[i], offsets[i]); err != nil {
			return err
		}
	}
	return nil
}

func pagesToIovec2(db *DB, pages []*page) ([]int64, [][][]byte) {
	var offsets []int64
	var iovecs [][][]byte

	// TODO: read from this from sysconf(_SC_IOV_MAX)?
	// linux and darwin default is 1024
	const maxVec = 1024

	lastPid := pages[0].id - 1
	begin := 0
	var curVecs [][]byte
	for i := 0; i < len(pages); i++ {
		p := pages[i]
		if p.id != (lastPid+1) || len(curVecs) >= maxVec {
			offsets = append(offsets, int64(pages[begin].id)*int64(db.pageSize))
			iovecs = append(iovecs, curVecs)

			begin = i
			curVecs = [][]byte{}
		}
		curVecs = append(curVecs, p.bytes(uint64(db.pageSize)))
		lastPid = p.id + pgid(p.overflow)
	}

	if len(curVecs) > 0 {
		offsets = append(offsets, int64(pages[begin].id)*int64(db.pageSize))
		iovecs = append(iovecs, curVecs)
	}
	return offsets, iovecs
}

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB, exclusive bool, timeout time.Duration) error {
	var t time.Time
	if timeout != 0 {
		t = time.Now()
	}
	fd := db.file.Fd()
	flag := syscall.LOCK_NB
	if exclusive {
		flag |= syscall.LOCK_EX
	} else {
		flag |= syscall.LOCK_SH
	}
	for {
		// Attempt to obtain an exclusive lock.
		err := syscall.Flock(int(fd), flag)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		// If we timed out then return an error.
		if timeout != 0 && time.Since(t) > timeout-flockRetryTimeout {
			return ErrTimeout
		}

		// Wait for a bit and try again.
		time.Sleep(flockRetryTimeout)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

// mmap memory maps a DB's data file.
func mmap(db *DB, sz int) error {
	// Map the data file to memory.
	b, err := unix.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	// Advise the kernel that the mmap is accessed randomly.
	err = unix.Madvise(b, syscall.MADV_RANDOM)
	if err != nil && err != syscall.ENOSYS {
		// Ignore not implemented error in kernel because it still works.
		return fmt.Errorf("madvise: %s", err)
	}

	// Save the original byte slice and convert to a byte array pointer.
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

// munmap unmaps a DB's data file from memory.
func munmap(db *DB) error {
	// Ignore the unmap if we have no mapped data.
	if db.dataref == nil {
		return nil
	}

	// Unmap using the original byte slice.
	err := unix.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}
