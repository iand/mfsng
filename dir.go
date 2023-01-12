package mfsng

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sync"

	// ipld "github.com/ipfs/go-ipld-format"
	prime "github.com/ipld/go-ipld-prime"
)

var _ fs.ReadDirFile = (*Dir)(nil)

type Dir struct {
	node prime.Node
	lsys *prime.LinkSystem
	ctx  context.Context // an embedded context for cancellation and deadline propogation
	info FileInfo

	namesOnce sync.Once
	names     []string // names is written once by namesOnce and read-only thereafter

	mu     sync.Mutex // guards access to all of following fields
	offset int        // number of entries read by prior calls to ReadDir
}

func newDir(ctx context.Context, name string, node prime.Node, lsys *prime.LinkSystem) (*Dir, error) {
	// TODO: size
	// size, err := node.Size()
	// if err != nil {
	// 	return nil, err
	// }

	return &Dir{
		node: node,
		lsys: lsys,
		ctx:  ctx,
		info: FileInfo{
			name: name,
			// size:     int64(size),
			filemode: fs.ModeDir,
			node:     node,
		},
	}, nil
}

// Stat returns a FileInfo describing the directory.
func (d *Dir) Stat() (fs.FileInfo, error) {
	return &d.info, nil
}

func (d *Dir) Name() string               { return d.info.name }
func (d *Dir) IsDir() bool                { return true }
func (d *Dir) Info() (fs.FileInfo, error) { return d.Stat() }
func (d *Dir) Type() fs.FileMode          { return fs.ModeDir }

func (d *Dir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.info.name, Err: fs.ErrInvalid}
}

func (d *Dir) Close() error {
	// no-op while fs is readonly
	return nil
}

// ReadDir reads the contents of the directory and returns a slice of up to n DirEntry values in directory order.
// Subsequent calls on the same file will yield further DirEntry values.
// If n > 0, ReadDir returns at most n DirEntry structures.
// In this case, if ReadDir returns an empty slice, it will return
// a non-nil error explaining why.
// At the end of a directory, the error is io.EOF.
//
// If n <= 0, ReadDir returns all the DirEntry values from the directory
// in a single slice. In this case, if ReadDir succeeds (reads all the way
// to the end of the directory), it returns the slice and a nil error.
// If it encounters an error before the end of the directory,
// ReadDir returns the DirEntry list read until that point and a non-nil error.
func (d *Dir) ReadDir(limit int) ([]fs.DirEntry, error) {
	// Read the names once
	var err error
	d.namesOnce.Do(func() {
		names, listErr := listNames(d.node)
		if listErr != nil {
			err = fmt.Errorf("list names: %w", listErr)
			return
		}
		d.names = names
		d.offset = 0
	})
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	offset := d.offset
	d.mu.Unlock()

	n := len(d.names) - offset
	if n == 0 && limit > 0 {
		return nil, io.EOF
	}
	if limit > 0 && n > limit {
		n = limit
	}

	entries := make([]fs.DirEntry, n)
	for i := range entries {
		name := d.names[offset+i]

		entry, err := dirEntry(d.ctx, d.node, d.lsys, name)
		if err != nil {
			d.mu.Lock()
			d.offset += i
			d.mu.Unlock()

			return entries, &fs.PathError{
				Op:   "readdir",
				Path: name,
				Err:  err,
			}
		}

		entries[i] = entry
	}

	d.mu.Lock()
	d.offset += n
	d.mu.Unlock()
	return entries, nil
}

var _ fs.ReadDirFile = (*DirPrime)(nil)

type DirPrime struct {
	info FileInfo
}

func (d *DirPrime) ReadDir(limit int) ([]fs.DirEntry, error) {
	panic("ReadDir: not implemented")
}

func (d *DirPrime) Close() error {
	panic("Close: not implemented")
}

func (d *DirPrime) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.info.name, Err: fs.ErrInvalid}
}

// Stat returns a FileInfo describing the directory.
func (d *DirPrime) Stat() (fs.FileInfo, error) {
	return &d.info, nil
}
