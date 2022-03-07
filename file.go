package mfsng

import (
	"context"
	"fmt"
	// "io"
	"io/fs"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-mfs"
)

var _ fs.File = (*File)(nil)

// var _ io.Seeker = (*File)(nil)

type File struct {
	file *mfs.File
	name string
	ctx  context.Context // an embedded context for cancellation and deadline propogation

	fd     mfs.FileDescriptor // fd is written once by fdOnce and read-only thereafter
	fdOnce sync.Once

	mu   sync.Mutex // mu guards access to all of following fields
	info *FileInfo
}

func (m *File) Stat() (fs.FileInfo, error) {
	m.mu.Lock()
	info := m.info
	m.mu.Unlock()

	if info != nil {
		return info, nil
	}

	info = &FileInfo{
		name: m.name,
	}

	var err error
	info.size, err = m.file.Size()
	if err != nil {
		return nil, fmt.Errorf("size: %w", err)
	}

	info.node, err = m.file.GetNode()
	if err != nil {
		return nil, fmt.Errorf("get node: %w", err)
	}

	m.mu.Lock()
	m.info = info
	m.mu.Unlock()
	return info, nil
}

func (m *File) Read(buf []byte) (int, error) {
	var err error
	m.fdOnce.Do(func() {
		fd, fdErr := m.file.Open(mfs.Flags{Read: true})
		if fdErr != nil {
			err = fmt.Errorf("open fd: %w", fdErr)
			return
		}
		m.fd = fd
	})
	if err != nil {
		return 0, err
	}

	if m.fd == nil {
		return 0, fmt.Errorf("file not open for reading")
	}
	return m.fd.CtxReadFull(m.ctx, buf)
}

// mfs.FileDescriptor.Seek is not compliant with Go spec
// func (m *File) Seek(offset int64, whence int) (int64, error) {
// 	var err error
// 	m.fdOnce.Do(func() {
// 		fd, fdErr := m.file.Open(mfs.Flags{Read: true})
// 		if fdErr != nil {
// 			err = fmt.Errorf("open fd: %w", fdErr)
// 			return
// 		}
// 		m.fd = fd
// 	})
// 	if err != nil {
// 		return 0, err
// 	}

// 	if m.fd == nil {
// 		return 0, fmt.Errorf("file not open for reading")
// 	}
// 	return m.fd.Seek(offset, whence)
// }

func (m *File) Close() error {
	// no-op while fs is readonly
	return nil
}

func (m *File) Name() string               { return m.name }
func (m *File) IsDir() bool                { return false }
func (m *File) Info() (fs.FileInfo, error) { return m.Stat() }
func (m *File) Type() fs.FileMode          { return fs.FileMode(0) }
func (m *File) Cid() cid.Cid               { n, _ := m.file.GetNode(); return n.Cid() }

var _ fs.FileInfo = (*FileInfo)(nil)

type FileInfo struct {
	name     string
	filemode fs.FileMode // just the file type bits
	size     int64
	modtime  time.Time
	node     ipld.Node
}

func (m *FileInfo) Name() string {
	return m.name
}

func (m *FileInfo) Size() int64 {
	return m.size
}

func (m *FileInfo) Mode() fs.FileMode {
	return m.filemode
}

func (m *FileInfo) ModTime() time.Time {
	return m.modtime
}

func (m *FileInfo) IsDir() bool {
	return m.filemode.IsDir()
}

func (m *FileInfo) Sys() interface{} {
	return m.node
}

func (m *FileInfo) Cid() cid.Cid {
	return m.node.Cid()
}
