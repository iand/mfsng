package mfsng

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	uio "github.com/ipfs/go-unixfs/io"
)

var (
	_ fs.File     = (*File)(nil)
	_ io.Seeker   = (*File)(nil)
	_ io.WriterTo = (*File)(nil)
)

type File struct {
	dr   uio.DagReader
	ctx  context.Context // an embedded context for cancellation and deadline propogation
	info FileInfo
}

func newFile(ctx context.Context, name string, node ipld.Node, getter ipld.NodeGetter) (*File, error) {
	dr, err := uio.NewDagReader(ctx, node, getter)
	if err != nil {
		return nil, fmt.Errorf("new dag reader: %w", err)
	}

	return &File{
		dr:  dr,
		ctx: ctx,
		info: FileInfo{
			name:     name,
			size:     int64(dr.Size()),
			filemode: dr.FileMode() & os.ModeType,
			modtime:  dr.ModTime(),
			node:     node,
		},
	}, nil
}

// Stat returns a FileInfo describing the file.
func (f *File) Stat() (fs.FileInfo, error) {
	return &f.info, nil
}

func (f *File) Read(buf []byte) (int, error) {
	return f.dr.CtxReadFull(f.ctx, buf)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.dr.Seek(offset, whence)
}

func (f *File) WriteTo(w io.Writer) (int64, error) {
	return f.dr.WriteTo(w)
}

func (f *File) Close() error {
	return f.dr.Close()
}

func (f *File) Name() string               { return f.info.name }
func (f *File) IsDir() bool                { return false }
func (f *File) Info() (fs.FileInfo, error) { return f.Stat() }
func (f *File) Type() fs.FileMode          { return fs.FileMode(0) }
func (f *File) Cid() cid.Cid               { return f.info.node.Cid() }

var _ fs.FileInfo = (*FileInfo)(nil)

type FileInfo struct {
	name     string
	filemode fs.FileMode // just the file type bits
	size     int64
	modtime  time.Time
	node     ipld.Node
}

// Name returns the base name of the file or directory.
func (f *FileInfo) Name() string {
	return f.name
}

// Size returns the length in bytes of a file or the size in bytes of the underlying node for a directory.
func (f *FileInfo) Size() int64 {
	return f.size
}

// Mode returns the file mode bits of the file or directory.
func (f *FileInfo) Mode() fs.FileMode {
	return f.filemode
}

// Mode returns the modification time of the file if known or the zero time otherwise.
func (f *FileInfo) ModTime() time.Time {
	return f.modtime
}

// IsDir reports whether the info describes a directory.
func (f *FileInfo) IsDir() bool {
	return f.filemode.IsDir()
}

// Sys returns the underlying root node of the file or directory.
func (f *FileInfo) Sys() interface{} {
	return f.node
}

// Cid returns the CID of the file or directory's root node.
func (f *FileInfo) Cid() cid.Cid {
	return f.node.Cid()
}
