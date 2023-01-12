package mfsng

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	// "github.com/ipfs/go-cid"
	// ipld "github.com/ipfs/go-ipld-format"
	// uio "github.com/ipfs/go-unixfs/io"
	gufdata "github.com/ipfs/go-unixfsnode/data"
	ufsnfile "github.com/ipfs/go-unixfsnode/file"
	dagpb "github.com/ipld/go-codec-dagpb"
	prime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

var (
	_ fs.File     = (*File)(nil)
	_ io.Seeker   = (*File)(nil)
	_ io.WriterTo = (*File)(nil)
)

type ReadSeekWriterTo interface {
	io.Reader
	io.Seeker
	io.WriterTo
}

type File struct {
	rs   ReadSeekWriterTo
	ctx  context.Context // an embedded context for cancellation and deadline propogation
	info FileInfo
}

func newFile(ctx context.Context, name string, node prime.Node, lsys *prime.LinkSystem) (*File, error) {
	f := &File{
		ctx: ctx,
		info: FileInfo{
			name: name,
			// size:     int64(dr.Size()),
			// filemode: dr.FileMode() & os.ModeType,
			// modtime:  dr.ModTime(),
			node: node,
		},
	}
	if lnode, ok := node.(datamodel.LargeBytesNode); ok {
		rs, err := lnode.AsLargeBytes()
		if err != nil {
			return nil, err
		}
		f.rs = &rsWriterToAdapter{rs}
	} else {
		data, err := node.AsBytes()
		if err != nil {
			return nil, err
		}
		f.rs = bytes.NewReader(data)
		f.info.size = int64(len(data))
	}

	return f, nil
}

// Stat returns a FileInfo describing the file.
func (f *File) Stat() (fs.FileInfo, error) {
	return &f.info, nil
}

func (f *File) Read(buf []byte) (int, error) {
	return f.rs.Read(buf)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.rs.Seek(offset, whence)
}

func (f *File) WriteTo(w io.Writer) (int64, error) {
	return f.rs.WriteTo(w)
}

func (f *File) Close() error {
	return nil
}

func (f *File) Name() string               { return f.info.name }
func (f *File) IsDir() bool                { return false }
func (f *File) Info() (fs.FileInfo, error) { return f.Stat() }
func (f *File) Type() fs.FileMode          { return fs.FileMode(0) }

// TODO: File.Cid
// func (f *File) Cid() cid.Cid               { return f.info.node.Cid() }
var _ fs.FileInfo = (*FileInfo)(nil)

type FileInfo struct {
	name     string
	filemode fs.FileMode // just the file type bits
	size     int64
	modtime  time.Time
	node     prime.Node
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
// TODO: FileInfo.Cid
// func (f *FileInfo) Cid() cid.Cid {
// 	return f.node.Cid()
// }

var _ fs.File = (*FilePrime)(nil)

// _ io.Seeker   = (*File)(nil)
// _ io.WriterTo = (*File)(nil)

type FilePrime struct {
	ls   *prime.LinkSystem
	ctx  context.Context // an embedded context for cancellation and deadline propogation
	node prime.Node
	info FileInfo
	data gufdata.UnixFSData
	uf   ufsnfile.LargeBytesNode
	rs   io.ReadSeeker
}

func newFilePrime(ctx context.Context, name string, root prime.Node, ls *prime.LinkSystem) (*FilePrime, error) {
	pbroot, ok := root.(dagpb.PBNode)
	if !ok {
		return nil, fmt.Errorf("root node was not a dagpb.PBNode")
	}
	if !pbroot.Data.Exists() {
		return nil, fmt.Errorf("no data in root node")
	}
	data, err := gufdata.DecodeUnixFSData(pbroot.Data.Must().Bytes())
	if err != nil {
		return nil, fmt.Errorf("decode UnixFS data: %w", err)
	}

	return &FilePrime{
		ls:   ls,
		ctx:  ctx,
		node: root,
		data: data,
		info: FileInfo{
			name: name,
		},
	}, nil
}

func (f *FilePrime) initUnixFSFile() error {
	var err error
	f.uf, err = ufsnfile.NewUnixFSFile(f.ctx, f.node, f.ls)
	if err != nil {
		return fmt.Errorf("NewUnixFSFile: %w", err)
	}
	if f.data.FileSize.Exists() {
		f.info.size = int64(f.data.FileSize.Must().Int())
	}
	if f.data.Mode.Exists() {
		f.info.filemode = fs.FileMode(f.data.Mode.Must().Int()) & os.ModeType
	}
	if f.data.Mtime.Exists() {
		f.info.modtime = interpretUnixTime(f.data.Mtime.Must())
	}
	return nil
}

func (f *FilePrime) Stat() (fs.FileInfo, error) {
	if f.uf == nil {
		if err := f.initUnixFSFile(); err != nil {
			return nil, err
		}
	}
	return &f.info, nil
}

func (f *FilePrime) Read(buf []byte) (int, error) {
	if f.uf == nil {
		if err := f.initUnixFSFile(); err != nil {
			return 0, err
		}
	}
	if f.rs == nil {
		var err error
		f.rs, err = f.uf.AsLargeBytes()
		if err != nil {
			return 0, fmt.Errorf("AsLargeBytes: %w", err)
		}
	}
	return f.rs.Read(buf)
}

func (f *FilePrime) Close() error {
	if rsc, ok := f.rs.(io.Closer); ok {
		return rsc.Close()
	}
	return nil
}

func interpretUnixTime(ut gufdata.UnixTime) time.Time {
	secs := int64(ut.Seconds.Int())
	nsecs := int64(0)
	if ut.FractionalNanoseconds.Exists() {
		fns := ut.FractionalNanoseconds.Must().Int()
		if fns < 1 || fns > 999999999 {
			// Invalid time, return an unspecified time
			return time.Time{}
		}
		nsecs = fns
	}

	return time.Unix(secs, nsecs)
}

type rsWriterToAdapter struct {
	io.ReadSeeker
}

func (r *rsWriterToAdapter) WriteTo(w io.Writer) (n int64, err error) {
	return io.Copy(w, r)
}
