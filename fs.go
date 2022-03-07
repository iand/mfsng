package mfsng

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-mfs"
	path "github.com/ipfs/go-path"
)

var (
	// Supported interfaces for FS
	_ fs.FS        = (*FS)(nil)
	_ fs.ReadDirFS = (*FS)(nil)
	_ fs.SubFS     = (*FS)(nil)
)

type FS struct {
	root *mfs.Directory
	ctx  context.Context // an embedded context for cancellation and deadline propogation, can be overridden by WithContext method
}

// ReadFS returns a read-only filesystem. It expects the supplied node to be the root of a UnixFS merkledag.
func ReadFS(node ipld.Node, getter ipld.NodeGetter) (*FS, error) {
	pbnode, ok := node.(*merkledag.ProtoNode)
	if !ok {
		return nil, fmt.Errorf("invalid node type")
	}

	root, err := mfs.NewRoot(context.Background(), roDagServ{getter}, pbnode, nil)
	if err != nil {
		return nil, fmt.Errorf("new root: %w", err)
	}

	return &FS{
		root: root.GetDirectory(),
	}, nil
}

// FromDir returns a new FS using the supplied mfs.Dirirectory as a root.
// Deprecated: don't rely on this function, use ReadFS if possible
func FromDir(dir *mfs.Directory) *FS {
	return &FS{
		root: dir,
	}
}

// WithContext returns an FS using the supplied context
func (fsys *FS) WithContext(ctx context.Context) fs.FS {
	return &FS{
		root: fsys.root,
		ctx:  ctx,
	}
}

func (fsys *FS) context() context.Context {
	if fsys.ctx == nil {
		return context.Background()
	}
	return fsys.ctx
}

func (fsys *FS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	if name == "." {
		name = ""
	}
	fsn, err := fsys.DirLookup(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, ipld.ErrNotFound) {
			return nil, &fs.PathError{
				Op:   "open",
				Path: name,
				Err:  fs.ErrNotExist,
			}
		}
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  err,
		}
	}

	baseName := filepath.Base(name)

	switch fsnt := fsn.(type) {
	case *mfs.Directory:
		return &Dir{
			dir:  fsnt,
			name: baseName,
			ctx:  fsys.context(),
		}, nil
	case *mfs.File:
		return &File{
			file: fsnt,
			name: baseName,
			ctx:  fsys.context(),
		}, nil
	default:
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}
}

// Sub returns an FS corresponding to the subtree rooted at dir.
func (fsys *FS) Sub(dir string) (fs.FS, error) {
	fsn, err := fsys.DirLookup(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, ipld.ErrNotFound) {
			return nil, &fs.PathError{
				Op:   "sub",
				Path: dir,
				Err:  fs.ErrNotExist,
			}
		}
		return nil, &fs.PathError{
			Op:   "sub",
			Path: dir,
			Err:  err,
		}
	}

	switch fsnt := fsn.(type) {
	case *mfs.Directory:
		return &FS{
			root: fsnt,
			ctx:  fsys.context(),
		}, nil
	default:
		return nil, &fs.PathError{
			Op:   "sub",
			Path: dir,
			Err:  fs.ErrInvalid,
		}
	}
}

// ReadDir reads the named directory
// and returns a list of directory entries sorted by filename.
func (fsys *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == "." {
		name = ""
	}

	fsn, err := fsys.DirLookup(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, ipld.ErrNotFound) {
			return nil, &fs.PathError{
				Op:   "readdir",
				Path: name,
				Err:  fs.ErrNotExist,
			}
		}
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  err,
		}
	}

	dir, ok := fsn.(*mfs.Directory)
	if !ok {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	names, err := dir.ListNames(fsys.ctx)
	if err != nil {
		return nil, fmt.Errorf("list names: %w", err)
	}
	sort.Strings(names)

	entries := []fs.DirEntry{}
	for _, name := range names {
		entry, err := dirEntry(fsys.ctx, dir, name)
		if err != nil {
			return entries, &fs.PathError{
				Op:   "readdir",
				Path: name,
				Err:  err,
			}
		}
		entries = append(entries, entry)

	}

	return entries, nil
}

// DirLookup will look up a file or directory at the given path
// under the directory 'd'
func (fsys *FS) DirLookup(pth string) (mfs.FSNode, error) {
	pth = strings.Trim(pth, "/")
	parts := path.SplitList(pth)
	if len(parts) == 1 && parts[0] == "" {
		return fsys.root, nil
	}

	var cur mfs.FSNode
	cur = fsys.root
	for i, p := range parts {
		chdir, ok := cur.(*mfs.Directory)
		if !ok {
			return nil, fmt.Errorf("cannot access %s: Not a directory", path.Join(parts[:i+1]))
		}

		child, err := chdir.Child(p)
		if err != nil {
			return nil, err
		}

		cur = child
	}
	return cur, nil
}

func dirEntry(ctx context.Context, dir *mfs.Directory, name string) (fs.DirEntry, error) {
	fsn, err := dir.Child(name)
	if err != nil {
		return nil, fmt.Errorf("child: %w", err)
	}

	switch fsnt := fsn.(type) {
	case *mfs.Directory:
		return &Dir{
			dir:  fsnt,
			name: name,
			ctx:  ctx,
		}, nil
	case *mfs.File:
		return &File{
			file: fsnt,
			name: name,
			ctx:  ctx,
		}, nil
	default:
		return nil, fs.ErrInvalid
	}
}

var errReadOnly = errors.New("dag service is read only")

type roDagServ struct {
	ipld.NodeGetter
}

func (roDagServ) Add(context.Context, ipld.Node) error {
	// TODO: Add ought to return errReadOnly but mfs' implementation of GetNode calls Add unconditionally
	return nil
}

func (roDagServ) AddMany(context.Context, []ipld.Node) error {
	return errReadOnly
}

func (roDagServ) Remove(context.Context, cid.Cid) error {
	return errReadOnly
}

func (roDagServ) RemoveMany(context.Context, []cid.Cid) error {
	return errReadOnly
}
