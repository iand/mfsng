package mfsng

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	ipath "github.com/ipfs/go-path"
	"github.com/ipfs/go-unixfs"
	uio "github.com/ipfs/go-unixfs/io"
)

var (
	// Supported interfaces for FS
	_ fs.FS        = (*FS)(nil)
	_ fs.ReadDirFS = (*FS)(nil)
	_ fs.SubFS     = (*FS)(nil)
)

type FS struct {
	udir   uio.Directory
	getter ipld.NodeGetter
	ctx    context.Context // an embedded context for cancellation and deadline propogation, can be overridden by WithContext method
}

// ReadFS returns a read-only filesystem. It expects the supplied node to be the root of a UnixFS merkledag.
func ReadFS(node ipld.Node, getter ipld.NodeGetter) (*FS, error) {
	udir, err := uio.NewDirectoryFromNode(merkledag.NewReadOnlyDagService(getter), node)
	if err != nil {
		return nil, fmt.Errorf("new directory from node: %w", err)
	}

	return &FS{
		udir:   udir,
		getter: getter,
		ctx:    context.Background(),
	}, nil
}

// WithContext returns an FS using the supplied context
func (fsys *FS) WithContext(ctx context.Context) fs.FS {
	return &FS{
		udir:   fsys.udir,
		getter: fsys.getter,
		ctx:    ctx,
	}
}

func (fsys *FS) context() context.Context {
	if fsys.ctx == nil {
		return context.Background()
	}
	return fsys.ctx
}

func (fsys *FS) Open(path string) (fs.File, error) {
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}

	if path == "." {
		path = ""
	}
	node, nodeName, err := fsys.locateNode(path)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  err,
		}
	}

	switch tnode := node.(type) {
	case *merkledag.ProtoNode:
		fsn, err := unixfs.FSNodeFromBytes(tnode.Data())
		if err != nil {
			return nil, &fs.PathError{
				Op:   "open",
				Path: path,
				Err:  err,
			}
		}

		switch fsn.Type() {
		case unixfs.TDirectory, unixfs.THAMTShard:
			return newDir(fsys.context(), nodeName, tnode, fsys.getter)

		case unixfs.TFile:
			return newFile(fsys.context(), nodeName, tnode, fsys.getter)

		case unixfs.TRaw:
			// TODO
		case unixfs.TSymlink:
			// TODO
		}
	}

	return nil, &fs.PathError{
		Op:   "open",
		Path: path,
		Err:  fs.ErrInvalid,
	}
}

// Sub returns an FS corresponding to the subtree rooted at dir.
func (fsys *FS) Sub(path string) (fs.FS, error) {
	node, _, err := fsys.locateNode(path)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "sub",
			Path: path,
			Err:  err,
		}
	}

	udir, err := uio.NewDirectoryFromNode(merkledag.NewReadOnlyDagService(fsys.getter), node)
	if err != nil {
		if errors.Is(err, uio.ErrNotADir) {
			return nil, &fs.PathError{
				Op:   "sub",
				Path: path,
				Err:  fs.ErrInvalid,
			}
		}
		return nil, &fs.PathError{
			Op:   "sub",
			Path: path,
			Err:  fmt.Errorf("new directory from node: %w", err),
		}
	}

	return &FS{
		getter: fsys.getter,
		udir:   udir,
		ctx:    fsys.context(),
	}, nil
}

// ReadDir reads the named directory
// and returns a list of directory entries sorted by filename.
func (fsys *FS) ReadDir(path string) ([]fs.DirEntry, error) {
	if path == "." {
		path = ""
	}

	node, _, err := fsys.locateNode(path)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: path,
			Err:  err,
		}
	}

	udir, err := uio.NewDirectoryFromNode(merkledag.NewReadOnlyDagService(fsys.getter), node)
	if err != nil {
		if errors.Is(err, uio.ErrNotADir) {
			return nil, &fs.PathError{
				Op:   "readdir",
				Path: path,
				Err:  fs.ErrInvalid,
			}
		}
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: path,
			Err:  fmt.Errorf("new directory from node: %w", err),
		}
	}

	var names []string
	if err := udir.ForEachLink(fsys.context(), func(l *ipld.Link) error {
		names = append(names, l.Name)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list names: %w", err)
	}
	sort.Strings(names)

	entries := []fs.DirEntry{}
	for _, name := range names {
		entry, err := dirEntry(fsys.context(), fsys.getter, udir, name)
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

func (fsys *FS) locateNode(path string) (ipld.Node, string, error) {
	path = strings.Trim(path, "/")
	parts := ipath.SplitList(path)
	if len(parts) == 1 && parts[0] == "" {
		node, err := fsys.udir.GetNode()
		if err != nil {
			return nil, "", fmt.Errorf("get root node: %w", err)
		}
		return node, "", nil
	}

	var cur uio.Directory
	cur = fsys.udir
	for i, segment := range parts {
		childNode, err := cur.Find(fsys.context(), segment)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, ipld.ErrNotFound{}) {
				return nil, "", fs.ErrNotExist
			}
			return nil, "", fmt.Errorf("find: %w", err)
		}

		if i == len(parts)-1 {
			// Last segment of path
			return childNode, segment, nil
		}

		childDir, err := uio.NewDirectoryFromNode(merkledag.NewReadOnlyDagService(fsys.getter), childNode)
		if err != nil {
			if errors.Is(err, uio.ErrNotADir) {
				return nil, "", fs.ErrInvalid
			}
			return nil, "", fmt.Errorf("new directory from node: %w", err)
		}

		cur = childDir
	}
	return nil, "", fs.ErrInvalid
}

func dirEntry(ctx context.Context, getter ipld.NodeGetter, dir uio.Directory, name string) (fs.DirEntry, error) {
	node, err := dir.Find(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}

	switch tnode := node.(type) {
	case *merkledag.ProtoNode:
		fsn, err := unixfs.FSNodeFromBytes(tnode.Data())
		if err != nil {
			return nil, err
		}

		switch fsn.Type() {
		case unixfs.TDirectory, unixfs.THAMTShard:
			return newDir(ctx, name, node, getter)

		case unixfs.TFile:
			return newFile(ctx, name, node, getter)

		case unixfs.TRaw:
		case unixfs.TSymlink:
		default:
			return nil, fs.ErrInvalid
		}
	}

	return nil, fs.ErrInvalid
}
