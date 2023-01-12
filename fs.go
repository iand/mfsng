package mfsng

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-fetcher"
	ipld "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	// "github.com/ipfs/go-merkledag"
	ipath "github.com/ipfs/go-path"
	// "github.com/ipfs/go-unixfs"
	// uio "github.com/ipfs/go-unixfs/io"
	// "github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	prime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	// basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/directory"
	"github.com/ipfs/go-unixfsnode/hamt"
	"github.com/ipld/go-ipld-prime/schema"

	_ "github.com/ipld/go-codec-dagpb"
)

var (
	// Supported interfaces for FS
	_ fs.FS        = (*FS)(nil)
	_ fs.ReadDirFS = (*FS)(nil)
	_ fs.SubFS     = (*FS)(nil)
)

type FS struct {
	udir    *ufsdir
	getter  ipld.NodeGetter
	fetcher fetcher.Fetcher
	ls      *prime.LinkSystem
	ctx     context.Context // an embedded context for cancellation and deadline propogation, can be overridden by WithContext method
}

// ReadFS returns a read-only filesystem. It expects the supplied node to be the root of a UnixFS merkledag.
func ReadFS(node prime.Node, lsys *prime.LinkSystem) (*FS, error) {
	udir, err := ReifyDir(linking.LinkContext{}, node, lsys)
	if err != nil {
		return nil, fmt.Errorf("new directory from node: %w", err)
	}

	return &FS{
		udir: udir,
		ls:   lsys,
		ctx:  context.Background(),
	}, nil
}

// WithContext returns an FS using the supplied context
func (fsys *FS) WithContext(ctx context.Context) *FS {
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
	node, name, err := fsys.locateNode(path)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  err,
		}
	}

	switch node.Kind() {
	case prime.Kind_Map:
		return newDir(fsys.context(), name, node, fsys.ls)
	case prime.Kind_Bytes:
		return newFile(fsys.context(), name, node, fsys.ls)
	default:
		return nil, &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  fs.ErrInvalid,
		}
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

	if node.Kind() != prime.Kind_Map {
		return nil, &fs.PathError{
			Op:   "sub",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}

	return &FS{
		ls:   fsys.ls,
		udir: node,
		ctx:  fsys.context(),
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

	if node.Kind() != prime.Kind_Map {
		return nil, &fs.PathError{
			Op:   "readdir",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}

	// TODO: get Links

	names, err := listNames(node)
	if err != nil {
		return nil, fmt.Errorf("list names: %w", err)
	}
	sort.Strings(names)

	entries := []fs.DirEntry{}
	for _, name := range names {
		fmt.Printf("found name: %s\n", name)
		entry, err := dirEntry(fsys.context(), node, fsys.ls, name)
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

func (fsys *FS) locateNode(path string) (prime.Node, string, error) {
	path = strings.Trim(path, "/")
	parts := ipath.SplitList(path)
	if len(parts) == 1 && parts[0] == "" {
		return fsys.udir, "", nil
	}

	var cur prime.Node
	cur = fsys.udir
	for i, segment := range parts {
		fmt.Printf("current node kind: %v\n", cur.Kind())
		fmt.Printf("segment: %s\n", segment)
		childLink, err := getChild(cur, segment)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, "", fs.ErrNotExist
			}
			return nil, "", fmt.Errorf("find: %w [%T]", err, err) // TODO: remove %T
		}
		fmt.Printf("childNode kind: %v\n", childLink.Kind())

		if childLink.Kind() != prime.Kind_Link {
			return nil, "", fs.ErrInvalid
		}
		cl, err := childLink.AsLink()
		if err != nil {
			return nil, "", fmt.Errorf("load child link: %w", nil)
		}

		childNode, err := fsys.ls.Load(prime.LinkContext{Ctx: fsys.context()}, cl, dagpb.Type.PBNode)
		if err != nil {
			return nil, "", fmt.Errorf("load child: %w", nil)
		}

		if i == len(parts)-1 {
			fmt.Printf("returning last segment\n")
			// Last segment of path
			return childNode, segment, nil
		}

		if childNode.Kind() != prime.Kind_Map {
			return nil, "", fs.ErrInvalid
		}

		cur = childNode
	}
	return nil, "", fs.ErrInvalid
}

func dirEntry(ctx context.Context, dir prime.Node, lsys *prime.LinkSystem, name string) (fs.DirEntry, error) {
	childNode, err := getChild(dir, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("find: %w (%T)", err, err) // TODO: remove %T
	}

	switch childNode.Kind() {
	case prime.Kind_Map:
		return newDir(ctx, name, childNode, lsys)

	case prime.Kind_Bytes:
		fmt.Printf("newFile: %s\n", name)
		return newFile(ctx, name, childNode, lsys)
	default:
		fmt.Printf("childNode kind: %v\n", childNode.Kind())
		return nil, fs.ErrInvalid
	}
}

// linkSystemGetter adapts a link system to the old ipld.NodeGetter interface
type linkSystemGetter struct {
	ls prime.LinkSystem
}

var _ ipld.NodeGetter = (*linkSystemGetter)(nil)

func (l *linkSystemGetter) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	nd, data, err := l.ls.LoadPlusRaw(prime.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, dagpb.Type.PBNode)
	if err != nil {
		return nil, err
	}

	_, ok := nd.(dagpb.PBNode)
	if !ok {
		fmt.Printf("loaded node is not a PBNode, it's a %T\n", nd)
	}

	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, err
	}

	return &legacy.LegacyNode{Block: blk, Node: nd}, nil
}

func (l *linkSystemGetter) GetMany(ctx context.Context, cs []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption)
	go func() {
		defer close(ch)
		for _, c := range cs {
			select {
			case <-ctx.Done():
				return
			default:
			}

			no := &ipld.NodeOption{}
			no.Node, no.Err = l.Get(ctx, c)
			ch <- no
		}
	}()

	return ch
}

func listNames(node prime.Node) ([]string, error) {
	names := make([]string, 0, node.Length())
	it := node.MapIterator()
	for !it.Done() {
		k, _, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("list names: %w", err)
		}
		name, err := k.AsString()
		if err != nil {
			return nil, fmt.Errorf("list names, name: %w", err)
		}
		names = append(names, name)
	}

	return names, nil
}

func getChild(node prime.Node, name string) (prime.Node, error) {
	childNode, err := node.LookupByString(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, ipld.ErrNotFound{}) {
			return nil, fs.ErrNotExist
		}

		var ensf schema.ErrNoSuchField
		if errors.As(err, &ensf) {
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("find: %w [%T]", err, err) // TODO: remove %T
	}

	return childNode, nil
}

// Reify looks at an ipld Node and tries to interpret it as a UnixFSNode
// if successful, it returns the UnixFSNode
func ReifyDir(lnkCtx prime.LinkContext, maybePBNodeRoot prime.Node, lsys *prime.LinkSystem) (*ufsdir, error) {
	pbNode, ok := maybePBNodeRoot.(dagpb.PBNode)
	if !ok {
		return nil, fmt.Errorf("node is not a dagpb")
	}
	if !pbNode.FieldData().Exists() {
		return nil, fmt.Errorf("node is does not contain dagpb data")
	}
	ufsdata, err := data.DecodeUnixFSData(pbNode.Data.Must().Bytes())
	if err != nil {
		return nil, fmt.Errorf("new directory from node: %w", err)
	}

	switch ufsdata.FieldDataType().Int() {
	case data.Data_Directory:
		nd, err := directory.NewUnixFSBasicDir(context.TODO(), pbNode, ufsdata, lsys)
		if err != nil {
			return nil, fmt.Errorf("NewUnixFSBasicDir: %w", err)
		}
		return &ufsdir{nd: nd}, nil
	case data.Data_HAMTShard:
		nd, err := hamt.NewUnixFSHAMTShard(context.TODO(), pbNode, ufsdata, lsys)
		if err != nil {
			return nil, fmt.Errorf("NewUnixFSHAMTShard: %w", err)
		}
		return &ufsdir{nd: nd}, nil
	}

	return nil, fmt.Errorf("node is not a dagpb")
}

type ufsdir struct {
	nd prime.Node
}

type ufsfile struct {
	nd prime.Node
}

type ufsentry struct {
	data data.UnixFSData
}

func (u *ufsentry) IsDir() bool {
	return u.data.FieldDataType().Int() == data.Data_Directory || u.data.FieldDataType().Int() == data.Data_HAMTShard
}

// var reifyFuncs = map[int64]reifyTypeFunc{
// 	data.Data_File:      unixFSFileReifier,
// 	data.Data_Metadata:  defaultUnixFSReifier,
// 	data.Data_Raw:       unixFSFileReifier,
// 	data.Data_Symlink:   defaultUnixFSReifier,
// 	data.Data_Directory: directory.NewUnixFSBasicDir,
// 	data.Data_HAMTShard: hamt.NewUnixFSHAMTShard,
// }
