package mfsng

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	uio "github.com/ipfs/go-unixfs/io"
)

// A Builder builds a unixfs. It is not safe for concurrent use.
type Builder struct {
	root fsnode
	node ipld.Node // cached version of the root node
	ds   ipld.DAGService
	ctx  context.Context // an embedded context for cancellation and deadline propogation, can be overridden by WithContext method
}

func NewBuilder(ds ipld.DAGService) *Builder {
	return &Builder{
		ds: ds,
	}
}

func (b *Builder) WithRootNode(n ipld.Node) *Builder {
	return &Builder{
		root: fsnode{
			cid: n.Cid(),
		},
		node: n,
		ds:   b.ds,
	}
}

// WithContext returns a Builder using the supplied context
func (b *Builder) WithContext(ctx context.Context) *Builder {
	return &Builder{
		root: b.root,
		node: b.node,
		ds:   b.ds,
		ctx:  ctx,
	}
}

func (b *Builder) context() context.Context {
	if b.ctx == nil {
		return context.Background()
	}
	return b.ctx
}

// MkdirAll creates a directory named path, along with any necessary parents.
func (b *Builder) MkdirAll(path string) error {
	parent := &b.root
	ctx := b.context()

	for name, remainder, _ := Cut(path, "/"); name != ""; name, remainder, _ = Cut(remainder, "/") {
		if err := parent.unpack(ctx, b.ds); err != nil {
			return err
		}
		parent = parent.findOrAddChild(name)
	}

	return nil
}

// WriteFileNode writes the file represented by node to the path. If the path does not exist, WriteFileNode creates it.
func (b *Builder) WriteFileNode(path string, node ipld.Node) error {
	parent := &b.root
	ctx := b.context()

	name, remainder, isdir := Cut(path, "/")
	for ; isdir; name, remainder, isdir = Cut(remainder, "/") {
		if err := parent.unpack(ctx, b.ds); err != nil {
			return fmt.Errorf("unpack: %w", err)
		}
		parent = parent.findOrAddChild(name)
	}

	if err := parent.unpack(ctx, b.ds); err != nil {
		return fmt.Errorf("unpack: %w", err)
	}
	fnode := &fsnode{name: name, cid: node.Cid()}
	parent.addChild(fnode)

	return nil
}

func (b *Builder) Flush() error {
	n, err := buildNode(&b.root, b.ds)
	if err != nil {
		return err
	}
	b.root.cid = n.Cid()
	b.node = n
	return nil
}

// ReadFS returns a read-only filesystem that incorporates all changes made by the builder.
func (b *Builder) ReadFS() (*FS, error) {
	if err := b.Flush(); err != nil {
		return nil, err
	}

	return ReadFS(b.node, b.ds)
}

// Cut slices s around the first instance of sep,
// returning the text before and after sep.
// The found result reports whether sep appears in s.
// If sep does not appear in s, cut returns s, "", false.
// This was introduced as strings.Cut in Go 1.18
func Cut(s, sep string) (before, after string, found bool) {
	if i := strings.Index(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return s, "", false
}

// fsnode is a node in an ephemeral filesystem
type fsnode struct {
	// name is the name of the node which, in ipfs, is the name that appears in
	// the link of the parent node
	name string

	// cid of the node that represents this fsnode. Must be set to cid.Undef when fsnode is mutated.
	cid cid.Cid

	// child is the first child of the fsnode. Must be nil when cid is defined
	child *fsnode

	// child is the next peer of the fsnode
	next *fsnode
}

// unpack populates the children of this fsnode using the reified node
func (p *fsnode) unpack(ctx context.Context, getter ipld.NodeGetter) error {
	if p.cid == cid.Undef {
		return nil
	}

	var links []*ipld.Link
	if lg, ok := getter.(ipld.LinkGetter); ok {
		// ignore errors and fall through to getting the node
		links, _ = lg.GetLinks(ctx, p.cid)
	}

	if links == nil {
		nd, err := getter.Get(ctx, p.cid)
		if err != nil {
			return fmt.Errorf("get node: %w", err)
		}
		links = nd.Links()
	}

	var c *fsnode
	for _, lnk := range links {
		n := &fsnode{
			name: lnk.Name,
			cid:  lnk.Cid,
		}

		if c == nil {
			p.child = n
		} else {
			c.next = n
		}
		c = n
	}

	p.cid = cid.Undef

	return nil
}

func (p *fsnode) addChild(c *fsnode) {
	if p.child == nil {
		p.child = c
		p.cid = cid.Undef // mark parent as mutated
		return
	}

	var n *fsnode
	for n = p.child; n.next != nil; n = n.next {
	}

	n.next = c
}

func (p *fsnode) findOrAddChild(name string) *fsnode {
	if p.child == nil {
		p.child = &fsnode{name: name}
		return p.child
	}

	n := p.child
	for {
		if n.name == name {
			return n
		}

		if n.next == nil {
			n.next = &fsnode{name: name}
			return n.next
		}

		n = n.next
	}
}

func buildNode(n *fsnode, ds ipld.DAGService) (ipld.Node, error) {
	if n.cid != cid.Undef {
		nd, err := ds.Get(context.TODO(), n.cid)
		if err != nil {
			return nil, fmt.Errorf("get node: %w", err)
		}
		return nd, nil
	}

	dir := uio.NewDirectory(ds)

	var c *fsnode
	for c = n.child; c != nil; c = c.next {
		cn, err := buildNode(c, ds)
		if err != nil {
			return nil, err
		}
		dir.AddChild(context.TODO(), c.name, cn)
	}

	nd, err := dir.GetNode()
	if err != nil {
		return nil, err
	}

	n.cid = nd.Cid()
	n.child = nil // preserve invariant that child must be nil when node is not
	if err := ds.Add(context.TODO(), nd); err != nil {
		return nil, fmt.Errorf("add node to dag service: %w", err)
	}
	return nd, err
}
