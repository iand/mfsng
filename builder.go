package mfsng

import (
	"context"
	"fmt"
	"strings"

	ipld "github.com/ipfs/go-ipld-format"
	// ufs "github.com/ipfs/go-unixfs"
	uio "github.com/ipfs/go-unixfs/io"
	// "github.com/ipfs/go-merkledag"
)

type Builder struct {
	root fsnode
	ds   ipld.DAGService
}

// TODO: create a builder based on an existing Read FS

func NewBuilder(ds ipld.DAGService) *Builder {
	return &Builder{
		ds: ds,
	}
}

// MkdirAll creates a directory named path, along with any necessary parents.
func (b *Builder) MkdirAll(path string) error {
	parent := &b.root

	for name, remainder, _ := Cut(path, "/"); name != ""; name, remainder, _ = Cut(remainder, "/") {
		parent = parent.findOrAddChild(name)
	}

	return nil
}

// WriteFileNode writes the file represented by node to the path. If the file does not exist, WriteFileNode creates it.
func (b *Builder) WriteFileNode(path string, node ipld.Node) error {
	parent := &b.root

	name, remainder, isdir := Cut(path, "/")
	for ; isdir; name, remainder, isdir = Cut(remainder, "/") {
		parent = parent.findOrAddChild(name)
	}

	fnode := &fsnode{name: name, node: node}
	parent.addChild(fnode)

	return nil
}

func (b *Builder) Flush() error {
	n, err := buildNode(&b.root, b.ds)
	if err != nil {
		return err
	}
	b.root.node = n
	return nil
}

// ReadFS returns a read-only filesystem that incorporates all changes made by the builder.
func (b *Builder) ReadFS() (*FS, error) {
	if err := b.Flush(); err != nil {
		return nil, err
	}

	return ReadFS(b.root.node, b.ds)
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

type fsnode struct {
	name  string
	node  ipld.Node
	child *fsnode
	next  *fsnode
}

func (p *fsnode) addChild(c *fsnode) {
	if p.child == nil {
		p.child = c
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

func (p *fsnode) rm(c *fsnode) {
	if p.child == nil || c == nil {
		return
	}

	if p.child == c {
		p.child = p.child.next
		return
	}

	var n *fsnode
	for n = p.child; n != nil; n = n.next {
		if n.next == c {
			n.next = n.next.next
			return
		}
	}
}

func dump(n *fsnode) {
	dumpindent(n, 0)
}

func dumpindent(n *fsnode, indent int) {
	if n.node == nil {
		fmt.Println(strings.Repeat("  ", indent) + n.name)
	} else {
		fmt.Println(strings.Repeat("  ", indent) + n.name + " " + n.node.Cid().String())
	}
	if n.child != nil {
		dumpindent(n.child, indent+1)
	}

	if n.next != nil {
		dumpindent(n.next, indent)
	}
}

func walkdf(n *fsnode, fn func(n *fsnode) error) error {
	if n.node == nil && n.child != nil {
		walkdf(n.child, fn)
	}
	if err := fn(n); err != nil {
		return err
	}
	if n.next != nil {
		walkdf(n.next, fn)
	}
	return nil
}

func buildNode(n *fsnode, ds ipld.DAGService) (ipld.Node, error) {
	if n.node != nil {
		return n.node, nil
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

	n.node = nd
	if err := ds.Add(context.TODO(), n.node); err != nil {
		return nil, fmt.Errorf("add node to dag service: %w", err)
	}
	return n.node, err
}
