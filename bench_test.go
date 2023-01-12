package mfsng

import (
	// "context"
	"fmt"
	"io"
	"math"
	"testing"

	chunker "github.com/ipfs/go-ipfs-chunker"
	iutil "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	mdtest "github.com/ipfs/go-merkledag/test"
	importer "github.com/ipfs/go-unixfs/importer"
)

type fsIterator interface {
	Next() bool
	Entry() *fsEntry
	Reset()
	Len() int
}

type fsEntry struct {
	path string
	name string
	node ipld.Node
}

type staticFS struct {
	files []*fsEntry
	idx   int // 1 based
}

func newStaticFS(files map[string]ipld.Node) *staticFS {
	s := &staticFS{
		files: make([]*fsEntry, 0, len(files)),
	}
	for pth, nd := range files {
		s.files = append(s.files, &fsEntry{
			path: pth,
			name: "afile",
			node: nd,
		})
	}
	return s
}

func generateStaticFS(tb testing.TB, ds ipld.DAGService, depth int, fanout int, filecount int) *staticFS {
	s := &staticFS{
		files: make([]*fsEntry, 0, int(math.Pow(float64(fanout), float64(depth)))*filecount),
	}

	for i := 0; i < fanout; i++ {
		populateStaticFSRec(tb, ds, s, fmt.Sprintf("%d", i), depth-1, fanout, filecount)
	}
	return s
}

func populateStaticFSRec(tb testing.TB, ds ipld.DAGService, s *staticFS, pth string, depth int, fanout int, filecount int) {
	if depth > 0 {
		for i := 0; i < fanout; i++ {
			populateStaticFSRec(tb, ds, s, fmt.Sprintf("%s/%d", pth, i), depth-1, fanout, filecount)
		}
		return
	}

	for k := 0; k < filecount; k++ {
		s.files = append(s.files, &fsEntry{
			path: pth,
			name: fmt.Sprintf("file%d", k),
			node: getRandFile(tb, ds, 512),
		})
	}
}

func (s *staticFS) Next() bool {
	if s.idx >= len(s.files) {
		return false
	}
	s.idx++
	return true
}

func (s *staticFS) Reset() {
	s.idx = 0
}

func (s *staticFS) Entry() *fsEntry {
	return s.files[s.idx-1]
}

func (s *staticFS) Len() int {
	return len(s.files)
}

func TestAddFileTree(t *testing.T) {
	// t.Skip()
	ds := mdtest.Mock()

	// fs := generateStaticFS(t, ds, 4, 6, 2)
	fs := generateStaticFS(t, ds, 1, 2, 2)

	b := NewBuilder(ds)
	b.root.name = "TestAddFileTree"
	expected := map[string][]namecid{}

	fs.Reset()
	for fs.Next() {
		e := fs.Entry()
		expected[e.path] = append(expected[e.path], namecid{Name: e.name, Cid: e.node.Cid()})

		if err := b.WriteFileNode(e.path+"/"+e.name, e.node); err != nil {
			t.Fatalf("failed to add child %s/%q: %v", e.path, e.name, err)
		}
	}

	fsys, err := b.ReadFS()
	if err != nil {
		t.Fatalf("failed to get read fs: %v", err)
	}

	assertFSStructure(t, fsys, expected)
}

func BenchmarkAddFileTree(b *testing.B) {
	b.Run("onefile", func(b *testing.B) {
		ds := mdtest.Mock()
		fs := newStaticFS(map[string]ipld.Node{
			"": getRandFile(b, ds, 1000),
		})
		benchAddFileTree(b, fs, ds)
	})

	b.Run("onefilepath", func(b *testing.B) {
		ds := mdtest.Mock()
		fs := newStaticFS(map[string]ipld.Node{
			"a/b/c/d": getRandFile(b, ds, 1000),
		})
		benchAddFileTree(b, fs, ds)
	})

	b.Run("onefiledeeppath", func(b *testing.B) {
		ds := mdtest.Mock()
		fi := getRandFile(b, ds, 1000)

		fs := newStaticFS(map[string]ipld.Node{
			"a/b/c/d/e/f/g/h/i/j/k/l/m/n": fi,
		})
		benchAddFileTree(b, fs, ds)
	})

	b.Run("deeptree", func(b *testing.B) {
		ds := mdtest.Mock()
		// depth 14, fanout 2
		// 2^14 * 2 = 32768 files
		fs := generateStaticFS(b, ds, 14, 2, 2)
		benchAddFileTree(b, fs, ds)
	})
	b.Run("widetree", func(b *testing.B) {
		ds := mdtest.Mock()
		// depth 3, fanout 16
		// 16^3 * 8 = 32768 files
		fs := generateStaticFS(b, ds, 3, 16, 8)
		benchAddFileTree(b, fs, ds)
	})
	b.Run("superwidetree", func(b *testing.B) {
		ds := mdtest.Mock()
		// depth 2, fanout 128
		// 128^2 * 2 = 32768 files
		fs := generateStaticFS(b, ds, 2, 128, 2)
		benchAddFileTree(b, fs, ds)
	})
	b.Run("flatheavy", func(b *testing.B) {
		ds := mdtest.Mock()
		// depth 1, fanout 16, 2048 files per leaf dir
		// 16^1 * 2048 = 32768 files
		fs := generateStaticFS(b, ds, 1, 16, 2048)
		benchAddFileTree(b, fs, ds)
	})
	b.Run("deepheavy", func(b *testing.B) {
		ds := mdtest.Mock()
		// depth 4, fanout 2, 2048 files per leaf dir
		// 2^4 * 2048 = 32768 files
		fs := generateStaticFS(b, ds, 4, 2, 2048)
		benchAddFileTree(b, fs, ds)
	})
}

func benchAddFileTree(b *testing.B, fs fsIterator, ds ipld.DAGService) {
	b.Helper()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bdr := NewBuilder(ds)
		fs.Reset()
		for fs.Next() {
			e := fs.Entry()
			if err := bdr.WriteFileNode(e.path+"/"+e.name, e.node); err != nil {
				b.Fatalf("failed to add child %s/%q: %v", e.path, e.name, err)
			}
		}
		if err := bdr.Flush(); err != nil {
			b.Fatalf("failed to flush builder: %v", err)
		}
	}
}

func getRandFile(t testing.TB, ds ipld.DAGService, size int64) ipld.Node {
	r := io.LimitReader(iutil.NewTimeSeededRand(), size)
	nd, err := importer.BuildDagFromReader(ds, chunker.DefaultSplitter(r))
	if err != nil {
		t.Fatal(err)
	}
	return nd
}
