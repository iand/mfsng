package mfsng

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	ipld "github.com/ipfs/go-ipld-format"
	mdtest "github.com/ipfs/go-merkledag/test"
	ufs "github.com/ipfs/go-unixfs"
	uio "github.com/ipfs/go-unixfs/io"
	utest "github.com/ipfs/go-unixfs/test"
	dagpb "github.com/ipld/go-codec-dagpb"
	prime "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestFS(t *testing.T) {
	fsys := buildFS(t, mdtest.Mock(), map[string][]byte{
		"hello.txt":          []byte("hello1"),
		"a/b/c/d/e/f/g/file": []byte("file content"),
		"a/b/c/d/e/f/g/h":    nil, // empty dir
	})

	if err := fstest.TestFS(fsys, "a/b/c/d/e/f/g/file"); err != nil {
		t.Fatal(err)
	}
}

func TestFSEmpty(t *testing.T) {
	fsys := buildFS(t, mdtest.Mock(), nil)

	if err := fstest.TestFS(fsys); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFile(t *testing.T) {
	expectedData := []byte("afile content")

	fsys := buildFS(t, mdtest.Mock(), map[string][]byte{
		"a/b/c/d/e/f/g/afile": expectedData,
	})

	f, err := fsys.Open("a/b/c/d/e/f/g/afile")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	if info.IsDir() {
		t.Errorf("got IsDir=true, wanted false")
	}

	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	if !bytes.Equal(data, expectedData) {
		t.Errorf("got data %v, wanted %v", data, expectedData)
	}
}

func TestOpenDir(t *testing.T) {
	fsys := buildFS(t, mdtest.Mock(), map[string][]byte{
		"a/b/c/d/e/f/g/afile": []byte("afile content"),
	})

	d, err := fsys.Open("a/b/c/d/e/f/g")
	if err != nil {
		t.Fatalf("failed to open directory: %v", err)
	}
	defer d.Close()

	info, err := d.Stat()
	if err != nil {
		t.Fatalf("failed to stat directory: %v", err)
	}

	if !info.IsDir() {
		t.Errorf("got IsDir=false, wanted true")
	}

	_, err = io.ReadAll(d)
	if !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("got %q read error, wanted %q", err, fs.ErrInvalid)
	}
}

func TestOpenNotFound(t *testing.T) {
	fsys := buildFS(t, mdtest.Mock(), map[string][]byte{
		"file1": []byte("file1 content"),
	})

	_, err := fsys.Open("unknown")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("got %q error, wanted %q", err, fs.ErrNotExist)
	}

	_, err = fsys.Open("path/to/unknown")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("got %q error, wanted %q", err, fs.ErrNotExist)
	}
}

func TestFSReadDir(t *testing.T) {
	ds := mdtest.Mock()
	fsys := buildFS(t, ds, map[string][]byte{
		"hello.txt":           []byte("hello1"),
		"test/hello2.txt":     []byte("hello2"),
		"test/sub/hello4.txt": []byte("hello4"),
		"test/sub/hello5.txt": []byte("hello5"),
		"test/goodbye.txt":    []byte("goodbye"),
	})

	entries, err := fsys.ReadDir("test")
	if err != nil {
		t.Fatalf("failed to glob: %v", err)
	}

	got := make([]fs.FileInfo, len(entries))
	for i := range entries {
		var err error
		got[i], err = entries[i].Info()
		if err != nil {
			t.Fatalf("failed to get entry info: %v", err)
		}
	}

	want := []fs.FileInfo{
		&FileInfo{name: "goodbye.txt", size: 7},
		&FileInfo{name: "hello2.txt", size: 6},
		&FileInfo{name: "sub", size: 228, filemode: fs.ModeDir},
	}

	fileInfoComparer := cmp.Comparer(func(a, b *FileInfo) bool {
		return a.name == b.name &&
			a.filemode == b.filemode &&
			a.size == b.size &&
			a.modtime.Equal(b.modtime)
	})

	if diff := cmp.Diff(want, got, ignoreSliceOrder, fileInfoComparer); diff != "" {
		t.Errorf("Glob() mismatch (-want +got):\n%s", diff)
	}
}

func TestOpenChecksForValidName(t *testing.T) {
	testCases := []struct {
		name  string
		valid bool
	}{
		{".", true},
		{"x", true},
		{"x/y", true},

		{"", false},
		{"..", false},
		{"/", false},
		{"x/", false},
		{"/x", false},
		{"x/y/", false},
		{"/x/y", false},
		{"./", false},
		{"./x", false},
		{"x/.", false},
		{"x/./y", false},
		{"../", false},
		{"../x", false},
		{"x/..", false},
		{"x/../y", false},
		{"x//y", false},
		{`x\`, true},
		{`x\y`, true},
		{`x:y`, true},
		{`\x`, true},
	}

	ds := mdtest.Mock()
	fsys := buildFS(t, ds, nil)

	for _, tc := range testCases {
		_, err := fsys.Open(tc.name)

		if errors.Is(err, fs.ErrInvalid) {
			if tc.valid {
				t.Errorf("%q invalid, expected name to be valid: %v", tc.name, err)
			}
		} else {
			if !tc.valid {
				t.Errorf("%q valid, expected name to be invalid: %v", tc.name, err)
			}
		}
	}
}

func TestGlob(t *testing.T) {
	ds := mdtest.Mock()
	fsys := buildFS(t, ds, map[string][]byte{
		"hello.txt":            []byte("hello1"),
		"test/hello2.txt":      []byte("hello2"),
		"test/sub/hello4.txt":  []byte("hello4"),
		"test/sub2/hello5.txt": []byte("hello5"),
		"test/goodbye.txt":     []byte("goodbye"),
	})

	got, err := fs.Glob(fsys, "*/*.txt")
	if err != nil {
		t.Fatalf("failed to glob: %v", err)
	}

	want := []string{
		"test/hello2.txt",
		"test/goodbye.txt",
	}

	if diff := cmp.Diff(want, got, ignoreSliceOrder); diff != "" {
		t.Errorf("Glob() mismatch (-want +got):\n%s", diff)
	}
}

// func TestOpenFileWithLinkSystem(t *testing.T) {
// 	expectedData := []byte("afile content")

// 	ds := mdtest.Mock()
// 	fsys := buildFS(t, ds, map[string][]byte{
// 		"a/b/c/d/e/f/g/afile": expectedData,
// 	})

// 	ls := newLinkSystem(t, ds)
// 	fsys = fsys.WithLinkSystem(ls)

// 	f, err := fsys.Open("a/b/c/d/e/f/g/afile")
// 	if err != nil {
// 		t.Fatalf("failed to open file: %v", err)
// 	}
// 	defer f.Close()

// 	info, err := f.Stat()
// 	if err != nil {
// 		t.Fatalf("failed to stat file: %v", err)
// 	}

// 	if info.IsDir() {
// 		t.Errorf("got IsDir=true, wanted false")
// 	}

// 	data, err := io.ReadAll(f)
// 	if err != nil {
// 		t.Fatalf("failed to read file: %v", err)
// 	}

// 	if !bytes.Equal(data, expectedData) {
// 		t.Errorf("got data %v, wanted %v", data, expectedData)
// 	}
// }

var ignoreSliceOrder = cmpopts.SortSlices(func(a, b string) bool { return a < b })

func buildFS(t *testing.T, ds ipld.DAGService, files map[string][]byte) *FS {
	t.Helper()

	dir := buildUnixFS(t, ds, files)
	dirnode, err := dir.GetNode()
	if err != nil {
		t.Fatalf("failed to get root directory node: %v", err)
	}
	if err := ds.Add(context.TODO(), dirnode); err != nil {
		t.Fatalf("add empty dir to dag service: %v", err)
	}

	lsys := newLinkSystem(t, ds)

	link := cidlink.Link{Cid: dirnode.Cid()}

	node, err := lsys.Load(prime.LinkContext{}, link, dagpb.Type.PBNode)
	if err != nil {
		t.Fatalf("failed to load node: %v", err)
	}

	fsys, err := ReadFS(node, lsys)
	if err != nil {
		t.Fatalf("failed to create fs: %v", err)
	}
	return fsys
}

func buildUnixFS(t *testing.T, ds ipld.DAGService, files map[string][]byte) uio.Directory {
	t.Helper()

	root := uio.NewDirectory(ds)

	for p, content := range files {
		var err error
		root, err = addFileToDir(t, root, ds, p, content)
		if err != nil {
			t.Fatalf("failed to add file %s: %v", p, err)
		}
	}

	return root
}

func addFileToDir(t *testing.T, parent uio.Directory, ds ipld.DAGService, fpath string, content []byte) (uio.Directory, error) {
	t.Helper()

	if !strings.Contains(fpath, "/") {
		var nd ipld.Node
		if content == nil {
			// empty directory
			nd = ufs.EmptyDirNode()
			if err := ds.Add(context.TODO(), nd); err != nil {
				return nil, fmt.Errorf("add empty dir to dag service: %w", err)
			}
		} else {
			// we have a file
			nd = utest.GetNode(t, ds, content, utest.UseCidV1)
		}

		if err := parent.AddChild(context.TODO(), fpath, nd); err != nil {
			return nil, fmt.Errorf("add file %s to directory node: %w", fpath, err)
		}
		return parent, nil

	}

	parts := strings.SplitN(fpath, "/", 2)
	thisDirName := parts[0]
	remainingPath := parts[1]

	nd, err := parent.Find(context.TODO(), thisDirName)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ipld.ErrNotFound{}) {
			// actual error
			return nil, fmt.Errorf("find %s: %w", thisDirName, err)
		}

		// need to create a new node
		nd = ufs.EmptyDirNode()
		if err := ds.Add(context.TODO(), nd); err != nil {
			return nil, fmt.Errorf("add empty dir to dag service: %w", err)
		}
	}

	thisDir, err := uio.NewDirectoryFromNode(ds, nd)
	if err != nil {
		return nil, fmt.Errorf("new directory from node: %w", err)
	}

	thisDir, err = addFileToDir(t, thisDir, ds, remainingPath, content)
	if err != nil {
		return nil, fmt.Errorf("add %s: %w", remainingPath, err)
	}

	thisNode, err := thisDir.GetNode()
	if err != nil {
		return nil, fmt.Errorf("get this dir node: %w", err)
	}
	if err := ds.Add(context.TODO(), thisNode); err != nil {
		return nil, fmt.Errorf("add this dir to dag service: %w", err)
	}

	if err := parent.AddChild(context.TODO(), thisDirName, thisNode); err != nil {
		return nil, fmt.Errorf("add file %s to directory node: %w", thisDirName, err)
	}

	return parent, nil
}

func newLinkSystem(t *testing.T, ds ipld.DAGService) *prime.LinkSystem {
	ls := cidlink.DefaultLinkSystem()
	o := &dagServiceOpener{ds: ds}
	ls.StorageReadOpener = o.OpenRead
	return &ls
}

type dagServiceOpener struct {
	ds ipld.DAGService
}

func (d *dagServiceOpener) OpenRead(lnkCtx prime.LinkContext, lnk prime.Link) (io.Reader, error) {
	cl, ok := lnk.(cidlink.Link)
	if !ok {
		return nil, fmt.Errorf("incompatible link type: %T", lnk)
	}
	block, err := d.ds.Get(lnkCtx.Ctx, cl.Cid)
	if err != nil {
		return nil, fmt.Errorf("dag service get: %w", err)
	}
	return bytes.NewReader(block.RawData()), nil
}
