package mfsng

import (
	"context"
	"io/fs"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/ipfs/go-cid"
	mdtest "github.com/ipfs/go-merkledag/test"
	utest "github.com/ipfs/go-unixfs/test"
)

func TestBuilderWriteFileNode(t *testing.T) {
	testCases := []struct {
		files map[string][]byte
	}{
		{
			files: map[string][]byte{
				"hello.txt": []byte("hello1"),
			},
		},
		{
			files: map[string][]byte{
				"foo/hello.txt": []byte("hello1"),
			},
		},
		{
			files: map[string][]byte{
				"foo/hello.txt":   []byte("hello1"),
				"foo/goodbye.txt": []byte("goodbye"),
			},
		},
		{
			files: map[string][]byte{
				"foo/hello.txt":   []byte("hello1"),
				"foo/welcome.txt": []byte("welcome"),
				"foo/goodbye.txt": []byte("goodbye"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ds := mdtest.Mock()
			b := NewBuilder(ds)
			b.root.name = "root"

			expected := map[string][]namecid{}

			for pth, content := range tc.files {
				edir := path.Dir(pth)
				ename := path.Base(pth)
				nd := utest.GetNode(t, ds, content, utest.UseCidV1)
				expected[edir] = append(expected[edir], namecid{Name: ename, Cid: nd.Cid()})

				t.Logf("writing file %s", pth)
				err := b.WriteFileNode(pth, nd)
				if err != nil {
					t.Fatalf("failed to write file node %q: %v", pth, err)
				}
			}

			fsys, err := b.ReadFS()
			if err != nil {
				t.Fatalf("failed to get read fs: %v", err)
			}

			assertFSStructure(t, fsys, expected)
		})
	}
}

func TestBuilderOverwriteFileNode(t *testing.T) {
	type version struct {
		path    string
		content []byte
	}
	testCases := []struct {
		versions []version
	}{
		{
			versions: []version{
				{
					path:    "hello.txt",
					content: []byte("hello1"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ds := mdtest.Mock()
			b := NewBuilder(ds)
			b.root.name = "root"

			expected := map[string][]namecid{}

			for i, v := range tc.versions {
				edir := path.Dir(v.path)
				ename := path.Base(v.path)
				nd := utest.GetNode(t, ds, v.content, utest.UseCidV1)

				if i == len(tc.versions)-1 {
					// last version
					expected[edir] = append(expected[edir], namecid{Name: ename, Cid: nd.Cid()})
				}

				t.Logf("writing file version %d at %s", i, v.path)
				err := b.WriteFileNode(v.path, nd)
				if err != nil {
					t.Fatalf("failed to write file node %q: %v", v.path, err)
				}
			}

			fsys, err := b.ReadFS()
			if err != nil {
				t.Fatalf("failed to get read fs: %v", err)
			}

			assertFSStructure(t, fsys, expected)
		})
	}
}

func TestBuilderWriteFileNodeAfterFlush(t *testing.T) {
	testCases := []struct {
		files map[string][]byte
	}{
		{
			files: map[string][]byte{
				"hello.txt": []byte("hello1"),
			},
		},
		{
			files: map[string][]byte{
				"foo/hello.txt": []byte("hello1"),
			},
		},
		{
			files: map[string][]byte{
				"foo/hello.txt":   []byte("hello1"),
				"foo/goodbye.txt": []byte("goodbye"),
			},
		},
		{
			files: map[string][]byte{
				"foo/hello.txt":   []byte("hello1"),
				"foo/welcome.txt": []byte("welcome"),
				"foo/goodbye.txt": []byte("goodbye"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ds := mdtest.Mock()
			b := NewBuilder(ds)
			b.root.name = "root"

			expected := map[string][]namecid{}

			for pth, content := range tc.files {
				edir := path.Dir(pth)
				ename := path.Base(pth)
				nd := utest.GetNode(t, ds, content, utest.UseCidV1)
				expected[edir] = append(expected[edir], namecid{Name: ename, Cid: nd.Cid()})

				t.Logf("writing file %s", pth)
				err := b.WriteFileNode(pth, nd)
				if err != nil {
					t.Fatalf("failed to write file node %q: %v", pth, err)
				}

				if err := b.Flush(); err != nil {
					t.Fatalf("failed to flush builder: %v", err)
				}
			}

			fsys, err := b.ReadFS()
			if err != nil {
				t.Fatalf("failed to get read fs: %v", err)
			}

			assertFSStructure(t, fsys, expected)
		})
	}
}

type namecid struct {
	Name string
	Cid  cid.Cid
}

func assertFSStructure(tb testing.TB, fsys *FS, expected map[string][]namecid) {
	tb.Helper()

	if len(expected) == 0 {
		entries, err := fsys.ReadDir(".")
		if err != nil {
			tb.Fatalf("failed to read root dir: %v", err)
		}

		if len(entries) != 0 {
			tb.Fatalf("got %d entries in root dir, wanted %d", len(entries), 0)
		}

		return
	}

	for path, dfiles := range expected {

		d, err := fsys.Open(path)
		if err != nil {
			tb.Fatalf("failed to open directory: %v", err)
		}
		defer d.Close()

		info, err := d.Stat()
		if err != nil {
			tb.Fatalf("failed to stat directory: %v", err)
		}

		if !info.IsDir() {
			tb.Errorf("got IsDir=false, wanted true")
		}

		entries, err := fsys.ReadDir(path)
		if err != nil {
			tb.Errorf("failed to read dir %q: %v", path, err)
			continue
		}

		files := make([]namecid, 0, len(entries))
		for i := range entries {
			if fe, ok := entries[i].(*File); ok {
				files = append(files, namecid{Name: fe.Name(), Cid: fe.Cid()})
			}
		}

		if diff := cmp.Diff(dfiles, files, cmpopts.SortSlices(func(a, b namecid) bool { return a.Name < b.Name }), cmpopts.IgnoreUnexported(cid.Cid{})); diff != "" {
			logFS(tb, fsys)
			tb.Errorf("ReadDir(%q) mismatch (-want +got):\n%s", path, diff)
		}
	}
}

func logFS(tb testing.TB, fsys fs.FS) {
	tb.Helper()
	fs.WalkDir(fsys, ".", func(path string, de fs.DirEntry, rerr error) error {
		tb.Logf("%s\n", path)
		return nil
	})
}

func TestBuilderMkdirAll(t *testing.T) {
	testCases := []struct {
		makes    []string
		expected []string
	}{
		{
			makes: []string{
				"",
			},
			expected: []string{},
		},
		{
			makes: []string{
				"a/b/c",
			},
			expected: []string{
				"a/b/c",
			},
		},
		{
			makes: []string{
				"a/b/c",
				"a/b/c",
			},
			expected: []string{
				"a/b/c",
			},
		},
		{
			makes: []string{
				"a",
				"a/b/c",
			},
			expected: []string{
				"a/b/c",
			},
		},
		{
			makes: []string{
				"a",
				"a/b",
				"a/b/c",
			},
			expected: []string{
				"a/b/c",
			},
		},
		{
			makes: []string{
				"a/b/c",
				"a/d/e",
			},
			expected: []string{
				"a/b/c",
				"a/d/e",
			},
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ds := mdtest.Mock()
			b := NewBuilder(ds)

			for _, dir := range tc.makes {
				if err := b.MkdirAll(dir); err != nil {
					t.Fatalf("failed to make directory: %v", err)
				}
			}

			expected := map[string][]namecid{}
			for _, path := range tc.expected {
				expected[path] = []namecid{}
			}

			fsys, err := b.ReadFS()
			if err != nil {
				t.Fatalf("failed to get read fs: %v", err)
			}

			assertFSStructure(t, fsys, expected)
		})
	}
}

func TestBuilderWithRoot(t *testing.T) {
	testCases := []struct {
		base  map[string][]byte
		files map[string][]byte
	}{
		{
			base: map[string][]byte{
				"afile": []byte("afile content"),
			},
			files: map[string][]byte{
				"hello.txt": []byte("hello1"),
			},
		},
		{
			base: map[string][]byte{
				"a/b/c/d/e/f/g/afile": []byte("afile content"),
			},
			files: map[string][]byte{
				"hello.txt": []byte("hello1"),
			},
		},
		{
			base: map[string][]byte{
				"foo/afile": []byte("afile content"),
			},
			files: map[string][]byte{
				"foo/hello.txt": []byte("hello1"),
			},
		},
		{
			base: map[string][]byte{
				"afile":     []byte("afile content"),
				"foo/bfile": []byte("bfile content"),
				"foo/cfile": []byte("cfile content"),
			},
			files: map[string][]byte{
				"foo/hello.txt":   []byte("hello1"),
				"foo/goodbye.txt": []byte("goodbye"),
			},
		},
		{
			base: map[string][]byte{
				"hello.txt": []byte("hello1"),
			},
			files: map[string][]byte{
				"hello.txt": []byte("hello2"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ds := mdtest.Mock()

			// Get a root node that represents the unixfs described by tc.base
			basedir := buildUnixFS(t, ds, tc.base)
			basenode, err := basedir.GetNode()
			if err != nil {
				t.Fatalf("failed to get root directory node: %v", err)
			}

			ds.Add(context.TODO(), basenode)
			if err != nil {
				t.Fatalf("failed to add root directory node: %v", err)
			}

			b := NewBuilder(ds).WithRootNode(basenode)

			combined := map[string][]byte{}
			for pth, content := range tc.base {
				combined[pth] = content
			}
			for pth, content := range tc.files {
				combined[pth] = content
			}

			expected := map[string][]namecid{}

			for pth, content := range combined {
				edir := path.Dir(pth)
				ename := path.Base(pth)

				nd := utest.GetNode(t, ds, content, utest.UseCidV1)
				expected[edir] = append(expected[edir], namecid{Name: ename, Cid: nd.Cid()})

				t.Logf("writing file %s", pth)
				err := b.WriteFileNode(pth, nd)
				if err != nil {
					t.Fatalf("failed to write file node %q: %v", pth, err)
				}
			}

			fsys, err := b.ReadFS()
			if err != nil {
				t.Fatalf("failed to get read fs: %v", err)
			}

			assertFSStructure(t, fsys, expected)
		})
	}
}
