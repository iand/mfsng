package mfsng

import (
	"io/fs"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
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

			expected := map[string][]string{}

			for pth, content := range tc.files {
				edir := path.Dir(pth)
				ename := path.Base(pth)
				expected[edir] = append(expected[edir], ename)

				nd := utest.GetNode(t, ds, content, utest.UseCidV1)
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

func assertFSStructure(tb testing.TB, fsys *FS, expected map[string][]string) {
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

		files := make([]string, len(entries))
		for i := range files {
			files[i] = entries[i].Name()
		}

		if diff := cmp.Diff(dfiles, files, ignoreSliceOrder); diff != "" {
			tb.Errorf("Entries() mismatch (-want +got):\n%s", diff)
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

			expected := map[string][]string{}
			for _, path := range tc.expected {
				expected[path] = []string{}
			}

			fsys, err := b.ReadFS()
			if err != nil {
				t.Fatalf("failed to get read fs: %v", err)
			}

			assertFSStructure(t, fsys, expected)
		})
	}
}
