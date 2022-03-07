# mfsng
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/iand/mfsng)

An implementation of Go's filesystem interface for the IPFS UnixFS format.

## Overview

`mfsng` is an implementation of [fs.FS](https://pkg.go.dev/io/fs#FS) over a [UnixFS](https://github.com/ipfs/specs/blob/master/UNIXFS.md) merkledag.


## Example Usage

In this example `printFile` prints the file `folder/hello.txt` held in the UnixFS represented by the supplied [ipld.Node](https://pkg.go.dev/github.com/ipld/go-ipld-prime#Node).
`getter` is something that implements [ipld.NodeGetter](https://pkg.go.dev/github.com/ipfs/go-ipld-format#NodeGetter), such as a [DagService](https://pkg.go.dev/github.com/ipfs/go-merkledag#NewDAGService)

```Go
func printFile(node ipld.Node, getter ipld.NodeGetter) {
	fsys, err := mfsng.ReadFS(node, getter)
	if err != nil {
		log.Fatalf("failed to create fs: %v", err)
	}

	f, err := fsys.Open("folder/hello.txt")
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("failed to read file: %v", err)
	}

	fmt.Println(string(data))
}
```

This example demonstrates how the FS can be used with Go's standard walk function and shows how the underlying CID can
be obtained for a file.

```Go
func walkFiles(node ipld.Node, getter ipld.NodeGetter) {
	fsys, err := mfsng.ReadFS(node, getter)
	if err != nil {
		log.Fatalf("failed to create fs: %v", err)
	}
	if err := fs.WalkDir(fsys, ".", func(path string, de fs.DirEntry, rerr error) error {
		if de.IsDir() {
			fmt.Printf("D %s\n", path)
		} else {
			f := de.(*mfsng.File)
			fmt.Printf("F %s (cid=%s)\n", path, f.Cid())
		}
		return nil
	}); err != nil {
		return log.Fatalf("failed walk: %v", err)
	}
}
```

An example of using the FS with Go's Glob functionality:
```Go
func matchFiles(node ipld.Node, getter ipld.NodeGetter) {
	fsys, err := mfsng.ReadFS(node, getter)
	if err != nil {
		log.Fatalf("failed to create fs: %v", err)
	}
	matches, err := fs.Glob(fsys, "some/*/folder/*.txt")
	if err != nil {
		log.Fatalf("failed to glob: %v", err)
	}

	for _, match := range matches {
		fmt.Println(match)
	}
}
```

## Status

This package is experimental. It has a number of limitations:

 - Read only
 - Depends on [go-mfs](https://github.com/ipfs/go-mfs) for much of its functionality.
 - No support for symlinks.
 - No support for modtimes since they are not exposed by go-unixfs (but see [go-unixfs#117](https://github.com/ipfs/go-unixfs/pull/117))

The aim is to remove the dependency on go-mfs entirely so that this package becomes a standalone alternative. 
Adding write capabilities is also planned but some thought is needed around the API since there is no official one. See [issue-45757](https://github.com/golang/go/issues/45757]).

## Contributing

Welcoming [new issues](https://github.com/iand/mfsng/issues/new) and [pull requests](https://github.com/iand/mfsng/pulls).

## License

This software is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](https://github.com/filecoin-project/sentinel-visor/blob/master/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](https://github.com/filecoin-project/sentinel-visor/blob/master/LICENSE-MIT) or http://opensource.org/licenses/MIT)
