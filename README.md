# mfsng
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/iand/mfsng)

An implementation of Go's filesystem interface for the IPFS UnixFS format.

## Overview

`mfsng` is an implementation of [fs.FS](https://pkg.go.dev/io/fs#FS) over a [UnixFS](https://github.com/ipfs/specs/blob/master/UNIXFS.md) merkledag.

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
