module github.com/iand/mfsng

go 1.17

require (
	github.com/google/go-cmp v0.5.8
	github.com/ipfs/go-cid v0.2.0
	github.com/ipfs/go-ipld-format v0.4.0
	github.com/ipfs/go-merkledag v0.6.0
	github.com/ipfs/go-path v0.3.0
	github.com/ipfs/go-unixfs v0.3.1
)

require (
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
	github.com/crackcomm/go-gitignore v0.0.0-20170627025303-887ab5e44cc3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/ipfs/bbloom v0.0.4 // indirect
	github.com/ipfs/go-bitfield v1.0.0 // indirect
	github.com/ipfs/go-block-format v0.0.3 // indirect
	github.com/ipfs/go-blockservice v0.3.0 // indirect
	github.com/ipfs/go-datastore v0.5.1 // indirect
	github.com/ipfs/go-ipfs-blockstore v1.2.0 // indirect
	github.com/ipfs/go-ipfs-chunker v0.0.5 // indirect
	github.com/ipfs/go-ipfs-ds-help v1.1.0 // indirect
	github.com/ipfs/go-ipfs-exchange-interface v0.1.0 // indirect
	github.com/ipfs/go-ipfs-exchange-offline v0.2.0 // indirect
	github.com/ipfs/go-ipfs-files v0.1.1 // indirect
	github.com/ipfs/go-ipfs-posinfo v0.0.1 // indirect
	github.com/ipfs/go-ipfs-util v0.0.2 // indirect
	github.com/ipfs/go-ipld-cbor v0.0.6 // indirect
	github.com/ipfs/go-ipld-legacy v0.1.1 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-log/v2 v2.5.1 // indirect
	github.com/ipfs/go-metrics-interface v0.0.1 // indirect
	github.com/ipfs/go-verifcid v0.0.1 // indirect
	github.com/ipld/go-codec-dagpb v1.4.1 // indirect
	github.com/ipld/go-ipld-prime v0.16.0 // indirect
	github.com/jbenet/goprocess v0.1.4 // indirect
	github.com/klauspost/cpuid/v2 v2.0.13 // indirect
	github.com/libp2p/go-buffer-pool v0.0.2 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.0.4 // indirect
	github.com/multiformats/go-base36 v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.1.0 // indirect
	github.com/multiformats/go-multihash v0.1.0 // indirect
	github.com/multiformats/go-varint v0.0.6 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/polydawn/refmt v0.0.0-20201211092308-30ac6d18308e // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/whyrusleeping/cbor-gen v0.0.0-20220514204315-f29c37e9c44c // indirect
	github.com/whyrusleeping/chunker v0.0.0-20181014151217-fe64bd25879f // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f // indirect
	golang.org/x/sys v0.0.0-20220610221304-9f5ed59c137d // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	lukechampine.com/blake3 v1.1.7 // indirect
)

// See https://github.com/ipfs/go-unixfs/pull/117
replace github.com/ipfs/go-unixfs => github.com/iand/go-unixfs v0.3.2-0.20220113124813-f455d36c5fd1
