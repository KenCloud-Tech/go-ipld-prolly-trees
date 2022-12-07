package schema

import "github.com/ipfs/go-cid"

type ProllyRoot struct {
	RootCid cid.Cid
	Config  ChunkConfig
}
