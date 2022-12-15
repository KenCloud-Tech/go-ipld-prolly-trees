package schema

import "github.com/ipfs/go-cid"

type ProllyTreeNode struct {
	RootCid   cid.Cid
	ConfigCid cid.Cid
}
