package tree

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"io"
	"sort"
)

type op int

const (
	Modify op = 1
	Add    op = 2
	Remove op = 3
)

type Mutation struct {
	Key []byte
	Val ipld.Node
	Op  op
}

// Mutations receives batch updates such as put, delete....., also used in ADL builder
type Mutations struct {
	muts        []*Mutation
	kmap        map[string]int
	finish      bool
	compareFunc CompareFunc
}

func NewMutations() *Mutations {
	return &Mutations{
		muts:        make([]*Mutation, 0),
		kmap:        make(map[string]int),
		compareFunc: DefaultCompareFunc,
	}
}

func (m *Mutations) keyMutation(item []byte) (int, *Mutation) {
	// KeyIndex finds the index that the closest but not smaller than the item
	length := len(m.muts)
	l, r := 0, length-1
	for l < r {
		mid := (l + r) / 2
		midKey := m.muts[mid].Key
		if m.compareFunc(midKey, item) == 0 {
			return mid, m.muts[mid]
		} else if m.compareFunc(midKey, item) > 0 {
			r = mid
		} else {
			l = mid + 1
		}
	}
	return r, m.muts[r]
}

func (m *Mutations) AddMutation(mut *Mutation) error {
	if m.finish {
		return fmt.Errorf("can not add mutation after finished")
	}

	if idx, exist := m.kmap[string(mut.Key)]; exist {
		m.muts[idx] = mut
		return nil
	}

	m.muts = append(m.muts, mut)
	m.kmap[string(mut.Key)] = len(m.muts) - 1
	return nil
}

func (m *Mutations) Finish() {
	// ignore repeated close
	if m.finish == true {
		return
	}
	m.finish = true

	sort.Slice(m.muts, func(i, j int) bool {
		if DefaultCompareFunc(m.muts[i].Key, m.muts[j].Key) < 0 {
			return true
		}
		return false
	})

	m.kmap = nil
}

func (m *Mutations) NextMutation() (*Mutation, error) {
	if !m.finish {
		return nil, fmt.Errorf("can not get mutation until finish input")
	}
	if len(m.muts) == 0 {
		return nil, io.EOF
	}
	mut := m.muts[0]
	m.muts = m.muts[1:]

	return mut, nil
}

func (m *Mutations) Get(item []byte) (ipld.Node, error) {
	if !m.finish {
		idx, exist := m.kmap[string(item)]
		if exist {
			mut := m.muts[idx]
			if mut.Op != Remove {
				return mut.Val, nil
			}
		}
	} else {
		_, mut := m.keyMutation(item)
		if m.compareFunc(mut.Key, item) == 0 {
			if mut.Op != Remove {
				return mut.Val, nil
			}
		}
	}

	return nil, nil
}
