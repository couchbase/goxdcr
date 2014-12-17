package metakv

import (
	"fmt"
)

func noPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func doAppend(s *store, path string, value []byte) error {
	oldv, rev, err := s.get(path)
	if err != nil {
		return err
	}
	if rev == nil {
		rev = RevCreate
	}
	oldv = append(oldv, value...)
	return s.set(path, oldv, rev)
}

func kvEqual(a, b KVEntry) bool {
	if a.Value == nil && b.Value != nil || b.Value == nil && a.Value != nil {
		return false
	}
	return a.Path == b.Path && string(a.Value) == string(b.Value)
}

// ExecuteBasicSanityTest runs basic sanity test.
func ExecuteBasicSanityTest(log func(msg string)) {
	doExecuteBasicSanityTest(log, defaultStore)
}

func doExecuteBasicSanityTest(log func(msg string), s *store) {
	log("Starting basic sanity test")
	l, err := s.listAllChildren("/_sanity/")
	noPanic(err)
	for _, kve := range l {
		err := s.delete(kve.Path, nil)
		noPanic(err)
	}
	log("cleaned up /_sanity/ subspace")

	v, r, err := s.get("/_sanity/nonexistant")
	noPanic(err)
	if v != nil || r != nil {
		panic("badness")
	}

	buf := make(chan KVEntry, 128)
	cancelChan := make(chan struct{})

	defer func() {
		if cancelChan != nil {
			close(cancelChan)
		}
	}()

	go func() {
		err := s.runObserveChildren("/_sanity/", func(path string, value []byte, rev interface{}) error {
			buf <- KVEntry{Path: path, Value: value, Rev: rev}
			return nil
		}, cancelChan)
		log("Sanity observe loop exited")
		close(buf)
		if err != nil {
			panic(err)
		}
	}()

	err = doAppend(s, "/_sanity/key", []byte("value"))
	noPanic(err)

	v, r, err = s.get("/_sanity/key")
	noPanic(err)
	if r == nil || string(v) != "value" {
		panic("badness")
	}

	err = s.set("/_sanity/key", []byte("new value"), r)
	noPanic(err)

	err = s.delete("/_sanity/key", r)
	if err != ErrRevMismatch {
		panic("must have ErrRevMismatch")
	}

	v, r, err = s.get("/_sanity/key")
	noPanic(err)
	if r == nil || string(v) != "new value" {
		panic("bad")
	}

	err = s.delete("/_sanity/key", r)
	noPanic(err)

	l, err = s.listAllChildren("/_sanity/")
	noPanic(err)
	if len(l) != 0 {
		panic("len is bad")
	}

	close(cancelChan)
	cancelChan = nil

	var allMutations []KVEntry
	for kve := range buf {
		allMutations = append(allMutations, kve)
	}

	if len(allMutations) != 3 {
		panic(fmt.Sprintf("bad mutations size: %d (%v)", len(allMutations), allMutations))
	}

	if !kvEqual(allMutations[0], KVEntry{Path: "/_sanity/key", Value: []byte("value")}) {
		panic("bad mutation")
	}

	if !kvEqual(allMutations[1], KVEntry{Path: "/_sanity/key", Value: []byte("new value")}) {
		panic("bad mutation")
	}

	// deletions signal rev that is interface{}([]byte(nil))
	if !kvEqual(allMutations[2], KVEntry{Path: "/_sanity/key", Value: nil}) || len(allMutations[2].Rev.([]byte)) != 0 {
		log(fmt.Sprintf("bad mutation: %v", allMutations[2]))
		panic("bad mutation")
	}

	log("Completed metakv sanity test")
}
