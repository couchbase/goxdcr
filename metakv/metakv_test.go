package metakv

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync"
	"testing"
)

type entry struct {
	v []byte
	r []byte
}

type mockKV struct {
	l           sync.Mutex
	counter     uint64
	data        map[string]entry
	subscribers map[uint64]chan KVEntry
	srv         *httptest.Server
}

func (kv *mockKV) runMock() func() {
	srv := httptest.NewServer(http.HandlerFunc(kv.Handle))
	if kv.data == nil {
		kv.data = make(map[string]entry)
		kv.subscribers = make(map[uint64]chan KVEntry)
	}
	kv.srv = srv
	return func() {
		srv.Close()
	}
}

func replyJSON(w http.ResponseWriter, value interface{}) {
	json.NewEncoder(w).Encode(value)
}

func (kv *mockKV) broadcast(kve KVEntry) {
	for _, s := range kv.subscribers {
		s <- kve
	}
}

func (kv *mockKV) setLocked(path string, value string) {
	rev := make([]byte, 8)
	binary.LittleEndian.PutUint64(rev, kv.counter)
	kv.counter++
	v := []byte(value)
	e := entry{v, rev}
	kv.data[path] = e

	kv.broadcast(KVEntry{Path: path, Value: v, Rev: rev})
}

func (kv *mockKV) subscribeLocked(ch chan KVEntry) func() {
	id := kv.counter
	kv.counter++
	kv.subscribers[id] = ch
	return func() {
		kv.l.Lock()
		defer kv.l.Unlock()
		delete(kv.subscribers, id)
	}
}

type entriesSlice []KVEntry

func (p entriesSlice) Len() int {
	return len(p)
}
func (p entriesSlice) Less(i, j int) bool {
	return string(p[i].Path) < string(p[j].Path)
}
func (p entriesSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (kv *mockKV) Handle(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	kv.l.Lock()
	locked := true
	defer func() {
		if locked {
			kv.l.Unlock()
		}
	}()

	form := req.PostForm
	method := form.Get("method")
	create := form.Get("create") != ""
	path := form.Get("path")
	value := form.Get("value")
	rev := form.Get("rev")
	revGiven := rev != ""

	if revGiven {
		e := kv.data[path]
		if string(e.r) != rev {
			w.WriteHeader(409)
			return
		}
	}

	switch method {
	case "get":
		e, exists := kv.data[path]
		if !exists {
			w.Write([]byte("{}"))
			return
		}
		replyJSON(w, map[string][]byte{"value": e.v, "rev": e.r})
	case "set":
		if create {
			if _, exists := kv.data[path]; exists {
				w.WriteHeader(409)
				return
			}
		}
		kv.setLocked(path, value)
	case "delete":
		kv.broadcast(KVEntry{path, nil, nil})
		delete(kv.data, path)
	case "iterate":
		continuous := (form.Get("continuous") == "true")
		entries := make([]KVEntry, 0, len(kv.data))
		for k, e := range kv.data {
			entries = append(entries, KVEntry{Path: k, Value: e.v, Rev: e.r})
		}
		sort.Sort(entriesSlice(entries))
		enc := json.NewEncoder(w)
		for _, e := range entries {
			err := enc.Encode(e)
			if err != nil {
				log.Fatal(err)
			}
		}
		if continuous {
			w.(http.Flusher).Flush()

			ch := make(chan KVEntry, 16)
			defer kv.subscribeLocked(ch)()

			kv.l.Unlock()
			locked = false

			log.Print("Waiting for rows")
			closed := w.(http.CloseNotifier).CloseNotify()

			for {
				select {
				case e := <-ch:
					log.Printf("Observed {%s, %s, %s}", e.Path, e.Value, e.Rev)
					err := enc.Encode(e)
					if err != nil {
						log.Printf("Got error in subscribe path: %v", err)
						return
					}
					w.(http.Flusher).Flush()
				case <-closed:
					log.Print("receiver is dead")
					return
				}
			}
		}
	default:
		w.WriteHeader(404)
	}
}

func (kv *mockKV) postForm(payload url.Values) (resp *http.Response, err error) {
	u := kv.srv.URL + "/_metakv"

	return http.PostForm(u, payload)
}

func (kv *mockKV) doCall(payload url.Values, response interface{}) (statusCode int, err error) {
	resp, err := kv.postForm(payload)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, json.NewDecoder(resp.Body).Decode(response)
}

type myT struct{ *testing.T }

func (t *myT) okStatus(statusCode int, err error) {
	if err != nil {
		t.Fatalf("Got error from http call: %v", err)
	}
	if statusCode != 200 {
		t.Fatalf("Expected code 200. Got: %d", statusCode)
	}
}

func (t *myT) emptyBody(resp *http.Response, err error) {
	defer resp.Body.Close()
	t.okStatus(resp.StatusCode, err)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got error trying to read body: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("Expected empty body. Got: `%s'", string(body))
	}
}

func must(t *testing.T) *myT { return &myT{t} }

func TestMock(t *testing.T) {
	kv := &mockKV{}
	defer kv.runMock()()

	var m map[string]interface{}
	must(t).okStatus(kv.doCall(url.Values{"method": {"get"}, "path": {"/test"}}, &m))
	if len(m) != 0 {
		t.Fatalf("Expected get against empty kv to return {}")
	}

	must(t).emptyBody(kv.postForm(url.Values{"method": {"set"}, "path": {"/test"}, "value": {"foobar"}}))

	var kve kvEntry
	must(t).okStatus(kv.doCall(url.Values{"method": {"get"}, "path": {"/test"}}, &kve))
	if string(kve.Value) != "foobar" {
		t.Fatalf("failed to get expected value (foobar). Got: %s", kve.Value)
	}
}

func TestSanity(t *testing.T) {
	kv := &mockKV{}
	defer kv.runMock()()

	log := func(msg string) {
		t.Log(msg)
	}
	mockStore := &store{
		url:    kv.srv.URL + "/_metakv",
		client: http.DefaultClient,
	}

	if err := mockStore.add("/_sanity/garbage", []byte("v")); err != nil {
		t.Log("add failed with: %v", err)
	}
	doExecuteBasicSanityTest(log, mockStore)
}
