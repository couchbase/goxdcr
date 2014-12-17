package metakvsanity

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/couchbase/goxdcr/metakv"
)

func doAppend(path string, value []byte) error {
	oldv, rev, err := metakv.Get(path)
	if err != nil {
		return err
	}
	if rev == nil {
		rev = metakv.RevCreate
	}
	oldv = append(oldv, value...)
	return metakv.Set(path, oldv, rev)
}

func serveDebugReq(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_list" {
		l, err := metakv.ListAllChildren("/")
		if err != nil {
			panic(err)
		}
		b, err := json.Marshal(l)
		if err != nil {
			panic(err)
		}
		w.Write(b)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/_changes/") {
		path := r.URL.Path[len("/_changes/")-1:]
		err := metakv.RunObserveChildren(path, func(path string, value []byte, rev interface{}) error {
			b, err := json.Marshal(map[string]interface{}{
				"path":  path,
				"value": string(value),
				"rev":   rev,
			})
			if err != nil {
				panic(err)
			}
			w.Write(b)
			w.Write([]byte("\n\n"))
			w.(http.Flusher).Flush()
			return nil
		}, make(chan struct{}))
		if err != nil {
			panic(err)
		}
		return
	}
	if strings.HasPrefix(r.URL.Path, "/_put/") && r.Method == "POST" {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		path := r.URL.Path[5:]
		metakv.Set(path, b, nil)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/_get/") && r.Method == "GET" {
		b, rev, err := metakv.Get(r.URL.Path[5:])
		if err != nil {
			panic(err)
		}
		h, _ := json.Marshal(rev)
		w.Header().Set("X-Rev", string(h))
		w.Write(b)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/_append/") && r.Method == "POST" {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}

		path := r.URL.Path[len("/_append/")-1:]
		err = doAppend(path, b)
		if err != nil {
			panic(err)
		}
		return
	}
	if r.Method == "DELETE" {
		err := metakv.Delete(r.URL.Path, nil)
		if err != nil {
			panic(err)
		}
		return
	}
	w.WriteHeader(404)
}

// GoRunDebugEndpoint function can be used to run simple http server
// for "manual" debugging of metakv facility.
func GoRunDebugEndpoint(listen string) {
	go func() {
		panic(http.ListenAndServe(listen, http.HandlerFunc(serveDebugReq)))
	}()
}
