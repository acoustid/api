// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package server

import (
	"bytes"
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/stretchr/testify/require"
	"log"
	"net/http/httptest"
	"testing"
)

func TestDeleteHandler(t *testing.T) {
	db, err := index.Open(vfs.CreateMemDir(), true, nil)
	require.NoError(t, err, "failed to create test db")
	defer db.Close()

	db.Add(1, []uint32{100})
	require.Equal(t, 1, db.NumDocs())
	require.Equal(t, 0, db.NumDeletedDocs())

	req := httptest.NewRequest("DELETE", "http://example.com/index/1", nil)
	w := httptest.NewRecorder()
	Handler(db).ServeHTTP(w, req)

	require.Equal(t, 200, w.Code, "status code should be 200 OK")
	require.JSONEq(t, `{}`, w.Body.String(), "unexpected response")
	require.Equal(t, 1, db.NumDocs())
	require.Equal(t, 1, db.NumDeletedDocs())
}

func TestDeleteAllHandler(t *testing.T) {
	db, err := index.Open(vfs.CreateMemDir(), true, nil)
	require.NoError(t, err, "failed to create test db")
	defer db.Close()

	db.Add(1, []uint32{100})
	require.Equal(t, 1, db.NumDocs())
	require.Equal(t, 0, db.NumDeletedDocs())

	req := httptest.NewRequest("DELETE", "http://example.com/index", nil)
	w := httptest.NewRecorder()
	Handler(db).ServeHTTP(w, req)

	require.Equal(t, 200, w.Code, "status code should be 200 OK")
	require.JSONEq(t, `{}`, w.Body.String(), "unexpected response")
	require.Equal(t, 0, db.NumDocs())
	require.Equal(t, 0, db.NumDeletedDocs())
}

func TestUpdateHandler(t *testing.T) {
	db, err := index.Open(vfs.CreateMemDir(), true, nil)
	require.NoError(t, err, "failed to create test db")
	defer db.Close()

	func() {
		body := bytes.NewBufferString(`{"terms": [1,2,3]}`)
		req := httptest.NewRequest("PUT", "http://example.com/index/1", body)
		w := httptest.NewRecorder()
		Handler(db).ServeHTTP(w, req)

		require.Equal(t, 200, w.Code, "status code should be 200 OK")
		require.JSONEq(t, `{}`, w.Body.String(), "unexpected response")
		require.Equal(t, 1, db.NumDocs())
		require.Equal(t, 0, db.NumDeletedDocs())
	}()

	func() {
		body := bytes.NewBufferString(`{"terms": [3,4,5]}`)
		req := httptest.NewRequest("PUT", "http://example.com/index/1", body)
		w := httptest.NewRecorder()
		Handler(db).ServeHTTP(w, req)

		log.Println(w.Body.String())

		require.Equal(t, 200, w.Code, "status code should be 200 OK")
		require.JSONEq(t, `{}`, w.Body.String(), "unexpected response")
		require.Equal(t, 2, db.NumDocs())
		require.Equal(t, 1, db.NumDeletedDocs())
	}()
}

func TestAddHandler(t *testing.T) {
	db, err := index.Open(vfs.CreateMemDir(), true, nil)
	require.NoError(t, err, "failed to create test db")
	defer db.Close()

	var body bytes.Buffer
	body.WriteString(`{"id": 1, "terms": [1,2,3]}`)
	body.WriteString(`{"id": 2, "terms": [3,4,5]}`)

	req := httptest.NewRequest("POST", "http://example.com/index", &body)
	w := httptest.NewRecorder()
	Handler(db).ServeHTTP(w, req)

	log.Println(w.Body)

	require.Equal(t, 200, w.Code, "status code should be 200 OK")
	require.JSONEq(t, `{}`, w.Body.String(), "unexpected response")
	require.Equal(t, 2, db.NumDocs())
	require.Equal(t, 0, db.NumDeletedDocs())
}

func TestStatsHandler(t *testing.T) {
	db, err := index.Open(vfs.CreateMemDir(), true, nil)
	require.NoError(t, err, "failed to create test db")
	defer db.Close()

	db.Add(1, []uint32{100})
	db.Add(2, []uint32{100})
	db.Delete(1)

	req := httptest.NewRequest("GET", "http://example.com/stats", nil)
	w := httptest.NewRecorder()
	Handler(db).ServeHTTP(w, req)

	expected := `{"num_docs": 2, "num_deleted_docs": 1, "num_segments": 2}`

	require.Equal(t, 200, w.Code, "status code should be 200 OK")
	require.JSONEq(t, expected, w.Body.String(), "unexpected response")
}
