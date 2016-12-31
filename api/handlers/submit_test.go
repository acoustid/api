// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package handlers

import (
	"testing"
	"os"
	"gopkg.in/mgo.v2/dbtest"
	"io/ioutil"
	"log"
	"net/http/httptest"
	"github.com/stretchr/testify/assert"
	"bytes"
	"net/url"
	"strconv"
)

type MockSubmissionStore struct {
	submissions []Submission
}

func (s *MockSubmissionStore) InsertSubmissions(submissions []Submission) error {
	s.submissions = append(s.submissions, submissions...)
	return nil
}

func (s *MockSubmissionStore) Close() { }

var dbServer dbtest.DBServer

func TestMain(m *testing.M) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatalf("unable to create temporary directory for a database: %v", err)
	}

	dbServer.SetPath(tempDir)

	exitCode := m.Run()

	dbServer.Stop()
	os.RemoveAll(tempDir)

	os.Exit(exitCode)
}

func TestSubmitHandler(t *testing.T) {
	dbSession := dbServer.Session()
	defer dbSession.Close()

	handler := NewSubmitHandler(NewMongoSubmissionStore(dbSession))
	defer handler.Close()

	data := url.Values{}
	data.Add("fingerprint", "AQAAE8moKpGkoQkd5N9xHBfxaF-QlMmRAldwLg6eRB8uEUWYKzOSOso0_I8wKkkCQBQTWQgBCSGICGAA")
	data.Add("duration", "216")

	body := []byte(data.Encode())

	request := httptest.NewRequest("POST", "https://acoustid.org/submit", bytes.NewReader(body))
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Content-Length", strconv.Itoa(len(body)))

	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	assert.Equal(t, 200, response.Code, "status code should be 200 OK")
	assert.JSONEq(t, `{"status": "ok"}`, response.Body.String(), "unexpected response")
}