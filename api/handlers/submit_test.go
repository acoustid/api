// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package handlers

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

type MockSubmissionStore struct {
	submissions []Submission
}

func (s *MockSubmissionStore) InsertSubmissions(submissions []Submission) error {
	s.submissions = append(s.submissions, submissions...)
	return nil
}

func (s *MockSubmissionStore) Close() {}

func TestSubmitHandler(t *testing.T) {
	submissionStore := &MockSubmissionStore{}

	handler := NewSubmitHandler(submissionStore)
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

	assert.Len(t, submissionStore.submissions, 1)
}
