// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package handlers

import (
	"fmt"
	"github.com/acoustid/go-acoustid/chromaprint"
	"gopkg.in/mgo.v2"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"github.com/pkg/errors"
	"time"
)

type SubmissionStore interface {
	InsertSubmissions(s []Submission) error
	Close()
}

type MongoSubmissionStore struct {
	session *mgo.Session
}

func NewMongoSubmissionStore(session *mgo.Session) *MongoSubmissionStore {
	return &MongoSubmissionStore{session: session.Clone()}
}

func (s *MongoSubmissionStore) InsertSubmissions(submissions []Submission) error {
	collection := "submission_" + time.Now().Format("2006_01")
	bulk := s.session.DB("").C(collection).Bulk()
	for _, submission := range submissions {
		bulk.Insert(submission)
	}
	_, err := bulk.Run()
	if err != nil {
		return err
	}
	log.Printf("inserted %d submission(s) to %s\n", len(submissions), collection)
	return nil
}

func (s *MongoSubmissionStore) Close() {
	s.session.Close()
}

type SubmitHandler struct {
	submissionStore SubmissionStore
}

type SubmitResponse struct {
	XMLName struct{} `json:"-" xml:"response"`
	Status  string   `json:"status" xml:"status"`
}

type Submission struct {
	Id          string `bson:"_id,omitempty"`
	Duration    int
	Fingerprint []byte
	MBID        string `bson:",omitempty"`
	Title       string `bson:",omitempty"`
	Artist      string `bson:",omitempty"`
	Album       string `bson:",omitempty"`
	AlbumArtist string `bson:",omitempty"`
	Year        int    `bson:",omitempty"`
	TrackNo     int    `bson:",omitempty"`
	DiscNo      int    `bson:",omitempty"`
}

func getIntOrZero(s string) int {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int(i)
}

func parseSubmission(values url.Values, suffix string) (*Submission, error) {
	fingerprintString := values.Get("fingerprint" + suffix)
	if fingerprintString == "" {
		return nil, errors.New("empty fingerprint")
	}
	fingerprint, err := chromaprint.DecodeFingerprintString(fingerprintString)
	if err != nil {
		return nil, fmt.Errorf("fingerprint is not a base64-encoded string (%s)", err)
	}
	if !chromaprint.ValidateFingerprint(fingerprint) {
		return nil, errors.New("invalid fingerprint")
	}

	duration := getIntOrZero(values.Get("duration" + suffix))
	if duration <= 0 {
		return nil, errors.New("invalid duration")
	}

	submission := &Submission{
		Fingerprint: fingerprint,
		Duration:    duration,
		MBID:        values.Get("mbid" + suffix),
		Title:       values.Get("track" + suffix),
		Artist:      values.Get("artist" + suffix),
		Album:       values.Get("album" + suffix),
		AlbumArtist: values.Get("albumartist" + suffix),
		Year:        getIntOrZero(values.Get("year" + suffix)),
		TrackNo:     getIntOrZero(values.Get("trackno" + suffix)),
		DiscNo:      getIntOrZero(values.Get("discno" + suffix)),
	}

	return submission, nil
}

func parseSubmissions(values url.Values) ([]Submission, error) {
	suffixes := make([]string, 0, 100)
	for key := range values {
		if key == "fingerprint" || strings.HasPrefix(key, "fingerprint.") {
			suffix := strings.TrimPrefix(key, "fingerprint")
			if len(suffixes) < cap(suffixes) {
				suffixes = append(suffixes, suffix)
			}
		}
	}

	if len(suffixes) == 0 {
		suffixes = append(suffixes, "")
	}

	submissions := make([]Submission, 0, len(suffixes))
	for _, suffix := range suffixes {
		submission, err := parseSubmission(values, suffix)
		if err != nil {
			return nil, err
		}
		submissions = append(submissions, *submission)
	}
	return submissions, nil
}

func NewSubmitHandler(submissionStore SubmissionStore) *SubmitHandler {
	return &SubmitHandler{
		submissionStore: submissionStore,
	}
}

func (h *SubmitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		WriteResponse(w, http.StatusBadRequest, NewErrorResponse(err.Error(), 1), JsonFormat)
		return
	}

	format, err := parseResponseFormat(r.Form, JsonFormat|XmlFormat)
	if err != nil {
		WriteResponse(w, http.StatusBadRequest, NewErrorResponse(err.Error(), 1), JsonFormat)
		return
	}

	submissions, err := parseSubmissions(r.Form)
	if err != nil {
		WriteResponse(w, http.StatusBadRequest, NewErrorResponse(err.Error(), 1), format)
		return
	}

	err = h.submissionStore.InsertSubmissions(submissions)
	if err != nil {
		WriteResponse(w, http.StatusInternalServerError, NewErrorResponse(err.Error(), 1), format)
		return
	}

	WriteResponse(w, http.StatusOK, SubmitResponse{Status: "ok"}, format)
}

func (h *SubmitHandler) Close() {
	h.submissionStore.Close()
}