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
)

type SubmitHandler struct {
	session *mgo.Session
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

func parseSubmission(values url.Values, suffix string) (*Submission, error) {
	fingerprintString := values.Get("fingerprint" + suffix)
	if fingerprintString == "" {
		return nil, fmt.Errorf("empty fingerprint")
	}
	fingerprint, err := chromaprint.DecodeFingerprintString(fingerprintString)
	if err != nil {
		return nil, fmt.Errorf("fingerprint is not a base64-encoded string (%s)", err)
	}
	if !chromaprint.ValidateFingerprint(fingerprint) {
		return nil, fmt.Errorf("invalid fingerprint")
	}

	durationString := values.Get("duration" + suffix)
	if durationString == "" {
		return nil, fmt.Errorf("empty duration")
	}
	duration, err := strconv.ParseInt(durationString, 10, 32)
	if err != nil || duration <= 0 {
		return nil, fmt.Errorf("invalid duration")
	}

	submission := &Submission{
		Fingerprint: fingerprint,
		Duration:    int(duration),
		MBID:        values.Get("mbid" + suffix),
		Title:       values.Get("track" + suffix),
		Artist:      values.Get("artist" + suffix),
		Album:       values.Get("album" + suffix),
		AlbumArtist: values.Get("albumartist" + suffix),
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

func NewSubmitHandler(session *mgo.Session) *SubmitHandler {
	return &SubmitHandler{
		session: session,
	}
}

func (h *SubmitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	session := h.session.Copy()
	defer session.Close()

	r.ParseForm()

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

	log.Printf("have %d submissions\n", len(submissions))

	bulk := session.DB("").C("submission").Bulk()
	for _, submission := range submissions {
		bulk.Insert(submission)
	}
	_, err = bulk.Run()
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	WriteResponse(w, http.StatusOK, SubmitResponse{Status: "ok"}, format)
}
