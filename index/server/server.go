// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package server

import (
	"encoding/json"
	"github.com/acoustid/go-acoustid/index"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strconv"
)

func writeResponse(w http.ResponseWriter, status int, response interface{}) {
	body, err := json.Marshal(response)
	if err != nil {
		log.Printf("error while serializing JSON response (%v)", err)
		writeErrorResponse(w, http.StatusInternalServerError, "JSON serialization error")
		return
	}
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	w.Write(body)
}

func writeErrorResponse(w http.ResponseWriter, status int, message string) {
	response := map[string]string{"message": message}
	writeResponse(w, status, response)
}

func Handler(db *index.DB) http.Handler {
	r := mux.NewRouter()
	r.Path("/index").Methods("POST").Handler(&AddHandler{db: db})
	r.Path("/index").Methods("DELETE").Handler(&DeleteAllHandler{db: db})
	r.Path("/index/{id:[0-9]+}").Methods("PUT").Handler(&UpdateHandler{db: db})
	r.Path("/index/{id:[0-9]+}").Methods("DELETE").Handler(&DeleteHandler{db: db})
	r.Path("/stats").Methods("GET").Handler(&StatsHandler{db: db})
	return r
}

func ListenAndServe(addr string, db *index.DB) error {
	return http.ListenAndServe(addr, Handler(db))
}
