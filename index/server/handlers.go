// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package server

import (
	"encoding/json"
	"fmt"
	"github.com/acoustid/go-acoustid/index"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"strconv"
)

type DeleteHandler struct {
	db *index.DB
}

func (h *DeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	docID, err := strconv.ParseUint(vars["id"], 10, 32)
	if err != nil {
		writeErrorResponse(w, 400, "invalid id")
		return
	}

	err = h.db.Delete(uint32(docID))
	if err != nil {
		log.Printf("delete failed: %v", err)
		writeErrorResponse(w, 500, "internal error")
		return
	}

	type Response struct{}
	writeResponse(w, http.StatusOK, Response{})
}

type DeleteAllHandler struct {
	db *index.DB
}

func (h *DeleteAllHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h.db.DeleteAll()
	if err != nil {
		log.Printf("delete failed: %v", err)
		writeErrorResponse(w, 500, "internal error")
		return
	}

	type Response struct{}
	writeResponse(w, http.StatusOK, Response{})
}

type UpdateHandler struct {
	db *index.DB
}

func (h *UpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	docID, err := strconv.ParseUint(vars["id"], 10, 32)
	if err != nil {
		writeErrorResponse(w, 400, "invalid id")
		return
	}

	var input struct {
		Terms []uint32 `json:"terms"`
	}
	err = json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		writeErrorResponse(w, 400, fmt.Sprintf("invalid body: %v", err))
		return
	}

	err = h.db.Add(uint32(docID), input.Terms)
	if err != nil {
		log.Printf("update failed: %v", err)
		writeErrorResponse(w, 500, "internal error")
		return
	}

	type Response struct{}
	writeResponse(w, http.StatusOK, Response{})
}

type AddHandler struct {
	db *index.DB
}

func (h *AddHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bulk, err := h.db.Transaction()
	if err != nil {
		log.Printf("failed to start transaction: %v", err)
		writeErrorResponse(w, 500, fmt.Sprintf("failed to start transaction: %v", err))
		return
	}
	defer bulk.Close()

	var input struct {
		DocID uint32   `json:"id"`
		Terms []uint32 `json:"terms"`
	}
	decoder := json.NewDecoder(r.Body)
	for {
		err = decoder.Decode(&input)
		if err == io.EOF {
			break
		}
		if err != nil {
			writeErrorResponse(w, 400, fmt.Sprintf("invalid body: %v", err))
			return
		}
		err = bulk.Add(input.DocID, input.Terms)
		if err != nil {
			log.Printf("add failed: %v", err)
			writeErrorResponse(w, 500, fmt.Sprintf("add failed: %v", err))
			return
		}
	}

	err = bulk.Commit()
	if err != nil {
		log.Printf("commit failed: %v", err)
		writeErrorResponse(w, 500, fmt.Sprintf("commit failed: %v", err))
		return
	}

	type Response struct{}
	writeResponse(w, http.StatusOK, Response{})
}

type StatsHandler struct {
	db *index.DB
}

func (h *StatsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	type Response struct {
		NumDocs        int `json:"num_docs"`
		NumDeletedDocs int `json:"num_deleted_docs"`
		NumSegments    int `json:"num_segments"`
	}
	response := Response{
		NumDocs:        h.db.NumDocs(),
		NumDeletedDocs: h.db.NumDeletedDocs(),
		NumSegments:    h.db.NumSegments(),
	}
	writeResponse(w, http.StatusOK, response)
}
