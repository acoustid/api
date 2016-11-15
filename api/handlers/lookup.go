// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package handlers

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"net/http"
)

type LookupHandler struct {
	session *mgo.Session
}

func NewLookupHandler(session *mgo.Session) *LookupHandler {
	return &LookupHandler{
		session: session,
	}
}

func (h *LookupHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	session := h.session.Copy()
	defer session.Close()

	r.ParseForm()

	format, err := parseResponseFormat(r.Form, JsonFormat|XmlFormat)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	switch format {
	case JsonFormat:
		fmt.Fprint(w, "{ok}")
	case XmlFormat:
		fmt.Fprint(w, "<count></count>")
	}
}
