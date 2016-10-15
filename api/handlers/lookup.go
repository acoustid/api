package handlers

import (
	"fmt"
	"net/http"
	"gopkg.in/mgo.v2"
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

	format, err := parseResponseFormat(r.Form, JsonFormat | XmlFormat)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	switch format {
	case JsonFormat:
		fmt.Fprintf(w, "{ok}")
	case XmlFormat:
		fmt.Fprintf(w, "<count></count>")
	}
}
