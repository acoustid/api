// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package handlers

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/pkg/errors"
	"net/http"
	"net/url"
)

type ErrorResponse struct {
	XMLName struct{}     `json:"-" xml:"response"`
	Status  string       `json:"status" xml:"status"`
	Error   ErrorDetails `json:"error" xml:"error"`
}

type ErrorDetails struct {
	Message string `json:"message" xml:"message"`
	Code    int    `json:"code" xml:"code"`
}

func NewErrorResponse(message string, code int) interface{} {
	return &ErrorResponse{
		Status: "error",
		Error: ErrorDetails{
			Message: message,
			Code:    code,
		},
	}
}

func MarshalResponse(response interface{}, format ResponseFormat) ([]byte, error) {
	switch format {
	case JsonFormat:
		return json.Marshal(response)
	case XmlFormat:
		return xml.Marshal(response)
	}
	return nil, errors.New("unsupported format")
}

func WriteResponse(w http.ResponseWriter, status int, response interface{}, format ResponseFormat) {
	data, _ := MarshalResponse(response, format)
	switch format {
	case JsonFormat:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	case XmlFormat:
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	}
	w.WriteHeader(status)
	if format == XmlFormat {
		fmt.Fprint(w, xml.Header)
	}
	w.Write(data)
}

type ResponseFormat int

const (
	UnknownFormat ResponseFormat = 0
	JsonFormat                   = 1 << iota
	JsonpFormat
	XmlFormat
)

func parseResponseFormat(values url.Values, allowed ResponseFormat) (ResponseFormat, error) {
	format := values.Get("format")
	if (format == "" || format == "json") && (JsonFormat&allowed != 0) {
		return JsonFormat, nil
	} else if format == "jsonp" && (JsonpFormat&allowed != 0) {
		return JsonpFormat, nil
	} else if format == "xml" && (XmlFormat&allowed != 0) {
		return XmlFormat, nil
	} else {
		return UnknownFormat, fmt.Errorf("unknown format %s", format)
	}
}
