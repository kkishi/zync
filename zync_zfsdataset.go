package main

import (
	"encoding"
	"strings"
)

type zfsDataset struct {
	host, path string
}

func (d *zfsDataset) UnmarshalText(text []byte) error {
	s := string(text)
	if i := strings.Index(s, ":"); i != -1 {
		d.host = s[0:i]
		d.path = s[i+1:]
	} else {
		d.path = s
	}
	return nil
}

func (d *zfsDataset) MarshalText() (text []byte, err error) {
	if d.host != "" {
		return ([]byte)(d.host + ":" + d.path), nil
	}
	return ([]byte)(d.path), nil
}

var _ encoding.TextUnmarshaler = (*zfsDataset)(nil)
var _ encoding.TextMarshaler = (*zfsDataset)(nil)
