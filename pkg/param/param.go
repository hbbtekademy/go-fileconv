package param

import (
	"fmt"
	"strings"
)

type Compression string

const (
	None            Compression = "none"
	Gzip            Compression = "gzip"
	Zstd            Compression = "zstd"
	AutoCompression Compression = "auto"
)

type Columns map[string]string

func (c Columns) Str(quoteKeys bool) string {
	if len(c) == 0 {
		return ""
	}

	cols := []string{}
	for k, v := range c {
		if quoteKeys {
			cols = append(cols, fmt.Sprintf("'%s': '%s'", k, v))
		} else {
			cols = append(cols, fmt.Sprintf("%s: '%s'", k, v))
		}
	}

	var sb strings.Builder
	sb.WriteString("{")
	sb.WriteString(strings.Join(cols, ","))
	sb.WriteString("}")
	return sb.String()
}