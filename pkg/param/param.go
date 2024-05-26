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

type Column struct {
	Name string
	Type string
}

type Columns []Column

func (c Columns) Format(quoteKeys bool) string {
	if len(c) == 0 {
		return ""
	}

	cols := []string{}

	for _, col := range c {
		if quoteKeys {
			cols = append(cols, fmt.Sprintf("'%s': '%s'", col.Name, col.Type))
		} else {
			cols = append(cols, fmt.Sprintf("%s: '%s'", col.Name, col.Type))
		}
	}

	var sb strings.Builder
	sb.WriteString("{")
	sb.WriteString(strings.Join(cols, ","))
	sb.WriteString("}")
	return sb.String()
}
