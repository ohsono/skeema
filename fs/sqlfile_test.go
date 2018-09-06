package fs

import (
	"fmt"
	"testing"
)

func TestSQLFileParse(t *testing.T) {
	dir, _ := NewDir("../testdata")
	sf := SQLFile{
		Dir:      dir,
		FileName: "setup.sql",
	}
	statements, err := sf.Parse()
	fmt.Printf("err=%s stmts=%+v\n", err, statements)
}
