package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"unicode"
	"unicode/utf8"
)

// SQLFile represents a file containing zero or more SQL statements.
type SQLFile struct {
	Dir      Dir
	FileName string
}

type Statement struct {
	File   SQLFile
	LineNo int
	CharNo int
	Text   string
}

// Path returns the full absolute path to a SQLFile.
func (sf SQLFile) Path() string {
	return path.Join(string(sf.Dir), sf.FileName)
}

func (sf SQLFile) String() string {
	return sf.Path()
}

func (sf SQLFile) Exists() (bool, error) {
	_, err := os.Stat(sf.Path())
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Create writes a new file, erroring if it already exists.
func (sf SQLFile) Create(contents string) error {
	if exists, err := sf.Exists(); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("Cannot create %s: already exists", sf)
	}
	return ioutil.WriteFile(sf.Path(), []byte(contents), 0666)
}

// Delete unlinks the file.
func (sf SQLFile) Delete() error {
	return os.Remove(sf.Path())
}

func (sf SQLFile) Parse() (result []Statement, err error) {
	byteContents, err := ioutil.ReadFile(sf.Path())
	if err != nil {
		return nil, err
	}
	last := len(byteContents) - 1
	contents := string(byteContents)

	stmt := Statement{ // tracks the "current" (not yet appended to result) statement
		File:   sf,
		LineNo: 1,
		CharNo: 1,
	}
	var inQuote rune
	var inRelevant, inLineComment, inCComment, escapeNext bool
	var startStatement, charNo int
	lineNo := 1
	for n, c := range contents {
		charNo++
		if c == '\n' {
			inLineComment = false
			escapeNext = false
			lineNo++
			charNo = 0
			// Merge trailing newlines onto *previous* statement
			if lastResult := len(result) - 1; lastResult >= 0 && startStatement == n {
				result[lastResult].Text += "\n"
				startStatement++
				stmt.LineNo++
				stmt.CharNo = 1
			}
			continue
		} else if inLineComment {
			continue
		} else if inCComment {
			if c == '/' && contents[n-1] == '*' {
				inCComment = false
			}
			continue
		} else if escapeNext {
			escapeNext = false
			continue
		} else if inQuote > 0 {
			if c == inQuote {
				// We're in a quote, and this char AND next char are that quote: this
				// quote effectively escapes the next one
				if n < last {
					if next, _ := utf8.DecodeRuneInString(contents[n+1:]); next == inQuote {
						escapeNext = true
					}
				}
				if !escapeNext {
					inQuote = 0
				}
			}
			continue
		}

		// Handle comments
		if c == '#' {
			inLineComment = true
			continue
		} else if n > 0 && c == '*' && contents[n-1] == '/' {
			inCComment = true
			continue
		} else if n > 1 && c == ' ' && contents[n-1] == '-' && contents[n-2] == '-' {
			inLineComment = true
			continue
		}

		if !unicode.IsSpace(c) && !inRelevant {
			// Move intervening whitespace and comments to a separate "statement", so
			// that future string manipulations on the file contents do not cause that
			// whitespace to be lost
			if startStatement < n {
				stmt.Text = contents[startStatement:n]
				result = append(result, stmt)
				stmt = Statement{
					File:   sf,
					LineNo: lineNo,
					CharNo: charNo,
				}
			}
			inRelevant = true
		}

		switch c {
		case ';':
			stmt.Text = contents[startStatement : n+1]
			result = append(result, stmt)
			stmt = Statement{
				File:   sf,
				LineNo: lineNo,
				CharNo: charNo + 1,
			}
			startStatement = n + 1
			inRelevant = false
		case '\\':
			escapeNext = true
		case '"', '`', '\'':
			inQuote = c
		}
	}

	if inQuote != 0 {
		err = fmt.Errorf("File %s has unterminated quote %c", sf, inQuote)
	} else if inCComment {
		err = fmt.Errorf("File %s has unterminated C-style comment", sf)
	}

	// Keep any dangling statement
	stmt.Text = contents[startStatement:]
	if len(stmt.Text) > 0 {
		result = append(result, stmt)
	}
	return result, err
}
