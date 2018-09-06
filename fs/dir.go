package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/skeema/mybase"
)

// Dir represents a directory path
type Dir string

type ParsedDir struct {
	Schemas []ParsedSchema
}

type ParsedSchema struct {
	Name         string
	CharSet      string
	Collation    string
	CreateTables map[string]*Statement // keyed by table name
}

// NewDir converts its input into a clean absolute path and returns it as a Dir.
func NewDir(path string) (Dir, error) {
	cleaned, err := filepath.Abs(filepath.Clean(path))
	return Dir(cleaned), err
}

// BaseName returns the name of the directory without the rest of its path.
func (dir Dir) BaseName() string {
	return path.Base(string(dir))
}

// CreateIfMissing creates the directory if it does not yet exist.
func (dir Dir) CreateIfMissing() (created bool, err error) {
	if exists, err := dir.Exists(); exists || err != nil {
		return false, err
	}
	err = os.MkdirAll(string(dir), 0777)
	if err != nil {
		return false, fmt.Errorf("Unable to create directory %s: %s", dir, err)
	}
	return true, nil
}

// Exists returns true if the dir already exists in the filesystem
func (dir Dir) Exists() (bool, error) {
	fi, err := os.Stat(string(dir))
	if err == nil {
		if !fi.IsDir() {
			err = fmt.Errorf("Path %s exists but is not a directory", dir)
		}
		return true, err
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Delete unlinks the directory and all files within.
func (dir Dir) Delete() error {
	return os.RemoveAll(string(dir))
}

// HasFile returns true if the specified filename exists in dir.
func (dir Dir) HasFile(name string) (bool, error) {
	_, err := os.Stat(path.Join(string(dir), name))
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// HasOptionFile returns true if the directory contains a .skeema option file
// and the dir isn't hidden. (We do not parse .skeema in hidden directories,
// to avoid issues with SCM metadata.)
func (dir Dir) HasOptionFile() (bool, error) {
	if dir.BaseName()[0] == '.' {
		return false, nil
	}
	return dir.HasFile(".skeema")
}

// Subdirs returns a slice of direct subdirectories of the current dir. An
// error will be returned if there are problems reading the directory list.
func (dir Dir) Subdirs() ([]Dir, error) {
	fileInfos, err := ioutil.ReadDir(string(dir))
	if err != nil {
		return nil, err
	}
	result := make([]Dir, 0, len(fileInfos))
	for _, fi := range fileInfos {
		if fi.IsDir() {
			subdir := Dir(path.Join(string(dir), fi.Name()))
			result = append(result, subdir)
		}
	}
	return result, nil
}

// CreateSubdir creates and returns a new subdir of the receiver dir.
func (dir Dir) CreateSubdir(name string) (Dir, error) {
	subdir, err := NewDir(path.Join(string(dir), name))
	if err != nil {
		return Dir(""), err
	}
	if created, err := subdir.CreateIfMissing(); err != nil {
		return Dir(""), err
	} else if !created {
		return Dir(""), fmt.Errorf("Directory %s already exists", subdir)
	}
	return subdir, nil
}

// OptionFile returns a pointer to a mybase.File for this directory, representing
// the dir's .skeema file, if one exists. The file will be read and parsed; any
// errors in either process will be returned.
// If there is no option file in this dir, both the returned values will be nil;
// this is not considered an error.
func (dir Dir) OptionFile(baseConfig *mybase.Config) (*mybase.File, error) {
	if has, err := dir.HasOptionFile(); !has || err != nil {
		return nil, err
	}
	f := mybase.NewFile(string(dir), ".skeema")
	if err := f.Read(); err != nil {
		return nil, err
	}
	if err := f.Parse(baseConfig); err != nil {
		return nil, err
	}
	_ = f.UseSection(baseConfig.Get("environment")) // we don't care if the section doesn't exist
	return f, nil
}

// SQLFiles returns a slice of SQLFiles, representing any files in the directory
// matching name *.sql. Does not recursively search subdirs and does not parse
// or validate the SQLFile contents in any way. An error will only be returned
// if the directory cannot be read.
func (dir Dir) SQLFiles() ([]SQLFile, error) {
	fileInfos, err := ioutil.ReadDir(string(dir))
	if err != nil {
		return nil, err
	}
	result := make([]SQLFile, 0, len(fileInfos))
	for _, fi := range fileInfos {
		name := fi.Name()
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			fi, err = os.Stat(path.Join(string(dir), name))
			if err != nil {
				// ignore symlink pointing to a missing path
				continue
			}
		}
		if strings.HasSuffix(name, ".sql") && fi.Mode().IsRegular() {
			sf := SQLFile{
				Dir:      dir,
				FileName: name,
			}
			result = append(result, sf)
		}
	}
	return result, nil
}

// Parse reads the .skeema and *.sql files in the dir, and returns appropriate
// parsed representations of them.
func (dir Dir) Parse(baseConfig *mybase.Config) (cfg *mybase.Config, pd ParsedDir, err error) {
	var optionFile *mybase.File
	var sqlFiles []SQLFile
	//var parsedSchemas []ParsedSchema
	//var statements []Statement

	// Parse the option file, if one exists
	optionFile, err = dir.OptionFile(baseConfig)
	if err != nil {
		return
	} else if optionFile == nil {
		cfg = baseConfig
	} else {
		cfg = baseConfig.Clone()
		cfg.AddSource(optionFile)
	}

	// Parse any *.sql files to build a ParsedDir
	if sqlFiles, err = dir.SQLFiles(); err != nil {
		return
	}
	for _, sf := range sqlFiles {
		_, err = sf.Parse()
		if err != nil {
			return
		}

		//pd.Schemas = append(pd.Schemas, parsedSchemas...)
	}

	return
}
