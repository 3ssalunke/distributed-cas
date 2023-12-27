package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const defaultRootFolderName = "ssnetwork"

type PathKey struct {
	Pathname string
	Filename string
}

type PathTransformFunc func(string) PathKey

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.Pathname, "/")
	if len(paths) == 0 {
		return ""
	}

	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("/%s/%s", p.Pathname, p.Filename)
}

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blockSize := 5
	sliceLen := len(hashStr) / blockSize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		Pathname: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type StoreOpts struct {
	Root              string
	PathTransformFunc PathTransformFunc
}

type Store struct {
	StoreOpts
}

func DefaultPathTransformFunc(key string) PathKey {
	return PathKey{
		Pathname: key,
		Filename: key,
	}
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}

	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Has(id string, key string) bool {
	pathKey := s.PathTransformFunc(key)
	root := strings.Split(s.Root, ":")[1]
	fullPathWithRoot := fmt.Sprintf("/%s/%s/%s", root, id, pathKey.FullPath())

	_, err := os.Stat(fullPathWithRoot)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) openFileForWriting(id string, key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	root := strings.Split(s.Root, ":")[1]
	pathNameWithRoot := fmt.Sprintf("/%s/%s/%s", root, id, pathKey.Pathname)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPathWithRoot := fmt.Sprintf("/%s/%s/%s", root, id, pathKey.FullPath())
	return os.Create(fullPathWithRoot)
}

func (s *Store) writeStream(id string, key string, reader io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	return io.Copy(f, reader)
}

func (s *Store) Write(id string, key string, reader io.Reader) (int64, error) {
	return s.writeStream(id, key, reader)
}

func (s *Store) readStream(id string, key string) (int64, io.Reader, error) {
	pathKey := s.PathTransformFunc(key)
	root := strings.Split(s.Root, ":")[1]
	fullPathWithRoot := fmt.Sprintf("/%s/%s/%s", root, id, pathKey.FullPath())

	f, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fs, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fs.Size(), f, nil
}

func (s *Store) Read(id string, key string) (int64, io.Reader, error) {
	return s.readStream(id, key)
}

func (s *Store) Delete(id string, key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	}()

	root := strings.Split(s.Root, ":")[1]
	firstPathnameWithRoot := fmt.Sprintf("%s/%s/%s", root, id, pathKey.FirstPathName())

	return os.RemoveAll(firstPathnameWithRoot)
}

func (s *Store) WriteDecrypt(encKey []byte, id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}

	n, err := copyDecrypt(encKey, r, f)
	return int64(n), err
}
