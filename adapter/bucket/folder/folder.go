package folder

import (
	"os"
)

type folder struct {
	path string
}

func New(path string) *folder {
	return &folder{path: path}
}

func (f *folder) GetObject(key string) ([]byte, error) {
	return os.ReadFile(f.path + "/" + key)
}

func (f *folder) PutObject(key string, data []byte) error {
	return os.WriteFile(f.path+"/"+key, data, 0777)
}
