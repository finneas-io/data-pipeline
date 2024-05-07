package bucket

type Bucket interface {
	PutObject(key string, data []byte) error
	GetObject(key string) ([]byte, error)
}
