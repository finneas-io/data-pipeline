package bucket

type Bucket interface {
	GetObject(key string) ([]byte, error)
	PutObject(key string, data []byte) error
}
