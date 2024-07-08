package vault

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
)

type vault struct {
	name   string
	client *glacier.Glacier
}

func New(awsSession *session.Session, name string) *vault {
	return &vault{client: glacier.New(awsSession), name: name}
}

func (v *vault) GetObject(key string) ([]byte, error) {
	return nil, nil
}

func (v *vault) PutObject(key string, data []byte) error {
	input := &glacier.UploadArchiveInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(v.name),
		Body:      bytes.NewReader(data),
	}
	_, err := v.client.UploadArchive(input)
	if err != nil {
		return err
	}
	return nil
}
