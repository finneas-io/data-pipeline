package auth

import (
	"errors"
	"time"

	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/domain/user"
	"github.com/google/uuid"
)

type Service interface {
	LoginUser(username, password string) (string, error)
	ValidateSession(token string) (uuid.UUID, error)
}

var InvalidCredsErr error = errors.New("Invalid Credentials")
var ExpiredSessErr error = errors.New("Session has been expired")

type service struct {
	db     database.Database
	logger logger.Logger
}

func New(db database.Database, l logger.Logger) *service {
	return &service{db: db, logger: l}
}

func (s *service) LoginUser(username, password string) (string, error) {

	u, err := s.db.GetUser(username)
	if err != nil {
		if err == database.NotFoundErr {
			return "", InvalidCredsErr
		}
		s.logger.Log(err.Error())
		return "", err
	}

	encPass, err := user.EncryptSHA256(password)
	if err != nil {
		s.logger.Log(err.Error())
		return "", err
	}
	if u.Password != encPass {
		return "", InvalidCredsErr
	}

	token, err := user.GenerateToken(time.Now())
	if err != nil {
		s.logger.Log(err.Error())
		return "", err
	}

	encToken, err := user.EncryptSHA256(token)
	if err != nil {
		s.logger.Log(err.Error())
		return "", err
	}

	err = s.db.InsertSession(&user.Session{
		Token:     encToken,
		User:      u,
		ExpiresAt: time.Now().AddDate(0, 0, 365),
	})
	if err != nil {
		s.logger.Log(err.Error())
		return "", err
	}

	return token, nil
}

func (s *service) ValidateSession(token string) (uuid.UUID, error) {

	encToken, err := user.EncryptSHA256(token)
	if err != nil {
		s.logger.Log(err.Error())
		return uuid.UUID{}, err
	}

	sess, err := s.db.GetSession(encToken)
	if err != nil {
		if err == database.NotFoundErr {
			return uuid.UUID{}, ExpiredSessErr
		}
		s.logger.Log(err.Error())
		return uuid.UUID{}, err
	}

	if sess.ExpiresAt.Before(time.Now()) {
		// try to delete the expired session
		err := s.db.DeleteSession(encToken)
		if err != nil {
			s.logger.Log(err.Error())
		}
		return uuid.UUID{}, ExpiredSessErr
	}

	return sess.User.Id, nil
}
