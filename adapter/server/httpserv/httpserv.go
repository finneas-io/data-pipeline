package httpserv

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/finneas-io/data-pipeline/service/auth"
	"github.com/finneas-io/data-pipeline/service/label"
	"github.com/google/uuid"
)

type httpServer struct {
	router *http.ServeMux
	port   int
	auth   auth.Service
	label  label.Service
}

func New(port int, authServ auth.Service, lblServ label.Service) *httpServer {
	s := &httpServer{port: port, auth: authServ, label: lblServ}
	router := http.NewServeMux()
	router.HandleFunc("/login", s.handleLogin)
	router.HandleFunc("/table", s.handleRandomTable)
	router.HandleFunc("/table/{id}", s.handleTableLabel)
	s.router = router
	return s
}

func (s *httpServer) Listen() error {
	return http.ListenAndServe(fmt.Sprintf(":%d", s.port), s.router)
}

func (s *httpServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body := struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if len(body.Username) < 1 {
		http.Error(w, "Missing username", http.StatusBadGateway)
		return
	}

	if len(body.Password) < 1 {
		http.Error(w, "Missing password", http.StatusBadGateway)
		return
	}

	token, err := s.auth.LoginUser(body.Username, body.Password)
	if err != nil {
		if err == auth.InvalidCredsErr {
			http.Error(w, err.Error(), http.StatusUnauthorized)
		} else {
			http.Error(w, "Internal Server", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, token)
}

func (s *httpServer) handleRandomTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	userId, err := s.handleAuth(w, r)
	if err != nil {
		return
	}

	tbl, err := s.label.RandomTable(userId)
	if err != nil {
		if err == label.NoTblLeftErr {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, "Internal Server", http.StatusNotFound)
		}
		return
	}

	b, err := json.Marshal(tbl)
	if err != nil {
		http.Error(w, "Internal Server", http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, string(b))
}

func (s *httpServer) handleTableLabel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	userId, err := s.handleAuth(w, r)
	if err != nil {
		return
	}

	body := struct {
		Label string `json:"label"`
	}{}

	err = json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	tblId, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		http.Error(w, "Invalid Table ID", http.StatusNotAcceptable)
		return
	}

	err = s.label.CreateLabel(tblId, userId, body.Label)
	if err != nil {
		if err == label.InvalidLabelErr {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, "Internal Server", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, "Success")
}

func (s *httpServer) handleAuth(w http.ResponseWriter, r *http.Request) (uuid.UUID, error) {

	// get issuer of the request an authenticate him
	token := r.Header.Get("X-Session-Token")
	if len(token) < 1 {
		http.Error(w, "Session token is missing in request header", http.StatusUnauthorized)
		return uuid.UUID{}, errors.New("")
	}
	userId, err := s.auth.ValidateSession(token)
	if err != nil {
		if err == auth.InvalidCredsErr || err == auth.ExpiredSessErr {
			http.Error(w, err.Error(), http.StatusUnauthorized)
		} else {
			http.Error(w, "Internal Server", http.StatusInternalServerError)
		}
		return uuid.UUID{}, err
	}

	return userId, nil
}
