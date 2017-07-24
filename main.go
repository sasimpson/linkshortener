package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

type Env struct {
	DB *sql.DB
}

type URL struct {
	ID      int64  `json:"id"`
	URL     string `json:"url"`
	Sig     string `json:"-"`
	TinyURL string `json:"tinyurl"`
}

func main() {
	var env Env

	db, err := sql.Open("sqlite3", "database.sqlite3")
	env.DB = db
	if err != nil {
		panic(err)
	}

	r := mux.NewRouter()
	r.Handle("/shorten", Shorten(&env))
	r.Handle("/{id}", Visit(&env))
	http.ListenAndServe(":5000", r)
}

func Shorten(env *Env) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var newURL URL
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&newURL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		newURL.Sig = fmt.Sprintf("%x", md5.Sum([]byte(newURL.URL)))
		//insert into database after verifying that the url doesn't exist.
		var count int
		log.Printf("checking to see if %s exists already", newURL.Sig)
		row := env.DB.QueryRow("SELECT COUNT(*) FROM urls WHERE sig = ?", newURL.Sig)
		err = row.Scan(&count)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Println(count)
		if count > 0 {
			http.Error(w, "url exists already", http.StatusConflict)
			return
		}

		result, err := env.DB.Exec("INSERT INTO urls (url, sig) values (?, ?)", newURL.URL, newURL.Sig)
		newURL.ID, _ = result.LastInsertId()
		w.WriteHeader(http.StatusCreated)
		encoder := json.NewEncoder(w)
		err = encoder.Encode(newURL)
		return
	})
}

func Visit(env *Env) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		if id, ok := vars["id"]; ok {
			var url URL
			row := env.DB.QueryRow("SELECT id, url, sig from urls where id = ?", id)
			err := row.Scan(&url.ID, &url.URL, &url.Sig)
			if err != nil {
				if err.Error() == "sql: no rows in result set" {
					http.Error(w, "Not Found", http.StatusNotFound)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if url.URL != "" {
				w.Header().Set("x-redirect-sum", url.Sig)
				http.Redirect(w, r, url.URL, http.StatusFound)
				return
			}
		}
		http.Error(w, "not found", http.StatusNotFound)
		return
	})
}
