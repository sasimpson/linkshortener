package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

type Env struct {
	DB *sql.DB
	MC *memcache.Client
}

type URL struct {
	ID      int64  `json:"id"`
	URL     string `json:"url"`
	Sig     string `json:"sig"`
	TinyURL string `json:"tinyurl"`
}

func main() {
	var env Env

	db, err := sql.Open("sqlite3", "database.sqlite3")
	env.DB = db

	env.MC = memcache.New("127.0.0.1:11211")

	if err != nil {
		panic(err)
	}

	r := mux.NewRouter()
	r.Handle("/shorten", Shorten(&env))
	r.Handle("/{id}", Visit(&env))
	http.ListenAndServe(":5000", r)
}

//Shorten - this handler will return json containing the url info
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

//Visit - this handler will take the id of the tinyurl and return a redirect to the proper url
func Visit(env *Env) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		if id, ok := vars["id"]; ok {
			var url URL
			//check for url in cache first:
			mcData, err := env.MC.Get(id)
			//hit:
			if err == nil {
				log.Printf("%s", mcData.Value)
				err = json.Unmarshal(mcData.Value, &url)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				if url.URL != "" {
					w.Header().Set("x-redirect-sum", url.Sig)
					w.Header().Set("x-cache-hit", "true")
					http.Redirect(w, r, url.URL, http.StatusFound)
					return
				}
			}

			//miss:
			if err == memcache.ErrCacheMiss {
				row := env.DB.QueryRow("SELECT id, url, sig from urls where id = ?", id)
				err = row.Scan(&url.ID, &url.URL, &url.Sig)
				if err != nil {
					if err.Error() == "sql: no rows in result set" {
						http.Error(w, "Not Found", http.StatusNotFound)
						return
					}
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if url.URL != "" {
					js, err := json.Marshal(url)
					if err == nil {
						env.MC.Set(&memcache.Item{Key: fmt.Sprintf("%d", url.ID), Value: js})
					}
					w.Header().Set("x-redirect-sum", url.Sig)
					w.Header().Set("x-cache-hit", "false")
					http.Redirect(w, r, url.URL, http.StatusFound)
					return
				}
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		http.Error(w, "not found", http.StatusNotFound)
		return
	})
}
