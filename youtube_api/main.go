package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

const (
	dbUser = "anurag"
	dbName = "youtube"
)

var db *sql.DB

type video struct {
	title       string
	description string
	publishTime time.Time
	thumbnail   string
}

func main() {
	// Connect to the database
	dbPassword := os.Getenv("DB_PASSWORD")
	API_KEY := os.Getenv("API_KEY")

	var err error
	psqlInfo := fmt.Sprintf("host=localhost user=%s password=%s dbname=%s sslmode=disable",
		dbUser, dbPassword, dbName)
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create the table to store videos if it doesn't exist
	createTable := `
    CREATE TABLE IF NOT EXISTS videos (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        publish_time TIMESTAMPTZ NOT NULL,
        thumbnail TEXT NOT NULL
    );
    `
	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatal(err)
	}

	// Create the indexes
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS publish_time_idx ON videos (publish_time)")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client, err := youtube.NewService(ctx, option.WithAPIKey(API_KEY))
	if err != nil {
		log.Fatalf("Error creating YouTube client: %v", err)
	}

	// Fetch videos in the background
	go fetchVideos(client, ctx)

	// Start the server
	http.HandleFunc("/videos", handleSearch)
	http.ListenAndServe(":8080", nil)
}

var apiKeys = []string{"API_KEY1", "API_KEY2", "API_KEY3"}
var currentKeyIndex = 0

func fetchVideos(client *youtube.Service, ctx context.Context) {
	for {
		// Search for new videos
		searchCall := client.Search.List([]string{"id,snippet"}).Q("football in:title OR football in:description").Type("video")
		searchResponse, err := searchCall.Do()
		if err != nil {

			if err == youtube.QuotaExceededError {
				// The current key has exceeded its quota
				currentKeyIndex++
				if currentKeyIndex >= len(apiKeys) {
					// All keys have been used, reset the index
					currentKeyIndex = 0
				}
				//create new client with new key
				client, err = youtube.NewService(ctx, option.WithAPIKey(apiKeys[currentKeyIndex]))
				if err != nil {
					log.Println(err)
					time.Sleep(10 * time.Second)
					continue
				}
			} else {
				log.Println(err)
				time.Sleep(10 * time.Second)
				continue
			}
		}
		// Insert the videos into the database
		for _, item := range searchResponse.Items {
			v := &video{
				title:       item.Snippet.Title,
				description: item.Snippet.Description,
				thumbnail:   item.Snippet.Thumbnails.Default.Url,
			}
			// parse the PublishedAt time string to time.Time
			pubTime, err := time.Parse(time.RFC3339, item.Snippet.PublishedAt)
			if err != nil {
				log.Println(err)
				continue
			}
			v.publishTime = pubTime

			// Insert the video into the database
			_, err = db.Exec("INSERT INTO videos (title, description, publish_time, thumbnail) VALUES ($1, $2, $3, $4)", v.title, v.description, v.publishTime, v.thumbnail)
			if err != nil {
				log.Println(err)
			}
		}

		// check if there is a next page of result
		if searchResponse.NextPageToken != "" {
			searchCall = searchCall.PageToken(searchResponse.NextPageToken)
		} else {
			// the async call/ goroutine for the calling videos after 10 sec
			time.Sleep(10 * time.Second)
		}
	}
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	title := r.URL.Query().Get("title")
	description := r.URL.Query().Get("description")
	if title == "" && description == "" {
		http.Error(w, http.StatusText(400), 400)
		return
	}

	var rows *sql.Rows
	var err error
	if title != "" && description != "" {
		// Fetch the matching videos from the database
		rows, err = db.Query("SELECT title, description, publish_time, thumbnail FROM videos WHERE title ILIKE $1 AND description ILIKE $2 ORDER BY publish_time DESC", "%"+title+"%", "%"+description+"%")
	} else if title != "" {
		// Fetch the matching videos from the database for the title
		rows, err = db.Query("SELECT title, description, publish_time, thumbnail FROM videos WHERE title ILIKE $1 ORDER BY publish_time DESC", "%"+title+"%")
	} else {
		// Fetch the matching videos from the database for the description
		rows, err = db.Query("SELECT title, description, publish_time, thumbnail FROM videos WHERE description ILIKE $1 ORDER BY publish_time DESC", "%"+description+"%")
	}

	if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}
	defer rows.Close()

	// Convert the rows to a slice of videos
	var videos []video
	for rows.Next() {
		var v video
		err := rows.Scan(&v.title, &v.description, &v.publishTime, &v.thumbnail)
		if err != nil {
			http.Error(w, http.StatusText(500), 500)
			return
		}
		videos = append(videos, v)
	}

	// Return the videos as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(videos)

}
