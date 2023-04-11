package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/ory/dockertest"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MONGODB_USERNAME = "root"
	MONGODB_PASSWORD = "mypassword"
	MONGODB_DATABASE = "testdb"
)

var mongodbUri string

var db *sql.DB

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	// uses pool to try to connect to Docker
	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("mongo", "latest", []string{
		"MONGO_INITDB_ROOT_USERNAME=" + MONGODB_USERNAME,
		"MONGO_INITDB_ROOT_PASSWORD=" + MONGODB_PASSWORD,
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	mongodbUri = fmt.Sprintf("mongodb://%s:%s@host.docker.internal:%s/?authSource=admin", MONGODB_USERNAME, MONGODB_PASSWORD, resource.GetPort("27017/tcp"))
	fmt.Printf("mongodb uri: '%s'\n", mongodbUri)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		clientOptions := options.Client().ApplyURI(mongodbUri)
		client, err := mongo.Connect(context.TODO(), clientOptions)
		if err != nil {
			return err
		}
		return client.Ping(context.TODO(), nil)
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	resource.Expire(120)

	defer pool.Purge(resource)

	code := m.Run()

	os.Exit(code)
}

func TestClient(t *testing.T) {
	client, err := NewDbClient(mongodbUri, MONGODB_DATABASE)
	if err != nil {
		t.Fatalf("cannot connect to mongodb: %v", err)
	}
	data, err := loadRelativeFile("../../mockdata/movies.json")
	if err != nil {
		t.Fatalf("cannot load movies data file: %v", err)
	}

	var movies []Movie
	err = json.Unmarshal(data, &movies)
	if err != nil {
		t.Fatalf("cannot unmarshal movies data: %v", err)
	}

	err = client.InsertMovies(movies)
	if err != nil {
		t.Fatalf("cannot insert movies: %v", err)
	}

	first := movies[0]
	mov, err := client.GetMovieById(first.Id)
	if err != nil {
		t.Fatalf("cannot insert movies: %v", err)
	}

	if mov.Title != first.Title || mov.Genre != first.Genre || mov.Director != first.Director {
		t.Fatalf("different data: %v != %v", mov, first)
	}
}

func loadRelativeFile(p string) ([]byte, error) {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		return nil, fmt.Errorf("cannot obtain caller information")
	}
	filepath := path.Join(path.Dir(filename), p)
	return os.ReadFile(filepath)
}
