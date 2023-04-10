package main

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/sandrolain/go-ci-example/src/client"
)

func main() {
	c, err := client.NewDbClient(os.Getenv("MONGODB_URI"), os.Getenv("MONGODB_DB"))
	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(os.Stdin)
	var data []byte
	_, err = reader.Read(data)
	if err != nil {
		panic(err)
	}

	var movies []client.Movie
	err = json.Unmarshal(data, &movies)
	if err != nil {
		panic(err)
	}

	err = c.InsertMovies(movies)
}
