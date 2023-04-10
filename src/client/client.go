package client

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Movie struct {
	Id       string `bson:"_id"`
	Title    string `bson:"title"`
	Year     int    `bson:"year"`
	Director string `bson:"director"`
	Genre    string `bson:"genre"`
}

type DbClient struct {
	client   *mongo.Client
	database string
}

func NewDbClient(uri string, database string) (*DbClient, error) {
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return nil, err
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, err
	}
	fmt.Println("Connected to MongoDB!")
	return &DbClient{
		client:   client,
		database: database,
	}, nil
}

func (c *DbClient) Collection(name string) *mongo.Collection {
	return c.client.Database(c.database).Collection(name)
}

func (c *DbClient) InsertMovies(movies []Movie) error {
	_, err := c.Collection("movies").InsertMany(context.TODO(), toSliceOfInterface(movies))
	return err
}

func (c *DbClient) GetMovieById(id string) (mov Movie, err error) {
	res := c.Collection("movies").FindOne(context.TODO(), bson.M{"_id": id})
	err = res.Err()
	if err != nil {
		return mov, fmt.Errorf("cannot find movie '%v': %w", id, err)
	}
	err = res.Decode(&mov)
	if err != nil {
		return mov, fmt.Errorf("cannot decode movie '%v': %w", id, err)
	}
	return mov, nil
}

func toSliceOfInterface[T interface{}](d []T) []interface{} {
	res := make([]interface{}, len(d))
	for i, m := range d {
		res[i] = m
	}
	return res
}
