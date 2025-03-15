package database

import (
	"context"
	"log"
	"time"

	"graphql/graph/model"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

var connectionString string ="mongodb+srv://adamskahiga:36596768Bantu.@cluster0.vvxjk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

type DB struct {
	client *mongo.Client
}
type DeleteJobResponse struct {
	JobID string `json:"job_id"`
}


func Connect() *DB {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(connectionString))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	return &DB{client: client}
}

func (db *DB) GetJobById(id string) *model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobCollection := db.client.Database("jobs").Collection("jobListings")
	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	var jobListing model.JobListing
	err := jobCollection.FindOne(ctx, filter).Decode(&jobListing)
	if err != nil {
		log.Fatal(err)
	}
	return &jobListing
}

func (db *DB) GetJobs() []*model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobCollection := db.client.Database("jobs").Collection("jobListings")
	var jobListings []*model.JobListing
	cursor, err := jobCollection.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	if err = cursor.All(context.TODO(), &jobListings); err != nil {
		log.Fatal(err)
	}

	return jobListings
}

func (db *DB) CreateJobListing(jobInfo *model.CreateJobListingInput) *model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobCollection := db.client.Database("jobs").Collection("jobListings")

	insert, err := jobCollection.InsertOne(ctx, bson.M{"title": jobInfo.Title, "description": jobInfo.Description, "url": jobInfo.URL, "company": jobInfo.Company})

	if err != nil {
		log.Fatal(err)
	}

	InsertedID := insert.InsertedID.(primitive.ObjectID).Hex()
	returnJobListing := model.JobListing{ID: InsertedID, Title: jobInfo.Title, Description: jobInfo.Description, URL: jobInfo.URL, Company: jobInfo.Company}

	return &returnJobListing
}

func (db *DB) UpdateJobListing(jobId string, jobInfo *model.UpdateJobListingInput) *model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobCollection := db.client.Database("jobs").Collection("jobListings")

	updateJobInfo := bson.M{}

	if jobInfo.Title != nil {
		updateJobInfo["title"] = *jobInfo.Title
	}
	if jobInfo.Description != nil {
		updateJobInfo["description"] = *jobInfo.Description
	}
	if jobInfo.URL != nil {
		updateJobInfo["url"] = *jobInfo.URL
	}

	_id, _ := primitive.ObjectIDFromHex(jobId)
	filter := bson.M{"_id": _id}
	update := bson.M{"$set": updateJobInfo}

	results, _ := jobCollection.UpdateOne(ctx, filter, update)
	
	var jobListing model.JobListing
	
	if results.ModifiedCount == 0 {
		log.Fatal("No job listing found with the given ID")
	} else {
		err := jobCollection.FindOne(ctx, filter).Decode(&jobListing)
		if err != nil {
			log.Fatal(err)
		}
	}
	return &jobListing
}

func (db *DB) DeleteJobListing(jobId string) *model.DeleteJobResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobCollection := db.client.Database("jobs").Collection("jobListings")
	_id, _ := primitive.ObjectIDFromHex(jobId)
	filter := bson.M{"_id": _id}
	_, err := jobCollection.DeleteOne(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	return &model.DeleteJobResponse{DeleteJobID: jobId}

}