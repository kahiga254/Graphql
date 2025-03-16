package database

import (
	"context"
	"log"
	"time"
	"fmt"	
	

	"graphql/graph/model"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
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

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	return &DB{client: client}
}

func (db *DB) GetJobById(id string) (*model.JobListing, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    jobCollection := db.client.Database("jobs").Collection("jobListings")
    log.Printf("Querying database: %s, collection: %s", db.client.Database("jobs").Name(), jobCollection.Name())
    count, err := jobCollection.CountDocuments(ctx, bson.M{})
    if err != nil {
        log.Printf("❌ Count error: %v", err)
    }
    log.Printf("Total documents in jobListings: %d", count)

    cursor, err := jobCollection.Find(ctx, bson.M{})
    if err != nil {
        log.Printf("❌ Find error: %v", err)
    }
    var results []bson.M
    if err = cursor.All(ctx, &results); err != nil {
        log.Printf("❌ Cursor error: %v", err)
    }
    for _, result := range results {
        switch v := result["_id"].(type) {
        case primitive.ObjectID:
            log.Printf("Document ID: %s", v.Hex())
        case string:
            log.Printf("Document ID (string): %s", v)
        default:
            log.Printf("Document ID (unexpected type): %v", v)
        }
    }

    _id, err := primitive.ObjectIDFromHex(id)
    if err != nil {
        return nil, fmt.Errorf("invalid ID format: %v", err)
    }

    log.Printf("🔍 Searching for job with ID: %v", _id)

    filter := bson.M{"_id": _id}
    singleResult := jobCollection.FindOne(ctx, filter)
    raw, err := singleResult.Raw()
    if err != nil {
        if err == mongo.ErrNoDocuments {
            log.Println("❌ Job not found")
            return nil, fmt.Errorf("job not found")
        }
        log.Printf("❌ Database error: %v", err)
        return nil, err
    }
    log.Printf("Raw result: %v", raw)

    var jobListing model.JobListing
    err = singleResult.Decode(&jobListing)
    if err != nil {
        log.Printf("❌ Decode error: %v", err)
        return nil, err
    }

	
    log.Printf("✅ Job found: %+v", jobListing)
    return &jobListing, nil
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