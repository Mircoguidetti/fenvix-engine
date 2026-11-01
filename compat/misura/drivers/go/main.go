// Driver Go: connessione, CRUD, aggregazione, transazione, change stream. Uso: go run . <uri>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/version"
)

var out = map[string]any{"driver": "go mongo-driver " + version.Driver, "steps": map[string]string{}}

func step(name string, fn func() error) {
	steps := out["steps"].(map[string]string)
	if err := fn(); err != nil {
		s := err.Error()
		if len(s) > 200 {
			s = s[:200]
		}
		steps[name] = "ERRORE: " + s
	} else {
		steps[name] = "ok"
	}
}

func main() {
	uri := "mongodb://localhost:27018/?replicaSet=rs0"
	if len(os.Args) > 1 {
		uri = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		panic(err)
	}
	c := client.Database("drv_go").Collection("t")
	step("connessione", func() error {
		return client.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err()
	})
	step("crud", func() error {
		if _, err := c.DeleteMany(ctx, bson.D{}); err != nil {
			return err
		}
		if _, err := c.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: 1}},
			bson.D{{Key: "_id", Value: 2}, {Key: "v", Value: 2}}, bson.D{{Key: "_id", Value: 3}, {Key: "v", Value: 3}}}); err != nil {
			return err
		}
		var d bson.M
		if err := c.FindOne(ctx, bson.D{{Key: "_id", Value: 2}}).Decode(&d); err != nil {
			return err
		}
		r, err := c.UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "v", Value: 10}}}})
		if err != nil || r.ModifiedCount != 1 {
			return fmt.Errorf("updateOne %v %v", r, err)
		}
		dr, err := c.DeleteOne(ctx, bson.D{{Key: "_id", Value: 2}})
		if err != nil || dr.DeletedCount != 1 {
			return fmt.Errorf("deleteOne %v %v", dr, err)
		}
		n, err := c.CountDocuments(ctx, bson.D{})
		if err != nil || n != 2 {
			return fmt.Errorf("count %d %v", n, err)
		}
		return nil
	})
	step("aggregazione", func() error {
		cur, err := c.Aggregate(ctx, mongo.Pipeline{{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "s", Value: bson.D{{Key: "$sum", Value: "$v"}}}}}}})
		if err != nil {
			return err
		}
		var res []bson.M
		if err := cur.All(ctx, &res); err != nil {
			return err
		}
		if fmt.Sprint(res[0]["s"]) != "14" {
			return fmt.Errorf("sum %v", res[0]["s"])
		}
		return nil
	})
	step("transazione", func() error {
		s, err := client.StartSession()
		if err != nil {
			return err
		}
		defer s.EndSession(ctx)
		_, err = s.WithTransaction(ctx, func(sc context.Context) (any, error) {
			if _, err := c.InsertOne(sc, bson.D{{Key: "_id", Value: 10}}); err != nil {
				return nil, err
			}
			return c.UpdateOne(sc, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "tx", Value: true}}}})
		})
		if err != nil {
			return err
		}
		n, _ := c.CountDocuments(ctx, bson.D{{Key: "$or", Value: bson.A{bson.D{{Key: "_id", Value: 10}}, bson.D{{Key: "tx", Value: true}}}}})
		if n != 2 {
			return fmt.Errorf("dopo commit %d", n)
		}
		return nil
	})
	step("indice e change stream", func() error {
		if _, err := c.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "v", Value: 1}}}); err != nil {
			return err
		}
		cs, err := c.Watch(ctx, mongo.Pipeline{})
		if err != nil {
			return err
		}
		defer cs.Close(ctx)
		if _, err := c.InsertOne(ctx, bson.D{{Key: "_id", Value: 20}}); err != nil {
			return err
		}
		tctx, tc := context.WithTimeout(ctx, 15*time.Second)
		defer tc()
		if !cs.Next(tctx) {
			return fmt.Errorf("nessun evento: %v", cs.Err())
		}
		if cs.Current.Lookup("operationType").StringValue() != "insert" {
			return fmt.Errorf("evento %v", cs.Current)
		}
		return nil
	})
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
	client.Disconnect(ctx)
}
