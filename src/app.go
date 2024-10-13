package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/caarlos0/env/v6"
	"github.com/jackc/pgx/v5"
)

type Config struct {
	User string `env:"DB_USER,required"`
	Password string `env:"DB_PASSWORD,required"`
}

type DataPoint struct {
	Country string
	Year int
	EnergyType string
}

func connect() (*pgx.Conn, error) {
	cfg := Config{}
	err := env.Parse(&cfg)
	if err != nil {
		panic(err)
	}
	sqlURL := "postgres://" + 
		   cfg.User + 
		   ":" + 
		   cfg.Password + 
		   "@172.18.0.3:5432/go" // url provided by docker network
	conn, err := pgx.Connect(context.Background(), sqlURL)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func queryData(conn *pgx.Conn, dataPoints *[]DataPoint) {
	rows, err := conn.Query(context.Background(),
		"SELECT Country, Year, EnergyType from energy")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dataPoint DataPoint

		// err := rows.Scan(&Country, &Year, &EnergyType)
		err := rows.Scan(&dataPoint.Country, &dataPoint.Year, &dataPoint.EnergyType)
		if err != nil {
			panic(err)
		}
		*dataPoints = append(*dataPoints, dataPoint)
		// fmt.Printf("Country: %s, Year: %d, Energy Type: %s\n", Country, Year, EnergyType)
		// fmt.Printf("Country: %s, Year: %d, Energy Type: %s\n", dataPoint.Country, dataPoint.Year, dataPoint.EnergyType)
	}
}

func query(w http.ResponseWriter, r *http.Request) {
	var dataPoints []DataPoint 

	conn, err := connect()
	if err != nil {
		panic(err)
	}
	queryData(conn, &dataPoints) //slices pass by ref but they dont modify the original unless you sent a pointer or return and assign
	fmt.Println(len(dataPoints))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(dataPoints)
	// for _, dataPoint := range dataPoints {
	// 	fmt.Printf("Country: %s, Year: %d, Energy Type: %s\n", dataPoint.Country, dataPoint.Year, dataPoint.EnergyType)
	// }
}

func main() {
	http.HandleFunc("/query", query)
	err := http.ListenAndServe(":8080", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Println("server closed")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
