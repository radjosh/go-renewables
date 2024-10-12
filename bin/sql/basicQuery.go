package main

import (
	"context"
	"fmt"

	"github.com/caarlos0/env/v6"
	"github.com/jackc/pgx/v5"
)

type Config struct {
	User string `env:"DB_USER,required"`
	Password string `env:"DB_PASSWORD,required"`
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
		   "@172.18.0.3:5432/go"
	conn, err := pgx.Connect(context.Background(), sqlURL)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func queryData(conn *pgx.Conn) {
	rows, err := conn.Query(context.Background(),
		"SELECT Country, Year, EnergyType from energy")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var Country string
		var Year int
		var EnergyType string
		err := rows.Scan(&Country, &Year, &EnergyType)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Country: %s, Year: %d, Energy Type\n", Country, Year, EnergyType)
	}
}


func main() {
	conn, err := connect()
	if err != nil {
		panic(err)
	}
	queryData(conn)

}
