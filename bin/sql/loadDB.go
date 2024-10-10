package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/caarlos0/env/v6"
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
	sqlURL := "postgres://" + cfg.User + ":" + cfg.Password + "@172.18.0.3:5432/go"
	conn, err := pgx.Connect(context.Background(), sqlURL)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func queryData(conn *pgx.Conn) {
	rows, err := conn.Query(context.Background(),
		"SELECT * from test")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var bar int
		var baz int
		err := rows.Scan(&bar, &baz)
		if err != nil {
			panic(err)
		}
		fmt.Printf("BAR: %d, BAZ: %d\n", bar, baz)
	}
}


func main() {
	conn, err := connect() // should we send it cfg? how w/o mixing named and unnamed params?
	if err != nil {
		panic(err)
	}
	queryData(conn)

}
