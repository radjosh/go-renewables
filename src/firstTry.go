package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
)

type Row struct {
	Country string
	Year string
	EnergyType string
}

var Rows = make(map[string]Row)

func getData(w http.ResponseWriter, r *http.Request) {
	rowPtr := new(Row)
	rowPtr.Country = "USA"
	rowPtr.Year = "1993"
	rowPtr.EnergyType = "nuclear"
	Rows[rowPtr.Country] = *rowPtr
	rowPtr = new(Row)
	rowPtr.Country = "Germany"
	rowPtr.Year = "2024"
	rowPtr.EnergyType = "fusion"
	Rows[rowPtr.Country] = *rowPtr

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(Rows)
}

func main() {
	http.HandleFunc("/getData", getData)
	err := http.ListenAndServe(":8080", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Println("server closed")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
