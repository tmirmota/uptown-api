package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"os"

	"google.golang.org/appengine"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var db *sql.DB

func main() {
	datastoreName := os.Getenv("POSTGRES_CONNECTION")

	var err error
	db, err = sql.Open("postgres", datastoreName)
	checkErr(err)

	r := mux.NewRouter()

	r.HandleFunc("/census-tract", censusTracts).Queries("swlng", "{swlng}", "swlat", "{swlat}", "nelng", "{nelng}", "nelat", "{nelat}")
	r.HandleFunc("/property-tax", propertyTax).Queries("pcoord", "{pcoord}")

	http.Handle("/", r)

	appengine.Main()
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

type property struct {
	LAND_COORDINATE            int
	CURRENT_LAND_VALUE         int
	BIG_IMPROVEMENT_YEAR       int
	CURRENT_IMPROVEMENT_VALUE  int
	PREVIOUS_IMPROVEMENT_VALUE int
	PREVIOUS_LAND_VALUE        int
	TAX_ASSESSMENT_YEAR        int
	TAX_LEVY                   int
	LEGAL_TYPE                 string
	PID                        string
	FROM_CIVIC_NUMBER          int
	TO_CIVIC_NUMBER            int
	PROPERTY_POSTAL_CODE       string
	STREET_NAME                string
	YEAR_BUILT                 int
	ZONE_CATEGORY              string
	ZONE_NAME                  string
}

func propertyTax(w http.ResponseWriter, r *http.Request) {
	pcoord := mux.Vars(r)["pcoord"]

	rows, err := db.Query(`
		SELECT 
			LAND_COORDINATE,
			CURRENT_LAND_VALUE,
			BIG_IMPROVEMENT_YEAR,
			CURRENT_IMPROVEMENT_VALUE,
			PREVIOUS_IMPROVEMENT_VALUE,
			PREVIOUS_LAND_VALUE,
			TAX_ASSESSMENT_YEAR,
			TAX_LEVY,
			LEGAL_TYPE,
			PID,
			FROM_CIVIC_NUMBER,
			TO_CIVIC_NUMBER,
			PROPERTY_POSTAL_CODE,
			STREET_NAME,
			YEAR_BUILT,
			ZONE_CATEGORY,
			ZONE_NAME
		FROM tbl_property_taxes 
		WHERE LAND_COORDINATE in ($1)`, pcoord)
	checkErr(err)

	defer rows.Close()

	properties := make([]property, 0)

	for rows.Next() {
		prop := property{}

		err = rows.Scan(&prop.LAND_COORDINATE, &prop.CURRENT_LAND_VALUE, &prop.BIG_IMPROVEMENT_YEAR, &prop.CURRENT_IMPROVEMENT_VALUE, &prop.PREVIOUS_IMPROVEMENT_VALUE, &prop.PREVIOUS_LAND_VALUE, &prop.TAX_ASSESSMENT_YEAR, &prop.TAX_LEVY, &prop.LEGAL_TYPE, &prop.PID, &prop.FROM_CIVIC_NUMBER, &prop.TO_CIVIC_NUMBER, &prop.PROPERTY_POSTAL_CODE, &prop.STREET_NAME, &prop.YEAR_BUILT, &prop.ZONE_CATEGORY, &prop.ZONE_NAME)
		checkErr(err)

		properties = append(properties, prop)
	}

	data, _ := json.Marshal(properties)
	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}

type censusTract struct {
	CTUID           string `json:"ctuid"`
	NumberOfRentals int    `json:"number_of_rentals"`
	AveragePrice    int    `json:"average_price"`
}

func censusTracts(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	rows, err := db.Query(`
		SELECT
  			ctuid,
  			count(*) as number_of_rentals,
  			round(AVG(price)) as average_price
		FROM (
  			SELECT r.*, ct.ctuid
  			FROM tbl_rentals r INNER JOIN tbl_census_tracts ct
      		ON ST_Contains(ct.wkb_geometry,r.wkb_geometry)
         		AND st_contains(
         			ST_MakeEnvelope($1,$2,$3,$4, 4326), 
         			ct.wkb_geometry
         			)
				) as ctr GROUP BY ctuid`, vars["swlng"], vars["swlat"], vars["nelng"], vars["nelat"])
	checkErr(err)

	defer rows.Close()

	cts := make([]censusTract, 0)

	for rows.Next() {
		ct := censusTract{}

		err = rows.Scan(&ct.CTUID, &ct.NumberOfRentals, &ct.AveragePrice)
		checkErr(err)

		cts = append(cts, ct)
	}

	data, _ := json.Marshal(cts)
	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}
