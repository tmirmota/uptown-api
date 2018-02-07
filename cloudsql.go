package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"fmt"
	"os"
	"reflect"

	"google.golang.org/appengine"
	_ "github.com/broady/gae-postgres"
)

var db *sql.DB

func main() {
	datastoreName := os.Getenv("POSTGRES_CONNECTION")
	fmt.Println(datastoreName)

	var err error
	db, err = sql.Open("gae-postgres", datastoreName)
	//db, err = sql.Open("postgres", datastoreName)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", handle)
	http.HandleFunc("/census-tract", censusTracts)
	http.HandleFunc("/property-tax", propertyTax)
	http.HandleFunc("/address-search", addressSearch)

	appengine.Main()
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

type rental struct {
	Price int
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	rows, err := db.Query("SELECT price FROM tbl_rentals LIMIT 50")
	checkErr(err)

	rentals := make([]rental, 0)

	for rows.Next() {
		rent := rental{}
		err = rows.Scan(&rent.Price)
		checkErr(err)
		rentals = append(rentals, rent)
	}

	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rentals)
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
	pcoord := r.URL.Query().Get("pcoord")

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
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}

type censusTract struct {
	CTUID           string     `json:"ctuid"`
	NumberOfRentals int        `json:"number_of_rentals"`
	AveragePrice    int        `json:"average_price"`
	MedianPrice     int        `json:"median_price"`
	MinPrice        NullInt64  `json:"min_price"`
	MaxPrice        int        `json:"max_price"`
	AverageSqFt     NullInt64  `json:"average_sqft"`
	AverageBed      NullString `json:"average_bed"`
	AverageBath     NullString `json:"average_bath"`
}

func censusTracts(w http.ResponseWriter, r *http.Request) {
	queries := r.URL.Query()
	bedrooms := queries.Get("bedrooms")

	q := fmt.Sprintf(`
		SELECT
		  ctuid,
		  count(*) as number_of_rentals,
		  round(AVG(NULLIF(price,0))) as average_price,
		  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price)) AS median_price,
		  round(percentile_cont(0.1) WITHIN GROUP (ORDER BY price)) AS min_price,
		  round(percentile_cont(0.9) WITHIN GROUP (ORDER BY price)) AS max_price,
		  round(AVG(NULLIF(sqft,0))) as average_sqft,
		  round(AVG(NULLIF(bedrooms, 0)),2) as average_bed,
		  round(AVG(NULLIF(bathrooms,0)),2) as average_bath
		FROM (
  			SELECT r.*, ct.ctuid
  			FROM tbl_rentals r INNER JOIN tbl_census_tracts ct
      		ON ST_Intersects(ct.wkb_geometry,r.wkb_geometry)
         	AND ST_Intersects(
         				ST_MakeEnvelope($1,$2,$3,$4, 4326), 
								ct.wkb_geometry
							)
			AND r.bedrooms IN (%v)
		) as ctr GROUP BY ctuid`, bedrooms)

	rows, err := db.Query(q, queries.Get("swlng"), queries.Get("swlat"), queries.Get("nelng"), queries.Get("nelat"))
	checkErr(err)

	defer rows.Close()

	cts := make([]censusTract, 0)

	for rows.Next() {
		ct := censusTract{}

		err = rows.Scan(&ct.CTUID, &ct.NumberOfRentals, &ct.AveragePrice, &ct.MedianPrice, &ct.MinPrice, &ct.MaxPrice, &ct.AverageSqFt, &ct.AverageBed, &ct.AverageBath)
		checkErr(err)

		cts = append(cts, ct)
	}

	data, _ := json.Marshal(cts)
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}

type propertyShort struct {
	LAND_COORDINATE			int
	TO_CIVIC_NUMBER			int
	FROM_CIVIC_NUMBER 		int
	STREET_NAME 			string
	PROPERTY_POSTAL_CODE 	string
}

func addressSearch(w http.ResponseWriter, r *http.Request) {
	queries := r.URL.Query()
	address := queries.Get("address")
	lat := queries.Get("lat")
	long := queries.Get("long")

	q := `
		SELECT
		    pt.LAND_COORDINATE,
			pt.TO_CIVIC_NUMBER,
			pt.FROM_CIVIC_NUMBER,
			pt.STREET_NAME,
			pt.PROPERTY_POSTAL_CODE
		FROM tbl_property_parcels AS pp
		JOIN tbl_property_taxes AS pt
		  ON pp.tax_coord = pt.land_coordinate
		WHERE ST_Contains(pp.wkb_geometry, ST_SetSRID(ST_MakePoint($1, $2), 4326))
		AND pt.to_civic_number = $3
	`

	rows, err := db.Query(q, long, lat, address)
	checkErr(err)

	defer rows.Close()

	propertiesShort := make([]propertyShort, 0)

	for rows.Next() {
		ps := propertyShort{}
		err = rows.Scan(
			&ps.LAND_COORDINATE,
			&ps.TO_CIVIC_NUMBER,
			&ps.FROM_CIVIC_NUMBER,
			&ps.STREET_NAME,
			&ps.PROPERTY_POSTAL_CODE )
		checkErr(err)

		propertiesShort = append(propertiesShort, ps)
	}

	data, _ := json.Marshal(propertiesShort)

	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}



// NullInt64 is an alias for sql.NullInt64 data type
type NullInt64 sql.NullInt64
// Scan implements the Scanner interface for NullInt64
func (ni *NullInt64) Scan(value interface{}) error {
	var i sql.NullInt64
	if err := i.Scan(value); err != nil {
		return err
	}

	// if nil then make Valid false
	if reflect.TypeOf(value) == nil {
		*ni = NullInt64{i.Int64, false}
	} else {
		*ni = NullInt64{i.Int64, true}
	}
	return nil
}
// NullString is an alias for sql.NullString data type
type NullString sql.NullString
// Scan implements the Scanner interface for NullString
func (ns *NullString) Scan(value interface{}) error {
	var s sql.NullString
	if err := s.Scan(value); err != nil {
		return err
	}

	// if nil then make Valid false
	if reflect.TypeOf(value) == nil {
		*ns = NullString{s.String, false}
	} else {
		*ns = NullString{s.String, true}
	}

	return nil
}
// MarshalJSON for NullInt64
func (ni *NullInt64) MarshalJSON() ([]byte, error) {
	if !ni.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ni.Int64)
}
// UnmarshalJSON for NullInt64
func (ni *NullInt64) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &ni.Int64)
	ni.Valid = (err == nil)
	return err
}
// MarshalJSON for NullString
func (ns *NullString) MarshalJSON() ([]byte, error) {
	if !ns.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ns.String)
}
// UnmarshalJSON for NullString
func (ns *NullString) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &ns.String)
	ns.Valid = (err == nil)
	return err
}