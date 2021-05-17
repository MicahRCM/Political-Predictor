package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gamezop/jinbe/pkg/array"
	"github.com/prithvihv/jinbe/pkg/confighelper"
	"github.com/prithvihv/jinbe/pkg/db_psql"
)

var (
	POSTGRES_USER string
	POSTGRES_PASS string
)

// "user" => [varibles] [{ 1, 192901} , {2,1221} ]
type MapSync struct {
	sync.Mutex
	Store map[string][]MetricsData

	csvString [][]string
}

var userVariblesMap MapSync

func StoreUserAttribute(m *MetricsData) {
	userVariblesMap.Lock()
	if metrics, ok := userVariblesMap.Store[m.author]; ok {
		metrics = append(metrics, *m)
		userVariblesMap.Store[m.author] = metrics
	} else {
		userVariblesMap.Store[m.author] = []MetricsData{*m}
	}
	userVariblesMap.Unlock()
}

func convertAndDump() {
	f, err := os.Create("user_variables.csv")
	defer f.Close()

	if err != nil {
		log.Fatalln("failed to open file", err)
	}

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write(append([]string{"author"}, array.NumberStrs(1, 12)...))

	for author, arrMetrics := range userVariblesMap.Store {
		var variableValues []string = append([]string{""}, array.SameNTimes(12, "0")...)
		variableValues[0] = author
		for _, metrics := range arrMetrics {
			variableValues[metrics.metricsNameCoded] = fmt.Sprintf("%.2f", metrics.metricsValue)
		}
		if err := w.Write(variableValues); err != nil {
			log.Fatalln("error writing record to file", err)
		}
	}
}

var DBComment *sql.DB

func init() {

	userVariblesMap.csvString = [][]string{append([]string{"author"}, array.NumberStrs(1, 12)...)}
	userVariblesMap.Store = map[string][]MetricsData{}

	confighelper.ParseEnvString(&POSTGRES_USER, "POSTGRES_USER")
	confighelper.ParseEnvString(&POSTGRES_PASS, "POSTGRES_PASS")
	var db = "gzp_data"
	if POSTGRES_USER != "grover" {
		db = "newredditcomments"
	}
	DBComment = db_psql.DBConnect("comment",
		db_psql.DB_OPTIONS{
			USER: POSTGRES_USER,
			PASS: POSTGRES_PASS,
			HOST: "localhost",
			DB:   db,
		})
}

type ComputePath struct {
	tableName string
	variable  string
}

type MetricsData struct {
	metricsNameCoded int
	metricsValue     float64
	author           string
}

func (c *ComputePath) tag() string {
	return fmt.Sprintf(`%s_%s`, c.tableName, c.variable)
}

// queries
func getQ(c *ComputePath) string {

	var computations string = ""
	if c.variable[0] == 'n' {
		computations = "sum(n)"
	} else {
		computations = "sum(points)::decimal / sum(n)::decimal"
	}

	return fmt.Sprintf(`
		SELECT author, %s as total_n, %s
		FROM %s
		WHERE %s is not null
		group by (author, %s)
	`, computations, c.variable, c.tableName, c.variable, c.variable)
}

// run on go
func getStore(c ComputePath, promise *sync.WaitGroup) {
	startTime := time.Now()
	log.Println(c.tag(), "starting")
	q := getQ(&c)
	log.Println(c.tag(), q)
	rows, err := DBComment.Query(q)
	sinceTime := time.Since(startTime)
	log.Println(c.tag(), " time to completed query:", sinceTime.Seconds())
	if err != nil {
		log.Fatalln(c.tag(), " error while querying", err)
		return
	}

	count := 0
	defer rows.Close()
	for rows.Next() {
		var user MetricsData
		err = rows.Scan(&user.author, &user.metricsValue, &user.metricsNameCoded)
		if err != nil {
			log.Fatalln(c.tag(), " error while computing", err)
			return
		}
		StoreUserAttribute(&user)
		count++
		if count%10000 == 0 {
			fmt.Print("*")
		}
	}
	log.Println(c.tag(), "COMPLETED! total time:", time.Since(startTime).Seconds(), " s and count:", count)
	promise.Done()
}

func main() {

	var allComputes = []ComputePath{
		{
			tableName: "predictions_comment_set",
			variable:  "n_sub_type",
		},
		{
			tableName: "predictions_comment_set",
			variable:  "avg_sub_type",
		},
		{
			tableName: "predictions_post_set",
			variable:  "n_sub_type",
		},
		{
			tableName: "predictions_post_set",
			variable:  "avg_sub_type",
		},
	}
	var allPromise sync.WaitGroup
	allPromise.Add(len(allComputes))

	for _, val := range allComputes {
		go getStore(val, &allPromise)
	}

	allPromise.Wait()
	convertAndDump()

	// -- nodejs script 1 will consume
	// select author, sum(n) as total_n, n_sub_type
	// from predictions_comment_set
	// group by (author,n_sub_type);

	// -- nodejs script 2 will consume
	// select author, sum(points)::decimal / sum(n) as total_ups, avg_sub_type
	// from predictions_post_set
	// group by (author,avg_sub_type);
}

// [1,2,3]
// avg1 = 2

// [1,3,57,41]
// avg2 = 25.5

// -> avg1 + avg2/2
// avgtotal1 = 13.75

// (2 * 3 + 25.5 * 4)/ (3 + 4)

// -> []/7
// avgtotal2 = 15.4
