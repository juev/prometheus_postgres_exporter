package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kkyr/fig"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

type Configuration struct {
	Host         string `fig:"host,default=0.0.0.0"`
	Port         string `fig:"port,default=9102"`
	QueryTimeout string `fig:"querytimeout,default=30"`
	Databases    []Database
}

type Database struct {
	Dsn          string
	Host         string  `fig:",default=127.0.0.1"`
	User         string  `fig:"user"`
	Password     string  `fig:"password"`
	Database     string  `fig:"database"`
	Port         string  `fig:"port,default=5432"`
	Driver       string  `fig:"driver,default=postgres"`
	MaxIdleConns string  `fig:",default=10"`
	MaxOpenConns string  `fig:",default=10"`
	Queries      []Query `fig:"queries"`
	db           *sql.DB
}

type Query struct {
	Sql      string `fig:"sql"`
	Name     string `fig:"name"`
	Interval string `fig:"interval,default=1"`
}

const (
	namespace = "postgresdb"
	exporter  = "exporter"
)

var (
	configuration Configuration
	metricMap     map[string]*prometheus.GaugeVec
	timeout       int
	maxIdleConns  int
	maxOpenConns  int
	err           error
	configFile    string
	logFile       string
)

func init() {
	flag.StringVarP(&configFile, "configFile", "c", "config.yaml", "Config file name (default: config.yaml)")
	flag.StringVarP(&logFile, "logFile", "l", "stdout", "Log filename (default: stdout)")

	metricMap = map[string]*prometheus.GaugeVec{
		"query": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "query_value",
			Help:      "Value of Business metrics from Database",
		}, []string{"database", "name", "col"}),
		"error": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "query_error",
			Help:      "Result of last query, 1 if we have errors on running query",
		}, []string{"database", "name"}),
		"duration": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "query_duration_seconds",
			Help:      "Duration of the query in seconds",
		}, []string{"database", "name"}),
		"up": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "up",
			Help:      "Database status",
		}, []string{"database"}),
	}
	for _, metric := range metricMap {
		prometheus.MustRegister(metric)
	}
}

func execQuery(database Database, query Query) {

	defer func(begun time.Time) {
		duration := time.Since(begun).Seconds()
		metricMap["duration"].WithLabelValues(database.Database, query.Name).Set(duration)
	}(time.Now())

	// Reconnect if we lost connection
	if err := database.db.Ping(); err != nil {
		if strings.Contains(err.Error(), "sql: database is closed") {
			logrus.Infoln("Reconnecting to DB: ", database.Database)
			database.db, _ = sql.Open("oci8", database.Dsn)
			database.db.SetMaxIdleConns(maxIdleConns)
			database.db.SetMaxOpenConns(maxOpenConns)
		}
	}

	// Validate connection
	if err := database.db.Ping(); err != nil {
		logrus.Errorf("Error on connect to database '%s': %v", database.Database, err)
		metricMap["up"].WithLabelValues(database.Database).Set(0)
		return
	}
	metricMap["up"].WithLabelValues(database.Database).Set(1)

	// query db
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	rows, err := database.db.QueryContext(ctx, query.Sql)
	if ctx.Err() == context.DeadlineExceeded {
		logrus.Errorf("oracle query '%s' timed out", query.Name)
		metricMap["error"].WithLabelValues(database.Database, query.Name).Set(1)
		return
	}
	if err != nil {
		logrus.Errorf("oracle query '%s' failed: %v", query.Name, err)
		metricMap["error"].WithLabelValues(database.Database, query.Name).Set(1)
		return
	}

	cols, _ := rows.Columns()
	vals := make([]interface{}, len(cols))
	defer func() {
		err := rows.Close()
		if err != nil {
			logrus.Fatal(err)
		}
	}()
	for rows.Next() {
		for i := range cols {
			vals[i] = &vals[i]
		}

		err = rows.Scan(vals...)
		if err != nil {
			break
		}

		for i := range cols {
			metricMap["error"].WithLabelValues(database.Database, query.Name).Set(0)
			float, err := dbToFloat64(vals[i])
			if err != true {
				logrus.Errorf("Cannot convert value '%s' to float on query '%s': %v", vals[i].(string), query.Name, err)
				metricMap["error"].WithLabelValues(database.Database, query.Name).Set(1)
				return
			}
			metricMap["query"].With(prometheus.Labels{"database": database.Database, "name": query.Name, "col": cols[i]}).Set(float)
		}
	}
}

// Convert database.sql types to float64s for Prometheus consumption. Null types are mapped to NaN. string and []byte
// types are mapped as NaN and !ok
func dbToFloat64(t interface{}) (float64, bool) {
	switch v := t.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case time.Time:
		return float64(v.Unix()), true
	case []byte:
		// Try and convert to string and then parse to a float64
		strV := string(v)
		result, err := strconv.ParseFloat(strV, 64)
		if err != nil {
			logrus.Errorln("Could not parse []byte:", err)
			return math.NaN(), false
		}
		return result, true
	case string:
		result, err := strconv.ParseFloat(v, 64)
		if err != nil {
			logrus.Errorln("Could not parse string:", err)
			return math.NaN(), false
		}
		return result, true
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	case nil:
		return math.NaN(), true
	default:
		return math.NaN(), false
	}
}

// Convert database.sql to string for Prometheus labels. Null types are mapped to empty strings.
//func dbToString(t interface{}) (string, bool) {
//	switch v := t.(type) {
//	case int64:
//		return fmt.Sprintf("%v", v), true
//	case float64:
//		return fmt.Sprintf("%v", v), true
//	case time.Time:
//		return fmt.Sprintf("%v", v.Unix()), true
//	case nil:
//		return "", true
//	case []byte:
//		// Try and convert to string
//		return string(v), true
//	case string:
//		return v, true
//	case bool:
//		if v {
//			return "true", true
//		}
//		return "false", true
//	default:
//		return "", false
//	}
//}

func main() {
	flag.Parse()
	if logFile == "stdout" {
		logrus.SetOutput(os.Stdout)
	} else {
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			logrus.SetOutput(file)
		} else {
			logrus.Info("Failed to log to file, using default stdout")
			logrus.SetOutput(os.Stdout)
		}
	}

	err = fig.Load(&configuration, fig.File(configFile))
	if err != nil {
		logrus.Fatal("Fatal error on reading configuration: ", err)
	}

	timeout, err = strconv.Atoi(configuration.QueryTimeout)
	if err != nil {
		logrus.Fatal("error while converting timeout option value: ", err)
	}
	cron := gocron.NewScheduler(time.UTC)
	for _, database := range configuration.Databases {
		// connect to database
		if database.Driver == "postgres" {
			database.Dsn = fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable", database.User, database.Password, database.Host, database.Port, database.Database)
		} else {
			database.Dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", database.User, database.Password, database.Host, database.Port, database.Database)
		}
		logrus.Infoln("Connecting to DB: ", database.Database)
		database.db, err = sql.Open(database.Driver, database.Dsn)
		if err != nil {
			logrus.Errorln("Error connecting to db: ", err)
		}
		maxIdleConns, err = strconv.Atoi(database.MaxIdleConns)
		if err != nil {
			logrus.Fatal("error while converting maxIdleConns option value: ", err)
		}

		maxOpenConns, err = strconv.Atoi(database.MaxOpenConns)
		if err != nil {
			logrus.Fatal("error while converting maxOpenConns option value: ", err)
		}

		database.db.SetMaxIdleConns(maxOpenConns)
		database.db.SetMaxOpenConns(maxOpenConns)

		// create cron jobs for every query on database
		if err := database.db.Ping(); err == nil {
			for _, query := range database.Queries {
				if n, err := strconv.Atoi(query.Interval); err == nil {
					cron.Every(uint64(n)).Minutes().Do(execQuery, database, query)
				} else {
					logrus.Errorln(query.Interval, " is not an integer.")
				}

			}
		} else {
			logrus.Errorf("Error connecting to db '%s': %v", database.Database, err)
		}
	}

	cron.StartAsync()

	prometheusConnection := configuration.Host + ":" + configuration.Port
	logrus.Printf("listen: %s", prometheusConnection)
	http.Handle("/metrics", promhttp.Handler())
	err = http.ListenAndServe(prometheusConnection, nil)
	if err != nil {
		logrus.Fatalln("Fatal error on serving metrics:", err)
	}
}
