package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v2"

	// "github.com/jaswdr/faker"
	// "github.com/bxcodec/faker/v4"
	"github.com/go-faker/faker/v4"

	_ "github.com/lib/pq"
)

type fakeDataStruct struct {
	UUID          string  `faker:"uuid_digit"`
	UserName      string  `faker:"username"`
	Email         string  `faker:"email"`
	FirstName     string  `faker:"first_name"`
	LastName      string  `faker:"last_name"`
	ProductName_0 string  `faker:"len=7"`
	ProductName_1 string  `faker:"len=12"`
	PhoneNumber   string  `faker:"phone_number"`
	Password      string  `faker:"password"`
	UnixTime      int64   `faker:"unix_time"`
	Quantity      int8    `faker:"boundary_start=1, boundary_end=999"`
	UnitPrice     float64 `faker:"boundary_start=0.09, boundary_end=99.99"`
}

type Config struct {
	Host                   string `yaml:"host"`
	Port                   int    `yaml:"port"`
	User                   string `yaml:"user"`
	Password               string `yaml:"password"`
	Dbname                 string `yaml:"dbname"`
	RunOnlyFaker           bool   `yaml:"runOnlyFaker"`
	NumWorkers             int    `yaml:"numWorkers"`
	DbRecords2Process      int    `yaml:"dbRecords2Process"`
	PcentOutput            int    `yaml:"pcentOutput"`
	OutPutRecordsProcessed int    `yaml:"outPutRecordsProcessed"`
	MinDays                int    `yaml:"minDays"`
	MaxDays                int    `yaml:"maxDays"`
	DelayLastLogin         int    `yaml:"delayLastLogin"`
}

var (
	host                   string
	port                   int
	user                   string
	password               string
	dbname                 string
	runOnlyFaker           bool
	numWorkers             int
	dbRecords2Process      int
	pcentOutput            int
	outPutRecordsProcessed int
	minDays                int
	maxDays                int
	delayLastLogin         int
)

func main() {
	var paramFromYaml string
	// Define command-line flags
	flag.StringVar(&host, "host", "localhost", "Host address")
	flag.IntVar(&port, "port", 5432, "Port number")
	flag.StringVar(&user, "user", "postgres", "Database Admin User")
	flag.StringVar(&password, "password", "postgres", "Database Admin  password")
	flag.StringVar(&dbname, "dbname", "mytestdb", "Database name to generate")
	flag.BoolVar(&runOnlyFaker, "runOnlyFaker", false, "Run only Faker mode")
	flag.IntVar(&numWorkers, "numWorkers", 3, "Max nb worker")
	flag.IntVar(&dbRecords2Process, "dbRecords2Process", 100, "Number of db records to be added")
	flag.IntVar(&pcentOutput, "pcentOutput", 10, "Output every x%")
	flag.IntVar(&minDays, "minDays", 259200, "Minimum number of days (3 days)")
	flag.IntVar(&maxDays, "maxDays", 31536000, "Maximum number of days (1 year)")
	flag.IntVar(&delayLastLogin, "delayLastLogin", 500, "Delay for last login")
	flag.StringVar(&paramFromYaml, "config", "", "YAML configuration file")

	// Calculate outPutRecordsProcessed
	outPutRecordsProcessed = dbRecords2Process / (100 / pcentOutput)

	// Parse command-line flags
	flag.Parse()

	var config Config
	if paramFromYaml != "" {
		yamlFile, err := os.ReadFile(paramFromYaml)
		if err != nil {
			log.Fatalf("Error reading YAML file: %v", err)
		}

		err = yaml.Unmarshal(yamlFile, &config)
		if err != nil {
			log.Fatalf("Error unmarshalling YAML: %v", err)
		}
	}

	if paramFromYaml != "" {
		if len(config.Host) != 0 {
			host = config.Host
		}
		if config.Port != 0 {
			port = config.Port
		}
		if len(config.User) != 0 {
			user = config.User
		}
		if len(config.Password) != 0 {
			password = config.Password
		}
		if len(config.Dbname) != 0 {
			dbname = config.Dbname
		}
		runOnlyFaker = config.RunOnlyFaker
		if config.NumWorkers != 0 {
			numWorkers = config.NumWorkers
		}
		if config.DbRecords2Process != 0 {
			dbRecords2Process = config.DbRecords2Process
		}
		if config.PcentOutput != 0 {
			pcentOutput = config.PcentOutput
		}
		if config.MinDays != 0 {
			minDays = config.MinDays
		}
		if config.MaxDays != 0 {
			maxDays = config.MaxDays
		}
		if config.DelayLastLogin != 0 {
			delayLastLogin = config.DelayLastLogin
		}
	}
	err := createDatabaseIfNotExists(host, port, user, password, dbname)
	if err != nil {
		log.Fatal(err)
	}
	// Establish a connection to the PostgreSQL server
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create tables
	createTables(db)

	// Populate tables with realistic data
	//populateData(db, 1000000)
	populateData(db, dbRecords2Process)
}

func createDatabaseIfNotExists(host string, port int, user string, password string, dbname string) error {
	// Connect to the PostgreSQL database server

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, "postgres")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println("Cannot talk to databases engine ", err)
		return err
	} else {
		log.Println("Connected to Databases Engine ")
	}
	defer db.Close()

	// Check if the target database exists
	rows, err := db.Query("SELECT 1 FROM pg_database WHERE datname=$1", dbname)
	if err != nil {
		log.Println("Query of pg_database has returned an error: ", err)
		return err
	}
	defer rows.Close()

	// If the database doesn't exist, create it
	if !rows.Next() {
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
		if err != nil {
			return err
		}
		log.Printf("Database '%s' created successfully.\n", dbname)

	} else {
		fmt.Printf("Database '%s' already exists.\n", dbname)
	}
	return nil
}

func createTables(db *sql.DB) {
	// Define CREATE TABLE statements
	paymentsTable := `
		CREATE TABLE IF NOT EXISTS payments (
			p_id SERIAL PRIMARY KEY,
			p_md5 VARCHAR(255),
			p_amount NUMERIC(10, 2),
			p_epoch BIGINT
		)
	`

	buyingStatsTable := `
		CREATE TABLE IF NOT EXISTS buying_stats (
			bstats_id SERIAL PRIMARY KEY,
			bstats_epoch BIGINT,
			bstats_user_id uuid,
			bstats_product_id uuid,
			bstats_quantity INT,
			bstats_total_amount NUMERIC(10, 2)
		)
	`

	productsTable := `
		CREATE TABLE IF NOT EXISTS products (
			prd_id uuid PRIMARY KEY,
			prd_name VARCHAR(100),
			prd_authors VARCHAR(100),
			prd_price NUMERIC(10, 2)
		)
	`
	customerTable := `
		CREATE TABLE IF NOT EXISTS accounts (
			bstats_id SERIAL PRIMARY KEY,
			acc_user_epoch BIGINT,
			acc_user_id uuid,
			acc_user_name VARCHAR(100) NOT NULL,
			acc_user_password VARCHAR (255) NOT NULL,
			acc_user_email VARCHAR (255) NOT NULL,
        	acc_user_last_login BIGINT
		)
	`
	// Execute CREATE TABLE statements
	_, err := db.Exec(paymentsTable)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(buyingStatsTable)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(productsTable)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(customerTable)
	if err != nil {
		log.Fatal(err)
	}
}

func populateData(db *sql.DB, numRecords int) {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
	// source := rand.NewSource(time.Now().UnixNano())
	// rng := rand.New(source)

	// rand.New(rand.NewSource(rand.New(rand.NewSource(seed)))

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create a channel to communicate the completion of worker goroutines
	done := make(chan bool)

	// Create a channel to send records to worker goroutines
	records := make(chan int)

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(db, records, &wg, done, i)
	}

	// Send records to worker goroutines
	go func() {
		for i := 1; i <= numRecords; i++ {
			records <- i
		}
		close(records)
	}()

	// Wait for all worker goroutines to finish
	go func() {
		log.Println("Wait for all worker goroutines to finish")
		wg.Wait()
		log.Println("All worker goroutines has finished")
		close(done)

	}()
	// Wait for the population process to complete
	<-done
	log.Printf("Successfully inserted %d records.\n", numRecords)
}

func worker(db *sql.DB, records <-chan int, wg *sync.WaitGroup, done chan<- bool, wrk_id int) {
	defer wg.Done()
	log.Printf("Starting Worker ID:%d\n", wrk_id)
	// Prepare the INSERT INTO statements
	insertPayment, err := db.Prepare("INSERT INTO payments (p_md5, p_amount, p_epoch) VALUES ($1, $2, $3)")
	if err != nil {
		log.Fatal("Error in db.Prepare insertPayment =>", err)
	}
	defer insertPayment.Close()

	insertBuyingStats, err := db.Prepare("INSERT INTO buying_stats (bstats_user_id, bstats_epoch, bstats_product_id, bstats_quantity, bstats_total_amount) VALUES ($1, $2, $3, $4, $5)")
	if err != nil {
		log.Fatal("Error in db.Prepare insertBuyingStats => ", err)
	}
	defer insertBuyingStats.Close()

	insertProduct, err := db.Prepare("INSERT INTO products (prd_id, prd_name, prd_authors, prd_price) VALUES ($1, $2, $3, $4)")
	if err != nil {
		log.Fatal("Error in db.Prepare insertProduct => ", err)
	}
	defer insertProduct.Close()

	insertAccount, err := db.Prepare("INSERT INTO accounts (acc_user_epoch, acc_user_id, acc_user_name, acc_user_password, acc_user_email, acc_user_last_login) VALUES ($1, $2, $3, $4, $5, $6)")
	if err != nil {
		log.Fatal("Error in db.Prepare insertAccount => ", err)
	}
	defer insertAccount.Close()

	// Generate and insert records
	for recordID := range records {
		if runOnlyFaker {
			log.Println("Worker : ", wrk_id, " RecordID", recordID) // Generate fake data
		}
		a := fakeDataStruct{}
		if runOnlyFaker {
			log.Println("Calling faker.FakeData Func") // Generate fake data
		}
		err = faker.FakeData(&a)
		if err != nil {
			log.Fatal("Unable to get Fake Data from faker => ", err)
		}
		// paymentDateString := a.Timestamp
		// paymentDate, _ := time.Parse("1976-11-12 17:01:40", paymentDateString)
		payementEpoch := a.UnixTime
		payementEpochString := fmt.Sprintf("%.2f", float64(payementEpoch))
		payementQuantity := a.Quantity
		payementTotalAmount := a.UnitPrice * float64(payementQuantity)
		// Combine payment variables into a single string
		combinedString := fmt.Sprintf("%s%d%.2f", payementEpochString, payementQuantity, payementTotalAmount)
		// then Calculate MD5 hash & covertit to string
		hash := md5.Sum([]byte(combinedString))
		// Convert MD5 hash to string
		paymentSerial := hex.EncodeToString(hash[:])
		// Set the seed value for randomization
		rand.Seed(time.Now().UnixNano())
		// Accounts entry
		userID := a.UUID
		// Generate a random integer between min and max
		randomInt := rand.Intn(maxDays-minDays+1) + minDays
		userEpoch := payementEpoch - int64(randomInt)
		userName := a.UserName
		userPwd := a.Password
		userEmail := a.Email
		// Generate a random integer between 1 and 500
		randomInt = rand.Intn(delayLastLogin) + 1
		userLastlogin := payementEpoch - int64(randomInt)

		// Products entry
		productName := a.ProductName_0 + " " + a.ProductName_1
		productAuthors := a.LastName + " " + a.FirstName
		productPrice := a.UnitPrice
		p_uuid := uuid.NewSHA1(uuid.Nil, []byte(productName+"-"+productAuthors))
		productID := p_uuid

		//
		if runOnlyFaker {

			log.Println("paymentSerial			:", paymentSerial)
			log.Println("payementQuantity		:", payementQuantity)
			log.Println("payementEpoch			:", payementEpoch)
			log.Println("payementTotalAmount	:", payementTotalAmount)

			log.Println("productID				:", productID)
			log.Println("productAuthor(s)		:", productAuthors)
			log.Println("productName			:", productName)
			log.Println("productPrice			:", productPrice)

			log.Println("userID					:", userID)
			log.Println("userEpoch				:", userEpoch)
			log.Println("userName				:", userName)
			log.Println("userEmail				:", userEmail)
			log.Println("userPwd				:", userPwd)
			log.Println("userLastlogin			:", userLastlogin)
		}
		// ```go
		// Insert records into the tables

		_, err = insertPayment.Exec(paymentSerial, payementTotalAmount, payementEpoch)
		if err != nil {
			log.Println("Error inserting payment record:", err)
		}

		_, err = insertBuyingStats.Exec(userID, payementEpoch, productID, payementQuantity, payementTotalAmount)
		if err != nil {
			log.Println("Error inserting buying stats record:", err)
		}

		_, err = insertProduct.Exec(productID, productName, productAuthors, productPrice)
		if err != nil {
			log.Println("Error inserting product record:", err)
		}

		_, err = insertAccount.Exec(userEpoch, userID, userName, userPwd, userEmail, userLastlogin)
		if err != nil {
			log.Println("Error inserting account record:", err)
		}
		// Print progress
		if runOnlyFaker {
			if recordID%outPutRecordsProcessed == 0 {
				log.Printf("Inserted record %d of %d\n", recordID, records)
			}
		}
	}
	log.Printf("Worker ID: %d stopping\n", wrk_id)
	// done <- true
}
