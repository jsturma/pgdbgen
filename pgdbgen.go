package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
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

const (
	host         = "localhost"
	port         = 30708
	user         = "admin"
	password     = "Password123!"
	dbname       = "mytestdb"
	runOnlyFaker = false
)

const (
	numWorkers             = 3                               // Number of worker goroutines
	dbRecords2Process      = 100                             // Number of db records to be added
	pcentOutput            = 10                              // Output every x%
	outPutRecordsProcessed = dbRecords2Process / pcentOutput // Output nb of Records processed interval
	// Define the minimum and maximum values
	minDays        = 259200
	maxDays        = 31536000
	delayLastLogin = 500
)

func main() {
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
		log.Println("Worker : ", wrk_id, " RecordID", recordID) // Generate fake data
		a := fakeDataStruct{}
		if runOnlyFaker == true {
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
		if runOnlyFaker == true {

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
		if recordID%10 == 0 {
			log.Printf("Inserted record %d of %d\n", recordID, records)
		}
	}
	log.Printf("Worker ID: %d stopping\n", wrk_id)
	// done <- true
}
