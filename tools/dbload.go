package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func logMessage(message string) {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println(currentTime, message)
}

func findYAMLFiles(root string) []string {
	var yamlFiles []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && (filepath.Ext(path) == ".yaml" || filepath.Ext(path) == ".yml") {
			yamlFiles = append(yamlFiles, path)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return yamlFiles
}

func main() {
	if len(os.Args) < 2 {
		logMessage("Usage  : " + os.Args[0] + " <dbname> <Max Digits of Random Records> <Max Random Wait Time in seconds>")
		logMessage("Example: " + os.Args[0] + " mytestdb 0000 60 (default)")
		logMessage("Example: " + os.Args[0] + " mytestdb 00000 120")
		logMessage("Example: " + os.Args[0] + " mytestdb 000 180")
		os.Exit(1)
	}
	dbname := os.Args[1]
	var dateFormat string
	if len(os.Args[2]) > 0 {
		var maxdigit4dateFormat int
		if len(os.Args[2]) > 6 {
			maxdigit4dateFormat = 6
		} else {
			maxdigit4dateFormat = len(os.Args[2])
		}
		i := 1
		for i <= maxdigit4dateFormat {
			dateFormat = dateFormat + "0"
			i = i + 1
		}
	} else {
		dateFormat = "0000"
	}

	var waitTime int
	var errAtoi error
	if len(os.Args[3]) > 0 {
		waitTime, errAtoi = strconv.Atoi(os.Args[3])
		if errAtoi != nil {
			//executes if there is any error
			logMessage("Warning WaitTime in sec value is incorect : " + errAtoi.Error())
			logMessage("Warning WaitTime is forced to a max of 60 sec")
			waitTime = 60
		}
	} else {
		waitTime = 60
	}
	var dryRun bool
	if len(os.Args[4]) > 0 {
		dryRun = true
	} else {
		dryRun = false
	}

	for {
		yamlFiles := findYAMLFiles(".")
		if len(yamlFiles) != 0 {
			// files := strings.Fields(yamlFiles)
			for _, yamlFile := range yamlFiles {
				logMessage("Starting to add rows to " + dbname)
				randomSleep := rand.Intn(waitTime) + 3 // Generate random sleep duration between 3 seconds and 1 minute
				dateFormat = "2006-01-02 15:04:05." + dateFormat
				// records := time.Now().Format("2006-01-02 15:04:05.0000")
				records := time.Now().Format(dateFormat)
				recordsSplit := strings.Split(records, ".")
				recordsInt, _ := strconv.Atoi(recordsSplit[1])
				logMessage("Processing config file " + yamlFile + ", trying to insert " + strconv.Itoa(recordsInt) + " records")
				if !dryRun {
					cmd := exec.Command("./pgdbgen", "-config", yamlFile, "-dbname", dbname, "-dbRecords2Process", strconv.Itoa(recordsInt))
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr
					err := cmd.Run()
					if err != nil {
						log.Fatal(err)
					}
				}
				logMessage("Pause for " + strconv.Itoa(randomSleep) + "s")
				time.Sleep(time.Duration(randomSleep) * time.Second)
			}
		} else {
			logMessage("Requires at least a yaml/yml file to present into the current directory.")
			break
		}
	}
}
