package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func logMessage(message string) {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println(currentTime, message)
}

func main() {
	if len(os.Args) < 2 {
		logMessage("Usage: " + os.Args[0] + " <dbname>")
		os.Exit(1)
	}
	dbname := os.Args[1]

	for {
		yamlFiles, err := exec.Command("find", ".", "-maxdepth", "1", "-type", "f", "\\(", "-name", "*.yaml", "-o", "-name", "*.yml", "\\)").Output()
		if err != nil {
			log.Fatal(err)
		}
		yamlFilesStr := strings.TrimSpace(string(yamlFiles))

		if yamlFilesStr != "" {
			files := strings.Fields(yamlFilesStr)
			for _, file := range files {
				logMessage("Starting to add rows to " + dbname)
				randomSleep := rand.Intn(58) + 3 // Generate random sleep duration between 3 seconds and 1 minute
				records := time.Now().Format("2006-01-02 15:04:05.0000")
				recordsSplit := strings.Split(records, ".")
				recordsInt, _ := strconv.Atoi(recordsSplit[1])
				logMessage("Processing config file " + file + ", trying to insert " + strconv.Itoa(recordsInt) + " records")
				cmd := exec.Command("./pgdbgen", "-config", file, "-dbname", dbname, "-dbRecords2Process", strconv.Itoa(recordsInt))
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				err := cmd.Run()
				if err != nil {
					log.Fatal(err)
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
