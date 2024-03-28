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
		logMessage("Usage: " + os.Args[0] + " <dbname>")
		os.Exit(1)
	}
	dbname := os.Args[1]

	for {
		yamlFiles := findYAMLFiles(".")
		if len(yamlFiles) != 0 {
			// files := strings.Fields(yamlFiles)
			for _, yamlFile := range yamlFiles {
				logMessage("Starting to add rows to " + dbname)
				randomSleep := rand.Intn(58) + 3 // Generate random sleep duration between 3 seconds and 1 minute
				records := time.Now().Format("2006-01-02 15:04:05.0000")
				recordsSplit := strings.Split(records, ".")
				recordsInt, _ := strconv.Atoi(recordsSplit[1])
				logMessage("Processing config file " + yamlFile + ", trying to insert " + strconv.Itoa(recordsInt) + " records")
				cmd := exec.Command("./pgdbgen", "-config", yamlFile, "-dbname", dbname, "-dbRecords2Process", strconv.Itoa(recordsInt))
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
