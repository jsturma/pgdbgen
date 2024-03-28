#!/bin/bash
# Search for YAML files in the current directory
log_message() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") $1"
}
# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ]; then
    log_message "Usage: $0 <dbname>"
    exit 1
fi
dbname="$1"

while true; do
yaml_files=$(find . -maxdepth 1 -type f \( -name "*.yaml" -o -name "*.yml" \))
if [ -n "$yaml_files" ]; then
     for file in $yaml_files; do
          log_message "Starting to add rows to $dbname "
          random_sleep=$(( (RANDOM % 60) + 3 ))  # Generate random sleep duration between 3 seconds and 1 minutes
	  records=$(date +"%Y-%m-%d %H:%M:%S.%4N" | awk -F '.' '{print $2}' | awk '{print int($0)}')
	  records=$((records+0))
          log_message "Procesing config file $file, trying to insert $records records"
          ./pgdbgen -config $file -dbname $dbname -dbRecords2Process $records   
          log_message "Pause for $random_sleep""s"
          sleep $random_sleep
      done
     
else
    log_message "No YAML files found in the current directory."
    break  
fi
done
