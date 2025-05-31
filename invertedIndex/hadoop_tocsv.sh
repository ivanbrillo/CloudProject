#!/bin/bash

MAIN_CLASS="it.unipi.hadoop.InvertedIndex"          # Main class and reducer count
TIMES=1

# List of input size directories
DATASET_SIZES=(10doc 20doc)                                # Dataset size: choose between "10doc" or "20doc"
SIZES=(512KB 1MB 512MB 1GB 2GB)
NUMBER_REDUCERS=(1 5 15)
OUTPUT_BASE_INDEX=1

for DATASET_SIZE in "${DATASET_SIZES[@]}"; do
    echo "=== Running with ${DATASET_SIZE} documents ==="

    for REDUCERS in "${NUMBER_REDUCERS[@]}"; do
        echo "=== Running with ${REDUCERS} reducer(s) ==="
    
        # Loop from 1 to TIMES
        for (( RUN_ID=1; RUN_ID<=TIMES; RUN_ID++ )); do
            echo "--- Iteration #${RUN_ID} ---"
        
            for SIZE in "${SIZES[@]}"; do
                INPUT_DIR="projectInput_${DATASET_SIZE}/${SIZE}"
                OUTPUT_DIR="output_${DATASET_SIZE}/${OUTPUT_BASE_INDEX}_${SIZE}"

                hadoop jar target/invertedIndex-1.0-SNAPSHOT.jar "$MAIN_CLASS" -D mapreduce.job.reduces="$REDUCERS" "$INPUT_DIR" "$OUTPUT_DIR" > output_log.txt 2>&1

                if [ $? -ne 0 ]; then
                    echo "ERRORE: il job Hadoop è fallito" >&2
                    exit 1
                fi

                # application ID
                ID_LINE=$(grep "Running job:" "output_log.txt")
                ID=$(echo "${ID_LINE}" | awk -F'job_' '{print $2}')

                 # Query YARN for application status
                yarn application -status application_"$ID" > output_yarn.txt 2>&1

                {
                    SPLIT_LINE=$(grep "number of splits" "output_log.txt")
                    SPLIT=$(echo "${SPLIT_LINE}" | awk -F'number of splits:' '{print $2}')

                    MEMORY_LINE=$(grep "Physical memory (bytes) snapshot" "output_log.txt")
                    MEMORY=$(echo "${MEMORY_LINE}" | awk -F'=' '{print $2}')

                    START_LINE=$(grep "Start-Time" "output_yarn.txt")
                    START=$(echo "${START_LINE}" | awk -F' : ' '{print $2}')

                    END_LINE=$(grep "Finish-Time" "output_yarn.txt")
                    END=$(echo "${END_LINE}" | awk -F' : ' '{print $2}')

                    DIFF_MS=$(( END - START ))

                    TS=$(date -d '+2 hours' '+%Y-%m-%d %H:%M:%S')
                    
                    echo "hadoop,$SIZE,$DATASET_SIZE,$SPLIT,$REDUCERS,$DIFF_MS,$MEMORY,$TS"
                } >> hadoop.csv

                echo "Results for $INPUT_DIR saved in hadoop.csv"

                # Increment output directory index for immutability
                ((OUTPUT_BASE_INDEX++))
            done
        done
    done
done

echo "Next output_index: $OUTPUT_BASE_INDEX"