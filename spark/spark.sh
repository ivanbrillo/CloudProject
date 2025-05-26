#!/bin/bash

TIMES=1

# List of input size directories
DATASET_SIZES=(10doc 20doc)                                # Dataset size: choose between "10doc" or "20doc"
SIZES=(512KB 1MB 512MB 1GB 2GB)
PARTITIONS=(-1 5 15)
OUTPUT_BASE_INDEX=302

for DATASET_SIZE in "${DATASET_SIZES[@]}"; do
    echo "=== Running with ${DATASET_SIZE} documents ==="

    for PARTITION in "${PARTITIONS[@]}"; do
        echo "=== Running with ${PARTITION} partitions ==="
    
        
        for (( RUN_ID=1; RUN_ID<=TIMES; RUN_ID++ )); do
            echo "--- Iteration #${RUN_ID} ---"
        
            for SIZE in "${SIZES[@]}"; do
                INPUT_DIR="projectInput_${DATASET_SIZE}/${SIZE}"
                OUTPUT_DIR="outputSpark_${DATASET_SIZE}/${OUTPUT_BASE_INDEX}_${SIZE}"

                spark-submit --master yarn sparkInvertedIndex2.py "$INPUT_DIR" "$OUTPUT_DIR" "$PARTITION" > spark_log.txt 2>&1

                if [ $? -ne 0 ]; then
                    echo "ERRORE: il job Spark è fallito" >&2
                    exit 1
                fi

                # application ID
                ID_LINE=$(grep "Submitted application application_" "spark_log.txt")
                ID=$(echo "${ID_LINE}" | awk -F'application_' '{print $2}')

                # scheduling YARN
                yarn application -status application_"$ID" > output_yarn.txt 2>&1

                {
                    PARTITION_LINE=$(grep "Number of partitions" "spark_log.txt")
                    REAL_PARTITION=$(echo "${PARTITION_LINE}" | awk -F': ' '{print $2}')

                    START_LINE=$(grep "Start-Time" "output_yarn.txt")
                    START=$(echo "${START_LINE}" | awk -F' : ' '{print $2}')

                    END_LINE=$(grep "Finish-Time" "output_yarn.txt")
                    END=$(echo "${END_LINE}" | awk -F' : ' '{print $2}')

                    DIFF_MS=$(( END - START ))

                    TS=$(date -d '+2 hours' '+%Y-%m-%d %H:%M:%S')
                    
                    echo "spark,$SIZE,$DATASET_SIZE,$PARTITION,$REAL_PARTITION,$DIFF_MS,$TS"
                } >> spark.csv

                echo "Results for $INPUT_DIR saved in spark.csv"

                ((OUTPUT_BASE_INDEX++))
            done
        done
    done
done

echo "Next output_index: $OUTPUT_BASE_INDEX"