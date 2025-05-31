#!/bin/bash

MAIN_CLASS="it.unipi.hadoop.InvertedIndex"
TIMES=5

DOCS=(10doc 20doc)
SIZES=(512KB 1MB 512MB 1GB 2GB)
NUMBER_REDUCERS=(1 5 15)
OUTPUT_BASE_INDEX=1

for DOC in "${DOCS[@]}"; do
    echo "=== Running with ${DOC} documents ==="

    for REDUCERS in "${NUMBER_REDUCERS[@]}"; do
        echo "=== Running with ${REDUCERS} reducer(s) ==="
    
        for (( RUN_ID=1; RUN_ID<=TIMES; RUN_ID++ )); do
            echo "--- Iteration #${RUN_ID} ---"
        
            for SIZE in "${SIZES[@]}"; do
                INPUT_DIR="projectInput_${DOC}/${SIZE}"
                OUTPUT_DIR="output_${DOC}/${OUTPUT_BASE_INDEX}_${SIZE}"

                hadoop jar target/invertedIndex-1.0-SNAPSHOT.jar "$MAIN_CLASS" -D mapreduce.job.reduces="$REDUCERS" "$INPUT_DIR" "$OUTPUT_DIR" > output_log.txt 2>&1

                if [ $? -ne 0 ]; then
                    echo "ERRORE: il job Hadoop è fallito" >&2
                    exit 1
                fi

                ID_LINE=$(grep "Running job:" "output_log.txt")
                ID=$(echo "${ID_LINE}" | awk -F'job_' '{print $2}')

                yarn application -status application_"$ID" > output_yarn.txt 2>&1

                TS=$(date -d '+2 hours' '+%Y-%m-%d %H:%M:%S')
                    
                SPLIT_LINE=$(grep "number of splits" "output_log.txt")
                SPLIT=$(echo "${SPLIT_LINE}" | awk -F'number of splits:' '{print $2}')

                PHYSICAL_MEMORY=$(grep "Physical memory (bytes) snapshot" "output_log.txt" | awk -F'=' '{print $2}')
                PEAK_MAP_PHYSICAL_MEMORY=$(grep "Peak Map Physical memory (bytes)" "output_log.txt" | awk -F'=' '{print $2}')
                PEAK_REDUCE_PHYSICAL_MEMORY=$(grep "Peak Reduce Physical memory (bytes)" "output_log.txt" | awk -F'=' '{print $2}')

                START=$(grep "Start-Time" "output_yarn.txt" | awk -F' : ' '{print $2}')
                END=$(grep "Finish-Time" "output_yarn.txt" | awk -F' : ' '{print $2}')
                ALLOC=$(awk '/Aggregate Resource Allocation/ { print $5 }' output_yarn.txt)

                DIFF_MS=$(( END - START ))

                echo "$TS,hadoop,$SIZE,$DOC,$SPLIT,$REDUCERS,$PHYSICAL_MEMORY,$PEAK_MAP_PHYSICAL_MEMORY,$PEAK_REDUCE_PHYSICAL_MEMORY,$ALLOC,$DIFF_MS" >> hadoop.csv

                echo "Results for $INPUT_DIR saved in hadoop.csv"
                ((OUTPUT_BASE_INDEX++))
            done
        done
    done
done

echo "Next output_index: $OUTPUT_BASE_INDEX"
