#!/bin/bash

TIMES=4

DOCS=(10doc 20doc)
SIZES=(512KB 1MB 512MB 1GB 2GB)
PARTITIONS=(-1 5 15)
OUTPUT_BASE_INDEX=31

for DOC in "${DOCS[@]}"; do
    echo "=== Running with ${DOC} documents ==="

    for PARTITION in "${PARTITIONS[@]}"; do
        echo "=== Running with ${PARTITION} partitions ==="
    
        for (( RUN_ID=1; RUN_ID<=TIMES; RUN_ID++ )); do
            echo "--- Iteration #${RUN_ID} ---"
        
            for SIZE in "${SIZES[@]}"; do
                INPUT_DIR="projectInput_${DOC}/${SIZE}"
                OUTPUT_DIR="outputSpark_${DOC}/${OUTPUT_BASE_INDEX}_${SIZE}"

                spark-submit --master yarn --conf spark.executor.processTreeMetrics.enabled=true sparkInvertedIndex2.py "$INPUT_DIR" "$OUTPUT_DIR" "$PARTITION" > spark_log.txt 2>&1

                if [ $? -ne 0 ]; then
                    echo "ERRORE: il job Spark è fallito" >&2
                    exit 1
                fi

                ID_LINE=$(grep "Submitted application application_" "spark_log.txt")
                ID=$(echo "${ID_LINE}" | awk -F'application_' '{print $2}')

                sleep 20

                curl -s "http://10.1.1.183:18080/api/v1/applications/application_${ID}/executors" > "executors.json"
                yarn application -status application_"$ID" > output_yarn.txt 2>&1

                EXECUTORS_JSON=$(cat "executors.json")
                DRIVER_MAX_MEMORY=$(echo "$EXECUTORS_JSON" | jq -r '.[] | select(.id=="driver") | .maxMemory // "null"')

                exec1=$(echo "$EXECUTORS_JSON" | jq -r '.[] | select(.id != "driver") | .id' | sort -n | sed -n 1p)
                exec2=$(echo "$EXECUTORS_JSON" | jq -r '.[] | select(.id != "driver") | .id' | sort -n | sed -n 2p)

                extract_executor_data() {
                    local id=$1
                    if [ "$id" != "" ]; then
                        max_memory=$(echo "$EXECUTORS_JSON" | jq -r ".[] | select(.id==\"$id\") | .maxMemory // \"null\"")
                        peak_heap=$(echo "$EXECUTORS_JSON" | jq -r ".[] | select(.id==\"$id\") | .peakMemoryMetrics.JVMHeapMemory // \"null\"")
                        peak_offheap=$(echo "$EXECUTORS_JSON" | jq -r ".[] | select(.id==\"$id\") | .peakMemoryMetrics.JVMOffHeapMemory // \"null\"")
                        rss_memory=$(echo "$EXECUTORS_JSON" | jq -r ".[] | select(.id==\"$id\") | .peakMemoryMetrics.ProcessTreeJVMRSSMemory // \"null\"")
                        echo "$max_memory,$peak_heap,$peak_offheap,$rss_memory"
                    else
                        echo "null,null,null,null"
                    fi
                }

                EXEC1_DATA=$(extract_executor_data "$exec1")
                EXEC2_DATA=$(extract_executor_data "$exec2")

                REAL_PARTITION_LINE=$(grep "Number of partitions" "spark_log.txt")
                REAL_PARTITION=$(echo "${REAL_PARTITION_LINE}" | awk -F': ' '{print $2}')

                START=$(grep "Start-Time" "output_yarn.txt" | awk -F' : ' '{print $2}')
                END=$(grep "Finish-Time" "output_yarn.txt" | awk -F' : ' '{print $2}')
                ALLOC=$(awk '/Aggregate Resource Allocation/ { print $5 }' output_yarn.txt)

                DIFF_MS=$(( END - START ))

                TS=$(date -d '+2 hours' '+%Y-%m-%d %H:%M:%S')

                echo "$TS,spark,$SIZE,$DOC,$PARTITION,$REAL_PARTITION,$DRIVER_MAX_MEMORY,$EXEC1_DATA,$EXEC2_DATA,$ALLOC,$DIFF_MS" >> spark2.csv

                echo "Results for $INPUT_DIR saved in spark2.csv"
                ((OUTPUT_BASE_INDEX++))
            done
        done
    done
done

echo "Next output_index: $OUTPUT_BASE_INDEX"