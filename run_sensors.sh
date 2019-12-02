#!/bin/bash
for b in {1..100}; do
    let a="(b-1)*10+1"
    let b="(b-1)*10+10"
    printf "Generating sensors $a to $b"
    for p in {0..9}; do
        let i="$a + $p"
        python populate_sensor_data.py --quiet --sensor $i sensor.csv &
        pids[$p]=$!
    done
    for pid in ${pids[*]}; do
        wait $pid
    done
    echo "...done"
done

