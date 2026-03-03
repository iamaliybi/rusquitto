#!/bin/bash

IP="127.0.0.1"
PORT="1883"
TOPIC="test/v5_stress"
INSTANCES=1000

trap "echo 'Stopping...'; kill 0" SIGINT

mosquitto_sub -h $IP -p $PORT -t $TOPIC -V 5 -v &

sleep 1

run_publisher() {
    local ID=$1
    local CID="V5_Client_${ID}_$RANDOM"
    local COUNT=1

    while true
    do
        MSG="v5_Msg_$COUNT"
        mosquitto_pub -h $IP -p $PORT -V 5 -i "$CID" -t $TOPIC -m "$MSG"
        echo "[ID: $CID] Published: $MSG"
        ((COUNT++))
        sleep 10
    done
}

for ((i=1; i<=INSTANCES; i++))
do
    run_publisher $i &
done

wait