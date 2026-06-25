#!/bin/bash

# ==========================================
# Rusquitto Stress Test Script
# ==========================================

IP="127.0.0.1"
PORT="1883"
PUBLISHER_INSTANCES=100  # Concurrent publishers per QoS level
MESSAGES_PER_PUB=50      # Messages sent per publisher

echo "Starting Rusquitto Test Environment..."

# Cleanup background processes on exit (Ctrl+C)
trap "echo -e '\nStopping all clients...'; kill 0" SIGINT SIGTERM

# ==========================================
# 1. Start Subscribers
# ==========================================
echo "--> Starting Subscribers..."

# QoS 0 (Fire and Forget)
mosquitto_sub -h $IP -p $PORT -t "test/qos0/#" -q 0 -V 5 -v > sub_qos0.log 2>&1 &

# QoS 1 (At least once)
mosquitto_sub -h $IP -p $PORT -t "test/qos1/#" -q 1 -V 5 -v > sub_qos1.log 2>&1 &

# QoS 2 (Exactly once)
mosquitto_sub -h $IP -p $PORT -t "test/qos2/#" -q 2 -V 5 -v > sub_qos2.log 2>&1 &

# Retained messages
mosquitto_sub -h $IP -p $PORT -t "test/retain/#" -q 1 -V 5 -v > sub_retain.log 2>&1 &

# Wait briefly for connections to establish
sleep 2

# ==========================================
# 2. Publisher Functions
# ==========================================

run_publisher_qos0() {
    local ID=$1
    local CID="Pub_QoS0_${ID}"
    for ((i=1; i<=MESSAGES_PER_PUB; i++)); do
        mosquitto_pub -h $IP -p $PORT -V 5 -i "$CID" -t "test/qos0/$ID" -m "Msg_$i_from_$CID" -q 0
    done
}

run_publisher_qos1() {
    local ID=$1
    local CID="Pub_QoS1_${ID}"
    for ((i=1; i<=MESSAGES_PER_PUB; i++)); do
        mosquitto_pub -h $IP -p $PORT -V 5 -i "$CID" -t "test/qos1/$ID" -m "Msg_$i_from_$CID" -q 1
    done
}

run_publisher_qos2() {
    local ID=$1
    local CID="Pub_QoS2_${ID}"
    for ((i=1; i<=MESSAGES_PER_PUB; i++)); do
        mosquitto_pub -h $IP -p $PORT -V 5 -i "$CID" -t "test/qos2/$ID" -m "Msg_$i_from_$CID" -q 2
    done
}

run_publisher_retain() {
    local ID=$1
    local CID="Pub_Retain_${ID}"
    # Publish a single retained message
    
    mosquitto_pub -h $IP -p $PORT -V 5 -i "$CID" -t "test/retain/$ID" -m "Retained_Data_from_$CID" -q 1 -r
}

# ==========================================
# 3. Spawn Publishers
# ==========================================
echo "--> Spawning $PUBLISHER_INSTANCES publishers per QoS..."

for ((i=1; i<=PUBLISHER_INSTANCES; i++))
do
    run_publisher_qos0 $i &
    run_publisher_qos1 $i &
    run_publisher_qos2 $i &

    # Distribute retained message tests evenly
    if (( i % 5 == 0 )); then
        run_publisher_retain $i &
    fi
done

# ==========================================
# 4. Wait & Finish
# ==========================================
echo "--> Waiting for publishers to finish..."
wait

echo "--> Test complete! Check the .log files for results."