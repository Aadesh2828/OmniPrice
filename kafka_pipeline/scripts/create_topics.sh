#!/bin/bash

# ============================================================
# OmniPrice Kafka Topic Creation
# Compatible with ZooKeeper-based Kafka
# ============================================================

KAFKA_ZOOKEEPER="localhost:2181"

CLICKSTREAM_TOPIC="omniprice_clickstream"

COMPETITOR_TOPIC="omniprice_competitor_prices"


echo "============================================================"
echo "OMNIPRICE KAFKA TOPIC CREATION"
echo "============================================================"

echo ""
echo "ZooKeeper: $KAFKA_ZOOKEEPER"

echo ""
echo "Creating Clickstream Topic..."

kafka-topics.sh \
    --create \
    --zookeeper "$KAFKA_ZOOKEEPER" \
    --topic "$CLICKSTREAM_TOPIC" \
    --partitions 3 \
    --replication-factor 1

echo ""

echo "Creating Competitor Price Topic..."

kafka-topics.sh \
    --create \
    --zookeeper "$KAFKA_ZOOKEEPER" \
    --topic "$COMPETITOR_TOPIC" \
    --partitions 3 \
    --replication-factor 1

echo ""

echo "============================================================"
echo "AVAILABLE OMNIPRICE KAFKA TOPICS"
echo "============================================================"

echo ""

kafka-topics.sh \
    --list \
    --zookeeper "$KAFKA_ZOOKEEPER"

echo ""

echo "============================================================"
echo "TOPIC CREATION PROCESS COMPLETED"
echo "============================================================"
