echo "\nList of current topics:"
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --list --zookeeper localhost:2181

echo "\n***** Deleting last topics *****"
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic wrongPurchases
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic cancelledPurchases
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic anomalyKmeans
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic anomalyBisection

echo "\n***** Creating new topics *****"
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic wrongPurchases --replication-factor 1 --partitions 3
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic cancelledPurchases --replication-factor 1 --partitions 3
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic anomalyKmeans --replication-factor 1 --partitions 3
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic anomalyBisection --replication-factor 1 --partitions 3

echo "\nList of topics:"
sh /opt/Kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --list --zookeeper localhost:2181