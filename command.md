

docker exec -it broker-1   /opt/kafka/bin/kafka-topics.sh  --create  --topic test-topic --partitions 3   --replication-factor 1  --bootstrap-server broker-1:19092
docker exec -it broker-1   /opt/kafka/bin/kafka-console-consumer.sh --topic test-topic  --bootstrap-server broker-1:19092  --from-beginning
docker exec -it broker-1   /opt/kafka/bin/kafka-console-producer.sh --topic test-topic --bootstrap-server broker-1:19092
docker exec -it broker-1   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server broker-1:19092  --list


Invoke-WebRequest -Uri "http://localhost:8088/connectors/pg-source/config" `
  -Method Put `
  -Headers @{ "Content-Type" = "application/json" } `
  -InFile ".\pg_source_config.json"