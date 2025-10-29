

docker exec -it broker-1   /opt/kafka/bin/kafka-topics.sh  --create  --topic test-topic --partitions 3   --replication-factor 1  --bootstrap-server broker-1:19092
docker exec -it broker-1   /opt/kafka/bin/kafka-console-consumer.sh --topic test-topic  --bootstrap-server broker-1:19092  --from-beginning
docker exec -it broker-1   /opt/kafka/bin/kafka-console-producer.sh --topic test-topic --bootstrap-server broker-1:19092
docker exec -it broker-1   /opt/kafka/bin/kafka-topics.sh   --bootstrap-server broker-1:19092  --list


Invoke-WebRequest -Uri "http://localhost:8088/connectors/pg-source/config" `
  -Method Put `
  -Headers @{ "Content-Type" = "application/json" } `
  -InFile ".\pg_source_config.json"

docker exec spark-master bash -c "
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioAccessKey \
  --conf spark.hadoop.fs.s3a.secret.key=minioSecretKey \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  /opt/spark/work-dir/Spark_ETL.py
"