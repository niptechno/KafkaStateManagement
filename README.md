# KafkaStateManagement

This project designed for purpose, to manage the state of kafka in MYSQL table.
This Project allows a consumer(designed using Spark Streaming->Dstream) to accept the message from multiple producer in real-time. Accordingly fetch the topic name its partition value, if topic name already exist in Mysql Table update the offset(value that represents the number of messages recieved from a topic) and partition.
