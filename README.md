# KafkaStateManager

This project designed for purpose, to manage the state of kafka in MYSQL table.
This Project allows a consumer(designed using Spark Streaming -> Dstream) to accept messages from multiple producers in real-time.Then, extract the topic name its partition value from the rdd(Dstream type), if topic name already exist in Mysql Table update the offset(value that represents the number of messages recieved from a topic) and partition value, otherwise insert the topic name, partition value in table and set the offset value to 1.
