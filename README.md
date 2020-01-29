# Message Bus
Basic framework for a Fabric Message Bus for Inter Actor Communication 


## Overview
Fabric communication across various actors in Control and Measurement Framework is implemented using Apache Kafka.

Apache Kafka is a distributed system designed for streams. It is built to be fault-tolerant, high-throughput, horizontally scalable, and allows geographically distributed data streams and stream processing applications.

Kafka enables event driven implementation of various actors/services. Events are both a Fact and a Trigger. Each fabric actor will be a producer for one topic following the Single Writer Principle and would subscribe to topics from other actors for communication. Messages are exchanged over Kafka using Apache Avro data serialization system. The following diagram gives an example of communication between Control Framework actors:

## Requirements
- Python 3.7+
- confluent-kafka
- confluent-kafka[avro]

## Usage
This package implements the interface for producer/consumer APIs to push/read messages to/from Kafka via Avro serialization. 

### Message and Schema
User is expected to inherit IMessage class(message.py) to define it's own members and over ride to_dict() function. It is also required to define the corresponding AVRO schema pertaining to the derived class. This new schema shall be used in producer and consumers.

Example schema for basic IMessage class is available in (schema/message.avsc)

### Producers
AvroProducerApi class implements the base functionality for an Avro Kafka producer. User is expected to inherit this class and override delivery_report method to handle message delivery for asynchronous produce. 

Example for usage available at the end of producer.py

### Consumers
AvroConsumerApi class implements the base functionality for an Avro Kafka consumer. User is expected to inherit this class and override process_message method to handle message processing for incoming message. 

Example for usage available at the end of consumer.py

### Admin API
AdminApi class provides support to carry out basic admin functions like create/delete topics/partions etc.


## How to bring up a test Kafka cluster to test
You can use the docker-compose.yaml file to bring up a simple Kafka cluster containing
- broker
- zookeeper 
- schema registry

Use the below command to bring up the cluster
```
docker-compose up -d
```

This should bring up following containers:
```
docker ps
CONTAINER ID        IMAGE                                   COMMAND                  CREATED             STATUS              PORTS                                              NAMES
8b9ebe91b61f        confluentinc/cp-schema-registry:5.4.0   "/etc/confluent/dock…"   6 hours ago         Up 6 hours          0.0.0.0:8081->8081/tcp                             schema-registry
aad65962159f        confluentinc/cp-kafka:5.4.0             "/etc/confluent/dock…"   6 hours ago         Up 6 hours          0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp   broker
4263ac0944ac        confluentinc/cp-zookeeper:5.4.0         "/etc/confluent/dock…"   6 hours ago         Up 6 hours          2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp         zookeeper
```
