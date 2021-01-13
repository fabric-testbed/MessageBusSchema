[![Requirements Status](https://requires.io/github/fabric-testbed/MessageBusSchema/requirements.svg?branch=master)](https://requires.io/github/fabric-testbed/MessageBusSchema/requirements/?branch=master)

[![PyPI](https://img.shields.io/pypi/v/fabric-message-bus?style=plastic)](https://pypi.org/project/fabric-message-bus/)

# Message Bus Schema and Message Definition
Basic framework for a Fabric Message Bus Schema and Messages for Inter Actor Communication 


## Overview
Fabric communication across various actors in Control and Measurement Framework is implemented using Apache Kafka.

Apache Kafka is a distributed system designed for streams. It is built to be fault-tolerant, high-throughput, horizontally scalable, and allows geographically distributed data streams and stream processing applications.

Kafka enables event driven implementation of various actors/services. Events are both a Fact and a Trigger. Each fabric actor will be a producer for one topic following the Single Writer Principle and would subscribe to topics from other actors for communication. Messages are exchanged over Kafka using Apache Avro data serialization system. 

## Requirements
- Python 3.7+
- confluent-kafka
- confluent-kafka[avro]

## Installation
```
$ pip3 install .
```

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
### Generate Credentials
You must generate CA certificates (or use yours if you already have one) and then generate a keystore and truststore for brokers and clients.
```
cd $(pwd)/secrets
./create-certs.sh
(Type yes for all "Trust this certificate? [no]:" prompts.)
cd -
```
Set the environment variable for the secrets directory. This is used in later commands. Make sure that you are in the MessageBus directory.
```
export KAFKA_SSL_SECRETS_DIR=$(pwd)/secrets
```
### Bring up the containers
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
CONTAINER ID        IMAGE                                    COMMAND                  CREATED             STATUS              PORTS                                                                                        NAMES
189ba0e70b97        confluentinc/cp-schema-registry:latest   "/etc/confluent/dock…"   58 seconds ago      Up 58 seconds       0.0.0.0:8081->8081/tcp                                                                       schemaregistry
49616f1c9b0a        confluentinc/cp-kafka:latest             "/etc/confluent/dock…"   59 seconds ago      Up 58 seconds       0.0.0.0:9092->9092/tcp, 0.0.0.0:19092->19092/tcp                                             broker1
c9d19c82558d        confluentinc/cp-zookeeper:latest         "/etc/confluent/dock…"   59 seconds ago      Up 59 seconds       2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp                                                   zookeeper
```
