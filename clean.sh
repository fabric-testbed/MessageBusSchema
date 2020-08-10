docker stop zookeeper broker1 schemaregistry
docker rm zookeeper broker1 schemaregistry
docker rmi confluentinc/cp-schema-registry:latest confluentinc/cp-kafka:latest confluentinc/cp-zookeeper:latest



