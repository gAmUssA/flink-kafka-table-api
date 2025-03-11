# Flink Kafka Transaction Processor

A real-time transaction processing application built with Apache Flink® Table API and Apache Kafka®. 
This application demonstrates how to use Flink's Table API to process transaction data from Kafka, 
apply transformations, and write the results back to Kafka.

## Overview

This application:
- Reads transaction data from a Kafka topic using Avro serialization with Confluent Schema Registry
- Filters out cancelled transactions
- Transforms the data (including static currency conversion to USD)
- Writes approved transactions to another Kafka topic

## Features

- **Streaming Processing**: Uses Flink's streaming capabilities for real-time transaction processing
- **Table API**: Demonstrates Flink's Table API for declarative data transformations
- **Avro Serialization**: Uses Avro for efficient data serialization with schema evolution support
- **Schema Registry Integration**: Works with Confluent Schema Registry for schema management
- **Testcontainers**: Includes integration tests using Testcontainers for Kafka testing

## Prerequisites

- Java 11+
- Maven 3.6+
- Docker (for running integration tests)

## Building the Application

```bash
mvn clean package
```

This will:
- Compile the Java code
- Generate Java classes from Avro schemas
- Run tests
- Create an uber JAR with all dependencies

## Running the Application

```bash
java -jar target/flink-kafka-processor-1.0-SNAPSHOT.jar \
  --bootstrap-servers localhost:9092 \
  --schema-registry-url http://localhost:8081 \
  --input-topic transactions \
  --output-topic approved_transactions
```

### Configuration Options

You can configure the application using command-line arguments:

- `--bootstrap-servers`: Kafka bootstrap servers (default: localhost:9092)
- `--schema-registry-url`: Schema Registry URL (default: http://localhost:8081)
- `--input-topic`: Input Kafka topic (default: transactions)
- `--output-topic`: Output Kafka topic (default: approved_transactions)

## Data Models

### Input Transaction

The input transaction data includes:
- `id`: Transaction identifier
- `amount`: Transaction amount
- `currency`: Currency code (e.g., USD, EUR, GBP)
- `timestamp`: Transaction timestamp
- `description`: Optional transaction description
- `merchant`: Merchant name
- `category`: Optional transaction category
- `status`: Transaction status (PENDING, APPROVED, CANCELLED, REJECTED)
- `userId`: User identifier
- `metadata`: Optional key-value metadata

### Output Approved Transaction

The output approved transaction data includes:
- `id`: Transaction identifier
- `amount`: Original transaction amount
- `currency`: Original currency code
- `timestamp`: Original transaction timestamp
- `merchant`: Merchant name
- `userId`: User identifier
- `amountInUsd`: Amount converted to USD
- `processingTimestamp`: Timestamp when the transaction was processed

## Development

### Project Structure

```
src/
├── main/
│   ├── avro/                  # Avro schema definitions
│   ├── java/
│   │   └── com/
│   │       └── example/
│   │           ├── model/     # Generated Avro model classes
│   │           ├── util/      # Utility classes
│   │           └── TransactionProcessor.java  # Main application class
│   └── resources/
│       └── log4j2.xml         # Logging configuration
└── test/
    ├── java/
    │   └── com/
    │       └── example/       # Test classes
    └── resources/             # Test resources
```

### Testing

The project includes several test classes:
- `BaseTransactionTest`: Base class for transaction tests
- `TransactionFlinkTest`: Tests for Flink-based processing logic
- `TransactionProducerTest`: Tests for producing transactions to Kafka
- `TransactionConsumerTest`: Tests for consuming transactions from Kafka

Run tests with:

```bash
mvn test
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details. 