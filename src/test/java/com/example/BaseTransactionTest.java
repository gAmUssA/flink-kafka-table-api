package com.example;

import com.example.model.Transaction;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Base test class with common setup for Kafka and Schema Registry */
@Testcontainers
public abstract class BaseTransactionTest {
  protected static final Logger LOG = LoggerFactory.getLogger(BaseTransactionTest.class);

  // Static initializer to ensure logging is properly configured
  static {
    // This is just to ensure the logging system is initialized
    LOG.info("Initializing logging for BaseTransactionTest");
  }

  protected static final String INPUT_TOPIC = "transactions";
  protected static final String OUTPUT_TOPIC = "approved_transactions";
  protected static final DockerImageName CONFLUENT_PLATFORM_IMAGE =
      DockerImageName.parse("confluentinc/cp-schema-registry:7.9.0");

  protected static final Network NETWORK = Network.newNetwork();

  // Start Kafka container
  @Container
  protected static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"))
          .withNetwork(NETWORK)
          .withNetworkAliases("kafka");

  // Start Schema Registry container
  @Container
  protected static final GenericContainer<?> SCHEMA_REGISTRY =
      new GenericContainer<>(CONFLUENT_PLATFORM_IMAGE)
          .withNetwork(NETWORK)
          .withExposedPorts(8081)
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
          // Disable Schema Registry compatibility check due to
          // https://issues.apache.org/jira/browse/FLINK-33045 and
          // https://issues.apache.org/jira/browse/FLINK-36650
          // Appears to be because the namespace + name doesn't match with what Flink would use
          .withEnv("SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL", "NONE")
          .dependsOn(KAFKA);

  // Flink MiniCluster
  protected static MiniClusterWithClientResource flinkCluster;

  @BeforeAll
  public static void setup() throws Exception {
    // Start Flink MiniCluster
    Configuration configuration = new Configuration();
    configuration.set(RestOptions.BIND_PORT, "8081-8089");

    flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumberSlotsPerTaskManager(4)
                .setNumberTaskManagers(2)
                .build());
    flinkCluster.before();

    // Create Kafka topics
    createKafkaTopic(INPUT_TOPIC);
    createKafkaTopic(OUTPUT_TOPIC);

    LOG.info("Test environment setup completed");
  }

  @AfterAll
  public static void teardown() {
    if (flinkCluster != null) {
      flinkCluster.after();
    }
  }

  protected static void createKafkaTopic(String topicName) throws Exception {
    // Create Kafka topics using AdminClient
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      // Configure the topic with 1 partition for testing
      NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

      // Create the topic and wait for it to complete
      adminClient.createTopics(Collections.singleton(newTopic)).all().get();
      LOG.info("Created Kafka topic: {}", topicName);
    } catch (Exception e) {
      LOG.error("Failed to create Kafka topic: {}", topicName, e);
      throw e;
    }
  }

  protected Transaction createTransaction(String currency, double amount, String status) {
    // Create an Instant for the timestamp since our Avro schema has logicalType="timestamp-millis"
    Instant timestamp = Instant.now();

    return Transaction.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setAmount(amount)
        .setCurrency(currency)
        .setTimestamp(timestamp)
        .setDescription("Test transaction")
        .setMerchant("Test Merchant")
        .setCategory("Test")
        .setStatus(status)
        .setUserId(UUID.randomUUID().toString())
        .setMetadata(Collections.emptyMap())
        .build();
  }

  protected List<Transaction> createTestTransactions() {
    List<Transaction> transactions = new java.util.ArrayList<>();

    // Create a mix of transactions with different statuses
    transactions.add(createTransaction("USD", 100.0, "APPROVED"));
    transactions.add(createTransaction("EUR", 200.0, "APPROVED"));
    transactions.add(createTransaction("GBP", 150.0, "APPROVED"));
    transactions.add(createTransaction("USD", 300.0, "CANCELLED")); // Should be filtered out
    transactions.add(createTransaction("EUR", 250.0, "PENDING"));

    return transactions;
  }

  protected void sendTransactionsToKafka(List<Transaction> transactions)
      throws InterruptedException, ExecutionException {
    try (KafkaProducer<String, Transaction> producer = createAvroProducer()) {
      // Send each transaction to Kafka
      for (Transaction transaction : transactions) {
        ProducerRecord<String, Transaction> record =
            new ProducerRecord<>(INPUT_TOPIC, transaction.getId().toString(), transaction);

        producer.send(record).get(); // Wait for acknowledgment
        LOG.info(
            "Sent transaction {}: {} {}",
            transaction.getId(),
            transaction.getAmount(),
            transaction.getCurrency());
      }

      producer.flush();
      LOG.info("Finished sending {} transactions to Kafka", transactions.size());
    }
  }

  protected static <T extends SpecificRecord> KafkaProducer<String, T> createAvroProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(
        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081));

    return new KafkaProducer<>(props);
  }
}
