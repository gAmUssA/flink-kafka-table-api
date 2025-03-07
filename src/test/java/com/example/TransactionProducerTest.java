package com.example;

import com.example.model.Transaction;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class that focuses on producing test data to Kafka */
public class TransactionProducerTest extends BaseTransactionTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionProducerTest.class);

  @Test
  public void testProduceTransactionsToKafka() throws Exception {
    // Create test transactions
    List<Transaction> inputTransactions = createTestTransactions();
    LOG.info("Created {} test transactions", inputTransactions.size());

    // Log details of each transaction
    for (Transaction tx : inputTransactions) {
      LOG.info(
          "Test transaction: {} {} {} {}",
          tx.getId(),
          tx.getAmount(),
          tx.getCurrency(),
          tx.getStatus());
    }

    // Send transactions to Kafka
    sendTransactionsToKafka(inputTransactions);
    LOG.info(
        "Successfully sent {} transactions to Kafka topic '{}'",
        inputTransactions.size(),
        INPUT_TOPIC);
  }
}
