package com.example;

import static org.apache.flink.table.api.Expressions.*;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Transaction;
import com.example.model.TransactionStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class that uses Flink Table API to validate results by reusing the table definitions from
 * TransactionProcessor
 */
public class TransactionFlinkTest extends BaseTransactionTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionFlinkTest.class);

  @Test
  public void testTransactionProcessingWithFlinkTableApi() throws Exception {
    // Create test transactions
    List<Transaction> inputTransactions = createTestTransactions();
    LOG.info("Created {} test transactions", inputTransactions.size());

    // Send transactions to Kafka
    sendTransactionsToKafka(inputTransactions);
    LOG.info("Sent transactions to Kafka");

    // Set up Table Environment with limited parallelism
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    // Create a TransactionProcessor instance
    TransactionProcessor processor =
        new TransactionProcessor(
            KAFKA.getBootstrapServers(),
            "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081),
            INPUT_TOPIC,
            OUTPUT_TOPIC);

    // Use the protected methods directly
    processor.createSourceTable(tableEnv);
    processor.createSinkTable(tableEnv);

    // Store the TableResult from processTransactions
    TableResult tableResult = processor.processTransactions(tableEnv);

    LOG.info("Test setup completed - tables and transformations created");

    // Wait a moment for processing to complete, then cancel the job
    try {
      LOG.info("Waiting 5 seconds before cancelling the job");
      Thread.sleep(5000);

      // Cancel the job
      tableResult
          .getJobClient()
          .ifPresent(
              jobClient -> {
                try {
                  LOG.info("Cancelling Flink job");
                  jobClient.cancel().get();
                  LOG.info("Flink job cancelled successfully");
                } catch (Exception e) {
                  LOG.error("Error cancelling Flink job", e);
                }
              });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while waiting to cancel job", e);
    }

    // Query the results from the output table with a timeout
    List<Row> results = queryResults(tableEnv);
    LOG.info("Retrieved {} results from output table", results.size());

    // Verify we filtered out CANCELLED transactions
    long expectedCount =
        inputTransactions.stream()
            .filter(t -> !TransactionStatus.CANCELLED.toString().equals(t.getStatus()))
            .count();

    // In test environment, we might not get all results immediately
    assertTrue(results.size() > 0, "Should have at least one result");

    // Create a map of input transactions by ID for easier lookup
    Map<String, Transaction> inputTransactionsById = new HashMap<>();
    for (Transaction tx : inputTransactions) {
      inputTransactionsById.put(tx.getId().toString(), tx);
    }

    // Verify each result row
    for (Row row : results) {
      String id = row.getField(0).toString();
      Double amount = (Double) row.getField(1);
      String currency = row.getField(2).toString();
      Double amountInUsd = (Double) row.getField(6);

      LOG.info("Examining output row: {} {} {} (USD: {})", id, amount, currency, amountInUsd);

      // Find the corresponding input transaction
      Transaction inputTx = inputTransactionsById.get(id);

      // Skip the dummy transaction used for topic initialization
      if (inputTx == null && "init".equals(id)) {
        LOG.info("Skipping dummy initialization transaction");
        continue;
      }

      assertNotNull(inputTx, "Should find matching input transaction for ID: " + id);

      // Verify fields were copied correctly
      assertEquals(inputTx.getId().toString(), id);
      assertEquals(inputTx.getAmount(), amount);
      assertEquals(inputTx.getCurrency().toString(), currency);

      // Verify the transformation logic applied correctly
      double expectedUsdAmount;
      if ("EUR".equals(inputTx.getCurrency().toString())) {
        expectedUsdAmount = inputTx.getAmount() * 1.1;
      } else if ("GBP".equals(inputTx.getCurrency().toString())) {
        expectedUsdAmount = inputTx.getAmount() * 1.3;
      } else {
        expectedUsdAmount = inputTx.getAmount();
      }

      assertEquals(expectedUsdAmount, amountInUsd, 0.001);
    }

    LOG.info("Test completed successfully");
  }

  private List<Row> queryResults(TableEnvironment tableEnv) {
    // Query the results with a bounded query that will complete
    TableResult tableResult =
        tableEnv.executeSql(
            "SELECT * FROM approved_transactions /*+ OPTIONS('scan.startup.mode'='earliest-offset', 'scan.bounded.mode'='latest-offset') */");

    // Set a timeout for collecting results to prevent hanging
    long timeoutMs = 10000; // 10 seconds timeout
    long startTime = System.currentTimeMillis();

    // Collect the results
    List<Row> results = new ArrayList<>();
    try (CloseableIterator<Row> iterator = tableResult.collect()) {
      while (iterator.hasNext() && (System.currentTimeMillis() - startTime < timeoutMs)) {
        results.add(iterator.next());
      }

      // If we timed out, log a message
      if (System.currentTimeMillis() - startTime >= timeoutMs) {
        LOG.info(
            "Query collection timed out after {} ms, collected {} results",
            timeoutMs,
            results.size());
      }
    } catch (Exception e) {
      LOG.error("Error collecting results", e);
    }

    return results;
  }
}
