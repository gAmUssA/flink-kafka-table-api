package com.example.util;

import java.io.IOException;
import java.util.Properties;
import org.apache.flink.util.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for loading and managing application configuration. */
public class ConfigUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

  /**
   * Loads configuration from both command line arguments and a properties file if specified.
   *
   * @param args Command line arguments
   * @return ParameterTool with loaded configuration
   */
  public static ParameterTool loadConfiguration(String[] args) {
    try {
      ParameterTool parameterTool = ParameterTool.fromArgs(args);

      // Check if a properties file was specified
      if (parameterTool.has("config-file")) {
        String propertiesFile = parameterTool.get("config-file");
        LOG.info("Loading configuration from: {}", propertiesFile);

        // Load properties file and merge with command line args (command line takes precedence)
        return ParameterTool.fromPropertiesFile(propertiesFile).mergeWith(parameterTool);
      }

      return parameterTool;
    } catch (IOException e) {
      LOG.error("Failed to load configuration", e);
      return ParameterTool.fromArgs(args);
    }
  }

  /**
   * Creates Kafka producer/consumer properties from the configuration.
   *
   * @param params ParameterTool with loaded configuration
   * @param prefix Property prefix to filter (e.g., "kafka.")
   * @return Properties object with Kafka configuration
   */
  public static Properties createKafkaProperties(ParameterTool params, String prefix) {
    Properties kafkaProps = new Properties();

    // Get all properties with the given prefix
    Properties filteredProps = params.getProperties();
    filteredProps.stringPropertyNames().stream()
        .filter(key -> key.startsWith(prefix))
        .forEach(
            key -> {
              String kafkaKey = key.substring(prefix.length());
              kafkaProps.setProperty(kafkaKey, filteredProps.getProperty(key));
            });

    return kafkaProps;
  }
}
