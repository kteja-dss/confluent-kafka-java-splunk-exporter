package io.confluent.developer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

public class ConsumerAuditlogExporter {
    private static final String SPLUNK_HEC_URL = "SPLUNK_URL";
    private static final String SPLUNK_HEC_TOKEN = "SPLUNK_TOKEN";
    private static final long BATCH_DURATION_MS = 30000; // 30 seconds
    private static final String BOOTSTRAP_URL = "YOUR_CONFLUENT_KAFKA_CLUSTER_BOOTSTRAP_URL";
    private static final String BOOTSTRAP_API_KEY = "YOUR_CONFLUENT_KAFKA_CLUSTER_BOOTSTRAP_KEY";
    private static final String BOOTSTRAP_API_SECRET = "YOUR_CONFLUENT_KAFKA_CLUSTER_BOOTSTRAP_SECRET";

    private static void sendToSplunk(List<String> jsonPayloads) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(SPLUNK_HEC_URL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Splunk " + SPLUNK_HEC_TOKEN);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            // Combine all payloads into a single JSON array
            StringBuilder combinedPayload = new StringBuilder("[");
            for (int i = 0; i < jsonPayloads.size(); i++) {
                combinedPayload.append(jsonPayloads.get(i));
                if (i < jsonPayloads.size() - 1) {
                    combinedPayload.append(",");
                }
            }
            combinedPayload.append("]");

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = combinedPayload.toString().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed: HTTP error code : " + responseCode);
            }

        } catch (Exception e) {
            System.err.println("Error sending data to Splunk: " + e.getMessage());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static Properties getKafkaConsumerProps() {
        Properties props = new Properties();
        // User-specific properties
        props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_URL);
        props.put(SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=" + BOOTSTRAP_API_KEY
                        + " password=" + BOOTSTRAP_API_SECRET + "';");

        // Fixed properties
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(GROUP_ID_CONFIG, "confluent-kafka-splunk-exporter");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(MAX_POLL_RECORDS_CONFIG, "100");
        props.put(MAX_POLL_INTERVAL_MS_CONFIG, "30000");
        props.put(SASL_MECHANISM, "PLAIN");

        return props;
    }

    public static void main(final String[] args) {
        Properties props = getKafkaConsumerProps();
        final String topic = "confluent-audit-log-events";

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            List<String> batch = new ArrayList<>();
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

            // Schedule task to send batch every BATCH_DURATION_MS
            scheduler.scheduleAtFixedRate(() -> {
                if (!batch.isEmpty()) {
                    sendToSplunk(batch);
                    batch.clear();
                }
            }, BATCH_DURATION_MS, BATCH_DURATION_MS, TimeUnit.MILLISECONDS);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    String jsonPayload = "{\"event\": \"" + value.replace("\"", "\\\"") + "\"}";
                    batch.add(jsonPayload);

                    System.out.println(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
                }
            }
        }
    }
}
