#Note this code primarily showcases a template of exporting data to Splunk from Confluent kafka topics. Please feel free to modify based on your requirements.

#Instructions to start this application

- Modify values in the ConsumerAuditlogExporter class
- Build the fatjar with the command
  "gradle clean shadowJar"
- Run the application with the following command
  "java -jar build/libs/confluent-kafka-java-splunk-exporter-0.0.1.jar"
