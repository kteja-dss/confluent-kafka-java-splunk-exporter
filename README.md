#Note this code primarily showcases a template of exporting data to Splunk from Confluent kafka topics, however, it's recommended to use the Connector on Confluent Cloud unless there's a special requirement like having clusters on the private vpcs. Please feel free to modify based on your requirements. 

#Instructions to start this application

Modify values in the ConsumerAuditlogExporter class
Build the fatjar with the command "gradle clean shadowJar"
Run the application with the following command "java -jar build/libs/confluent-kafka-java-splunk-exporter-0.0.1.jar"
