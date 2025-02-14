package org.meeuw.util.kafka;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class Consume implements Callable<Integer> {

    @Option(names = {"-s", "--bootstrap-servers"}, description = "The bootstrap servers", defaultValue = "", required = false)
    String bootstrapServers;

    @Option(names = {"-c", "--configuration"}, description = "configuration file", required = false)
    String configFile;

    @Parameters(index = "0..*", description = "The topics to consume from")
    String[] topics;




    public static void main(String[] argv) {
        int exitCode = new CommandLine(new Consume()).setTrimQuotes(true).execute(argv);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        if (configFile == null) {
            configFile = System.getProperty("user.home") + File.separator + "conf" + File.separator + "kafka.properties";
        }
        File config = new File(configFile);
        Properties properties = new Properties();
        if (config.canRead()) {
            try (InputStream is = new FileInputStream(config)) {
                properties.load(is);
            } catch (IOException e) {
                System.err.println("Could not read configuration file " + configFile);
            }
        }
        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if ("".equals(bootstrapServers)) {
            properties.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        String accessKeyId = (String) properties.get("kafka.aws.accessKeyId");
        System.setProperty("aws.accessKeyId", accessKeyId);
        String secretKey = (String) properties.get("kafka.aws.secretKey");
        System.setProperty("aws.secretAccessKey", secretKey);

        boolean aws = accessKeyId != null;
        if (aws) {
            properties.putIfAbsent("security.protocol", "SASL_SSL");
            properties.putIfAbsent("sasl.mechanism", "AWS_MSK_IAM");
            properties.putIfAbsent("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            properties.putIfAbsent("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }

        getRecords(properties, topics)
            .forEach((r) -> {
                System.out.println("\n\n-----------" + r.topic() + "\t" + r.partition() + "\t" + r.offset() + "\n");
                System.out.println(Instant.ofEpochMilli(r.timestamp()) + "\t" + r.key());
                r.headers().forEach((h) -> {
                    System.out.println(h.key() + ": " + new String(h.value()));
                });
                System.out.println("\n\n" + r.serializedValueSize() + " bytes\n" + r.value());
            });
            return 0;
    }

    public static Stream<ConsumerRecord<String, String>> getRecords(Properties properties, String[] topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));
        final List<ConsumerRecord<String, String>> retval = new ArrayList<>();
        return Stream.generate(() -> {
            while (retval.isEmpty()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> record : records) {
                    retval.add(record);
                }
            }
            return retval.remove(0);
        }).onClose(consumer::close);
    }

}
