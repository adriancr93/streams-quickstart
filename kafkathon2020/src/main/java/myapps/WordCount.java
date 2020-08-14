package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WPN_Test_KafkaStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://127.0.0.1:8081");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test-group");

        /*props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Stream-Kafka");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Stream-Kafka-Client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"UWHNZRVDZQO72PYC\" password=\"rb/NnyK2emH5nZsOc7h3a5xkTRgvmziMZBYh1gHRPbiM5lh0a1L5n39neJWNPlke\";");
        props.put(SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "https");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty("schema.registry.url"));

        props.put("producer.confluent.batch.expiry.ms", 920000000);
        props.put("zookeerper.connect", "localhost:2181");
        props.put("group.id", "test-group");
        props.put("schema.registry.url", "https://psrc-4rw99.us-central1.gcp.confluent.cloud");
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("schema.registry.basic.auth.user.info", "FCFTOSXJOTAMX57N:nCTOUD" + "/" +"jxEY3cyv7msFe3gXxWY4jqUxa7ELWV8eDReZaZHPKv/gB0xCSQiX6OTMh");*/

        return props;
    }

    public static void main(final String[] args) throws Exception{
        final Properties props = getStreamsConfig();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("TextLinesTopic");
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}