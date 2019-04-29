package kstest;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;

import static org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

@Slf4j
@Configuration
public class KafkaStreamsConfig implements ApplicationListener<ContextRefreshedEvent> {

  private String bootstrapServers = "localhost:9030";
  @Autowired
  private Tracing tracing;

  private KafkaStreamsTracing kafkaStreamsTracing;

  Properties kStreamsConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-test");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArray().getClass().getName());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-streams-test-clientid");
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafka-streams-test-tx-id");
    props.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, READ_COMMITTED);
    Properties properties = new Properties();
    properties.putAll(props);
    return properties;
  }

  @Override public void onApplicationEvent(ContextRefreshedEvent event) {
    kafkaStreamsTracing = KafkaStreamsTracing.create(tracing);
    StreamsBuilder sb = new StreamsBuilder();
    KStream<String, byte[]> stream = sb.stream("input-topic");
    stream.transform(kafkaStreamsTracing.filter("test-filter", (key, value) -> true))
        .to("output-topic");
    Topology topology = sb.build();
    KafkaStreams kafkaStreams = kafkaStreamsTracing.kafkaStreams(topology, kStreamsConfig());
    kafkaStreams.start();
  }
}