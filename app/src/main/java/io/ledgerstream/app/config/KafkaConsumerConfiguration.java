package io.ledgerstream.app.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.ledgerstream.adapter.in.messaging.KafkaConsumerProperties;
import io.ledgerstream.adapter.in.messaging.LedgerEventKafkaListener;
import io.ledgerstream.adapter.in.messaging.LedgerEventRecordMapper;
import io.ledgerstream.adapter.in.messaging.LedgerEventRecordProcessor;
import io.ledgerstream.application.port.in.ProjectLedgerEventUseCase;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaConsumerProperties.class)
public class KafkaConsumerConfiguration {

    public record DeserializationFailureMarker(String topic, String errorMessage) {
    }

    @Bean
    public LedgerEventRecordMapper ledgerEventRecordMapper() {
        return new LedgerEventRecordMapper();
    }

    @Bean
    public LedgerEventRecordProcessor ledgerEventRecordProcessor(
            final ProjectLedgerEventUseCase projectLedgerEventUseCase,
            final LedgerEventRecordMapper recordMapper,
            final ConsumerEventObserver consumerEventObserver
    ) {
        return new LedgerEventRecordProcessor(projectLedgerEventUseCase, recordMapper, consumerEventObserver);
    }

    @Bean
    public ConsumerFactory<String, Object> ledgerConsumerFactory(
            final KafkaProperties kafkaProperties,
            final ObjectProvider<KafkaConnectionDetails> connectionDetailsProvider,
            final KafkaConsumerProperties consumerProperties
    ) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        KafkaConnectionDetails connectionDetails = connectionDetailsProvider.getIfAvailable();
        if (connectionDetails != null) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getConsumer().getBootstrapServers());
        }
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.groupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        KafkaAvroDeserializer avroDelegate = new KafkaAvroDeserializer();
        avroDelegate.configure(props, false);

        ErrorHandlingDeserializer<Object> valueDeserializer = new ErrorHandlingDeserializer<>(avroDelegate);
        valueDeserializer.setFailedDeserializationFunction(info -> new DeserializationFailureMarker(
                info.getTopic(),
                info.getException() != null ? info.getException().getMessage() : "deserialization failed"
        ));

        Deserializer<String> keyDeserializer = new StringDeserializer();
        return new DefaultKafkaConsumerFactory<>(props, keyDeserializer, valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> ledgerKafkaListenerContainerFactory(
            final ConsumerFactory<String, Object> ledgerConsumerFactory,
            final KafkaConsumerProperties consumerProperties
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(ledgerConsumerFactory);
        factory.setConcurrency(consumerProperties.concurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    @ConditionalOnProperty(name = "ledger.kafka.consumer.enabled", havingValue = "true", matchIfMissing = true)
    public LedgerEventKafkaListener ledgerEventKafkaListener(final LedgerEventRecordProcessor recordProcessor) {
        return new LedgerEventKafkaListener(recordProcessor);
    }
}
