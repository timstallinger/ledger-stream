package io.ledgerstream.app.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.ledgerstream.adapter.in.messaging.DlqRecordPersister;
import io.ledgerstream.adapter.in.messaging.KafkaConsumerProperties;
import io.ledgerstream.adapter.in.messaging.LedgerConsumerMessage;
import io.ledgerstream.adapter.in.messaging.LedgerEventKafkaListener;
import io.ledgerstream.adapter.in.messaging.LedgerEventRecordMapper;
import io.ledgerstream.adapter.in.messaging.LedgerEventRecordProcessor;
import io.ledgerstream.adapter.in.messaging.RawPayloadPreservingAvroDeserializer;
import io.ledgerstream.adapter.in.messaging.RawPayloadPreservingSerializer;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.application.port.in.ProjectLedgerEventUseCase;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import io.ledgerstream.application.port.out.DlqRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaConsumerProperties.class)
public class KafkaConsumerConfiguration {

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
    public DlqRecordPersister dlqRecordPersister(
            final DlqRepository dlqRepository,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        return new DlqRecordPersister(dlqRepository, transactionRunner, clock);
    }

    @Bean
    public ConsumerFactory<String, LedgerConsumerMessage> ledgerConsumerFactory(
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

        RawPayloadPreservingAvroDeserializer valueDeserializer =
                new RawPayloadPreservingAvroDeserializer(avroDelegate);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LedgerConsumerMessage> ledgerKafkaListenerContainerFactory(
            final ConsumerFactory<String, LedgerConsumerMessage> ledgerConsumerFactory,
            final KafkaConsumerProperties consumerProperties
    ) {
        ConcurrentKafkaListenerContainerFactory<String, LedgerConsumerMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(ledgerConsumerFactory);
        factory.setConcurrency(consumerProperties.concurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    @Primary
    public KafkaTemplate<String, LedgerBookingEvent> avroKafkaTemplate(
            final KafkaProperties kafkaProperties,
            final ObjectProvider<KafkaConnectionDetails> connectionDetailsProvider
    ) {
        Map<String, Object> props = baseProducerProps(kafkaProperties, connectionDetailsProvider);
        ProducerFactory<String, LedgerBookingEvent> factory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }

    @Bean
    public KafkaTemplate<String, Object> rawBytesRetryKafkaTemplate(
            final KafkaProperties kafkaProperties,
            final ObjectProvider<KafkaConnectionDetails> connectionDetailsProvider
    ) {
        Map<String, Object> props = baseProducerProps(kafkaProperties, connectionDetailsProvider);
        ProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(
                props,
                new StringSerializer(),
                new RawPayloadPreservingSerializer()
        );
        return new KafkaTemplate<>(factory);
    }

    @Bean
    public KafkaTemplate<String, byte[]> rawBytesReplayKafkaTemplate(
            final KafkaProperties kafkaProperties,
            final ObjectProvider<KafkaConnectionDetails> connectionDetailsProvider
    ) {
        Map<String, Object> props = baseProducerProps(kafkaProperties, connectionDetailsProvider);
        ProducerFactory<String, byte[]> factory = new DefaultKafkaProducerFactory<>(
                props,
                new StringSerializer(),
                new ByteArraySerializer()
        );
        return new KafkaTemplate<>(factory);
    }

    @Bean
    @ConditionalOnProperty(name = "ledger.kafka.consumer.enabled", havingValue = "true", matchIfMissing = true)
    public LedgerEventKafkaListener ledgerEventKafkaListener(
            final LedgerEventRecordProcessor recordProcessor,
            final DlqRecordPersister dlqRecordPersister,
            final ConsumerEventObserver consumerEventObserver
    ) {
        return new LedgerEventKafkaListener(recordProcessor, dlqRecordPersister, consumerEventObserver);
    }

    private static Map<String, Object> baseProducerProps(
            final KafkaProperties kafkaProperties,
            final ObjectProvider<KafkaConnectionDetails> connectionDetailsProvider
    ) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        KafkaConnectionDetails connectionDetails = connectionDetailsProvider.getIfAvailable();
        if (connectionDetails != null) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getProducer().getBootstrapServers());
        }
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }
}
