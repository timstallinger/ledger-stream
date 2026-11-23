package io.ledgerstream.app.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.adapter.in.rest.AccountController;
import io.ledgerstream.adapter.in.rest.BookingController;
import io.ledgerstream.adapter.in.rest.ProblemDetailExceptionHandler;
import io.ledgerstream.adapter.out.messaging.KafkaOutboxEventPublisher;
import io.ledgerstream.adapter.out.messaging.KafkaTopicProperties;
import io.ledgerstream.adapter.out.messaging.OutboxRelayProperties;
import io.ledgerstream.adapter.out.messaging.OutboxRelayScheduler;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.adapter.out.persistence.JacksonOutboxPayloadSerializer;
import io.ledgerstream.adapter.out.persistence.JdbcAccountRepository;
import io.ledgerstream.adapter.out.persistence.JdbcBookingRepository;
import io.ledgerstream.adapter.out.persistence.JdbcIdempotencyStore;
import io.ledgerstream.adapter.out.persistence.JdbcOutboxRepository;
import io.ledgerstream.adapter.out.persistence.SpringTransactionRunner;
import io.ledgerstream.application.port.in.RelayOutboxUseCase;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.application.port.out.BookingRepository;
import io.ledgerstream.application.port.out.IdempotencyStore;
import io.ledgerstream.application.port.out.OutboxEventPublisher;
import io.ledgerstream.application.port.out.OutboxPayloadSerializer;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.application.service.AccountService;
import io.ledgerstream.application.service.BookingService;
import io.ledgerstream.application.service.OutboxRelayService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;

@Configuration
@EnableConfigurationProperties({KafkaTopicProperties.class, OutboxRelayProperties.class})
@Import({
        AccountController.class,
        BookingController.class,
        ProblemDetailExceptionHandler.class
})
public class ApplicationConfiguration {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public TransactionRunner transactionRunner(final PlatformTransactionManager transactionManager) {
        return new SpringTransactionRunner(transactionManager);
    }

    @Bean
    public AccountRepository accountRepository(final JdbcClient jdbcClient) {
        return new JdbcAccountRepository(jdbcClient);
    }

    @Bean
    public BookingRepository bookingRepository(final JdbcClient jdbcClient) {
        return new JdbcBookingRepository(jdbcClient);
    }

    @Bean
    public IdempotencyStore idempotencyStore(final JdbcClient jdbcClient) {
        return new JdbcIdempotencyStore(jdbcClient);
    }

    @Bean
    public OutboxRepository outboxRepository(final JdbcClient jdbcClient) {
        return new JdbcOutboxRepository(jdbcClient);
    }

    @Bean
    public OutboxPayloadSerializer outboxPayloadSerializer(final ObjectMapper objectMapper) {
        return new JacksonOutboxPayloadSerializer(objectMapper);
    }

    @Bean
    public OutboxEventPublisher outboxEventPublisher(
            final KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate,
            final KafkaTopicProperties topicProperties,
            final ObjectMapper objectMapper,
            final OutboxRelayProperties relayProperties
    ) {
        return new KafkaOutboxEventPublisher(
                kafkaTemplate,
                topicProperties,
                objectMapper,
                relayProperties.publishTimeoutMs()
        );
    }

    @Bean
    public AccountService accountService(
            final AccountRepository accountRepository,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        return new AccountService(accountRepository, transactionRunner, clock);
    }

    @Bean
    public BookingService bookingService(
            final AccountRepository accountRepository,
            final BookingRepository bookingRepository,
            final IdempotencyStore idempotencyStore,
            final OutboxRepository outboxRepository,
            final OutboxPayloadSerializer outboxPayloadSerializer,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        return new BookingService(
                accountRepository,
                bookingRepository,
                idempotencyStore,
                outboxRepository,
                outboxPayloadSerializer,
                transactionRunner,
                clock
        );
    }

    @Bean
    public OutboxRelayService outboxRelayService(
            final OutboxRepository outboxRepository,
            final OutboxEventPublisher outboxEventPublisher,
            final TransactionRunner transactionRunner,
            final Clock clock,
            final OutboxRelayProperties relayProperties
    ) {
        return new OutboxRelayService(
                outboxRepository,
                outboxEventPublisher,
                transactionRunner,
                clock,
                relayProperties.batchSize()
        );
    }

    @Bean
    public Gauge outboxUnsentTotalGauge(
            final OutboxRepository outboxRepository,
            final MeterRegistry meterRegistry
    ) {
        return Gauge.builder("outbox_unsent_total", outboxRepository, OutboxRepository::countPending)
                .description("Number of PENDING outbox events waiting to be relayed to Kafka")
                .register(meterRegistry);
    }

    @Bean
    public NewTopic ledgerEventsTopic(final KafkaTopicProperties topicProperties) {
        return TopicBuilder.name(topicProperties.ledgerEvents())
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ledgerEventsDlqTopic(final KafkaTopicProperties topicProperties) {
        return TopicBuilder.name(topicProperties.ledgerEventsDlq())
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Configuration
    @EnableScheduling
    @ConditionalOnProperty(name = "ledger.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
    static class OutboxSchedulerConfiguration {

        @Bean
        public OutboxRelayScheduler outboxRelayScheduler(final RelayOutboxUseCase relayOutboxUseCase) {
            return new OutboxRelayScheduler(relayOutboxUseCase);
        }
    }
}
