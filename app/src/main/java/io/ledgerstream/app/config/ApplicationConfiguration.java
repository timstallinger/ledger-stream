package io.ledgerstream.app.config;

import io.ledgerstream.adapter.in.rest.AccountController;
import io.ledgerstream.adapter.in.rest.BookingController;
import io.ledgerstream.adapter.in.rest.ProblemDetailExceptionHandler;
import io.ledgerstream.adapter.out.messaging.KafkaTopicProperties;
import io.ledgerstream.adapter.out.persistence.JdbcAccountRepository;
import io.ledgerstream.adapter.out.persistence.JdbcBookingRepository;
import io.ledgerstream.adapter.out.persistence.JdbcIdempotencyStore;
import io.ledgerstream.adapter.out.persistence.SpringTransactionRunner;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.application.port.out.BookingRepository;
import io.ledgerstream.application.port.out.IdempotencyStore;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.application.service.AccountService;
import io.ledgerstream.application.service.BookingService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;

@Configuration
@EnableConfigurationProperties(KafkaTopicProperties.class)
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
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        return new BookingService(
                accountRepository,
                bookingRepository,
                idempotencyStore,
                transactionRunner,
                clock
        );
    }
}
