package io.ledgerstream.app.config;

import io.ledgerstream.adapter.in.rest.ProblemDetailExceptionHandler;
import io.ledgerstream.adapter.out.messaging.KafkaTopicProperties;
import io.ledgerstream.adapter.out.persistence.SpringTransactionRunner;
import io.ledgerstream.application.port.out.TransactionRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableConfigurationProperties(KafkaTopicProperties.class)
@Import(ProblemDetailExceptionHandler.class)
public class ApplicationConfiguration {

    @Bean
    public TransactionRunner transactionRunner(final PlatformTransactionManager transactionManager) {
        return new SpringTransactionRunner(transactionManager);
    }
}
