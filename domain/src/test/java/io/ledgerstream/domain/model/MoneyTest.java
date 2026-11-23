package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.CurrencyMismatchException;
import io.ledgerstream.domain.exception.InvalidDomainValueException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class MoneyTest {

    private static final CurrencyCode EUR = CurrencyCode.of("EUR");
    private static final CurrencyCode USD = CurrencyCode.of("USD");

    @Test
    void normalizesValidBigDecimalToScaleTwo() {
        Money money = Money.of(new BigDecimal("42.5"), EUR);

        assertThat(money.amount()).isEqualByComparingTo("42.50");
        assertThat(money.amount().scale()).isEqualTo(2);
        assertThat(money.currency()).isEqualTo(EUR);
    }

    @Test
    void rejectsAmountWithMoreThanTwoDecimalPlaces() {
        assertThatThrownBy(() -> Money.of(new BigDecimal("10.125"), EUR))
                .isInstanceOf(InvalidDomainValueException.class)
                .hasMessageContaining("decimal");
    }

    @Test
    void addsAndSubtractsMatchingCurrencies() {
        Money hundred = Money.of(new BigDecimal("100.00"), EUR);
        Money thirtyFive = Money.of(new BigDecimal("35.25"), EUR);

        assertThat(hundred.add(thirtyFive)).isEqualTo(Money.of(new BigDecimal("135.25"), EUR));
        assertThat(hundred.subtract(thirtyFive)).isEqualTo(Money.of(new BigDecimal("64.75"), EUR));
    }

    @Test
    void rejectsArithmeticAcrossDifferentCurrencies() {
        Money euro = Money.of(new BigDecimal("10.00"), EUR);
        Money dollar = Money.of(new BigDecimal("10.00"), USD);

        assertThatThrownBy(() -> euro.add(dollar))
                .isInstanceOf(CurrencyMismatchException.class);
        assertThatThrownBy(() -> euro.subtract(dollar))
                .isInstanceOf(CurrencyMismatchException.class);
    }
}
