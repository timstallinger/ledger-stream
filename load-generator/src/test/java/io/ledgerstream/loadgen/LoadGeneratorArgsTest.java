package io.ledgerstream.loadgen;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class LoadGeneratorArgsTest {

    @Test
    void parsesStandardCliArguments() {
        LoadGeneratorArgs args = LoadGeneratorArgs.parse(new String[]{
                "--bookings=100000",
                "--duplicates=0.05",
                "--poison=0.01",
                "--seed=424242",
                "--threads=32",
                "--base-url=http://127.0.0.1:8085/",
                "--rate=1500"
        });

        assertThat(args.bookings()).isEqualTo(100_000);
        assertThat(args.duplicateCount()).isEqualTo(5_000);
        assertThat(args.poisonCount()).isEqualTo(1_000);
        assertThat(args.totalPlannedRequests()).isEqualTo(106_000);
        assertThat(args.seed()).isEqualTo(424242L);
        assertThat(args.threads()).isEqualTo(32);
        assertThat(args.baseUrl()).isEqualTo("http://127.0.0.1:8085");
        assertThat(args.ratePerSecond()).isEqualTo(1500.0);
        assertThat(args.warmupSeconds()).isEqualTo(5);
    }

    @Test
    void rejectsInvalidBounds() {
        assertThatThrownBy(() -> LoadGeneratorArgs.parse(new String[]{"--bookings=0"}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LoadGeneratorArgs.parse(new String[]{"--duplicates=1.5"}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LoadGeneratorArgs.parse(new String[]{"--threads=32", "--accounts=8"}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
