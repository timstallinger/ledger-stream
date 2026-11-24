package io.ledgerstream.loadgen;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

final class DeterministicWorkloadPlanTest {

    @Test
    void sameSeedProducesIdenticalAllocationsAndBookingSequence() {
        long seed = 20261124L;
        List<DeterministicWorkloadPlan.WorkerAllocation> a1 =
                DeterministicWorkloadPlan.allocateWorkers(seed, 100_000, 5_000, 1_000, 32);
        List<DeterministicWorkloadPlan.WorkerAllocation> a2 =
                DeterministicWorkloadPlan.allocateWorkers(seed, 100_000, 5_000, 1_000, 32);

        assertThat(a1).isEqualTo(a2);
        assertThat(a1.stream().mapToInt(DeterministicWorkloadPlan.WorkerAllocation::regularCount).sum()).isEqualTo(100_000);
        assertThat(a1.stream().mapToInt(DeterministicWorkloadPlan.WorkerAllocation::duplicateCount).sum()).isEqualTo(5_000);
        assertThat(a1.stream().mapToInt(DeterministicWorkloadPlan.WorkerAllocation::poisonCount).sum()).isEqualTo(1_000);

        SplittableRandom r1 = new SplittableRandom(a1.get(0).workerSeed());
        SplittableRandom r2 = new SplittableRandom(a2.get(0).workerSeed());
        BigDecimal bal1 = BigDecimal.ZERO;
        BigDecimal bal2 = BigDecimal.ZERO;
        for (int i = 0; i < 50; i++) {
            DeterministicWorkloadPlan.BookingSpec s1 =
                    DeterministicWorkloadPlan.nextRegularBooking(r1, seed, 0, i, bal1);
            DeterministicWorkloadPlan.BookingSpec s2 =
                    DeterministicWorkloadPlan.nextRegularBooking(r2, seed, 0, i, bal2);
            assertThat(s1).isEqualTo(s2);
            bal1 = "CREDIT".equals(s1.type()) ? bal1.add(s1.amount()) : bal1.subtract(s1.amount());
            bal2 = "CREDIT".equals(s2.type()) ? bal2.add(s2.amount()) : bal2.subtract(s2.amount());
            assertThat(bal1).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        }
    }
}
