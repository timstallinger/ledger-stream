package io.ledgerstream.application.port.out;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DlqRepository {

    void save(DlqEntry entry);

    Optional<DlqEntry> findById(UUID id);

    List<DlqEntry> findUnhandled();

    boolean markReplayed(UUID id, Instant replayedAt, String replayedBy, String note);

    long countUnhandled();
}
