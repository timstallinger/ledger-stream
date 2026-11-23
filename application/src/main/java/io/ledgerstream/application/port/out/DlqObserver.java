package io.ledgerstream.application.port.out;

import java.util.UUID;

public interface DlqObserver {

    void onDlqReplayed(UUID dlqEntryId, String topic, String key);
}
