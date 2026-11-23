package io.ledgerstream.application.port.in;

import io.ledgerstream.application.port.out.DlqEntry;

public interface ReplayDlqUseCase {

    DlqEntry replay(ReplayDlqCommand command);
}
