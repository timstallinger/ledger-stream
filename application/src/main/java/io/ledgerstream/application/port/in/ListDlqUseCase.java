package io.ledgerstream.application.port.in;

import io.ledgerstream.application.port.out.DlqEntry;

import java.util.List;

public interface ListDlqUseCase {

    List<DlqEntry> listUnhandled();
}
