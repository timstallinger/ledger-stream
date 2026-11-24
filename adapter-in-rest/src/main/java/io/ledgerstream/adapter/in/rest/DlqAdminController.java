package io.ledgerstream.adapter.in.rest;

import io.ledgerstream.application.port.in.ListDlqUseCase;
import io.ledgerstream.application.port.in.ReplayDlqCommand;
import io.ledgerstream.application.port.in.ReplayDlqUseCase;
import io.ledgerstream.application.port.out.DlqEntry;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@RestController
@RequestMapping("/admin/dlq")
public final class DlqAdminController {

    private final ListDlqUseCase listDlqUseCase;
    private final ReplayDlqUseCase replayDlqUseCase;

    public DlqAdminController(
            final ListDlqUseCase listDlqUseCase,
            final ReplayDlqUseCase replayDlqUseCase
    ) {
        this.listDlqUseCase = Objects.requireNonNull(listDlqUseCase, "listDlqUseCase must not be null");
        this.replayDlqUseCase = Objects.requireNonNull(replayDlqUseCase, "replayDlqUseCase must not be null");
    }

    @GetMapping
    public ResponseEntity<List<DlqEntryResponse>> listUnhandled() {
        List<DlqEntryResponse> entries = listDlqUseCase.listUnhandled().stream()
                .map(DlqEntryResponse::from)
                .toList();
        return ResponseEntity.ok(entries);
    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<DlqEntryResponse> replay(
            @PathVariable final UUID id,
            @Valid @RequestBody(required = false) final ReplayDlqRequest request,
            final Principal principal
    ) {
        String requestedBy = request != null ? request.replayedBy() : null;
        String replayedBy = (requestedBy != null && !requestedBy.isBlank())
                ? requestedBy
                : (principal != null ? principal.getName() : null);
        String note = request != null ? request.note() : null;
        DlqEntry replayed = replayDlqUseCase.replay(new ReplayDlqCommand(id, replayedBy, note));
        return ResponseEntity.ok(DlqEntryResponse.from(replayed));
    }
}
