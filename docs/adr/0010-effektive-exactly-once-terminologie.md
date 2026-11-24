# ADR-0010: „Effektive Exactly-once-Verarbeitung“ als verbindliche Terminologie

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

Im Umfeld von Apache Kafka wird häufig pauschal „Exactly-once Delivery“ versprochen. In einem verteilten System über eine Netzwerkgrenze zwischen zwei unabhängigen Datenspeichern (PostgreSQL 16 und Apache Kafka) ist **Exactly-once Delivery über das Netzwerk mathematisch unmöglich** (Zwei-Generäle-Problem / Absturz zwischen Commit und Ack). Wer trotzdem „Exactly-once Delivery“ behauptet, weckt falsche Erwartungen und verdeckt die tatsächlichen Fehlerfälle im Betrieb.

## Entscheidung

`ledger-stream` verwendet verbindlich den Begriff **„effektive Exactly-once-Verarbeitung“** und trennt in Architektur, Dokumentation, Metriken und Tests konsequent drei Schichten:

1. **Zustellung (Delivery): At-least-once** — Transactional Outbox (`FOR UPDATE SKIP LOCKED`) publiziert nach Commit; ein Absturz zwischen Broker-Ack und `markSent` führt zu erneutem Publish.
2. **Verarbeitung (Processing): At-least-once** — Manuelles Offset-Commit erfolgt erst **nach** PostgreSQL-Commit; Absturz dazwischen oder Retry-Topics liefern Records erneut aus.
3. **Wirkung (Effect): Genau einmal (Exactly-once)** — Schreibseite: `UNIQUE (account_id, idempotency_key)` + `request_fingerprint` (`SHA-256`) + byte-gleiches Replay gecachter `2xx`-Antworten (`422` bei abweichendem Payload). Leseseite: `INSERT INTO processed_events ... ON CONFLICT (event_id) DO NOTHING` in derselben Transaktion wie das `account_summary`-Update.

Verbindliche Kurzformulierung:
> „Kafka liefert at-least-once, der Consumer dedupliziert über `event_id` in derselben Transaktion wie das Saldo-Update. Die Wirkung tritt genau einmal ein — nachweisbar im Lasttest: Summe der Buchungen = akzeptierte Anfragen, bei 5 % Duplikaten.“

## Alternativen

### 1. Pauschaler Begriff „Exactly-once Delivery“
- **Vorteil:** Marketing-taugliches Schlagwort.
- **Nachteil:** Technisch falsch über die Grenze PostgreSQL ↔ Kafka; verleitet Entwickler dazu, auf `processed_events` oder `Idempotency-Key` zu verzichten.

### 2. Nur „At-least-once“ ohne Hervorhebung der Wirkungsgarantie
- **Vorteil:** Technisch korrekt auf Transportebene.
- **Nachteil:** Beschreibt nicht die zentrale fachliche Eigenschaft des Ledgers (keine doppelte Buchung, exakter Saldo trotz Redelivery).

## Konsequenzen

### Positiv
- Präzise, angreifbare und in Tests (`TransactionalOutboxIT`, `IdempotentConsumerIT`, `AccountBookingApiIT`, Phase-10-Lasttest) maschinell nachprüfbare Garantien.
- Klare Trennung zwischen HTTP-Idempotenz (im 100k-Lasttest bewiesen: `5.000` L2-Replays → `0` zweite Buchungen, `consumer_duplicates_rejected_total{reason="duplicate"} == 0`) und Consumer-Idempotenz (in `IdempotentConsumerIT` bewiesen: 3-fach-Zustellung & C1-Crash → `1` Wirkung, `2` bzw. `1` abgewiesene Duplikate).

### Negativ
- Erfordert präzise Formulierungen in README, ADRs und Betriebsrunbook statt vereinfachender Schlagworte.

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit unpräzisem „Exactly-once Delivery“:** Ein kritisches Architektur-Review hätte die Aussage sofort widerlegt, sobald der Crash-Fall nach Broker-Ack / vor `markSent` oder nach DB-Commit / vor Offset-Commit hinterfragt worden wäre. Zudem wäre im Phase-10-Lastlauf unklar geblieben, warum `consumer_duplicates_rejected_total{reason="duplicate"}` trotz 5 % Duplikaten `0` sein muss.
