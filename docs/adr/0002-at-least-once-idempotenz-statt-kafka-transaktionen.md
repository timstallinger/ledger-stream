# ADR-0002: At-least-once + Idempotenz statt Kafka-Transaktionen über Systemgrenzen

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

Kafka bietet mit `enable.idempotence=true`, `transactional.id` und `isolation.level=read_committed` transaktionale Semantik (EOS) **innerhalb** von Kafka (Consume-Transform-Produce zwischen Topics). In `ledger-stream` endet der Verarbeitungsfluss jedoch nicht in einem weiteren Kafka-Topic, sondern in **PostgreSQL** (`account_summary` Read-Projektion). Eine Kafka-Transaktion kann niemals atomar mit einer PostgreSQL-Transaktion committen.

## Entscheidung

Wir trennen Zustellung, Verarbeitung und Wirkung explizit:
1. **Producer:** `acks=all` + `enable.idempotence=true` (verhindert Broker-seitige Producer-Duplikate bei Netzwerk-Retries), kombiniert mit Transactional Outbox (at-least-once Delivery).
2. **Consumer:** `enable-auto-commit=false`, manuelles Offset-Commit (`AckMode.MANUAL_IMMEDIATE`) **erst nach** PostgreSQL-Commit (at-least-once Processing).
3. **Wirkung:** Atomare Deduplizierung über `INSERT INTO processed_events (event_id, ...) ON CONFLICT (event_id) DO NOTHING` in **derselben** PostgreSQL-Transaktion wie das `account_summary`-Update (effektive Exactly-once-Wirkung).

## Alternativen

### 1. Kafka Transactions (`KafkaTransactionManager` / `read_committed`) über DB-Grenzen
- **Vorteil:** Vertraute Spring-Kafka-Abstraktion für Kafka-zu-Kafka-Streams.
- **Nachteil:** Löst das DB-Commit-Fenster nicht — ein Absturz zwischen `db.commit()` und `kafkaTx.commit()` redelivered den Record trotzdem; erhöht Broker-Overhead (`__transaction_state`) ohne die nötige DB-Deduplizierung einzusparen.

### 2. At-most-once (`enable-auto-commit=true` vor DB-Verarbeitung)
- **Vorteil:** Keine `processed_events`-Tabelle nötig.
- **Nachteil:** Absturz nach Offset-Commit und vor DB-Commit führt zu unwiederbringlichem Datenverlust im Saldo-Read-Model.

### 3. Kafka-Offsets in PostgreSQL speichern statt in `__consumer_offsets`
- **Vorteil:** Offset und Projektion in derselben DB-Transaktion, keine separate `processed_events`-Tabelle für Normalbetrieb.
- **Nachteil:** Standard-Kafka-Monitoring (`consumer lag` via `__consumer_offsets`), Rebalance-Protokoll und Spring-Kafka `@RetryableTopic` funktionieren nicht mehr out-of-the-box; schützt zudem nicht gegen Outbox-Redelivery desselben `event_id` auf verschiedenen Offsets.

## Konsequenzen

### Positiv
- Nachweisbar keine Doppelwirkung bei 3-fach-Zustellung, Consumer-Crash nach DB-Commit vor Offset-Commit und DLQ-Replay (`IdempotentConsumerIT`, `DlqAndRetryIT`).
- Standard-Kafka-Consumer-Groups, Lag-Metriken und `@RetryableTopic` bleiben voll nutzbar.

### Negativ
- `processed_events` wächst linear mit der Event-Anzahl (benötigt im Langzeitbetrieb Partitionierung/Retention-Strategie).
- Jedes Event muss eine global eindeutige, deterministische `event_id` tragen.

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Nur Kafka-Transaktionen ohne `processed_events`:** Im C1-Crash-Test (`IdempotentConsumerIT`: Absturz nach DB-Commit, vor Offset-Commit) sowie bei doppeltem Outbox-Publish wäre das Saldo-Update zweimal angewendet worden — doppelter Kontostand in `account_summary`.
- **Auto-Commit (`at-most-once`):** Ein Pod-Kill während einer DB-Transaktion hätte Buchungen dauerhaft aus `account_summary` verloren.
