# Architektur von `ledger-stream`

## Hexagonal Architecture (Ports & Adapters)

Die Schichttrennung wird durch ArchUnit ([`HexagonalArchitectureTest`](../app/src/test/java/io/ledgerstream/app/architecture/HexagonalArchitectureTest.java)) und das `maven-enforcer-plugin` im Build erzwungen:

- `domain` und `application` sind vollständig framework-frei (keine Spring-, Jakarta-, Kafka- oder Avro-Imports).
- `adapter-in-*` und `adapter-out-*` kommunizieren ausschließlich über Ports in `application` und kennen einander nicht.
- Transaktionsgrenzen werden explizit über den Port `TransactionRunner` gesteuert (`PROPAGATION_REQUIRED`).

```mermaid
flowchart LR
    subgraph Inbound["Inbound Adapters"]
        REST["adapter-in-rest<br/>REST API, RFC 7807, Correlation-ID"]
        KCONS["adapter-in-messaging<br/>Kafka Consumer, Retry/DLQ"]
    end

    subgraph Core["Framework-freier Kern"]
        APP["application<br/>Use Cases, Inbound/Outbound Ports"]
        DOM["domain<br/>Account, Booking, Money, Events, Regeln"]
    end

    subgraph Outbound["Outbound Adapters"]
        PERS["adapter-out-persistence<br/>PostgreSQL 16, JdbcClient, Flyway"]
        KPROD["adapter-out-messaging<br/>Outbox Relay (Batch 500), Kafka Producer, Avro"]
    end

    APP --> DOM
    REST --> APP
    KCONS --> APP
    PERS --> APP
    KPROD --> APP
```

## End-to-End-Konsistenzfluss (Outbox & Idempotenter Consumer)

```mermaid
sequenceDiagram
    participant C as Client
    participant API as REST Adapter
    participant UC as CreateBookingUseCase
    participant DB as PostgreSQL 16
    participant RELAY as Outbox Relay
    participant KAFKA as Kafka (KRaft) + SR
    participant CONS as Idempotent Consumer

    C->>API: POST /api/v1/accounts/{id}/bookings<br/>Idempotency-Key + X-Correlation-Id
    API->>UC: execute(CreateBookingCommand)
    UC->>DB: TX1 BEGIN
    UC->>DB: Check idempotency_keys (account_id, idempotency_key)
    UC->>DB: Update account (optimistic lock: version)
    UC->>DB: Insert booking + Insert idempotency_keys + Insert outbox_events
    UC->>DB: TX1 COMMIT
    API-->>C: 201 Created (Replay: byte-identische gespeicherte Antwort)

    loop @Scheduled Relay (Batch 500)
        RELAY->>DB: TX2: SELECT ... FOR UPDATE SKIP LOCKED LIMIT 500
        RELAY->>KAFKA: Publish Avro Event (acks=all, idempotence=true)
        RELAY->>DB: UPDATE outbox_events SET status = 'PUBLISHED', published_at = NOW()
        RELAY->>DB: TX2 COMMIT
    end

    KAFKA->>CONS: Poll Record (auto-commit=false)
    CONS->>DB: TX3: INSERT processed_events ON CONFLICT DO NOTHING
    alt Neues Event (rows = 1)
        CONS->>DB: Update account_summary Projection
        CONS->>DB: TX3 COMMIT
    else Duplikat (rows = 0)
        CONS->>CONS: Metric consumer_duplicates_rejected_total++
        CONS->>DB: TX3 COMMIT
    end
    CONS->>KAFKA: Manual Offset Ack (erst NACH DB-Commit)
```
