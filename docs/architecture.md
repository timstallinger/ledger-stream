# Architektur von `ledger-stream`

Dieses Dokument visualisiert Kontext, Container-Topologie, Hexagonal-Architektur und den End-to-End-Konsistenzfluss.

## 1. Systemkontext (C4 Level 1)

```mermaid
flowchart LR
    USER["Benutzer (Browser)<br/>viewer / writer / operator"] -->|HTTPS / PKCE S256| SPA["ledger-stream Web UI<br/>(Vue 3 + TS + Nginx)"]
    LOAD["load-generator CLI<br/>(Java 21, 3 Kohorten)"] -->|HTTP/1.1 Keep-Alive + JWT| API["ledger-stream Backend<br/>(Java 21 / Spring Boot 3.5)"]
    LOAD -.->|Transport Fault Injection<br/>1% Avro Poison| KAFKA["Apache Kafka 3.9.1<br/>(KRaft, 6 Partitionen)"]
    SPA -->|OIDC Login / JWKS| KC["Keycloak 26<br/>(OIDC IdP)"]
    SPA -->|REST API + Bearer JWT| API
    OPS["Betrieb / Monitoring"] -->|Dashboards| GRAF["Grafana 11"]
```

## 2. Container-Architektur (C4 Level 2 — k3s Namespace `ledger-stream`)

```mermaid
flowchart TB
    subgraph NS["k3s Namespace: ledger-stream (ResourceQuota + default-deny NetworkPolicies)"]
        FE["frontend<br/>Nginx unprivileged :8080"]
        APP["app (Spring Boot 3.5)<br/>REST API + Outbox Relay + Consumer :8080"]
        PG[("postgres<br/>PostgreSQL 16 :5432<br/>DBs: ledger + keycloak")]
        KF["kafka<br/>Kafka 3.9.1 KRaft :29092<br/>6 Partitionen"]
        SR["schema-registry<br/>Confluent 7.9.9 :8081<br/>FULL Compatibility"]
        KC["keycloak<br/>Keycloak 26.2.5 :8080 / :9000"]
        PROM["prometheus<br/>Prometheus v3.2.1 :9090"]
        GRAF["grafana<br/>Grafana 11.5.2 :3000"]

        FE -->|/api/v1/**| APP
        FE -->|OIDC PKCE| KC
        APP -->|JdbcClient + Flyway| PG
        APP -->|Avro Produce & Consume| KF
        APP -->|Avro Schema| SR
        APP -->|JWKS / OIDC| KC
        SR -->|_schemas| KF
        KC -->|KC_DB=postgres| PG
        PROM -->|/actuator/prometheus| APP
        PROM -->|/metrics| KC
        GRAF -->|PromQL| PROM
    end
```

## 3. Hexagonal Architecture (Ports & Adapters)

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

## 4. End-to-End-Konsistenzfluss (Outbox & Idempotenter Consumer)

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
    UC->>DB: Check idempotency_keys (account_id, idempotency_key, request_fingerprint)
    UC->>DB: Update account (optimistic lock: version)
    UC->>DB: Insert booking + Insert idempotency_keys + Insert outbox_events
    UC->>DB: TX1 COMMIT
    API-->>C: 201 Created (Replay: 201 + Idempotency-Replayed: true / Key-Reuse: 422)

    loop @Scheduled Relay (Batch 500, Poll 200ms, Wave-Pipelining)
        RELAY->>DB: TX2: SELECT ... FOR UPDATE SKIP LOCKED LIMIT 500
        RELAY->>KAFKA: Publish Avro v2 Event (acks=all, idempotence=true)
        RELAY->>DB: UPDATE outbox_events SET status = 'SENT', sent_at = NOW()
        RELAY->>DB: TX2 COMMIT
    end

    KAFKA->>CONS: Poll Record (auto-commit=false, Concurrency 6)
    CONS->>DB: TX3: INSERT processed_events ON CONFLICT DO NOTHING
    alt Neues Event (rows = 1)
        CONS->>DB: Update account_summary Projection
        CONS->>DB: TX3 COMMIT
    else Duplikat (rows = 0)
        CONS->>CONS: Metric consumer_duplicates_rejected_total{reason="duplicate"}++
        CONS->>DB: TX3 COMMIT
    end
    CONS->>KAFKA: Manual Offset Ack (erst NACH DB-Commit)
```
