# ledger-stream

Event-getriebenes Buchungssystem in **Java 21** und **Spring Boot 3.5.x**, das messbar zeigt, wie effektive **Exactly-once-Verarbeitung** zwischen PostgreSQL 16 und Apache Kafka (KRaft) über **Transactional Outbox**, **idempotente Consumer** und **klassifiziertes Fehlerhandling mit DLQ & Replay** hergestellt wird.

## Problemstellung

Der schwierige Teil event-getriebener Systeme ist nicht das Produzieren einer Kafka-Nachricht. Die eigentliche Konsistenzaufgabe liegt an der Systemgrenze zwischen relationaler Datenbank und Broker:

- **Absturz nach DB-Commit, vor Kafka-Publish** → Buchung existiert, Event fehlt (Datenverlust im Downstream).
- **Absturz nach Kafka-Publish, vor Bestätigung / Redelivery** → Event kommt mehrfach an (Doppelbuchung ohne Idempotenz).
- **Doppelter Client-Request bei Netzwerk-Timeout** → ohne expliziten `Idempotency-Key` entsteht eine doppelte Buchung.

`ledger-stream` löst diese Probleme explizit und ohne verdeckende Bibliotheken (kein Debezium, kein Spring Cloud Stream, kein Axon, kein Lombok, kein JPA/Hibernate).

## Architektur

Die vollständige Architektur inklusive Hexagonal-Schnitt und End-to-End-Sequenzdiagramm ist in [docs/architecture.md](docs/architecture.md) dokumentiert.

```mermaid
flowchart LR
    REST["adapter-in-rest"] --> APP["application (framework-frei)"]
    KCONS["adapter-in-messaging"] --> APP
    APP --> DOM["domain (framework-frei)"]
    PERS["adapter-out-persistence<br/>(JdbcClient + Flyway)"] --> APP
    KPROD["adapter-out-messaging<br/>(Outbox Relay + Avro)"] --> APP
```

Architekturentscheidungen werden als ADRs unter [`docs/adr/`](docs/adr/) gepflegt (u. a. [ADR-0001: Spring `JdbcClient` statt JPA/Hibernate](docs/adr/0001-jdbcclient-statt-jpa.md)).

## Idempotenz-Semantik (HTTP-API)

Jede schreibende Buchungsoperation (`POST /api/v1/accounts/{id}/bookings`, `POST /api/v1/accounts/{id}/bookings/{bookingId}/cancel`) erfordert den Header `Idempotency-Key` (1–128 Zeichen, eindeutig pro Konto über `UNIQUE (account_id, idempotency_key)`):

1. **Atomare Persistenz der Erfolgsantwort:** Statuscode und serialisierter JSON-Response-Body werden in derselben Datenbanktransaktion wie die Konto- und Buchungsänderung in `idempotency_keys` persistiert.
2. **Byte-gleiches Replay:** Ein wiederholter Request mit demselben `(account_id, idempotency_key)` führt keinen zweiten Buchungseffekt aus und liefert den persistierten HTTP-Status sowie den **byte-identischen** Response-Body zurück (ergänzt um den informativen Header `Idempotency-Replayed: true`).
3. **Nur `2xx` wird gecacht:** Ablehnungen (`400`, `404`, `422`) rollen die Transaktion zurück und werden **nicht** in `idempotency_keys` gespeichert (zusätzlich durch `CHECK (response_status BETWEEN 200 AND 299)` in PostgreSQL erzwungen). Ein korrigierter Retry mit demselben Schlüssel wird danach regulär verarbeitet.
4. **Gleichzeitige Requests mit identischem Schlüssel:** Treffen parallele Requests mit demselben `(account_id, idempotency_key)` ein, gewinnt genau eine Transaktion über den Unique-Index bzw. das optimistische Locking. Verlierer-Requests lesen die gespeicherte Antwort des Gewinners und liefern sie byte-gleich zurück — **kein `409 Conflict`**.
5. **Lock-Timeout-Schutz (`lock_timeout = 3s`):** Blockiert ein konkurrierender Request am Unique-Index oder Zeilen-Lock länger als `3s` (z. B. durch eine langsame Gewinner-Transaktion), bricht PostgreSQL mit `SQLSTATE 55P03` ab. Die API übersetzt dies in HTTP **`503 Service Unavailable`** mit Header **`Retry-After: 1`** (RFC 7807 `errorCode: LOCK_TIMEOUT`), sodass kein Request-Thread unbegrenzt hängt.

## Outbox-Betrieb, Metriken & PostgreSQL-Parameter

### Outbox-Alarmierung (Prometheus-Beispiel)

Ein reiner Zähler wie `outbox_unsent` reicht nicht aus, um ein stehendes Relay von normalem Last-Backlog zu unterscheiden. Entscheidend ist das Alter des ältesten `PENDING`-Events (`outbox_oldest_pending_age_seconds`):

```yaml
- alert: OutboxRelayStalled
  expr: outbox_oldest_pending_age_seconds > 300
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Outbox-Relay hängt oder ist ausgefallen"
    description: "Ältestes PENDING-Outbox-Event wartet seit > 300s (Wert: {{ $value }}s)."
```

Weitere Outbox-Metriken: `outbox_unsent` (Micrometer: `outbox_unsent_total`), `outbox_events_failed_total` (Events im Status `FAILED` nach `N=10` fehlgeschlagenen Versuchen mit exponentiellem Backoff `1s..60s` ±20 % Jitter).

### Verifizierte PostgreSQL-Parameter (Docker Compose)

Nach Start von `postgres` per `SHOW` bestätigt:

```text
shared_buffers       = 64MB
work_mem             = 4MB
maintenance_work_mem = 32MB
lock_timeout         = 3s
```

## Schnellstart

```bash
# 1. Lokale Infrastruktur starten (PostgreSQL 16, Kafka 3.9.1 KRaft, Schema Registry)
docker compose up -d --wait

# 2. Build, ArchUnit-Prüfung und Testcontainers-Integrationstests ausführen
./mvnw verify
```

> Hinweis: PostgreSQL in `docker-compose.yml` ist standardmäßig nur innerhalb des Compose-Netzwerks erreichbar (kein Host-Port-Mapping). Alle Integrationstests laufen vollständig isoliert über Testcontainers.

## Lastnachweis (Phase 10)

> Die folgenden Messwerte werden in Phase 10 reproduzierbar mit festem Seed erhoben.
>
> **Klare Trennung der Nachweise:**
> - **Retry-Leiter (5 Versuche, exponentieller Backoff + Jitter):** nachgewiesen über dedizierten Integrationstest (Phase 5), nicht über den Lasttest.
> - **DLQ-Anteil im Lasttest (1 %):** transportseitige Fault Injection (schema-valide Poison-Events mit unbekanntem Konto und `amount <= 0` direkt ins Topic durch `load-generator`), kein Test-Schalter im Produktivcode.

| Gesendet | Akzeptiert | Validierung abgelehnt | Duplikate abgewiesen | DLQ-Einträge | Dauer | Durchsatz | Seed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| _Phase 10_ | _Phase 10_ | _Phase 10_ | _Phase 10_ | _Phase 10_ | _Phase 10_ | _Phase 10_ | _Phase 10_ |
