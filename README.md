# ledger-stream

Event-getriebenes Buchungssystem in **Java 21** und **Spring Boot 3.5.x**, das messbar zeigt, wie effektive **Exactly-once-Verarbeitung** zwischen PostgreSQL 16 und Apache Kafka (KRaft) über **Transactional Outbox**, **idempotente Consumer** und **klassifiziertes Fehlerhandling mit DLQ & Replay** hergestellt wird.

## Problemstellung

Der schwierige Teil event-getriebener Systeme ist nicht das Produzieren einer Kafka-Nachricht. Die eigentliche Konsistenzaufgabe liegt an der Systemgrenze zwischen relationaler Datenbank und Broker:

- **Absturz nach DB-Commit, vor Kafka-Publish** → Buchung existiert, Event fehlt (Datenverlust im Downstream).
- **Absturz nach Kafka-Publish, vor Bestätigung / Redelivery** → Event kommt mehrfach an (Doppelbuchung ohne Idempotenz).
- **Doppelter Client-Request bei Netzwerk-Timeout** → ohne expliziten `Idempotency-Key` entsteht eine doppelte Buchung.

`ledger-stream` löst diese Probleme explizit und ohne verdeckende Bibliotheken (kein Debezium, kein Spring Cloud Stream, kein Axon, kein Lombok, kein JPA/Hibernate).

## Effektive Exactly-once-Semantik

In einem verteilten System aus PostgreSQL und Kafka existiert kein magisches „Exactly-once-Delivery“ über die Netzwerkgrenze. `ledger-stream` trennt deshalb bewusst drei Schichten:

| Schicht | Garantie | Mechanismus in `ledger-stream` |
|---|---|---|
| **Zustellung (Delivery)** | **At-least-once** | Transactional Outbox (`FOR UPDATE SKIP LOCKED`, Batch 500) publiziert nach Commit; Absturz zwischen Kafka-Ack und `markSent` führt zu erneutem Publish. |
| **Verarbeitung (Processing)** | **At-least-once** | Manuelles Offset-Commit erst nach DB-Commit; Absturz zwischen DB-Commit und Offset-Commit sowie Non-Blocking Retry Topics liefern Records erneut aus. |
| **Wirkung (Effect)** | **Genau einmal (Exactly-once)** | Schreibseite: `UNIQUE (account_id, idempotency_key)` + byte-gleiches Replay gecachter `2xx`-Antworten. Leseseite: `INSERT INTO processed_events ... ON CONFLICT DO NOTHING` in derselben Transaktion wie das `account_summary`-Update. |

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

Architekturentscheidungen werden als ADRs unter [`docs/adr/`](docs/adr/) gepflegt:
- [ADR-0001: Spring `JdbcClient` statt JPA/Hibernate](docs/adr/0001-jdbcclient-statt-jpa.md)
- [ADR-0002: Avro-Kompatibilität `FULL` statt `BACKWARD`](docs/adr/0002-avro-full-compatibility-statt-backward.md)
- Bewusste Entwurfsgrenzen (u. a. Per-Account-Reihenfolge bei Non-Blocking Retry-Topics): [docs/design-decisions.md](docs/design-decisions.md)

## Idempotenz-Semantik (HTTP-API)

Jede schreibende Buchungsoperation (`POST /api/v1/accounts/{id}/bookings`, `POST /api/v1/accounts/{id}/bookings/{bookingId}/cancel`) erfordert den Header `Idempotency-Key` (1–128 Zeichen, eindeutig pro Konto über `UNIQUE (account_id, idempotency_key)`):

1. **Atomare Persistenz & Payload-Fingerprint:** Statuscode, serialisierter JSON-Response-Body und `request_fingerprint` (`CHAR(64)`, SHA-256 über die kanonischen Geschäftsfelder `accountId`, `type`, normalisierter `amount`, `currency`, `reference` — **nicht** über den rohen JSON-Body) werden in derselben Datenbanktransaktion wie die Konto- und Buchungsänderung in `idempotency_keys` persistiert.
2. **Byte-gleiches Replay (gleicher Fingerprint):** Ein wiederholter Request mit demselben `(account_id, idempotency_key)` **und** identischem `request_fingerprint` führt keinen zweiten Buchungseffekt aus und liefert den persistierten HTTP-Status sowie den **byte-identischen** Response-Body zurück (`Idempotency-Replayed: true`).
3. **Key-Reuse mit abweichendem Inhalt (`422`):** Wird derselbe `(account_id, idempotency_key)` mit **anderem** Inhalt (abweichender `request_fingerprint`, z. B. anderer Betrag) wiederverwendet, lehnt die API den Request mit HTTP **`422 Unprocessable Entity`** (RFC 7807 `type: urn:ledger-stream:error:idempotency-key-reuse`, `errorCode: IDEMPOTENCY_KEY_REUSE`) ab — keine zweite Buchung, Saldo unverändert.
4. **Nur `2xx` wird gecacht:** Ablehnungen (`400`, `404`, `422`) rollen die Transaktion zurück und werden **nicht** in `idempotency_keys` gespeichert (`CHECK (response_status BETWEEN 200 AND 299)`).
5. **Abgrenzung `409` / `503` vs. `422`:**
   - **Paralleler gleicher Request (gleicher Key & gleicher Inhalt):** Verlierer liest die Antwort des Gewinners und liefert sie byte-gleich zurück. Bei konkurrierendem Konflikt ohne abgelegte Idempotenz-Antwort (Optimistic Locking) entsteht **`409 Conflict`**; überschreitet das Warten auf den Zeilen-/Index-Lock `lock_timeout = 3s` (`SQLSTATE 55P03`), liefert die API **`503 Service Unavailable`** mit `Retry-After: 1` (`LOCK_TIMEOUT`).
   - **Gleicher Key mit anderem Inhalt:** deterministisch **`422 Unprocessable Entity`** (`urn:ledger-stream:error:idempotency-key-reuse`).

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

## Fehlerbehandlung, Non-Blocking Retry & DLQ

### Warum Retry-Topics (`@RetryableTopic`) statt `DefaultErrorHandler`-Backoff oder Pause/Resume?

- Ein blockierender `DefaultErrorHandler` mit `ExponentialBackOff` schläft direkt im Consumer-Thread und **blockiert alle nachfolgenden Events derselben Partition** — zudem droht bei längeren Backoffs ein Überschreiten von `max.poll.interval.ms` (`300000 ms` = 5 min) und damit ungewollte Consumer-Group-Rebalances.
- Ein manuelles `pause()`/`resume()` auf Partitionsebene stoppt ebenfalls die gesamte Partition (Head-of-Line-Blocking) und verkompliziert Offset-Management sowie Rebalance-Handling erheblich.
- **Retry-Topics (`ledger.events-retry-0..3` → `ledger.events.dlq`)** verschieben fehlerhafte technische Nachrichten sofort asynchron aus dem Haupt-Topic und committen dessen Offset, sodass nachfolgende Nachrichten derselben Partition verzögerungsfrei weiterlaufen und `max.poll.interval.ms` nie gefährdet wird.

### Fehlerklassifikation & Metriken

- **Deserialisierungsfehler & fachliche Fehler** (`unknown_account`, `amount <= 0`, ungültiger Typ): **0 Retries**, direkt in `ledger.events.dlq` und `dlq_entries` unter Beibehaltung der unveränderten Rohbytes (`RawPayloadPreservingAvroDeserializer`, Base64 in Header `x-original-payload`).
- **Technische Fehler** (DB-Ausfall, Timeout, Schreibfehler): nicht-blockierend bis zu **5 Versuche** mit exponentiellem Backoff und Jitter (`random = true`), danach `ledger.events.dlq` + `dlq_entries`.
- **Metriken (`/actuator/prometheus`):**
  - `dlq_entries_unhandled` (Gauge): offene DLQ-Einträge (`replayed_at IS NULL`) — Zielwert `0`, maßgeblich für Alarmierung.
  - `dlq_replayed_total` (Counter): über `POST /admin/dlq/{id}/replay` erneut eingestellte Einträge.
  - `consumer_retries_total` (Counter): ausgelöste nicht-blockierende Retry-Versuche (Frühindikator vor DLQ-Wachstum).
- **Admin-Endpunkte abgesichert (Phase 7):** `GET /admin/dlq` und `POST /admin/dlq/{id}/replay` erfordern `ledger.admin.enabled=true` **und** ein gültiges OIDC-Token mit Rolle `operator` (`ROLE_operator`). Standardmäßig (`ledger.admin.enabled=false`, auch im k3s-Overlay [deploy/kustomize/overlays/prod/kustomization.yaml](deploy/kustomize/overlays/prod/kustomization.yaml) explizit gepinnt) liefern sie `404` vor `401`.
- **Zwei unabhängige Replay-Garantien & unveränderliche Replay-Historie:** Ein erneuter Replay eines bereits wieder eingespielten DLQ-Eintrags wird an der Admin-API über den `replayed_at`-Guard mit HTTP **`409 Conflict`** abgewiesen; selbst bei einem manuellen Re-Publish am Admin-Endpunkt vorbei verhindert `UNIQUE (event_id)` auf `processed_events` eine doppelte Buchungswirkung. Jeder Replay-Versuch wird unveränderlich in `dlq_replay_attempts` protokolliert; scheitert ein Replay erneut oder trifft derselbe unlesbare Deserialisierungs-Poison mehrfach ein, dedupliziert der Partial-Unique-Index `uq_dlq_entries_unhandled_dedup_key` auf `COALESCE(event_id::text, payload_sha256) WHERE replayed_at IS NULL` in eine einzige offene Zeile und erhöht `attempt_count`.
- Vollständiges Runbook (Analyse, Replay-Kriterien, Begründung Outbox `10` vs. Consumer `5` Retries): [docs/operations.md](docs/operations.md).

## Security (Keycloak 26 + OIDC Resource Server)

### Rollenmodell (Least Privilege)

| Rolle | Authority | Erlaubte Endpunkte | Bewusst verweigert (`403`) |
|---|---|---|---|
| `viewer` | `ROLE_viewer` | `GET /api/v1/accounts/**` | `POST /api/v1/accounts/**`, `/admin/dlq/**` |
| `writer` | `ROLE_writer` | `GET` & `POST /api/v1/accounts/**` (Kontoanlage, Buchung, Storno) | `/admin/dlq/**` |
| `operator` | `ROLE_operator` | `GET` & `POST /api/v1/accounts/**` sowie `/admin/dlq/**` (bei `ledger.admin.enabled=true`) | — |

**Entscheidung:** Drei Rollen statt zwei — Buchungs-Clients (`writer`) können geschäftliche Buchungen ausführen, dürfen aber niemals DLQ-Payloads inspizieren oder Replays auslösen (`Separation of Duties`).

### Durchsetzung & Härtung

- **404-vor-401-Invariante:** Ist `ledger.admin.enabled=false` (Default), schneidet eine `@Order(1)`-`SecurityFilterChain` **jede** Route unter `/admin/**` noch vor der OIDC-Prüfung ab und liefert RFC-7807 **`404 Not Found`** (unauthentifiziert, als `viewer` und als `operator`) — selbst dann, wenn unter `/admin/**` ein Controller ohne `@ConditionalOnProperty` registriert wäre (abgesichert über `AdminPathInvariantDisabledIT`/`AdminPathInvariantEnabledIT` mit Testprofil-Route `/admin/test-only`).
- **Explizite Token-Prüfung & Allowlist-Rollen-Mapper:** `JwtAuthenticationConverter` mappt ausschließlich die Allowlist `{viewer, writer, operator}` aus `realm_access.roles` auf `ROLE_*` und verwirft technische Keycloak-Rollen (`default-roles-*`, `offline_access`, `uma_authorization`). Der `NimbusJwtDecoder` prüft Signatur über JWKS (kein `alg=none`, kein fremder Key), `exp`/`nbf`, `iss` sowie explizit `aud = ledger-stream-api`. `401` und `403` liefern strukturierte RFC-7807-Antworten (`application/problem+json`).
- **CORS:** Explizite Origin-Allowlist (`ledger.security.cors-allowed-origins`, kein `*` bei `allowCredentials=true`), `Allow-Headers` inkl. `Authorization` und `Idempotency-Key`, `Access-Control-Expose-Headers` inkl. `Idempotency-Replayed`, `X-Correlation-Id` und `Retry-After` (verifiziert in `SecurityNegativeJwtIT`), Preflight für `POST` verifiziert.
- **Keine Secrets im Repository & Testinfrastruktur-Markierung:** [keycloak/realm-export.json](keycloak/realm-export.json) enthält den Public-PKCE-Client `ledger-stream-spa` (`S256`, ohne Secret) sowie den vertraulichen Client `ledger-stream-cli` mit Platzhalter `${LEDGER_CLI_CLIENT_SECRET}` (Injektion per Env / Kubernetes `secretKeyRef` in [deploy/kustomize/base/keycloak-deployment.yaml](deploy/kustomize/base/keycloak-deployment.yaml)). `ledger-stream-cli` mit `directAccessGrantsEnabled=true` dient ausschließlich als Test- und CI-Infrastruktur; produktive Benutzer-Logins erfolgen ausschließlich über `ledger-stream-spa` (Authorization Code + PKCE `S256`).

## Frontend (Vue 3 + TypeScript + Vite + `oidc-client-ts`)

- **Idempotency-Key-Lebenszyklus (`frontend/src/composables/useBookingForm.ts`):** Der `Idempotency-Key` entsteht per `crypto.randomUUID()` beim **Öffnen** des Buchungsformulars, ist im Formular sichtbar und kopierbar und wird **niemals** im Submit-Handler erzeugt. Erst nach bestätigter `2xx`-Antwort wird ein neuer Key gezogen; „Erneut senden (gleicher Key)“ sowie parallele Doppelklicks behalten exakt denselben Key.
- **Kein deaktivierter Submit-Button:** Während laufender Requests zeigt der Submit-Button einen Spinner (`aria-busy="true"`) statt `disabled`, ergänzt um **„Absichtlich doppelt senden (2× parallel)“** mit Live-Doppelklick-Protokoll (2 Requests, 1 Buchung, byte-gleicher Response-Body).
- **Geld als String (`frontend/src/utils/money.ts`):** `BigDecimal`-Werte werden vom Backend als JSON-Strings (`@JsonFormat(shape = STRING)`) geliefert und direkt per `Intl.NumberFormat('de-DE', { style: 'currency', currency })` formatiert — ohne `number`-Parsing und ohne Arithmetik.
- **Token-Lifecycle & Sichtbarkeit:** `automaticSilentRenew`, explizite Re-Login-Ansicht bei abgelaufenem Token (kein leerer Bildschirm), Logout über `end_session_endpoint`, fünf sichtbare Fehlerzustände (`401`, `403`, `422` RFC-7807 Feldfehler, `409`/Idempotenz-Replay, Netzwerk mit Retry) sowie `document.visibilityState`-gesteuertes Polling mit Backoff und `clearInterval` in `onUnmounted`.
- **Strikte Typisierung:** `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, ESLint `@typescript-eslint/no-explicit-any: error` und `consistent-type-assertions: never` (0 `any`, 0 `as unknown as X`).

## Observability (Micrometer, Prometheus, Grafana, JSON-Logging & Correlation-ID)

- **Micrometer-Metriken (`/actuator/prometheus`, strikt begrenzte Kardinalität):** Ausschließlich geschlossene Label-Mengen (`outcome`, `reason`, `type`, `result`) — **niemals** `accountId`, `eventId`, `correlationId` oder Beträge als Metrik-Tag. Exponiert u. a. `outbox_unsent`, `outbox_oldest_pending_age_seconds`, `outbox_events_failed_total`, `outbox_events_relayed_total{type,result}`, `consumer_processed_total{outcome}`, `consumer_duplicates_rejected_total{reason="duplicate"}`, `consumer_poison_total` (getrennter Poison-Zähler statt Vermischung mit Duplikaten), `consumer_retries_total`, `dlq_entries_unhandled`, `dlq_replayed_total`, `http_server_requests_seconds` (inkl. Histogram-Buckets).
- **Prometheus & Grafana (Zero-Click-Provisioning):** [observability/prometheus.yml](observability/prometheus.yml) scrapt `app:8080/actuator/prometheus` und `keycloak:9000/metrics` mit `--storage.tsdb.retention.time=2d --storage.tsdb.wal-compression`. [observability/grafana/provisioning/datasources/prometheus.yml](observability/grafana/provisioning/datasources/prometheus.yml) provisioniert die Datasource mit fester `uid: prometheus`; [observability/grafana/dashboards/ledger-stream.json](observability/grafana/dashboards/ledger-stream.json) (`uid: ledger-stream-overview`, 8 Panels inkl. roter 300s-Schwelle auf `outbox_oldest_pending_age_seconds` sowie separater HTTP-**p95**- und **p99**-Panels mit `uri!~"/actuator.*"` und `sum by (le, uri)`) referenziert diese UID ohne `__inputs`.
- **Strukturiertes JSON-Logging & 3-stufige Correlation-ID:** [logback-spring.xml](app/src/main/resources/logback-spring.xml) (`logstash-logback-encoder`) schreibt flaches JSON (`ts`, `level`, `logger`, `message`, `correlationId`, `eventId` nur im Event-Kontext; keine Tokens, `Authorization`-Header oder Roh-Payloads). `CorrelationIdFilter` validiert eingehende `X-Correlation-Id` strikt gegen `^[A-Za-z0-9._-]{8,64}$` (ungültige Werte werden verworfen, mit neuer UUID ersetzt und `correlationIdRejected=true` geloggt), persistiert sie in `outbox_events.correlation_id`, überträgt sie per Kafka-Header in den Consumer (`MDC` + `finally MDC.clear()`) und erhält sie beim DLQ-Replay.

## Ressourcenbudget (`/sys/fs/cgroup/memory.stat` `anon` vs. `file` — ALLE 8 Container)

Keycloak 26 läuft mit `start --optimized --import-realm`, `-Xmx256m`, `KC_CACHE=local` auf der **bestehenden** PostgreSQL-16-Instanz (eigene Datenbank `keycloak`, kein H2, kein Dev-Mode). Weil `docker stats` den reclaimbaren Linux-Page-Cache (`file`) einrechnet, wurde im 100.000-Buchungen-Peak zusätzlich `/sys/fs/cgroup/memory.stat` (`anon` = nicht-reclaimbarer Working-Set vs. `file` = reclaimbarer Page-Cache) je Container erhoben (M2):

| Container | Compose-Limit (k3s-Limit) | Tuning | Peak inkl. Cache (`memory.current`) | davon `anon` (Working-Set) | davon `file` (reclaimbar) |
|---|---|---|---:|---:|---:|
| `ledger-stream-postgres` | `320 MiB` (`320Mi`) | `shared_buffers=64MB`, `work_mem=4MB`, `max_connections=60`, `lock_timeout=3s` | `319,74 MiB` | `65,64 MiB` | `232,70 MiB` |
| `ledger-stream-kafka` | `448 MiB` (`448Mi`) | KRaft, 6 Partitionen, `-Xms128m -Xmx192m`, `dedupe_buffer=16MB` | `301,58 MiB` | `228,81 MiB` | `64,14 MiB` |
| `ledger-stream-schema-registry` | `384 MiB` (`384Mi`) | `FULL`, `-Xmx192m -XX:MaxMetaspaceSize=96m` | `216,25 MiB` | `211,77 MiB` | `0,06 MiB` |
| `ledger-stream-keycloak` | `640 MiB` (`640Mi`) | `start --optimized`, `KC_DB=postgres`, `KC_CACHE=local`, `-Xms128m -Xmx256m` | `412,65 MiB` | `408,21 MiB` | `0,08 MiB` |
| `ledger-stream-app` | `512 MiB` Demo (`640Mi` k3s) | Java 21 JRE, `-XX:MaxRAMPercentage=60.0` (k3s: `55.0`), Consumer Conc. `6` | `507,27 MiB` | `498,38 MiB` | `0,05 MiB` |
| `ledger-stream-frontend` | `48 MiB` (`48Mi`) | `nginxinc/nginx-unprivileged:1.27-alpine` (statisches Vue-Bundle) | `19,91 MiB` | `15,82 MiB` | `0,21 MiB` |
| `ledger-stream-prometheus` | `192 MiB` (`192Mi`) | `--storage.tsdb.retention.time=2d --storage.tsdb.wal-compression` | `22,81 MiB` | `20,54 MiB` | `0,16 MiB` |
| `ledger-stream-grafana` | `192 MiB` (`192Mi`) | `GF_INSTALL_PLUGINS=""`, `GF_ANALYTICS_CHECK_FOR_UPDATES=false` | `108,49 MiB` | `66,70 MiB` | `36,11 MiB` |
| **Summe (alle 8 Container, 100k Peak)** | **k3s `ResourceQuota` `requests`: `1536Mi`** | **k3s Summe `limits.memory`: `2864Mi` (`1,86×` Burst-Overcommit)** | **`1908,70 MiB`** | **`1515,87 MiB`** *(Puffer `+20,13 MiB` zu `1536 MiB`)* | **`333,51 MiB`** |

> **Befunde aus M2 & M3 (`jcmd 1 VM.native_memory summary` im 100k-Peak):**
> - **1,5-GiB-Nachweis (M2):** Im Unconstrained-100k-Peak beträgt der gesamte nicht-reclaimbare `anon`-Anteil aller 8 Container **`1515,87 MiB`** — das System bleibt innerhalb von `1536 MiB` mit knapp **`20,13 MiB` Luft** (`333,51 MiB` entfallen auf reclaimbaren Page-Cache bei Postgres `232,70 MiB`, Kafka `64,14 MiB` und Grafana `36,11 MiB`). Unter harter 1,5-vCPU-Drosselung (`keycloak: 0.10 vCPU`) steigt die `anon`-Summe temporär auf `1662,00 MiB` (`-126,00 MiB`), weil Keycloak unter 0,10-vCPU-Starvation während wiederholter Token-Refreshes auf `508,87 MiB` `anon` anwächst (App bleibt bei `465,14 MiB`).
> - **App-Non-Heap-Diagnose (M3):** `jcmd 1 VM.native_memory summary` im 100k-Peak (`committed=522059 KB` ≈ `509,8 MiB`) belegt, dass Direct Buffers (`Other`) mit **`12,8 MiB` unauffällig** sind (kein Direct-Buffer-Leak). Der reale Non-Heap (~220–255 MiB) setzt sich aus **Metaspace + Class + Shared Class (`98,2 MiB`, 17.952 Klassen)**, **G1GC-Strukturen (`51,5 MiB`)**, **JIT CodeCache (`45,3 MiB`)**, **Symbol-Tabelle (`25,9 MiB`)**, **178 Threads (`18,3 MiB` u. a. Tomcat + 36 `@RetryableTopic`-Consumer-Threads)** sowie **C2-Compiler-Arena-Spitzen (`~55 MiB`)** zusammen. Deshalb ist **`512 MiB` das knappe Demo-Limit** (Docker Compose) und **`640 MiB` die gemessene k3s-Produktionsgrenze** (`281 MiB` Heap-Peak + `~240 MiB` Non-Heap + `~120 MiB` Reserve).
> - **Warum `limits.memory`-Summe `2864 MiB` (`1,86×`) > `ResourceQuota` `requests.memory` (`1536 MiB`):** Kubernetes erzwingt das 1,5-GiB-Budget hart über `ResourceQuota` auf **`requests.memory: 1536Mi`** (`1424Mi` belegt, orientiert an `anon`). Die höheren Einzellimits (`2864Mi` Summe) erlauben reclaimbaren Page-Cache (Postgres/Kafka) und nicht-gleichzeitige JVM-Bursts ohne vorzeitige OOMKills.

## Schema-Evolution (Avro + Confluent Schema Registry)

- **Kompatibilitätsstufe:** `FULL` (beidseitig: v2-Consumer liest v1-Daten **und** v1-Consumer liest v2-Daten; Begründung gegenüber `BACKWARD` und `FULL_TRANSITIVE` in [ADR-0002](docs/adr/0002-avro-full-compatibility-statt-backward.md)).
- **Evolution v1 → v2 (`LedgerBookingEvent.avsc`):** Neues optionales Feld `initiationChannel` (`"type": "string", "default": "API"`). Beim Lesen älterer v1-Nachrichten löst der Consumer den nicht-null Default `"API"` auf; `schemaVersion = 2` im Payload korrespondiert mit der v2-Schema-ID im 5-Byte Confluent-Wire-Header.
- **CI-Erzwingung:** `SchemaEvolutionIT` prüft im CI-Build gegen eine echte Schema Registry (`confluentinc/cp-schema-registry:7.9.9`) die `FULL`-Konfiguration, beide Leserichtungen, die Ablehnung eines inkompatiblen Schemas (Pflichtfeld ohne Default → HTTP `409`) sowie byte-gleiche Rohbytes bei DLQ-Replay von v2-Events. Der CI-Workflow bricht ab, falls `SchemaEvolutionIT` nicht mit 0 Skips läuft.

## Schnellstart

```bash
# 1. Lokale Infrastruktur starten (PostgreSQL 16, Kafka 3.9.1 KRaft, Schema Registry)
docker compose up -d --wait

# 2. Build, ArchUnit-Prüfung und Testcontainers-Integrationstests ausführen
./mvnw verify
```

> Hinweis: PostgreSQL in `docker-compose.yml` ist standardmäßig nur innerhalb des Compose-Netzwerks erreichbar (kein Host-Port-Mapping). Alle Integrationstests laufen vollständig isoliert über Testcontainers.

## Lastnachweis (Phase 10 — 100.000 Buchungen, reproduzierbar)

> **Klare Trennung der Nachweise:**
> - **Retry-Leiter (5 Versuche, exponentieller Backoff + Jitter):** nachgewiesen über dedizierten Integrationstest (`DlqAndRetryIT`, Phase 5), nicht über den Lasttest.
> - **Duplikat-Kohorte (5 %, L2):** echte Replays bereits akzeptierter Anfragen aus einem Ringpuffer (`Idempotency-Key` + identischer Payload → `201 Created` + `Idempotency-Replayed: true`, byte-identischer Body, keine zweite Buchung, kein zweites Kafka-Event → `consumer_duplicates_rejected_total{reason="duplicate"} == 0`). Consumer-seitige Duplikat-Abweisung (`UNIQUE(event_id)`) ist separat in `IdempotentConsumerIT` nachgewiesen.
> - **DLQ-Anteil im Lasttest (1 %):** transportseitige Fault Injection (schema-valide Avro-v2-Poison-Events — 50 % `unknown_account`, 50 % `non_positive_amount` — direkt ins Topic `ledger.events` durch `load-generator`), kein Test-Schalter im Produktivcode.
> - **Aufwärmphase:** erste `5 s` gegen dedizierte Warmup-Konten, vollständig aus Durchsatz- und Latenzwerten ausgeschlossen (in der Gesamt-DB-Invariante jedoch exakt gegen `bookings` und `processed_events` mitgeprüft).

### Umgebung & Reproduzierbarkeits-Parameter (L6)

| Parameter | Wert |
|---|---|
| **Host-Hardware** | Intel Core Ultra 7 155H (`22` vCPU / 11 Kerne × 2 Threads), `31 GiB` RAM, `1 TB` virtuelle Disk (`/dev/sdd`), virtualisiert (`WSL2` / Microsoft Hypervisor) |
| **Container-Konfiguration** | `docker-compose.yml` + `docker-compose.loadtest.yml` (identische `mem_limit`-Werte wie oben; CPU-Caps für den Lastlauf entdrosselt: `app: 6.0`, `postgres: 4.0`, `kafka: 2.0`, `keycloak: 2.0`, `schema-registry: 1.0`) |
| **JVM (App-Container)** | `eclipse-temurin:21-jre-alpine`, `JAVA_OPTS="-XX:MaxRAMPercentage=60.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"` (`512 MiB` Limit → `~307 MiB` Heap) |
| **JVM (Client `load-generator`)** | `OpenJDK 25.0.2 LTS (Corretto-25.0.2.10.1)` (kompiliert mit `--release 21`), `-Djdk.httpclient.keepalive.timeout=60` |
| **Kafka-Partitionen & Consumer (L3a)** | `ledger.events`: **6 Partitionen**, `ledger.events.dlq`: **6 Partitionen**, Consumer-Concurrency: **`6`** (`KAFKA_CONSUMER_CONCURRENCY=6`) |
| **Outbox-Relay-Deckel (L3b)** | Batch `500`, Poll-Intervall `200 ms` (`OUTBOX_RELAY_FIXED_DELAY_MS=200`, theoretischer Deckel `2.500 Events/s`; Wave-basiertes Kafka-Pipelining je distinktem Konto) |
| **Client-Setup (L3c)** | `load-generator` CLI (ohne Spring-Kontext), `--threads=32`, `128` vorangelegte Benchmark-Konten, HTTP/1.1 Keep-Alive Connection-Pool |
| **Aufruf (reproduzierbar)** | `java -Djdk.httpclient.keepalive.timeout=60 -jar load-generator/target/load-generator-0.1.0-SNAPSHOT-shaded.jar --bookings=100000 --duplicates=0.05 --poison=0.01 --seed=20261124 --threads=32 --warmup-seconds=5 --base-url=http://127.0.0.1:8085` |

### Ergebnisse (Unconstrained vs. 1,5-vCPU-Budget — jeweils 100.000 Buchungen, Seed `20261124`)

> **Abgrenzung Consumer-Idempotenz (M4c):** Der 100k-Lauf erzeugt **keine** Consumer-Duplikate, da alle 5 % L2-Duplikate bereits synchron an der HTTP-API über `idempotency_keys` abgefangen werden (`consumer_duplicates_rejected_total{reason="duplicate"} == 0`). Die Consumer-seitige Idempotenz (`UNIQUE(event_id)` auf `processed_events`) ist durch die dedizierten Integrationstests (`IdempotentConsumerIT`, u. a. 3-fach-Zustellung & Crash zwischen DB-Commit und Offset-Commit) belegt, nicht durch den Benchmark.

| Profil | Gesendet (Regulär / Dup / Poison) | Akzeptiert (DB-Diff) | Abgelehnt | L2-Duplikate (`201` + Header) | Consumer-Dup (`reason="duplicate"`) | DLQ / `consumer_poison_total` (Diff) | `max(outbox_unsent)` | HTTP-Dauer (ohne 5s Warmup) | Drain-Zeit (Gesamt bis Projektion) | Durchsatz HTTP (akzept. Buch./s) | Durchsatz Consumer (Prom 2m) | Invarianten-Check | Seed |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| **Unconstrained** (`docker-compose.loadtest.yml`) | `106.000` (`100k`/`5k`/`1k`) | `100.000` *(Diff `0`)* | `0` | `5.000` *(0 Mismatch)* | `0` | `1.000` *(Diff `0`)* | `8.549` | `93,97 s` | `103,35 s` *(`197,32 s`)* | `1064,2 /s` *(1117,4 HTTP/s)* | `659,2 Events/s` *(6 Part., Conc. 6)* | **PASS** (`EXIT 0`, 10/10 Konten exakt) | `20261124` |
| **1,5 vCPU Budget** (`app 0.6`, `pg 0.35`, `kafka 0.25`, `sr 0.15`, `kc 0.10`, `rest 0.05`) | `106.000` (`100k`/`5k`/`1k`) | `100.000` *(Diff `0`)* | `0` | `5.000` *(0 Mismatch)* | `0` | `1.000` *(Diff `0`)* | `899` | `998,03 s` | `278,64 s` *(`1276,67 s`)* | `100,2 /s` *(105,2 HTTP/s)* | `254,2 Events/s` *(Prom 5m, 6 Part.)* | **PASS** (`EXIT 0`, 10/10 Konten exakt) | `20261124` |

### Latenzvergleich: Clientseitig (`load-generator`) vs. Serverseitig (Prometheus `http_server_requests_seconds`)

| Profil | Messpunkt | p50 | p95 | p99 |
|---|---|---:|---:|---:|
| **Unconstrained** | **Client** (`load-generator`, regulär / alle HTTP) | `22,94 ms` / `22,44 ms` | `60,85 ms` / `60,39 ms` | `92,27 ms` / `91,27 ms` |
| **Unconstrained** | **Server** (Prometheus `POST /api/v1/accounts/{accountId}/bookings`, `5m`-Fenster) | `16,47 ms` | `49,91 ms` | `79,19 ms` |
| **1,5 vCPU Budget** | **Client** (`load-generator`, regulär / alle HTTP) | `276,29 ms` / `231,83 ms` | `694,04 ms` / `691,09 ms` | `1053,07 ms` / `1012,74 ms` |
| **1,5 vCPU Budget** | **Server** (Prometheus `POST_LOAD`, `5m`-Fenster) | `197,36 ms` | `411,35 ms` | `528,83 ms` |

> **Determinismus-Nachweis (Seed `20261124`):** Alle Läufe (inkl. Unconstrained-Wiederholungslauf und 1,5-vCPU-Lauf) auf frisch zurückgesetzten Stacks liefern exakt identische Kohortenzahlen (`100.000` / `5.000` / `1.000`), exakt `0` Abweichung auf allen 5 Invarianten sowie centgenau identische Endsalden auf allen 10 Stichprobenkonten (`DE-BENCH-1352904-0000 = 11737.39 EUR`, `DE-BENCH-1352904-0001 = 12307.11 EUR`, …, `DE-BENCH-1352904-0009 = 12112.79 EUR`, je `782/782` Buchungen).

## Deployment (Phase 11 — k3s, Kustomize, Sealed Secrets, NetworkPolicies, ArgoCD, GHCR + Cosign/SBOM)

- **Kustomize (`deploy/kustomize/`):** Vollständige Trennung zwischen [`base/`](deploy/kustomize/base/kustomization.yaml) und reinen Delta-Overlays [`overlays/local/`](deploy/kustomize/overlays/local/kustomization.yaml) sowie [`overlays/prod/`](deploy/kustomize/overlays/prod/kustomization.yaml) (`LEDGER_ADMIN_ENABLED=false`, `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=FULL`, unveränderliches `@sha256:`-Digest-Pinning ohne `:latest`).
- **Ressourcen-Erzwingung (`deploy/kustomize/base/resource-quota.yaml`):** `ResourceQuota` erzwingt `requests.memory <= 1536Mi` (`1424Mi` belegt) und `requests.cpu <= 1500m` (`1300m` belegt) direkt bei Kubernetes-Admission; `LimitRange` setzt verbindliche Defaults (`64Mi`/`50m` Request, `256Mi`/`250m` Limit, `640Mi` Max).
- **NetworkPolicies (`deploy/kustomize/base/network-policies.yaml`):** `default-deny-all` (Ingress + Egress), `allow-dns-egress` (UDP/TCP `53` zu CoreDNS) und explizite Service-Policies (`app` → `postgres`/`kafka`/`schema-registry`/`keycloak`; `frontend` → `app`/`keycloak`; `prometheus` → `app:8080`/`keycloak:9000`/`kafka-exporter:9308`).
- **Sealed Secrets (`deploy/kustomize/base/sealed-secrets.yaml`):** Alle Zugangsdaten liegen ausschließlich als Bitnami `SealedSecret` vor — null Klartext-Secrets im Repository.
- **SecurityContext & Probes:** Namespace mit `pod-security.kubernetes.io/enforce: restricted`; jeder Container läuft mit `runAsNonRoot: true`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]` und gezielten `emptyDir`-Mounts (u. a. Frontend `/tmp` für Runtime-`config.js`, Kafka `/opt/kafka/config`, Schema Registry `/etc/schema-registry`). Keycloak nutzt `startupProbe` (`/health/started`), separate `readinessProbe` ≠ `livenessProbe`; Kafka-KRaft prüft Readiness über `/opt/kafka/bin/kafka-broker-api-versions.sh`.
- **GitOps & Supply Chain (ArgoCD + GHCR + Cosign + CycloneDX):** [`deploy/argocd/application.yaml`](deploy/argocd/application.yaml) synchronisiert `deploy/kustomize/overlays/prod` automatisiert mit `selfHeal: true` und `prune: true` (kein imperatives Apply in `.github/workflows/ci.yml`). Der CI-Job `container-supply-chain` pusht unveränderliche `sha`-Images nach GHCR, generiert CycloneDX-1.6-SBOMs via `syft`, signiert per Keyless-Cosign (Fulcio + Rekor), hängt SBOM-Attestations an und bricht bei fehlgeschlagener `cosign verify` / `cosign verify-attestation`-Prüfung ab. Rollbacks erfolgen rein deklarativ per `git revert` auf den vorherigen Digest (ArgoCD rollt automatisch zurück).
