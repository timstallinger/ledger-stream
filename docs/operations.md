# Betrieb & Runbook (`ledger-stream`)

Dieses Dokument beschreibt, wie Betriebsstörungen in `ledger-stream` erkannt, analysiert und ohne Datenverlust oder Doppelbuchungen behoben werden.

---

## 1. Outbox-Rückstand interpretieren

| Metrik | Bedeutung | Bewertung |
|---|---|---|
| `outbox_unsent` *(Micrometer: `outbox_unsent_total`)* | Aktuelle Anzahl `PENDING`-Events in `outbox_events` | Kurzfristige Spitzen unter Last sind normal; entscheidend ist das Alter. |
| `outbox_oldest_pending_age_seconds` | Alter des ältesten `PENDING`-Events in Sekunden (`0`, wenn leer) | **`> 300s` (Alarm):** Relay hängt, ist tot oder Kafka ist dauerhaft nicht erreichbar. Unterscheidet zuverlässig „Relay tot“ von normalem Last-Backlog. |
| `outbox_events_failed_total` | Events im Status `FAILED` nach 10 Fehlversuchen | Erfordert manuelle Analyse (`SELECT id, aggregate_id, retry_count, last_error FROM outbox_events WHERE status = 'FAILED'`). |

---

## 2. DLQ-Runbook (Consumer-Fehlerbehandlung)

### 2.1 Warum unterschiedliche Retry-Zahlen? (Outbox `10` vs. Consumer `5`)

- **Outbox-Relay: 10 Versuche (Backoff `1s..60s`, ±20 % Jitter, Gesamtfenster ~6–8 Minuten):**
  Das Outbox-Relay scheitert fast ausschließlich an **Infrastruktur-Störungen** (Broker-Restart, Netzwerk-Glitch, Schema-Registry-Neustart). Ein längeres Retry-Fenster überbrückt typische Infrastruktur-Neustarts ohne manuellen Eingriff, während Per-Account-Head-of-Line-Schutz verhindert, dass neuere Events desselben Kontos überholen.
- **Kafka-Consumer: 5 Versuche (exponentieller Backoff `1s..30s` mit Jitter über Retry-Topics, Gesamtfenster ~30–45 Sekunden):**
  Der Consumer verarbeitet **einzelne Nachrichten**. Ist eine Nachricht nach 5 Versuchen weiterhin nicht verarbeitbar (z. B. dauerhafter DB-Constraint-Fehler, Bug in der Projektionslogik oder fehlendes Schema), bringt weiteres automatisches Wiederholen selten Heilung — die Nachricht soll zügig und sichtbar in `ledger.events.dlq` / `dlq_entries` landen, damit der Betrieb reagieren kann, ohne dass die Hauptpartition je blockiert.

### 2.2 DLQ-Rückstand erkennen

| Metrik | Typ | Interpretation |
|---|---|---|
| `dlq_entries_unhandled` | Gauge | **Primäre Betriebsmetrik:** Anzahl offener DLQ-Einträge (`replayed_at IS NULL`). Zielwert im Normalbetrieb ist **`0`**. Jeder Wert `> 0` zeigt unbearbeitete Fehler an. |
| `dlq_replayed_total` | Counter | Kumulierte Anzahl erfolgreich über den Admin-Endpoint erneut eingestellter DLQ-Einträge. |
| `consumer_retries_total` | Counter | Kumulierte Anzahl ausgelöster nicht-blockierender Retry-Versuche (Versuche 2–5). Steigende Rate warnt früh vor DB-/Netzwerkproblemen, noch bevor die DLQ wächst. |
| `consumer_duplicates_rejected_total{reason="duplicate\|poison"}` | Counter | `duplicate` = erwartete Idempotenz-Deduplizierung bei Redelivery/Rebalance/Replay; `poison` = fachlich/deserialisierungsseitig abgelehnte Events. |

### 2.3 DLQ-Eintrag analysieren

1. Offene Einträge abfragen (ohne Binär-Payload):
   ```sql
   SELECT id, topic, partition, "offset", event_id, error_class, attempt_count, created_at
   FROM dlq_entries
   WHERE replayed_at IS NULL
   ORDER BY created_at ASC;
   ```
2. Detailanalyse eines Eintrags (inkl. Fehlertext und Metadaten-Headern):
   ```sql
   SELECT id, record_key, event_id, error_class, error_message, attempt_count, headers
   FROM dlq_entries
   WHERE id = '<dlq-entry-uuid>';
   ```
   Wichtige Header in `headers`:
   - `errorClass`, `errorMessage`: konkrete Exception-Klasse und Ursache.
   - `originalTopic`, `originalPartition`, `originalOffset`, `attemptCount`, `failedAt`.
   - `correlationId`: zum Nachverfolgen im strukturierten JSON-Log über HTTP → Outbox → Consumer.
   - `schemaId`: Avro-Schema-ID aus dem 5-Byte-Confluent-Header (sofern vorhanden).
   - `x-original-payload`: Base64-kodierte Original-Rohbytes (bei Deserialisierungsfehlern).

### 2.4 Entscheidungsmatrix: Wann replayen — wann nicht?

| Fehlerklasse | Ursache | Aktion |
|---|---|---|
| `TransientDataAccessResourceException`, `QueryTimeoutException`, Verbindungsabbruch | Vorübergehende DB-/Netzwerkstörung länger als 5 Retries | **Replay nach Wiederherstellung** der DB. |
| `SerializationException` (z. B. fehlende `schemaId` in Schema Registry) | Schema nicht registriert / Registry-Synchronisationsproblem | Schema korrigieren bzw. registrieren, danach **Replay** (Rohbytes liegen unverändert in `dlq_entries.payload`). |
| `NonRetryableEventException` (`unknown_account`, `non_positive_amount`, `invalid_booking_type`) | Fachlich ungültiges Event / Upstream-Vertragsbruch | **NICHT blind replayen!** Ein Replay erzeugt denselben Fehler erneut. Zuerst Datenlage bzw. Producer klären; falls dauerhaft ungültig, dokumentiert schließen statt replayen. |
| Deserialisierungsfehler mit korrupten Nicht-Avro-Bytes | Defekter Producer / fremdes Format auf dem Topic | **NICHT replayen**, da kein gültiges Avro-Payload existiert. Ursache am Producer beheben. |

### 2.5 Replay durchführen

> **Sicherheitshinweis:** Bis Phase 7 (Keycloak OIDC + Rolle `operator`) ist der Admin-Endpoint standardmäßig über `ledger.admin.enabled=false` deaktiviert und liefert `404`. Für lokale/administrative Tests oder nach Aktivierung von OIDC:

```bash
curl -X POST "http://localhost:8080/admin/dlq/<dlq-entry-uuid>/replay" \
  -H "Content-Type: application/json" \
  -d '{"replayedBy":"<operator-name>","note":"Ursache behoben, erneute Zustellung"}'
```

- **Idempotenz:** Der Eintrag wird nicht gelöscht, sondern mit `replayed_at`, `replayed_by` und `note` markiert. Ein zweiter HTTP-Aufruf auf dieselbe DLQ-ID liefert `409 Conflict`. Selbst bei doppelter Zustellung desselben `event_id` auf Kafka verhindert `processed_events (UNIQUE(event_id))` jeden zweiten Effekt in `account_summary`.
