# Betrieb & Runbook (`ledger-stream`)

Dieses Runbook beschreibt die fünf zentralen Betriebsszenarien für `ledger-stream`: Erkennung über Metriken, Analyse, konkrete Eingriffe und Grenzen.

---

## 1. Outbox-Rückstand (`outbox_oldest_pending_age_seconds > 300s`)

### Metriken & Schwellenwerte

| Metrik | Bedeutung | Schwelle / Bewertung |
|---|---|---|
| `outbox_unsent` *(Micrometer: `outbox_unsent_total`)* | Aktuelle Anzahl `PENDING`-Events in `outbox_events` | Kurzfristige Spitzen unter Burst-Last sind normal (im Unconstrained-100k-Lauf bis `8.549`, unter 1,5 vCPU `899`). Allein kein ausreichendes Alarmkriterium. |
| `outbox_oldest_pending_age_seconds` | Alter des ältesten `PENDING`-Events in Sekunden (`0`, wenn leer) | **`> 300s` über `1m` (`OutboxRelayStalled`, `critical`):** Unterscheidet zuverlässig „Relay hängt / Kafka down“ von normalem Last-Backlog. |
| `outbox_events_failed_total` | Dauerhaft in Status `FAILED` überführte Events (nach `10` Fehlversuchen) | **`> 0` (`warning`/`critical`):** Erfordert manuelle Diagnose. |

### Was tun bei Alarm `OutboxRelayStalled`?

1. **Prüfen, ob Kafka und Schema Registry erreichbar sind:**
   ```bash
   kubectl -n ledger-stream get pods
   kubectl -n ledger-stream exec kafka-0 -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092 | head -n 5
   ```
2. **Prüfen, ob das Relay läuft und Fehler loggt:**
   ```bash
   kubectl -n ledger-stream logs deploy/app --tail=100 | grep -E 'stage=relay|Outbox|FAILED|Kafka'
   ```
3. **Älteste `PENDING`- bzw. `FAILED`-Einträge in PostgreSQL inspizieren:**
   ```sql
   SELECT id, aggregate_id, event_type, status, retry_count, next_attempt_at, last_error, created_at
   FROM outbox_events
   WHERE status <> 'SENT'
   ORDER BY created_at ASC
   LIMIT 20;
   ```
4. **Wiederherstellung:**
   - Bei transientem Broker-/Netzwerkausfall: Sobald Kafka/Schema Registry wieder erreichbar sind, draint das Relay alle `PENDING`-Events automatisch nach Ablauf von `next_attempt_at` (Backoff `1s..60s`, ±20 % Jitter).
   - Bei dauerhaft in `FAILED` gelandeten Events (nach behobener Ursache): gezieltes Zurücksetzen auf `PENDING` (`UPDATE outbox_events SET status = 'PENDING', retry_count = 0, next_attempt_at = NOW() WHERE id = '<uuid>';`).

---

## 2. Dead-Letter-Queue (DLQ) — Erkennen, Analysieren, Replay

### 2.1 Warum unterschiedliche Retry-Zahlen? (Outbox `10` vs. Consumer `5`)

- **Outbox-Relay (`10` Versuche, `1s..60s` + Jitter, ~6–8 Min.):** Scheitert fast ausschließlich an Infrastruktur-Störungen (Broker-Restart, Netzwerk-Glitch) → längeres Fenster überbrückt Neustarts ohne manuellen Eingriff.
- **Kafka-Consumer (`5` Versuche, `1s..30s` + Jitter über `@RetryableTopic`, ~30–45 s):** Verarbeitet einzelne Records. Scheitert eine Nachricht nach 5 Versuchen weiterhin, soll sie zügig und sichtbar in `dlq_entries` landen, ohne die Hauptpartition je zu blockieren.

### 2.2 DLQ-Rückstand erkennen

| Metrik | Typ | Interpretation |
|---|---|---|
| `dlq_entries_unhandled` | Gauge | **Primäre DLQ-Alarmmetrik:** Offene DLQ-Einträge (`replayed_at IS NULL`). Zielwert im Normalbetrieb: **`0`**. |
| `dlq_replayed_total` | Counter | Kumulierte Anzahl erfolgreich über den Admin-Endpoint wieder eingestellter Einträge. |
| `consumer_retries_total` | Counter | Ausgelöste nicht-blockierende Retry-Versuche (Frühindikator vor DLQ-Wachstum). |
| `consumer_poison_total` | Counter | Direkt ohne Retry abgewiesene Poison-Events (Deserialisierung oder fachlich ungültig). |
| `consumer_duplicates_rejected_total{reason="duplicate"}` | Counter | Echte Consumer-Duplikate, abgewiesen über `UNIQUE(event_id)` auf `processed_events`. |

### 2.3 Analysieren

```sql
-- Offene DLQ-Einträge (ohne Binär-Payload)
SELECT id, topic, partition, "offset", event_id, error_class, attempt_count, created_at
FROM dlq_entries
WHERE replayed_at IS NULL
ORDER BY created_at ASC;

-- Detailanalyse inkl. Correlation-ID, Schema-ID und Original-Payload-Header
SELECT id, record_key, event_id, error_class, error_message, attempt_count, headers
FROM dlq_entries
WHERE id = '<dlq-entry-uuid>';

-- Unveränderliche Replay-Historie eines Eintrags
SELECT id, attempted_at, attempted_by, outcome, error_class, note
FROM dlq_replay_attempts
WHERE dlq_entry_id = '<dlq-entry-uuid>'
ORDER BY attempted_at ASC;
```

### 2.4 Entscheidungsmatrix: Wann replayen — wann nicht?

| Fehlerklasse | Ursache | Aktion |
|---|---|---|
| `TransientDataAccessResourceException`, `QueryTimeoutException`, Verbindungsabbruch | DB-/Netzwerkstörung länger als 5 Consumer-Retries | **Replay nach DB-Wiederherstellung.** |
| `SerializationException` (z. B. fehlende `schemaId`) | Schema Registry zeitweise nicht synchron / Schema fehlte | Schema korrigieren bzw. registrieren, danach **Replay** (unveränderte Rohbytes in `dlq_entries.payload`). |
| Out-of-Order `BookingCancelled` nach transientem `BookingCreated`-Retry | Retry-Topic-Reihenfolge-Trade-off ([`docs/design-decisions.md`](design-decisions.md)) | Sobald `BookingCreated` projiziert ist: **Replay**. |
| `NonRetryableEventException` (`unknown_account`, `non_positive_amount`, `invalid_booking_type`) | Fachlich ungültiges Event / Producer-Bug | **NICHT blind replayen!** Erzeugt denselben Fehler erneut (wird per Partial-Unique-Index auf `COALESCE(event_id::text, payload_sha256) WHERE replayed_at IS NULL` in dieselbe offene Zeile dedupliziert). Ursache am Producer beheben. |
| Korrupte Nicht-Avro-Rohbytes | Defekter Producer / fremdes Format | **NICHT replayen.** Rohbytes aus `x-original-payload` (Base64) forensisch auswerten, Producer korrigieren. |

### 2.5 Wer darf replayen — und wie?

- **Berechtigung (Separation of Duties):** Ausschließlich Benutzer mit der Keycloak-Realm-Rolle **`operator`** (`ROLE_operator`) — `viewer` und `writer` erhalten `403 Forbidden`.
- **Feature-Flag-Schutz (404-vor-401-Invariante):** Im Produktions-Overlay [`deploy/kustomize/overlays/prod/kustomization.yaml`](../deploy/kustomize/overlays/prod/kustomization.yaml) ist `LEDGER_ADMIN_ENABLED=false` gepinnt (`/admin/**` liefert stets `404` vor OIDC-Prüfung). Ein Replay erfordert ein bewusstes Wartungsfenster / kurzzeitige Freischaltung oder einen dedizierten Operator-Pod mit `LEDGER_ADMIN_ENABLED=true`.
- **Aufruf (im Cluster per `port-forward`):**
  ```bash
  # In-Cluster-Service lokal weiterleiten (separates Terminal)
  kubectl -n ledger-stream port-forward svc/app 8080:8080

  curl -fsS -X POST "http://127.0.0.1:8080/admin/dlq/<dlq-entry-uuid>/replay" \
    -H "Authorization: Bearer <operator-jwt>" \
    -H "Content-Type: application/json" \
    -d '{"note":"DB-Störung behoben, kontrolliertes Replay"}'
  ```
- **Doppelte Sicherheit:** Ein zweiter HTTP-Replay auf dieselbe ID liefert `409 Conflict` (`replayed_at`-Guard); selbst bei manuellem Re-Publish auf Kafka verhindert `UNIQUE(event_id)` auf `processed_events` jede Doppelbuchung in `account_summary`.

### 2.6 DLQ-Replay im `prod`-Overlay aktivieren

In [`deploy/kustomize/overlays/prod/kustomization.yaml`](../deploy/kustomize/overlays/prod/kustomization.yaml#L16) ist `LEDGER_ADMIN_ENABLED=false` hart gepinnt (`/admin/**` liefert `404 Not Found` noch vor der OIDC-Prüfung). Für ein kontrolliertes Produktions-Replay gelten vier verbindliche Schritte:

1. **Wartungsfenster öffnen:** In [`deploy/kustomize/overlays/prod/kustomization.yaml`](../deploy/kustomize/overlays/prod/kustomization.yaml#L16) `LEDGER_ADMIN_ENABLED=true` setzen.
2. **Deklarativ ausrollen:** `git commit` + `git push origin main` → ArgoCD synct selbstständig; warten, bis `argocd app get ledger-stream` wieder **`Synced + Healthy`** meldet.
3. **Port-Forward öffnen & Replay als `operator` ausführen:**
   ```bash
   # In-Cluster-Port 8080 lokal verfügbar machen (separates Terminal)
   kubectl -n ledger-stream port-forward svc/app 8080:8080

   curl -fsS -X POST "http://127.0.0.1:8080/admin/dlq/<dlq-entry-uuid>/replay" \
     -H "Authorization: Bearer <operator-jwt>" \
     -H "Content-Type: application/json" \
     -d '{"note":"Ursache behoben, kontrolliertes Prod-Replay"}'
   ```
4. **Wartungsfenster sofort schließen:** `LEDGER_ADMIN_ENABLED=false` zurücksetzen, `git commit` + `git push origin main`, ArgoCD-Sync abwarten und erneut per `port-forward` verifizieren, dass `GET http://127.0.0.1:8080/admin/dlq` wieder **`404 Not Found`** (ohne Token und mit `operator`-Token) liefert.

- **Berechtigung & Audit-Trail:** Ausschließlich die Keycloak-Rolle **`operator`** (`ROLE_operator`) darf `/admin/dlq/**` aufrufen (`viewer`/`writer` erhalten `403`). Jeder Versuch wird unveränderlich in `dlq_replay_attempts` (`attempted_at`, `attempted_by` aus dem JWT-Subject, `outcome`, `error_class`, `note`) protokolliert.
- **Sicherheitshinweis:** Während des geöffneten Wartungsfensters ist `/admin/dlq/**` über das Cluster-Netz erreichbar (abgesichert durch OIDC + `ROLE_operator`); das Fenster ist daher unmittelbar nach Abschluss des Replays wieder zu schließen.

---

## 3. Rollback (`git revert` → ArgoCD) & Verhalten laufender Buchungen

### Durchführung (rein deklarativ, kein `kubectl apply`)

1. Fehlerhaften Digest-Bump-Commit identifizieren (`git log --oneline deploy/kustomize/overlays/prod/kustomization.yaml`).
2. Commit auf `main` zurücknehmen und pushen:
   ```bash
   git revert --no-edit <bad-commit-sha>
   git push origin main
   ```
3. **ArgoCD (`automated.selfHeal: true`, `prune: true`)** erkennt den Revert automatisch und rollt das betroffene Deployment auf den vorherigen unveränderlichen `@sha256:`-Digest zurück (`argocd app get ledger-stream --core` → `Synced + Healthy`).

### Grenze von ArgoCD Self-Heal unter `ServerSideApply=true`

Unter `ServerSideApply=true` bleiben zusätzliche, nicht Git-verwaltete Felder (z. B. `kubectl set env deploy/frontend FOO=bar`, Field-Manager `kubectl-set`) erhalten. ArgoCD meldet dabei weiterhin `Synced`, weil der Controller per SSA nur seine eigenen Git-verwalteten Felder abgleicht. Zur vollständigen Bereinigung solcher extern hinzugefügten Felder dient `argocd app sync ledger-stream --force` oder das Umschalten auf `Replace` statt `ServerSideApply`.

### Was passiert während des Rollbacks mit laufenden Buchungen?

- **Strategie `RollingUpdate` im `prod`-Overlay (`app`) & Graceful Shutdown:** Im `prod`-Overlay fährt `app` `RollingUpdate` (`maxSurge: 25%`, `maxUnavailable: 25%` — bei `replicas: 1` startet Kubernetes erst den neuen `app`-Pod bis `Ready`, bevor der alte terminiert; Quota-Hard `2000m`/`2304Mi` hält genau diesen einen Surge-Pod vor). Beim Beenden des alten Pods wartet der `preStop`-Hook (`sleep 5`) fünf Sekunden, bis der Endpoints-Controller den Pod aus dem Service entfernt hat; anschließend empfängt Spring Boot `SIGTERM` mit `server.shutdown: graceful`, `spring.lifecycle.timeout-per-shutdown-phase: 30s` und `terminationGracePeriodSeconds: 45` (Tomcat-Requests, Kafka-Consumer und Outbox-Relay fahren sauber herunter, Container endet mit `exitCode: 0`, `reason: Completed`). In `local` (`Recreate`) läuft derselbe Graceful-Shutdown ohne parallelen Surge-Pod.
- **Clients während des kurzen Neustartfensters:** Neue HTTP-Requests erhalten kurzzeitig Verbindungsfehler / `503`. Dank clientseitigem `Idempotency-Key` (im Frontend vor Submit erzeugt und beibehalten) kann jeder Client gefahrlos mit demselben Key wiederholen — wurde die Buchung vor `SIGTERM` bereits committed, liefert die zurückgerollte Version byte-gleich die gecachte Antwort (`201 + Idempotency-Replayed: true`); wurde sie gerollt, wird sie genau einmal neu gebucht.
- **Outbox & Consumer:** Unversendete `PENDING`-Events verbleiben sicher in PostgreSQL (`outbox_events`) und werden nach Start des alten Pods weiter ausgeliefert. Dank Schema-Kompatibilität **`FULL`** ([ADR-0005](adr/0005-avro-full-compatibility-statt-backward.md)) kann die ältere App-Version auch bereits von der neueren Version geschriebene Avro-Events dekodieren.
- **Wichtige Grenze:** Flyway-Migrationen sind vorwärtsgerichtet (Expand-Contract-Prinzip). Ein App-Rollback ist nur dann sicher, wenn Datenbank-Migrationen der neueren Version abwärtskompatibel zur vorherigen App-Version geblieben sind (keine destruktiven `DROP COLUMN` im selben Release).

---

## 4. Kafka-Broker ausgefallen (`Broker down`)

### Was läuft weiter — was stoppt?

| Komponente | Zustand bei Kafka-Ausfall | Auswirkung |
|---|---|---|
| **HTTP Write-API (`POST /api/v1/accounts/**`)** | **Läuft vollständig weiter** | Buchungen, Konto-Salden (`accounts`), `idempotency_keys` und `outbox_events` (`PENDING`) werden atomar in PostgreSQL committed. |
| **HTTP Read-API auf Schreibmodell (`GET /accounts`, `GET /bookings`)** | **Läuft vollständig weiter** | Liest direkt aus `accounts` und `bookings` (stets konsistent). |
| **Outbox-Relay** | **Puffert in PostgreSQL** | `outbox_unsent` und `outbox_oldest_pending_age_seconds` steigen an; Relay retryed mit exponentiellem Backoff (`1s..60s`, bis 10 Versuche). Bei Ausfall `> 6–8 Min.` können älteste Events in `FAILED` wechseln (siehe Abschnitt 1). |
| **Kafka-Consumer & Read-Projektion (`account_summary`)** | **Stoppt / verzögert** | `account_summary` friert auf dem letzten verarbeiteten Stand ein (eventual consistency lag). |
| **Schema Registry** | **Degradiert** | Kann ohne Kafka (`_schemas`) keine neuen Schemas registrieren; gecachte Schemas bleiben lesbar. |

### Neustart & Wiederanlauf

1. Broker wiederherstellen (`kubectl -n ledger-stream rollout restart statefulset/kafka` bzw. `docker compose restart kafka`).
2. Readiness prüfen (`/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092`).
3. Beobachten, dass `outbox_unsent` und `outbox_oldest_pending_age_seconds` auf `0` abfallen und `consumer_processed_total{outcome="processed"}` aufholt.
4. Prüfen, ob während eines längeren Ausfalls Outbox-Events den Status `FAILED` erreicht haben (`SELECT COUNT(*) FROM outbox_events WHERE status = 'FAILED';`) und diese ggf. gemäß Abschnitt 1 reaktivieren.

---

## 5. Consumer skalieren (Partitionsgrenze & Reihenfolge-Effekt)

- **Aktuelle Dimensionierung:** `ledger.events` und `ledger.events.dlq` haben **`6` Partitionen**, `KAFKA_CONSUMER_CONCURRENCY=6` (1 Pod mit 6 Consumer-Threads).
- **Skalierungsgrenze:** Die effektive Parallelität einer Consumer-Group (`ledger-stream-projection`) ist hart durch die **Partitionszahl (`6`)** begrenzt:
  - `1 Pod × Concurrency 6` = `6` aktive Consumer (1 Partition pro Thread).
  - `2 Pods × Concurrency 3` = `6` aktive Consumer.
  - `> 6` Consumer-Threads insgesamt bringen **null zusätzlichen Durchsatz** — überschüssige Consumer bleiben im Leerlauf (`idle`).
- **Partitionen erhöhen (`6` → `12`):** Ist online möglich (`kafka-topics.sh --alter --partitions 12`), verändert aber die Hash-Zuordnung `hash(accountId) % partitions`!
  - **Effekt auf die Reihenfolge:** Während der Umstellung können ältere Events für Konto `A` noch auf der alten Partition `p1` liegen, während neue Events für Konto `A` bereits auf der neuen Partition `p2` landen → kurzzeitige Out-of-Order-Verarbeitung für Konto `A` möglich, bis die alten Offsets abgebaut sind.
  - **Sicherer Ablauf:** Consumer kurzzeitig vollständig leerlaufen lassen (`lag == 0`, `outbox_unsent == 0`), Partitionen erhöhen, danach `KAFKA_CONSUMER_CONCURRENCY` bzw. Pod-Replicas anpassen.
