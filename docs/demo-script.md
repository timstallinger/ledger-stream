# Live-Demo-Skript (`ledger-stream`)

**Gesamtdauer:** ~12 Minuten (Unconstrained-Profil, Lastlauf ~2–3 min) · alternativ ~25–30 Minuten unter harter `1,5 vCPU`-Drosselung (`docker-compose.loadtest-1.5vcpu.yml`, Lastlauf ~20 min).

**Einstiegs-Pitch (30 Sekunden, vor Schritt 1):**

> „Kafka liefert at-least-once. Der Consumer dedupliziert über `event_id` in derselben Transaktion wie das Saldo-Update — deshalb tritt die Wirkung genau einmal ein. Bewiesen im Lasttest: 100.000 Buchungen, 5 % Duplikate, Summe im Ledger exakt gleich den akzeptierten Anfragen, 0,00 € Abweichung.“

---

## Vorbereitung (vor dem Termin, T − 5 min)

```bash
# 1. Kompletten 8-Container-Stack im Unconstrained-Demo-Profil starten
#    (für die 1,5-vCPU-Demo stattdessen -f docker-compose.loadtest-1.5vcpu.yml verwenden)
export SERVER_PORT=8085 POSTGRES_HOST_PORT=5433
docker compose -f docker-compose.yml -f docker-compose.loadtest.yml up -d --wait

# 2. Drei Browser-Tabs vorab öffnen:
#    Tab 1: SPA       http://localhost:3000
#    Tab 2: Grafana   http://localhost:3001/d/ledger-stream-overview  (admin / change_me_local_grafana)
#    Tab 3: Keycloak  http://localhost:8180/admin/master/console/     (admin / change_me_local_admin)
```

---

## Die 9 Schritte (Ablauf, Klickpfade, Befehle & Erzählbogen)

### Schritt 1 — Systemüberblick & OIDC-PKCE-Login (00:00 – 01:00)

- **Klickpfad:** Tab 1 (`http://localhost:3000`) → Klick auf **„Anmelden (Keycloak)“** → Redirect auf `http://localhost:8180/realms/ledger-stream` (Authorization Code + PKCE `S256`, Public Client `ledger-stream-spa`).
- **Login:** Benutzer `writer` / Passwort `writer` (Rolle `ROLE_writer`).
- **Erzählbogen:** Kurz zeigen, dass das Frontend kein Client-Secret trägt, der JWT-Token im Resource-Server gegen JWKS + `aud=ledger-stream-api` geprüft wird und nur die Allowlist `{viewer, writer, operator}` gemappt wird (technische Keycloak-Rollen werden verworfen).

### Schritt 2 — Konto anlegen & erste Buchung (01:00 – 02:00)

- **Klickpfad (SPA):**
  1. **„Neues Konto“** → Nummer `DE-DEMO-001`, Währung `EUR` → **Anlegen**.
  2. Konto `DE-DEMO-001` öffnen → **„Neue Buchung“** → auf das sichtbare Feld **`Idempotency-Key`** zeigen.
  3. Typ `CREDIT`, Betrag `100.00`, Referenz `Ersteinlage` → **Buchen**.
- **Erzählbogen:** „Der `Idempotency-Key` entsteht beim *Öffnen* des Formulars, nicht im Submit-Handler. Kontoänderung, gecachte `201`-Antwort und Outbox-Event landen in **derselben** PostgreSQL-Transaktion.“

### Schritt 3 — Doppelklick-Demo *vor* dem Lastlauf (02:00 – 03:00)

- **Klickpfad (SPA):**
  1. Im Buchungsformular für `DE-DEMO-001` Betrag `25.00`, Referenz `Doppelklick-Test` eingeben.
  2. Klick auf **„Absichtlich doppelt senden (2× parallel)“** (Button wird **nicht** deaktiviert, zeigt `aria-busy="true"`).
  3. Live-Doppelklick-Protokoll in der UI zeigen: **2 parallele HTTP-Requests**, derselbe `Idempotency-Key`, beide `201 Created`, zweiter mit `Idempotency-Replayed: true`, **byte-gleicher Response-Body**, Saldo exakt `+25,00 €` (`125,00 €` statt `150,00 €`).
- **Erzählbogen:** „Bewusst *vor* dem 100k-Lauf im Kleinen sichtbar: selbst bei echtem Parallel-Klick entsteht nur eine Buchung und nur ein Outbox-Event.“

### Schritt 4 — `422`-Fall: Idempotenz vs. Deduplizierung (03:00 – 04:00)

- **Befehl (Terminal, nutzt denselben `Idempotency-Key` mit abweichendem Betrag):**
  ```bash
  WRITER_TOKEN=$(curl -fsS -X POST "http://localhost:8180/realms/ledger-stream/protocol/openid-connect/token" \
    -d "client_id=ledger-stream-cli&client_secret=change_me_local_cli_secret&grant_type=password&username=writer&password=writer" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

  ACCOUNT_ID=$(curl -fsS "http://127.0.0.1:8085/api/v1/accounts" \
    -H "Authorization: Bearer $WRITER_TOKEN" \
    | python3 -c 'import sys,json; print([a["id"] for a in json.load(sys.stdin) if a["accountNumber"]=="DE-DEMO-001"][0])')

  KEY="demo-422-$(date +%s)"

  # 1. Anfrage: 10.00 EUR -> 201 Created
  curl -i -s -X POST "http://127.0.0.1:8085/api/v1/accounts/${ACCOUNT_ID}/bookings" \
    -H "Authorization: Bearer $WRITER_TOKEN" -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${KEY}" \
    -d '{"type":"CREDIT","amount":"10.00","currency":"EUR","reference":"idemp-check"}' | head -n 10

  # 2. Anfrage: GLEICHER Key, ANDERER Betrag (99.00 EUR) -> 422 Unprocessable Entity (RFC 7807)
  curl -i -s -X POST "http://127.0.0.1:8085/api/v1/accounts/${ACCOUNT_ID}/bookings" \
    -H "Authorization: Bearer $WRITER_TOKEN" -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${KEY}" \
    -d '{"type":"CREDIT","amount":"99.00","currency":"EUR","reference":"idemp-check"}'
  ```
- **Erwartung:** `HTTP/1.1 422`, `type: urn:ledger-stream:error:idempotency-key-reuse`, Saldo bleibt unverändert (`135,00 €`).
- **Erzählbogen:** „Genau hier trennt sich **Idempotenz** von bloßer **Deduplizierung**: Wir hashen die kanonischen Geschäftsfelder (`request_fingerprint`, SHA-256). Gleicher Key + gleicher Inhalt liefert die gespeicherte Antwort; gleicher Key + anderer Betrag darf niemals stillschweigend die alte Antwort zurückgeben, sondern knallt sauber mit `422`.“

### Schritt 5 — `404`-vor-`401`-Invariante, Poison → DLQ & Replay als `operator` (04:00 – 06:00)

- **1. `404`-vor-`401` demonstrieren (im Default-/`prod`-Betrieb ist `LEDGER_ADMIN_ENABLED=false`):**
  ```bash
  # Ohne Token UND mit writer-Token -> immer 404 (verraet die Admin-Route nicht einmal)
  curl -i -s "http://127.0.0.1:8085/admin/dlq" | head -n 5
  ```
  - **Hinweis:** Für die Live-Replay-Demo lokal mit `LEDGER_ADMIN_ENABLED=true` starten (bzw. im `prod`-Overlay gemäß [`docs/operations.md` Abschnitt 2.6](operations.md#26-dlq-replay-im-prod-overlay-aktivieren) per GitOps-Wartungsfenster).
- **2. Poison-Event direkt in Kafka einspeisen & in `dlq_entries` sichtbar machen:**
  ```bash
  # Korruptes Nicht-Avro-Record direkt ins Topic ledger.events -> 0 Retries, landet sofort in DLQ
  printf 'corrupt-key:{"broken":true}\n' | docker exec -i ledger-stream-kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 \
    --topic ledger.events --property "parse.key=true" --property "key.separator=:"

  # Pruefen: Rohbytes erhalten (x-original-payload) & genau 1 offener DLQ-Eintrag
  docker exec ledger-stream-postgres psql -U ledger -d ledger -c \
    "SELECT id, error_class, attempt_count, replayed_at FROM dlq_entries ORDER BY created_at DESC LIMIT 3;"
  ```
- **3. RBAC-Nachweis (`writer` → `403`, `operator` → `200` + Replay-Audit-Trail):**
  ```bash
  # Mit writer-Token (bei LEDGER_ADMIN_ENABLED=true) -> 403 Forbidden
  curl -i -s "http://127.0.0.1:8085/admin/dlq" -H "Authorization: Bearer $WRITER_TOKEN" | head -n 5

  # Mit operator-Token -> 200 OK (Liste) & POST /admin/dlq/{id}/replay (Audit in dlq_replay_attempts)
  OP_TOKEN=$(curl -fsS -X POST "http://localhost:8180/realms/ledger-stream/protocol/openid-connect/token" \
    -d "client_id=ledger-stream-cli&client_secret=change_me_local_cli_secret&grant_type=password&username=operator&password=operator" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

  DLQ_ID=$(curl -fsS "http://127.0.0.1:8085/admin/dlq" -H "Authorization: Bearer $OP_TOKEN" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["id"])')

  curl -i -s -X POST "http://127.0.0.1:8085/admin/dlq/${DLQ_ID}/replay" \
    -H "Authorization: Bearer $OP_TOKEN" -H "Content-Type: application/json" \
    -d '{"note":"Live-Demo Replay"}' | head -n 10
  ```
- **Erzählbogen:** „Deserialisierungs- und fachliche Fehler bekommen 0 Retries — sie blockieren keine Partition, und `RawPayloadPreservingAvroDeserializer` rettet die exakten Rohbytes. Ein `writer` darf niemals DLQ-Payloads sehen (`403`), im `prod`-Overlay liefert `/admin/**` standardmäßig sogar `404` vor `401`.“

### Schritt 6 — 100.000-Buchungen-Lastlauf starten (06:00 – 06:30)

- **Befehl (Terminal, reproduzierbarer Seed `20261124`):**
  ```bash
  # Frischen Stack für saubere 100k-Invarianten starten (falls Schritte 2–5 auf demselben Stack liefen)
  java -Djdk.httpclient.keepalive.timeout=60 \
    -jar load-generator/target/load-generator-0.1.0-SNAPSHOT-shaded.jar \
    --bookings=100000 --duplicates=0.05 --poison=0.01 --seed=20261124 \
    --threads=32 --warmup-seconds=5 \
    --base-url=http://127.0.0.1:8085 \
    --kafka-bootstrap=127.0.0.1:9092 \
    --schema-registry-url=http://127.0.0.1:8081 \
    --db-url=jdbc:postgresql://127.0.0.1:5433/ledger \
    --db-user=ledger --db-password=ledger_local_dev \
    --prometheus-url=http://127.0.0.1:9090 \
    --oidc-token-url=http://localhost:8180/realms/ledger-stream/protocol/openid-connect/token \
    --oidc-client-id=ledger-stream-cli --oidc-client-secret=change_me_local_cli_secret \
    --oidc-username=writer --oidc-password=writer \
    --drain-timeout-seconds=1800
  ```

### Schritt 7 — Was du *sagst*, während die 100.000 Buchungen laufen (06:30 – 09:00 · bzw. ~20 min unter `1,5 vCPU`)

- **Klickpfad:** Sofort auf **Tab 2 (Grafana `http://localhost:3001/d/ledger-stream-overview`)** wechseln, Auto-Refresh auf **`5s`** stellen.
- **Konkrete Panels zeigen & erklären (in dieser Reihenfolge):**
  1. **Panel `Outbox Unsent` (`outbox_unsent`) & `Oldest Pending Age`:**
     - **Wörtlicher Erzählpunkt:** „Unconstrained ist der Schreibpfad 1,6× schneller als die Projektion (`1064,2 Buch./s` vs. `659,2 Events/s`). Die Outbox puffert die Differenz — `max(outbox_unsent) = 8.549` — und drainiert sie nach dem HTTP-Burst in `103,35 s` sauber auf `0`. Unter `1,5 vCPU` (~20 min Gesamtlauf) kehrt sich der Engpass um: Die API ist auf `84,8 Buch./s` gedrosselt, die Projektion kommt mit `249,8 Events/s` problemlos hinterher (`max 1.320`). Der Rückstand ist kein Defekt, sondern der sichtbare Beweis, dass Annahme und Verarbeitung entkoppelt sind: Die Differenz wird sichtbar statt zum Datenverlust.“
     - Auf die rote `300s`-Alarmschwelle in `outbox_oldest_pending_age_seconds` zeigen: „Solange das Alter weit unter `300 s` bleibt, arbeitet das Relay — alarmiert wird erst bei echtem Stillstand.“
  2. **Panel `HTTP Latency p95 / p99`:** Unterschied Client-Latenz vs. Server-Histogramm (`uri!~"/actuator.*"`) kurz benennen.
  3. **Panel `DLQ Unhandled` & `Consumer Poison vs. Duplicates`:** Zeigen, wie `consumer_poison_total` und `dlq_entries_unhandled` synchron auf `1.000` laufen, während `consumer_duplicates_rejected_total{reason="duplicate"}` auf **`0`** bleibt (weil alle `5.000` L2-Duplikate bereits an der HTTP-API abgefangen wurden).
  4. **Parallel im Terminal (wenn unter `1,5 vCPU` mehr Zeit ist):** Keycloak-Speicheraufschlüsselung aus [`README.md`](../README.md) ansprechen („Kern-Stack `1,14 GiB` `anon`, Keycloak als externer IdP weitere `473,82 MiB` = `28,9 %` → ehrlich `1,75 GiB` mit IdP“).

### Schritt 8 — Invarianten-Check & Ergebnisabnahme (09:00 – 10:30)

- **Blick ins Terminal (Ende des `load-generator`-Laufs):**
  - `INVARIANT_CHECK_RESULT=PASS (EXIT_CODE=0)`
  - Alle 5 Prüfungen auf `diff = 0`:
    1. `bookings` (regulär + Warmup) == akzeptierte HTTP-Anfragen (`100.000` regulär, `diff = 0`)
    2. `processed_events` == `bookings` (`diff = 0`)
    3. `L2-Replays`: `5.000/5.000` (`201` + `Idempotency-Replayed: true`, `0` Mismatch)
    4. `dlq_entries` == `consumer_poison_total` == `1.000` (`diff = 0`)
    5. 10/10 Stichprobenkonten: Summe in `bookings` == Saldo in `account_summary` (centgenau `0,00 EUR` Abweichung, deterministisch bei Seed `20261124`).

### Schritt 9 — GitOps, Supply Chain & Abschluss-Satz (10:30 – 12:00)

- **Kurz zeigen (Terminal / Browser):**
  - [`deploy/kustomize/overlays/prod/kustomization.yaml`](../deploy/kustomize/overlays/prod/kustomization.yaml): `LEDGER_ADMIN_ENABLED=false`, `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=FULL`, `app`-`RollingUpdate`, unveränderliche `@sha256:`-Digests (signiert mit Keyless-Cosign + CycloneDX-1.6-SBOM).
  - [`deploy/argocd/application.yaml`](../deploy/argocd/application.yaml): deklaratives Pull-GitOps (`selfHeal: true`, `prune: true`, Rollback per `git revert`).
- **Abschluss-Satz:**
  > „100.000 Buchungen akzeptiert, 5.000 echte Idempotenz-Replays ohne Zweiteffekt, 1.000 Poison-Events isoliert in der DLQ — die Summe im Ledger entspricht exakt den akzeptierten Anfragen, 0,00 EUR Abweichung, 10 von 10 Stichprobenkonten centgenau identisch über alle drei Lastprofile.“

---

## Offline-Fallback (falls der Live-Stack nicht startet)

Falls vor Ort kein Docker/Netzwerk verfügbar ist oder ein Container beim Aufbau hängt, liegen direkt im Repository zwei echte Aufzeichnungen aus dem Live-100k-Lauf (`Seed 20261124`, Unconstrained):

1. **Terminal-Aufzeichnung des vollständigen 100k-Laufs (`asciicast v2`):**
   - Datei: [`docs/demo/loadtest-100k.cast`](demo/loadtest-100k.cast)
   - Wiedergabe (mit doppelter Geschwindigkeit für zügige Demo):
     ```bash
     asciinema play -s 2 docs/demo/loadtest-100k.cast
     ```
2. **Statischer Grafana-Screenshot eines Live-Lastlaufs (`20.000` Buchungen, `1.000` L2-Duplikate, `200` Poison-Events — alle 8 Panels inkl. Outbox-Spike & Drain auf `0`, Durchsatz Outbox vs. Consumer, `duplicates = 0` vs. `poison = 200`, HTTP p95/p99):**
   - Datei: [`docs/demo/grafana-overview-loadtest.png`](demo/grafana-overview-loadtest.png)
   - Direkt im Browser/Image-Viewer öffnen und Schritt 7 anhand der realen Kurven erzählen.
