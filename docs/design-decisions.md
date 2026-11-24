# Design-Entscheidungen & bewusste Grenzen (`ledger-stream`)

Dieses Dokument hält zentrale Architektur-Trade-offs in Prosa fest — insbesondere dort, wo Verteiltheit, Verfügbarkeit, Ressourcenbudget und Striktheit gegeneinander abgewogen werden müssen. Die einzelnen Architekturentscheidungen im ADR-Format liegen unter [`docs/adr/`](adr/).

---

## 1. Reihenfolge pro Konto unter nicht-blockierenden Retry-Topics

### Das konkrete Szenario

Auf der Producer-Seite (Outbox-Relay) erzwingt `ledger-stream` strikte Reihenfolge pro Konto (`accountId` als Kafka-Record-Key + Wave-Pipelining mit max. 1 Event pro `aggregateId` je Wave sowie Account-Blocking im Fehlerfall). Auf der **Consumer-Seite** setzen wir jedoch bewusst **nicht-blockierende Retry-Topics** (`@RetryableTopic`: `ledger.events-retry-0..3` → `ledger.events.dlq`) ein.

Diese Entscheidung hat eine explizite Konsequenz für die Reihenfolge **pro Konto im Fehlerfall**:

1. **Event 1** für Konto `A` (`BookingCreated`, Buchung `B1`) schlägt im Consumer **technisch** fehl (z. B. kurzzeitiger DB-Lock-Timeout oder Verbindungsabbruch) und wird sofort auf `ledger.events-retry-0` ausgelagert; der Offset auf der Hauptpartition rückt weiter.
2. **Event 2** für dasselbe Konto `A` (`BookingCancelled` auf Buchung `B1`) trifft unmittelbar danach auf der Hauptpartition ein und wird verarbeitet, **bevor** Event 1 aus dem Retry-Topic zurückkehrt.
3. Prüft eine nachgelagerte Konsistenzregel die Existenz der Originalbuchung `B1`, schlägt Event 2 fachlich mit **„unbekannte Buchung“** fehl (`NonRetryableEventException`) und landet mit `0` Retries in `ledger.events.dlq` / `dlq_entries`.

### Drei bewertete Alternativen

| Alternative | Vorteil | Nachteil |
|---|---|---|
| **1. Akzeptieren + DLQ-Replay (gewählt)** | Hauptpartition blockiert nie (`< 2s`, verifiziert in `DlqAndRetryIT`), keine `max.poll.interval.ms`-Rebalances, volle Durchsatz-Isolation zwischen unbeteiligten Konten. Nach erfolgreichem Retry von Event 1 lässt sich Event 2 idempotent aus `dlq_entries` replayen. | Bei einem technischen Fehler auf Konto `A` können Folge-Events desselben Kontos überholen und bis zum manuellen Replay in der DLQ landen. |
| **2. Partition pausieren (`pause()` / `resume()`)** | Strikte Reihenfolge innerhalb der gesamten Partition bleibt auch im Fehlerfall erhalten. | Ein einziges fehlerhaftes Event blockiert **alle** Konten auf derselben Partition (Head-of-Line-Blocking); komplexe Offset-/Rebalance-Steuerung und direkte Latenz-Verletzung für unbeteiligte Konten. |
| **3. Out-of-Order-Puffer (`pending_cancel`-Tabelle)** | Fängt vertauschte Storno-Events lokal in einer Puffer-Tabelle ab und wendet sie an, sobald das fehlende `BookingCreated`-Event eintrifft. | Vervielfacht die Zustandsautomaten-Komplexität in jedem einzelnen Consumer (Timeouts, Bereinigung verwaister Puffer-Zeilen, zusätzliche Reconciliation). |

### Warum wir diesen Trade-off akzeptieren

Die Read-Projektion `account_summary` ist für reine `CREDIT`/`DEBIT`-Deltas additiv kommutativ (in Kombination mit `UNIQUE(event_id)` auf `processed_events`). **Blockierungsfreiheit aller übrigen Konten** wiegt schwerer als das Abfangen extrem seltener Out-of-Order-Stornos während einer transienten DB-Störung. Fachliche Inkonsistenzen werden niemals still verworfen, sondern sichtbar über `dlq_entries_unhandled > 0` alarmiert und über `POST /admin/dlq/{id}/replay` verlustfrei nachgeholt.

### CPU-Trade-off der Retry-Stufen (F4)

Bei `6` Partitionen und `KAFKA_CONSUMER_CONCURRENCY=6` starten durch `@RetryableTopic` (`attempts=5`: 1 Haupt-Topic + 4 Retry-Topics + 1 DLQ) insgesamt **36 Consumer-Threads** (von 178 JVM-Threads). Der Speicheranteil liegt laut `jcmd VM.native_memory summary` lediglich bei ca. `3,7 MiB` (bei `18,3 MiB` Gesamt-Thread-Stack), der eigentliche Hebel ist die **CPU-Poll-Last** bei engen vCPU-Limits (`0,50–0,60 vCPU`). Eine Reduktion auf **2 Retry-Stufen (`attempts=3` → 18 Threads)** halbiert den Poll-Overhead, verkürzt aber das automatische Heilungsfenster auf ~3–5 s. Wir bleiben bewusst bei 5 Versuchen (~30–45 s Gesamtfenster) als belastbarem Default und machen die Stufenzahl über `KAFKA_RETRY_MAX_ATTEMPTS` konfigurierbar (siehe [ADR-0006](adr/0006-retry-topics-statt-pause-resume.md)).

---

## 2. Projektion als Funktion der Events (Rebuild) — und wo das endet

Grundsätzlich gilt für die Read-Seite in `ledger-stream`:

```text
account_summary = fold(initialState, ledger.events[0..N])  (dedupliziert nach event_id)
```

Ein vollständiger **Rebuild** der Read-Projektion ist möglich, indem `account_summary` und `processed_events` geleert werden und eine neue Consumer-Group ab Offset `0` über `ledger.events` liest (`auto-offset-reset=earliest`). Dank `FULL`-Kompatibilität in der Schema Registry ([ADR-0005](adr/0005-avro-full-compatibility-statt-backward.md)) dekodiert ein v2-Consumer dabei auch ältere v1-Records verlustfrei.

**Wo Event-Rebuild als reines Funktionsmodell endet:**

1. **Topic-Retention (Standard 7 Tage / Größenlimit):** Sobald Kafka alte Segmente löscht, ist `ledger.events` kein vollständiges historisches Log mehr. Ohne `cleanup.policy=compact` (was bei Buchungs-Events mit gleichem `accountId`-Key ältere Buchungen überschreiben würde!) oder eine separate Snapshot-/Cold-Storage-Archivierung kann nur noch aus PostgreSQL (`bookings`) neu aufgebaut werden.
2. **Stammdaten-Abhängigkeit (`accounts`):** `ProjectLedgerEventService` liest beim Projizieren den Kontodatensatz (`accountNumber`, `currency`) aus `accounts`. Existiert ein Konto nicht (oder wurde seine Währung/Nummer historisch verändert), ist die Projektion keine rein geschlossene Funktion des Event-Streams allein.
3. **Zeitpunkt-abhängige Seiteneffekte & externe Aufrufe:** Sobald ein Downstream-Consumer E-Mails versendet, Webhooks auslöst oder externe Abrechnungen anstößt, darf ein Rebuild diese Seiteneffekte nicht blind erneut ausführen.
4. **Inkompatible Schema-Wechsel jenseits von 2 Generationen:** Solange nur `FULL` (und noch nicht `FULL_TRANSITIVE`) konfiguriert ist, gilt die Kompatibilitätsgarantie formal nur zwischen `v(n)` und `v(n-1)` — bei ≥ 3 historischen Generationen im Topic muss vorab auf `FULL_TRANSITIVE` umgestellt werden.

---

## 3. Was `ledger-stream` bewusst **nicht** löst

- **Keine Hochverfügbarkeit (kein HA):** Single-Instance PostgreSQL 16, Single-Broker Apache Kafka KRaft (`replication.factor=1`, [ADR-0004](adr/0004-kraft-single-broker.md)), Single-Replica App/Keycloak/Schema-Registry auf Single-Node k3s. Broker- oder DB-Ausfall führt zu temporärer Nichtverfügbarkeit der abhängigen Schicht (siehe Runbook [`docs/operations.md`](operations.md)).
- **Keine verteilte Transaktion mit Fremdsystemen:** Es gibt kein 2PC/XA und kein orchestriertes Cross-Service-Saga-Protokoll für externe Zahlungsnetzwerke oder Drittbanken ([ADR-0001](adr/0001-transactional-outbox-statt-2pc-xa.md)).
- **Keine Outbox- und `processed_events`-Retention:** Weder `outbox_events` (Status `SENT`) noch `processed_events` werden automatisch archiviert oder partitioniert/bereinigt. Im mehrjährigen Produktivbetrieb wären monatliche PostgreSQL-Partitionen (`RANGE (created_at)`) und Archivierungsjobs erforderlich.
- **DLQ-Bearbeitung ist bewusst manuell:** Es gibt kein automatisches zeitgesteuertes Blind-Replay aus `dlq_entries`. Ein menschlicher `operator` muss Ursache und Fehlerklasse prüfen, bevor `POST /admin/dlq/{id}/replay` ausgelöst wird (Schutz gegen Endlosschleifen bei fachlichem Poison).
- **Keine doppelte Buchführung über Gegenkonten / kein FX:** Buchungen wirken kontobezogen (`CREDIT`/`DEBIT`) in einer festen ISO-4217-Währung pro Konto; kein 2-Konten-Transfer mit Währungsumrechnung.
- **Keine Multi-Instanz-Per-Account-Reihenfolge im Outbox-Relay:** `FOR UPDATE SKIP LOCKED` verhindert Doppelverarbeitung bei mehreren Relay-Instanzen, garantiert strikte Konto-Reihenfolge aber nur pro Instanz. Skalierung > 1 Relay-Pod erfordert Hash-Partitionierung nach `accountId` oder Single-Leader-Execution.
