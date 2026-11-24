# ADR-0006: Nicht-blockierende Retry-Topics (`@RetryableTopic`) statt `pause()`/`resume()` oder blockierendem Backoff

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

Bei der Verarbeitung von `ledger.events` treten zwei grundverschiedene Fehlerklassen auf:
1. **Nicht-wiederholbare Fehler (Poison):** Deserialisierungsfehler (`RawPayloadPreservingAvroDeserializer`) sowie fachliche Fehler (`unknown_account`, `non_positive_amount`, `invalid_booking_type`). Retries sind hier sinnlos.
2. **Transiente technische Fehler:** Kurzzeitige DB-Verbindungsabbrüche, Lock-Timeouts oder Deadlocks. Diese erfordern gestaffelte Wiederholungsversuche mit exponentiellem Backoff und Jitter, bevor die Nachricht in `ledger.events.dlq` und `dlq_entries` landet.

Entscheidend ist: Ein fehlerhaftes Event auf Partition `P` darf **nicht** alle nachfolgenden Events anderer Konten auf derselben Partition blockieren (Head-of-Line-Blocking) und darf `max.poll.interval.ms` (`300.000 ms`) nicht verletzen.

## Entscheidung

Wir nutzen **Spring Kafka `@RetryableTopic`** mit **5 Versuchen** (Initial + 4 Retry-Topics `ledger.events-retry-0..3`, exponentieller Backoff `1s..30s` mit Jitter `random=true`, danach `ledger.events.dlq`). `NonRetryableEventException` ist über `exclude` ausgenommen und geht mit **0 Retries** direkt in die DLQ.

### Bewusste CPU-Abwägung (F4): 5 vs. 2 Retry-Stufen
Bei `6` Partitionen und `KAFKA_CONSUMER_CONCURRENCY=6` erzeugt `@RetryableTopic` (1 Haupt-Topic + 4 Retry-Topics + 1 DLQ = 6 Topics × 6 Threads) insgesamt **36 Consumer-Threads** (von insgesamt 178 JVM-Threads laut M3-NMT). Der reine Speicheranteil dieser 36 Threads ist gering (`~3,7 MiB` von `18,3 MiB` Thread-Stack committed), der reale Hebel liegt bei **CPU-Polling-Overhead unter engen vCPU-Limits (`0,50–0,60 vCPU`)**:
- **Option 2 Retry-Stufen (`attempts=3` → 18 statt 36 Consumer-Threads):** Halbiert den Leerlauf-Poll-Overhead bei stark gedrosselten CPU-Caps, verkürzt aber das Retry-Zeitfenster auf ~3–5 Sekunden.
- **Unsere Entscheidung (5 Versuche / 4 Retry-Stufen):** Wir bleiben bewusst bei 5 Versuchen (~30–45 s Gesamtfenster mit Jitter), weil dies typische PostgreSQL-Verbindungs-Resets und kurzzeitige Lock-Spitzen ohne DLQ-Eskalation abfängt (`DlqAndRetryIT`) und die CPU-Last im 1,5-vCPU-Lauf dennoch beherrschbar bleibt (bei extrem CPU-knappen Deployments lässt sich die Stufenzahl über `KAFKA_RETRY_MAX_ATTEMPTS` ohne Code-Änderung reduzieren).

## Alternativen

### 1. Blockierender `DefaultErrorHandler` + `ExponentialBackOff`
- **Vorteil:** Minimale Topic- und Thread-Anzahl; erhält strikte Partitionsreihenfolge.
- **Nachteil:** Schläft direkt im Consumer-Thread → blockiert alle unbeteiligten Konten derselben Partition; bei längeren Backoffs droht Überschreiten von `max.poll.interval.ms` und damit eine Rebalance-Kaskade.

### 2. Manuelles `pause()` / `resume()` pro Partition
- **Vorteil:** Kein Blockieren des `poll()`-Loops (verhindert Rebalance-Timeouts), erhält Partitionsreihenfolge.
- **Nachteil:** Stoppt weiterhin die gesamte Partition für alle Konten (Head-of-Line-Blocking) und erfordert fehleranfällige Zustandslogik für Pause-Timer und Rebalance-Callbacks.

## Konsequenzen

### Positiv
- Hauptpartitionen laufen auch bei dauerhaften technischen Fehlern einzelner Records verzögerungsfrei weiter (`DlqAndRetryIT` weist `< 2s` Durchlauf nachfolgender Events nach).
- Klare Trennung von Poison (`consumer_poison_total`, 0 Retries) und transienten Fehlern (`consumer_retries_total`, bis zu 5 Versuche).

### Negativ
- **Preis: Verlust der strikten Reihenfolge pro Konto im Fehlerfall** (ein nachfolgendes `BookingCancelled` auf demselben Konto kann ein fehlgeschlagenes `BookingCreated` im Retry-Topic überholen; ausführliche Analyse und Begründung in [`docs/design-decisions.md`](../design-decisions.md)).
- Zusätzliche Topics (`-retry-0..3`) und Consumer-Threads (36 bei 6 Partitionen).

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit `DefaultErrorHandler` + `ExponentialBackOff`:** Der Nicht-Blockierungs-Integrationstest (`DlqAndRetryIT`) wäre fehlgeschlagen, weil ein fehlerhaftes Event mit ~30 s Backoff alle regulären Buchungen derselben Partition blockiert hätte.
- **Ohne `exclude = NonRetryableEventException.class`:** Die 1.000 Poison-Events im 100k-Lastlauf hätten unnötig 4.000 Retry-Zyklen (~30–45 s Verzögerung) durchlaufen, bevor sie in `dlq_entries` sichtbar geworden wären.
