# Design-Entscheidungen & bewusste Grenzen (`ledger-stream`)

Dieses Dokument hält zentrale Architektur-Trade-offs in Prosa fest — insbesondere dort, wo Verteiltheit, Verfügbarkeit und Striktheit gegeneinander abgewogen werden müssen.

---

## 1. Reihenfolge pro Konto unter nicht-blockierenden Retry-Topics

### Das konkrete Szenario

Im Outbox-Relay (Producer-Seite) erzwingen wir strikte Reihenfolge pro Konto (`accountId` als Kafka-Record-Key + Per-Account-Head-of-Line-Schutz im Relay). Auf der **Consumer-Seite** setzen wir jedoch bewusst **nicht-blockierende Retry-Topics** (`@RetryableTopic`: `ledger.events-retry-0..3` → `ledger.events.dlq`) ein.

Diese Entscheidung hat eine explizite Konsequenz für die Reihenfolge **pro Konto im Fehlerfall**:

1. Event 1 für Konto `A` (`BookingCreated`) schlägt im Consumer technisch fehl (z. B. kurzzeitiger DB-Lock-Timeout) und wird sofort auf `ledger.events-retry-0` ausgelagert; der Offset auf der Hauptpartition rückt weiter.
2. Event 2 für dasselbe Konto `A` (`BookingCancelled` auf Buchung 1) trifft unmittelbar danach auf der Hauptpartition ein und wird verarbeitet, **bevor** Event 1 aus dem Retry-Topic zurückkehrt.
3. Prüft eine nachgelagerte Konsistenzregel die Existenz der Originalbuchung, schlägt Event 2 fachlich mit „unbekannte Buchung“ fehl und landet in der DLQ (`dlq_entries`).

### Bewertete Alternativen

| Alternative | Vorteil | Nachteil |
|---|---|---|
| **1. Akzeptieren + DLQ-Replay (gewählt)** | Hauptpartition blockiert nie (`< 2s`), keine `max.poll.interval.ms`-Timeouts/Rebalances, volle Durchsatz-Isolation zwischen Konten. Nach erfolgreichem Retry von Event 1 lässt sich Event 2 idempotent aus `dlq_entries` replayen. | Bei einem technischen Fehler auf Konto `A` können Folge-Events desselben Kontos überholen und bis zum Replay in der DLQ landen. |
| **2. Partition pausieren (`pause()`/`resume()`)** | Strikte Reihenfolge innerhalb der gesamten Partition bleibt auch bei Fehlern erhalten. | Ein einziges fehlerhaftes Event blockiert **alle** Konten auf derselben Partition (Head-of-Line-Blocking); komplexe Offset-/Rebalance-Steuerung und direkte SLA-Verletzung für unbeteiligte Kunden. |
| **3. Kompensation als `pending_cancel` (Out-of-Order-Puffer)** | Fängt vertauschte Events lokal in einer eigenen Puffer-Tabelle ab und wendet sie an, sobald das fehlende Event eintrifft. | Vervielfacht die Zustandsautomaten-Komplexität in jedem einzelnen Consumer (Timeouts, Bereinigung verwaister `pending_cancel`-Zeilen, zusätzliche Reconciliation). |

### Begründung

Für ein kommutatives Saldo-Read-Model (`account_summary`, Addition/Subtraktion von Beträgen pro Event mit `UNIQUE(event_id)`) wiegt **Blockierungsfreiheit aller Konten** schwerer als das Verhindern extrem seltener Out-of-Order-Stornos während eines transienten DB-Fehlers. Fachliche Inkonsistenzen werden nicht verschluckt, sondern sichtbar in `dlq_entries` gesichert (`dlq_entries_unhandled > 0`) und sind nach Behebung der Ursache über `POST /admin/dlq/{id}/replay` verlustfrei und dank `processed_events` duplikatfrei nachholbar.

---

## 2. Was `ledger-stream` bewusst **nicht** löst

- **Kein verteiltes Multi-Region-Active-Active:** Single-Region PostgreSQL + Single-Broker Kafka KRaft (kein `replication.factor = 3`, kein Cross-DC-Failover).
- **Keine doppelte Buchführung über Gegenkonten (Double-Entry Transfer):** Buchungen erfolgen kontobezogen (`DEBIT`/`CREDIT`), kein verteiltes 2-Konten-Transfer-Saga-Protokoll.
- **Keine Fremdwährungs-Umrechnung (FX):** Jedes Konto führt genau eine ISO-4217-Währung; Währungsabweichungen werden abgelehnt (`422`).
- **Kein automatisches Multi-Instanz-Per-Account-Ordering im Outbox-Relay:** `FOR UPDATE SKIP LOCKED` verhindert Doppelverarbeitung über mehrere Relay-Instanzen, erzwingt strikte Konto-Reihenfolge aber nur pro Instanz (bei Skalierung > 1 Relay-Pod ist Partitionierung nach `accountId` oder Single-Leader-Relay erforderlich).
