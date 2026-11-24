# ADR-0001: Transactional Outbox statt 2PC/XA

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

Jede Buchung in `ledger-stream` muss sowohl im relationalen Ledger (`accounts`, `bookings`, `idempotency_keys` in PostgreSQL 16) persistiert als auch als Domain-Event auf Apache Kafka (`ledger.events`) veröffentlicht werden. Zwischen Datenbank und Broker liegt eine Netzwerkgrenze ohne gemeinsamen Transaktionskoordinator. Ein naives Dual-Write (`db.commit(); kafka.send();`) führt bei einem Absturz zwischen beiden Schritten entweder zu stillen Datenverlusten (Buchung in DB, kein Event) oder zu Phantom-Events (Event gesendet, DB-Commit gerollt).

## Entscheidung

Wir nutzen das **Transactional-Outbox-Pattern**: Buchung, Konto-Update, Idempotenz-Eintrag und der Eintrag in `outbox_events` werden in **einer einzigen lokalen PostgreSQL-Transaktion** (`PROPAGATION_REQUIRED` über den Port `TransactionRunner`) festgeschrieben. Ein asynchrones Relay (`OutboxRelayService` + `OutboxRelayScheduler`, Batch `500`, `FOR UPDATE SKIP LOCKED`, Poll `200 ms`) publiziert `PENDING`-Events mit Wave-basiertem Pipelining nach Kafka und markiert sie nach Broker-Ack als `SENT`.

## Alternativen

### 1. Zwei-Phasen-Commit (2PC / XA / JTA)
- **Vorteil:** Scheinbare atomare Transaktion über mehrere Ressourcenmanager.
- **Nachteil:** Apache Kafka unterstützt kein XA/JTA; 2PC hält Datenbank-Locks über Netzwerk-Roundtrips hinweg, blockiert bei Koordinator-Ausfall („in-doubt transactions“) und bricht unter Last den Durchsatz.

### 2. Change Data Capture (Debezium / PostgreSQL Logical Decoding)
- **Vorteil:** Kein Polling auf der DB, extrem niedrige Latenz durch Lesen des WAL.
- **Nachteil:** Erfordert Kafka Connect + Debezium-Cluster (zusätzliche JVM mit ≥ 384 MiB RAM — sprengt das 1,5-GiB-Budget) und versteckt die Outbox-Mechanik hinter Infrastruktur-Magie statt sie im Anwendungscode nachweisbar zu machen.

### 3. Naives Dual-Write mit `TransactionSynchronization.afterCommit()`
- **Vorteil:** Minimaler Code.
- **Nachteil:** Kein Schutz gegen Prozess-Kill (`SIGKILL`/OOM) nach DB-Commit und vor Kafka-Send; verlorene Events sind nicht automatisch reproduzierbar.

## Konsequenzen

### Positiv
- Garantierte Atomarität zwischen Buchung und Event-Entstehung ohne verteilte Locks.
- HTTP-Buchungen laufen auch bei komplett ausgefallenem Kafka-Broker fehlerfrei weiter (`outbox_events` puffert in PostgreSQL; verifiziert durch `TransactionalOutboxIT`).
- Vollständige Beobachtbarkeit über `outbox_unsent` und `outbox_oldest_pending_age_seconds`.

### Negativ
- At-least-once-Zustellung: Ein Absturz zwischen Kafka-Ack und `markSent` führt zu erneutem Publish (erfordert zwingend idempotente Consumer, siehe ADR-0002).
- `outbox_events` wächst dauerhaft ohne Retention-/Archivierungsjob (bewusst als offene Grenze dokumentiert).

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit 2PC/XA:** Technisch mit Kafka nicht umsetzbar; ein Ersatz über JTA-Bridges hätte Lock-Eskalationen und `503 LOCK_TIMEOUT` im 100k-Lasttest ausgelöst.
- **Mit `afterCommit()`:** Der Crash-Test (`TransactionalOutboxIT`) hätte stillen Event-Verlust erzeugt; die Read-Projektion `account_summary` wäre dauerhaft vom Hauptbuch (`accounts`) abgewichen.
- **Mit Debezium:** Das 1,5-GiB-Speicherbudget wäre bereits im Leerlauf durch Kafka Connect gerissen worden.
