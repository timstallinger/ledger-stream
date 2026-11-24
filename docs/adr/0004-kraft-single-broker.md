# ADR-0004: Apache Kafka im KRaft-Single-Broker-Modus

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

`ledger-stream` benötigt Apache Kafka für die entkoppelte Ereignisverteilung (`ledger.events`, `ledger.events-retry-0..3`, `ledger.events.dlq`, je 6 Partitionen) sowie als Speicherbackend der Confluent Schema Registry (`_schemas`). Das gesamte System (8 Container) soll reproduzierbar lokal per Docker Compose und in einem Single-Node-k3s-Cluster innerhalb von ~1,5 GiB RAM laufen.

## Entscheidung

Wir betreiben **Apache Kafka 3.9.1 im KRaft-Modus** (ohne Apache ZooKeeper) als **Single-Node Combined Broker+Controller** (`KAFKA_PROCESS_ROLES=broker,controller`, `replication.factor=1`, `-Xms128m -Xmx192m`, `mem_limit: 448Mi`, 6 Partitionen).

## Alternativen

### 1. Klassisches Kafka + Apache ZooKeeper
- **Vorteil:** Historisch weit verbreitet.
- **Nachteil:** ZooKeeper ist ab Kafka 3.x abgekündigt und in Kafka 4.0 entfernt; erfordert einen zusätzlichen JVM-Container (~200–256 MiB RAM) ohne funktionalen Mehrwert.

### 2. Multi-Broker KRaft-Cluster (3 Broker, `replication.factor=3`, `min.insync.replicas=2`)
- **Vorteil:** Echte Hochverfügbarkeit bei Broker-Ausfall, Schutz gegen Datenverlust auf Disk-Ebene.
- **Nachteil:** 3 × ~230 MiB `anon` (~690 MiB allein für Kafka) sprengt das 1,5-GiB-Gesamtbudget vollständig und bietet auf einem Single-Node-k3s keine echte Ausfalldomänen-Trennung.

### 3. Redpanda (C++-Broker)
- **Vorteil:** Kein JVM-Overhead, schneller Start.
- **Nachteil:** Ziel des Referenzprojekts ist explizit Apache Kafka mit Confluent Schema Registry und Standard-Kafka-Client-Konfiguration.

## Konsequenzen

### Positiv
- Einsparung einer kompletten ZooKeeper-JVM (~200 MiB RAM) und schneller KRaft-Start.
- `ledger-stream-kafka` belegt im 100k-Peak nur `228,81 MiB` `anon` (+ `64,14 MiB` reclaimbarer Page-Cache).
- Identisches KRaft-Setup in Testcontainers, Docker Compose und k3s-StatefulSet.

### Negativ
- **Keine Hochverfügbarkeit (Single Point of Failure):** Fällt der eine Broker aus, stoppt die asynchrone Event-Verteilung und Projektion bis zum Neustart (HTTP-Buchungen laufen dank Transactional Outbox in PostgreSQL weiter).
- `acks=all` entspricht bei `replication.factor=1` effektiv `acks=1` (Bestätigung durch den einzigen Leader).

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit 3 Brokern + ZooKeeper:** Der Speicherverbrauch hätte > 2,2 GiB `anon` erreicht; ein lokaler Testlauf oder ein einzelner k3s-Node mit 1,5 GiB `ResourceQuota` wäre unmöglich gewesen.
- **Ohne explizites `publishNotReadyAddresses: true` im k3s-StatefulSet:** Der KRaft-Controller-Quorum-Voter hätte sich beim Erststart nicht über DNS selbst auflösen können (im Phase-11-Live-Cluster verifiziert und gefixt).
