# ADR-0001: Spring `JdbcClient` mit explizitem SQL statt JPA/Hibernate

- **Status:** Akzeptiert
- **Datum:** 2026-11-23

## Kontext

`ledger-stream` ist ein event-getriebenes Buchungssystem mit drei harten technischen Randbedingungen:

1. **PostgreSQL-spezifische Konsistenz- und Parallelitätsmechanismen:**
   - Transactional Outbox Relay mit `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 500` für nebenläufiges, blockierungsfreies Batch-Polling.
   - Idempotente Consumer-Deduplizierung über atomares `INSERT INTO processed_events ... ON CONFLICT (event_id) DO NOTHING`.
   - Explizites optimistisches Locking auf Konten (`UPDATE accounts SET balance = :balance, version = version + 1 WHERE id = :id AND version = :version`) sowie direkte Auswertung von PostgreSQL-Constraint-Verletzungen (`UNIQUE (account_id, idempotency_key)`, `CHECK (balance >= 0)`).
2. **Ressourcenbudget:** Der gesamte Stack (App, PostgreSQL 16, Kafka KRaft, Schema Registry, Keycloak 26, Frontend) muss in einem Kubernetes-Namespace mit **1,5 GB RAM und 1,5 vCPU** laufen.
3. **Durchsatz & Nachvollziehbarkeit:** Der Lastnachweis umfasst 100.000 Buchungen; der Code soll ohne ORM-Magie, Proxy-Fallen oder versteckte Queries lesbar sein.

## Entscheidung

Für `adapter-out-persistence` wird **Spring `JdbcClient`** in Verbindung mit explizitem PostgreSQL-SQL und **Flyway** für Schema-Migrationen eingesetzt. JPA und Hibernate werden per `maven-enforcer-plugin` und ArchUnit ausgeschlossen.

## Alternativen

### Spring Data JPA / Hibernate

**Gegenpunkt (explizit anerkannt):** JPA/Hibernate ist im Java-Enterprise-Umfeld der etablierte Standard. Für klassische Geschäftsdomänen mit tiefen Objektgraphen, dynamischen Suchabfragen, standardisiertem Dirty-Checking und schneller CRUD-Entwicklung bietet JPA erhebliche Produktivitätsvorteile und ist den meisten Teams unmittelbar vertraut.

Unsere Entscheidung gegen JPA ist daher **keine generelle Ablehnung von JPA, sondern strikt problemgetrieben**:

- `FOR UPDATE SKIP LOCKED` und `INSERT ... ON CONFLICT DO NOTHING` erfordern in JPA ohnehin native Queries und unterlaufen den Persistence Context (First-Level-Cache, Dirty-Checking, Flush-Reihenfolge).
- Im Hexagonal-Design müssten neben unveränderlichen Domänenobjekten separate mutable `@Entity`-Klassen inklusive Mapping-Schicht gepflegt werden, was Komplexität ohne fachlichen Gewinn erzeugt.
- Hibernate erhöht den Basis-Heap- und Metaspace-Bedarf (Metamodel, ByteBuddy-Proxies, Query-Plan-Cache) spürbar — relevant bei einem engen Speicherbudget von 1,5 GB für den Gesamtstack.
- Bei 100.000 Buchungen im Lasttest vermeidet explizites JDBC den Overhead von Entity-State-Snapshots und Dirty-Checking.

### jOOQ

- Typsicheres SQL-DSL mit hervorragender PostgreSQL-Unterstützung.
- Abgelehnt, weil Code-Generierung gegen eine laufende Datenbank den Build-Prozess verkompliziert und das Projekt die zugrunde liegenden SQL-Mechanismen unmittelbar sichtbar halten soll.

## Konsequenzen

### Positiv

- Jede SQL-Anweisung (`FOR UPDATE SKIP LOCKED`, `ON CONFLICT DO NOTHING`, optimistisches Locking, `CHECK`-Constraints) ist explizit im Code lesbar und direkt im `EXPLAIN ANALYZE` prüfbar.
- Keine versteckten N+1-Queries, kein `LazyInitializationException`, kein Persistence-Context-Flushing.
- Geringerer RAM-Bedarf und schnellerer Anwendungsstart; hilft das 1,5-GB-Namespace-Budget einzuhalten.
- Saubere Trennung zwischen unveränderlichen Domänenobjekten (`domain`) und relationalem Schema (`Flyway`).

### Negativ

- SQL-Statements und Row-Mapper müssen manuell gepflegt werden; Tippfehler in SQL-Strings fallen erst in Integrationstests (Testcontainers gegen echtes PostgreSQL) auf, nicht zur Compile-Zeit.
- Entwickler, die ausschließlich Spring Data JPA gewohnt sind, müssen SQL-Dialekt-Details (z. B. PostgreSQL SQLStates `23505`, `23514`) explizit verstehen und testen.
