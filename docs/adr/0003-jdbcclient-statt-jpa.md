# ADR-0003: Spring `JdbcClient` mit explizitem SQL statt JPA/Hibernate

- **Status:** Akzeptiert
- **Datum:** 2026-11-23 (ergänzt 2026-11-24)

## Kontext

`ledger-stream` ist ein event-getriebenes Buchungssystem mit drei harten technischen Randbedingungen:

1. **PostgreSQL-spezifische Konsistenz- und Parallelitätsmechanismen:**
   - Transactional Outbox Relay mit `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 500` für nebenläufiges, blockierungsfreies Batch-Polling.
   - Idempotente Consumer-Deduplizierung über atomares `INSERT INTO processed_events ... ON CONFLICT (event_id) DO NOTHING`.
   - Explizites optimistisches Locking auf Konten (`UPDATE accounts SET balance = :balance, version = version + 1 WHERE id = :id AND version = :version`) sowie direkte Auswertung von PostgreSQL-Constraint-Verletzungen (`UNIQUE (account_id, idempotency_key)`, `CHECK (balance >= 0)`, `SQLSTATE 55P03` bei `lock_timeout = 3s`).
2. **Ressourcenbudget:** Der gesamte Stack (App, PostgreSQL 16, Kafka KRaft, Schema Registry, Keycloak 26, Frontend, Prometheus, Grafana) zielt auf ein enges Kubernetes-Namespace-Budget von **1,5 GiB RAM und 1,5 vCPU**.
3. **Durchsatz & Nachvollziehbarkeit:** Im 100.000-Buchungen-Lastnachweis soll jede SQL-Anweisung ohne ORM-Magie, Proxy-Fallen oder versteckte Queries lesbar und messbar bleiben.

## Entscheidung

Für `adapter-out-persistence` wird **Spring `JdbcClient`** in Verbindung mit explizitem PostgreSQL-SQL und **Flyway** für Schema-Migrationen eingesetzt. JPA und Hibernate werden per `maven-enforcer-plugin` und ArchUnit (`HexagonalArchitectureTest`) ausgeschlossen.

## Alternativen

### Spring Data JPA / Hibernate
**Gegenpunkt (explizit anerkannt):** JPA/Hibernate ist im Java-Enterprise-Umfeld der etablierte Standard. Für klassische Geschäftsdomänen mit tiefen Objektgraphen, dynamischen Suchabfragen, standardisiertem Dirty-Checking und schneller CRUD-Entwicklung bietet JPA erhebliche Produktivitätsvorteile und ist den meisten Teams unmittelbar vertraut.

Unsere Entscheidung gegen JPA ist daher **keine generelle Ablehnung von JPA, sondern strikt problemgetrieben**:
- `FOR UPDATE SKIP LOCKED` und `INSERT ... ON CONFLICT DO NOTHING` erfordern in JPA ohnehin native Queries und unterlaufen den Persistence Context (First-Level-Cache, Dirty-Checking, Flush-Reihenfolge).
- Im Hexagonal-Design müssten neben unveränderlichen Domänenobjekten separate mutable `@Entity`-Klassen inklusive Mapping-Schicht gepflegt werden.
- Hibernate erhöht den Basis-Heap- und Metaspace-Bedarf spürbar — relevant, da `app` bereits ohne Hibernate `73,9 MiB` Metaspace (17.952 Klassen) und `498 MiB` `anon` im 100k-Peak belegt (M3-NMT-Befund).

### jOOQ
- Typsicheres SQL-DSL mit hervorragender PostgreSQL-Unterstützung.
- Abgelehnt, weil Code-Generierung gegen eine laufende Datenbank den Build verkompliziert und die SQL-Mechanismen unmittelbar im Code sichtbar bleiben sollen.

## Konsequenzen

### Positiv
- Jede SQL-Anweisung (`FOR UPDATE SKIP LOCKED`, `ON CONFLICT DO NOTHING`, optimistisches Locking, `CHECK`-Constraints) ist explizit lesbar und direkt per `EXPLAIN ANALYZE` prüfbar.
- Keine versteckten N+1-Queries, keine `LazyInitializationException`, geringerer Metaspace- und Heap-Footprint.
- Saubere Trennung zwischen unveränderlichen Domänenobjekten (`domain`) und relationalem Schema (`Flyway`).

### Negativ
- SQL-Statements und Row-Mapper müssen manuell gepflegt werden; Tippfehler fallen erst in Testcontainers-Integrationstests gegen echtes PostgreSQL auf.
- Entwickler müssen PostgreSQL-SQLStates (`23505`, `23514`, `55P03`) explizit kennen und behandeln.

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit JPA/Hibernate:** Der zusätzliche Metaspace-/Proxy-Overhead (~40–70 MiB Non-Heap) hätte die ohnehin knappe `512 MiB`-Compose-Grenze (`507,27 MiB` Peak im 100k-Lauf) direkt in einen `OOMKill` getrieben. Zudem hätten native `ON CONFLICT DO NOTHING`-Statements neben EntityManager-Flush-Zyklen leicht zu subtilen Cache-Inkonsistenzen bei konkurrierenden Idempotenz-Anfragen geführt.
