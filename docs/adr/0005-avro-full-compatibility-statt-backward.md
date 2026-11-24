# ADR-0005: Avro + Schema Registry mit Kompatibilitätsstufe `FULL` statt `BACKWARD`

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

In `ledger-stream` publiziert das Outbox-Relay binär serialisierte Avro-Events (`LedgerBookingEvent`) auf `ledger.events`, während Consumer die Projektion `account_summary` fortschreiben. Bei einer Schema-Evolution (z. B. Einführung des Felds `initiationChannel` in v2) sollen Producer und Consumer **unabhängig voneinander** in beliebiger Reihenfolge ausgerollt oder zurückgerollt werden können (Rolling Deployment / Canary / GitOps-Rollback).

Zunächst war `BACKWARD` konfiguriert. `BACKWARD` garantiert jedoch **nur**, dass ein neuer Consumer (v2) alte Daten (v1) lesen kann — **nicht**, dass ein alter Consumer (v1) neue Daten (v2) lesen kann (das ist `FORWARD`). Dass ein neues Feld mit Default-Wert zufällig in beide Richtungen funktioniert, würde ohne Registry-Erzwingung bei späteren Änderungen (z. B. Löschen eines Pflichtfelds) unbemerkt gebrochen.

## Entscheidung

Wir setzen **Apache Avro** mit der **Confluent Schema Registry (`7.9.9`)** ein und konfigurieren die Kompatibilitätsstufe **`FULL`** (`SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=FULL`, in Compose, k3s-Overlay `prod` und `SchemaEvolutionIT` erzwungen). Jede neue Version muss sowohl `BACKWARD` als auch `FORWARD` zum Vorgänger kompatibel sein.

## Upgrade-Reihenfolge & Bruchregeln unter `FULL`

- **Erlaubte Upgrade-Reihenfolge:** **beliebig** — Producer zuerst, Consumer zuerst oder gemischter Rolling-Update-Betrieb inkl. sicherem Rollback auf v1.
- **Erlaubt:** Neues Feld **mit Default-Wert** hinzufügen; bestehendes Feld entfernen, **sofern es im Vorgänger einen Default hatte**.
- **Mit HTTP `409` abgelehnt:** Neues Pflichtfeld ohne Default (bricht `BACKWARD`), Löschen eines Pflichtfelds ohne Default (bricht `FORWARD`), inkompatible Typänderung (bricht beide).

## Alternativen

### 1. `BACKWARD`
- Erlaubt nur die Upgrade-Reihenfolge **Consumer vor Producer**; ein Producer-Vorlauf oder Consumer-Rollback bricht die Deserialisierung.

### 2. `FULL_TRANSITIVE`
- Prüft gegen **alle** historischen Versionen (`v1..v(n-1)`). Erst ab **drei koexistierenden Schema-Generationen** funktional anders als `FULL`; bei zwei Generationen (`v1` und `v2`) reicht `FULL`.

### 3. JSON ohne Schema Registry
- **Vorteil:** Keine zusätzliche Registry-JVM (~212 MiB `anon`).
- **Nachteil:** Kein maschineller Vertrags-Gate vor dem Publish; Breaking Changes fallen erst zur Laufzeit im Consumer als Poison-Events auf.

## Konsequenzen

### Positiv
- Rolling Upgrades und GitOps-Rollbacks von Producer und Consumer sind vollständig entkoppelt.
- `SchemaEvolutionIT` erzwingt im CI-Build gegen eine echte Schema Registry beide Leserichtungen (`v2` liest `v1`, `v1` liest `v2`), die `409`-Ablehnung inkompatibler Schemas und byte-gleiche Rohbytes beim DLQ-Replay.

### Negativ
- Strengere Evolution-Disziplin (Defaults verpflichtend, zweistufige Deprecation bei Feldlöschungen).
- Zusätzlicher Speicherbedarf der Schema Registry (`~212 MiB` `anon`).

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit `BACKWARD`:** Ein GitOps-Rollback des Consumers auf v1, während bereits v2-Events im Topic liegen, hätte zu `SerializationException` und DLQ-Überlauf geführt, sobald einmal ein Feld ohne Default entfernt worden wäre.
- **Ohne Schema Registry (reines JSON):** Ein versehentlich umbenanntes oder typgeändertes Feld wäre erst nach Deployment in Produktion im Consumer aufgefallen statt bereits vor dem Merge im CI-Gate (`SchemaEvolutionIT`).
