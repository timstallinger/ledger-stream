# ADR-0002: Avro-Kompatibilitätsstufe `FULL` statt `BACKWARD`

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

In `ledger-stream` publiziert das Outbox-Relay Avro-Events auf das Topic `ledger.events`, während ein oder mehrere Consumer die Read-Projektion `account_summary` fortschreiben. Bei einer Schema-Evolution (z. B. Einführung des Felds `initiationChannel` in v2) sollen Producer und Consumer **unabhängig voneinander** in beliebiger Reihenfolge ausgerollt werden können (Rolling Deployment / Canary / Rollback).

Zunächst war in der Confluent Schema Registry `BACKWARD` konfiguriert. `BACKWARD` garantiert jedoch **nur**, dass ein neuer Consumer (v2) alte Daten (v1) lesen kann — **nicht**, dass ein alter Consumer (v1) neue Daten (v2) lesen kann (das ist `FORWARD`). Dass ein neues Feld mit Default-Wert zufällig in beide Richtungen kompatibel ist, würde ohne explizite Registry-Durchsetzung bei späteren Änderungen (z. B. Löschen eines Pflichtfelds ohne Default) unbemerkt gebrochen werden.

## Entscheidung

Die Schema Registry wird auf **`FULL`** konfiguriert (`SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=FULL`), sodass jede neue Schema-Version sowohl **`BACKWARD`** als auch **`FORWARD`** zum Vorgänger kompatibel sein muss.

## Upgrade-Reihenfolge & Bruchregeln unter `FULL`

- **Erlaubte Upgrade-Reihenfolge unter `FULL`:** **beliebig** — Producer zuerst (v2 schreibt, v1-Consumer liest weiter), Consumer zuerst (v2-Consumer liest v1-Daten mit Default) oder gemischter Rolling-Update-Betrieb inklusive sicherem Rollback auf v1.
- **Erlaubte Schema-Änderungen unter `FULL`:**
  - Neues Feld **mit Default-Wert** hinzufügen (v2-Reader nutzt den Default für v1-Daten; v1-Reader ignoriert das neue Feld).
  - Bestehendes Feld entfernen, **sofern es im Vorgängerschema einen Default-Wert hatte**.
- **In welcher Richtung gebrochen wird (von der Registry mit HTTP `409` abgelehnt):**
  - Neues Pflichtfeld **ohne Default** → bricht **`BACKWARD`** (v2-Reader kann v1-Daten nicht dekodieren).
  - Pflichtfeld **ohne Default** löschen → bricht **`FORWARD`** (v1-Reader erwartet das Feld in v2-Daten).
  - Feldtyp inkompatibel ändern (z. B. `string` → `int`) → bricht **beide** Richtungen.

## Alternativen

### `BACKWARD`
- Erlaubt nur die Upgrade-Reihenfolge **Consumer vor Producer**. Ein Rollback des Consumers oder ein vorauseilender Producer-Rollout führt zu Deserialisierungsfehlern.

### `FORWARD`
- Erlaubt nur die Upgrade-Reihenfolge **Producer vor Consumer**. Beim Nachlesen älterer Offsets oder Rebuild aus dem Topic ab Offset `0` drohen Fehler für neuere Consumer.

### `FULL_TRANSITIVE`
- Prüft Kompatibilität nicht nur gegen die unmittelbar vorherige Version (`v(n-1)`), sondern gegen **alle** jemals registrierten Versionen (`v1..v(n-1)`).
- **Bewusst erst ab drei koexistierenden Schema-Generationen erforderlich:** Bei zwei aufeinanderfolgenden Generationen (`v1` und `v2`) sind `FULL` und `FULL_TRANSITIVE` funktional identisch. Erst wenn drei oder mehr Generationen gleichzeitig im Topic (z. B. kompaktiertes Topic mit alter Retention) oder in aktiven Consumern leben müssen, verhindert `FULL_TRANSITIVE` das schleichende Wiederverwenden oder schrittweise Entfernen von Feldern über mehrere Generationen hinweg.

## Konsequenzen

### Positiv
- Rolling Upgrades und Rollbacks von Producer und Consumer sind vollständig entkoppelt.
- Die Registry erzwingt mechanisch (und in CI getestet), dass weder `BACKWARD` noch `FORWARD` versehentlich verletzt wird.

### Negativ
- Strengere Evolution-Disziplin: Jedes neue Feld erfordert einen sinnvollen Default-Wert; echte Feld-Entfernungen erfordern ein zweistufiges Deprecation-Verfahren.
