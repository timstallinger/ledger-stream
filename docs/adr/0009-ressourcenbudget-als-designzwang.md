# ADR-0009: Ressourcenbudget 1,5 GiB / 1,5 vCPU als Designzwang — mit ehrlichem Messergebnis

- **Status:** Akzeptiert (RAM-Ziel auf reale `1792Mi` `requests.memory` korrigiert)
- **Datum:** 2026-11-24

## Kontext

Zu Projektbeginn wurde ein hartes Ressourcenbudget von **1,5 GiB RAM (`1536 MiB`) und 1,5 vCPU (`1500m`)** für den gesamten Stack als bewusster Designzwang gesetzt, um Architektur-Aufblähung (kein Debezium/Kafka Connect, kein ZooKeeper, kein Hibernate/JPA, kein Multi-Broker-Setup) konsequent zu verhindern.

In Phase 10 und 11 wurde dieses Ziel gegen echte 100.000-Buchungen-Lastläufe über `/sys/fs/cgroup/memory.stat` (`anon`, `file`, `kernel`/`slab`) sowie `jcmd 1 VM.native_memory summary` geprüft.

## Entscheidung & ehrliches Ergebnis

Der Designzwang hat seinen architektonischen Zweck erfüllt (schlanker Stack ohne ORM/ZooKeeper/Connect), **die reale 100k-Messung widerlegt jedoch die Annahme, dass alle 8 Container (4 JVMs: App, Keycloak 26, Kafka KRaft, Schema Registry + PostgreSQL 16 + Nginx + Prometheus + Grafana) unter gleichzeitig 1,5 vCPU und 1,5 GiB `anon` laufen**:

| Lauf | CPU-Budget | `anon`-Summe (8 Container) | `file` (reclaimbar) | `kernel`/Rest | `memory.current` Peak | Ergebnis vs. `1536 MiB` |
|---|---|---:|---:|---:|---:|---|
| **Unconstrained** | entdrosselt | `1515,87 MiB` | `333,51 MiB` | `59,32 MiB` | `1908,70 MiB` | `anon` hält (`+20,13 MiB`), CPU nicht |
| **1,5 vCPU (initial, KC 0.10)** | `1,50 vCPU` | `1662,00 MiB` | `280,15 MiB` | `59,27 MiB` | `2001,42 MiB` | CPU hält, `anon` `-126,00 MiB` (KC GC-Rückstau `508,87 MiB`) |
| **1,5 vCPU (F1 rebalanced, KC 0.30 / `-Xmx224m`)** | `1,50 vCPU` | **`1637,44 MiB`** | `350,21 MiB` | `72,89 MiB` *(davon `kernel=49,94`, `slab=27,09`)* | `2060,54 MiB` | CPU hält exakt (`1500m`), `anon` real `1637,44 MiB` (`+101,44 MiB` über 1,5 GiB; Core 6 ohne Monitoring: `1544,14 MiB`) |

**Konsequenz (F1/F2):**
1. **Keine Schönrechnung:** Wir korrigieren das kommunizierte Vollstack-RAM-Budget im `README.md` und in der k3s-`ResourceQuota` ehrlich auf **`1792Mi` (`1,75 GiB`) `requests.memory`** (Summe der 8 Pod-`requests` = `1728Mi`, direkt abgeleitet aus den gemessenen `anon`-Peaks) bei weiterhin exakt **`1500m` (`1,50 vCPU`) `requests.cpu`**.
2. **Limits vs. Requests (`1,66×` Overcommit):** `limits.memory` summiert sich auf `2864Mi` (`ResourceQuota` `limits.memory: 2944Mi`). Limits sind Obergrenzen für reclaimbaren Page-Cache (Postgres `236 MiB`, Kafka `86 MiB`) und nicht-gleichzeitige JVM-Bursts (`app: 640Mi`, `keycloak: 640Mi`), keine reservierte Kapazität.

## Alternativen

### 1. `requests.memory` künstlich unter realem `anon` halten (`1424Mi` ≤ `1536Mi`)
- **Nachteil:** Unaufrichtige Quota — Kubernetes-Scheduler verlässt sich auf `requests` für die Node-Kapazitätsplanung; zu niedrige `requests` provozieren Node-Memory-Evictions unter Last.

### 2. Keycloak oder Observability aus dem Stack streichen
- **Vorteil:** Ohne Prometheus/Grafana (`93,3 MiB`) bzw. ohne Keycloak (`473,8 MiB`) läge der Stack komfortabel unter `1536 MiB`.
- **Nachteil:** Würde zentrale Produktionsanforderungen (echtes OIDC/PKCE, Prometheus/Grafana Observability) opfern, nur um eine willkürliche Ausgangszahl zu retten.

## Konsequenzen

### Positiv
- `ResourceQuota` in k3s spiegelt nun die real gemessene `anon`-Baseline jedes einzelnen Pods wider (`1728Mi / 1792Mi` RAM, `1500m / 1500m` CPU).
- `app` erhält in k3s `limits.memory: 640Mi` (begründet durch `jcmd VM.native_memory summary`: `281 MiB` Heap-Peak + `~240 MiB` Non-Heap aus Metaspace `98,2 MiB`, G1GC `51,5 MiB`, CodeCache `45,3 MiB`, Symbol `25,9 MiB`, 178 Threads `18,3 MiB`).

### Negativ
- Der 8-Container-Vollstack benötigt für gleichzeitige 100k-Burst-Last unter `1,5 vCPU` real `1,60–1,75 GiB` statt `1,50 GiB` Arbeits-RAM.

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Ohne Designzwang:** JPA/Hibernate (~+50–70 MiB), ZooKeeper (~+200 MiB) und Debezium/Kafka Connect (~+400 MiB) hätten den Stack unbemerkt auf > 2,4 GiB `anon` aufgebläht.
- **Mit künstlich zu kleiner `ResourceQuota` (`1424Mi` vs. `1637Mi` realem `anon`):** Auf einem echten 2-GiB-k3s-VPS hätte der Kubelet-Eviction-Manager unter Last Pods abgeschossen, obwohl die Quota grün aussah.
