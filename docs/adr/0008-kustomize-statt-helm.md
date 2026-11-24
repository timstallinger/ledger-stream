# ADR-0008: Kustomize (`base` + `overlays`) statt Helm

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

Für Phase 11 (k3s-Deployment mit ArgoCD GitOps) müssen alle 8 Workloads (`postgres`, `kafka`, `schema-registry`, `keycloak`, `app`, `frontend`, `prometheus`, `grafana`), `ResourceQuota`, `LimitRange`, 10 `NetworkPolicy`-Objekte, 3 `SealedSecret`s und ConfigMaps deklarativ verwaltet und zwischen `local` und `prod` differenziert werden.

## Entscheidung

Wir nutzen **Kustomize** (nativ in `kubectl kustomize` und ArgoCD integriert) mit einer klaren Trennung zwischen [`deploy/kustomize/base/`](../../deploy/kustomize/base/kustomization.yaml) und reinen Delta-Overlays [`overlays/local/`](../../deploy/kustomize/overlays/local/kustomization.yaml) sowie [`overlays/prod/`](../../deploy/kustomize/overlays/prod/kustomization.yaml). Image-Digests (`@sha256:...`) werden deklarativ über den Kustomize-`images:`-Transformer gepinnt.

## Alternativen

### 1. Helm Chart (`templates/*.yaml` + `values.yaml`)
- **Vorteil:** Standard für extern verteilbare Drittanbieter-Pakete mit komplexer Parametrisierung und Release-Packaging.
- **Nachteil:** Go-Template-Syntax (`{{ include ... | nindent 8 }}`) zerstört die YAML-Lesbarkeit und IDE-Validierung; für eine einzelne, vollständig im selben Repository verwaltete Anwendung erzeugen `values.yaml`-Abstraktionen mehr Indirektion als Nutzen.

### 2. Reine statische YAML-Dateien ohne Overlays
- **Vorteil:** Null Tooling.
- **Nachteil:** Duplizierung kompletter Deployment-Manifeste zwischen `local` und `prod` → Drift-Risiko (z. B. versehentliches `LEDGER_ADMIN_ENABLED=true` in `prod`).

## Konsequenzen

### Positiv
- Jedes Basis-Manifest ist gültiges, direkt lesbares Kubernetes-YAML ohne Template-Steuerzeichen.
- `overlays/prod` enthält ausschließlich echte Deltas (erzwingt `LEDGER_ADMIN_ENABLED=false`, `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=FULL` und unveränderliche GHCR-`@sha256:`-Digests).
- Zero-Dependency-Integration in ArgoCD und im CI-Gate (`kubectl kustomize deploy/kustomize/overlays/prod`).

### Negativ
- Keine eingebaute Chart-Versionierung/Distribution für externe Endnutzer-Installationen (`helm install`).
- `configMapGenerator` erzwingt mit ArgoCDs Standard-Load-Restrictor lokale Dateien unterhalb der Kustomization-Wurzel (`deploy/kustomize/base/files/`).

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit Helm-Templates:** Ein kleiner Tippfehler in einem `nindent`-Filter oder ein vergessener Override-Wert für `LEDGER_ADMIN_ENABLED` wäre im Diff schlechter erkennbar gewesen als ein 15-zeiliges Delta-Overlay.
- **Ohne Overlays:** Manuelle Duplikate für `local` und `prod` hätten bei jedem SecurityContext- oder Probe-Fix (`kafka-0` `initContainer`, `enableServiceLinks: false`) doppelt gepflegt werden müssen und leicht auseinanderdriften können.
