# ADR-0007: Keycloak 26 als OIDC Identity Provider statt Spring Authorization Server

- **Status:** Akzeptiert
- **Datum:** 2026-11-24

## Kontext

`ledger-stream` benötigt OIDC/OAuth2-Authentifizierung und rollenbasierte Autorisierung (`viewer`, `writer`, `operator` mit strikter Separation of Duties) sowohl für die Vue-3-SPA (Authorization Code + PKCE `S256`, `automaticSilentRenew`, RP-Initiated Logout) als auch für automatisierte Integrationstests und den CLI-Lastgenerator. Gleichzeitig soll das Projekt produktionsnahe Sicherheitsmuster ohne selbstgebautes Passwort- oder Session-Handling zeigen.

## Entscheidung

Wir betreiben **Keycloak 26 (`26.2.5`, `start --optimized --import-realm`, `KC_CACHE=local`)** auf der bestehenden PostgreSQL-16-Instanz (eigene Datenbank `keycloak`) mit deklarativem Realm-Export (`keycloak/realm-export.json`), Public-PKCE-Client `ledger-stream-spa` und vertraulichem Test/CI-Client `ledger-stream-cli` (Secret per Env / `SealedSecret`, niemals im Klartext). Die Spring-Boot-App agiert ausschließlich als zustandsloser OAuth2 Resource Server (`NimbusJwtDecoder` mit `iss`/`exp`/`aud`-Prüfung und Allowlist-Rollen-Mapper).

## Alternativen

### 1. Spring Authorization Server (eingebettet oder als zweiter Spring-Service)
- **Vorteil:** Geringerer RAM-Footprint (~150–200 MiB statt ~400 MiB für Keycloak).
- **Nachteil:** Erfordert eigene Implementierung von Login-UI, Benutzer-/Rollenverwaltung, Passwort-Hashing, Token-Endpoint, Session-Management, Logout-UI und Admin-API. Hohes Risiko sicherheitsrelevanter Eigenbau-Fehler und erheblicher Code-Ballast außerhalb der Ledger-Domäne.

### 2. Statische / selbst signierte JWTs ohne IdP
- **Vorteil:** Null Infrastruktur-RAM.
- **Nachteil:** Kein echter PKCE-Browser-Flow, kein JWKS-Rotationstest, nicht realistisch für Enterprise-Produktion.

## Konsequenzen

### Positiv
- Produktionsstandard für OIDC/PKCE (`S256`), JWKS-Discovery, Audience-Mapper (`ledger-stream-api`), Rollen-Allowlist und Admin-REST-API ohne eine einzige Zeile eigenem Auth-Server-Code.
- Vollständig reproduzierbarer Realm-Import ohne H2 und ohne Dev-Mode.

### Negativ
- **Größter Einzel-Speicherverbraucher im Infra-Stack:** `~380–410 MiB` `anon` im Normalbetrieb (Quarkus + Metaspace ~120–150 MiB). Benötigt im k3s-Cluster `limits.memory: 640Mi` und eine eigene `startupProbe` (`/health/started`), sowie ausreichend CPU (`>= 0,30 vCPU`), da extreme CPU-Drosselung (`0,10 vCPU`) zu GC-Rückstau bis `508 MiB` `anon` führt (siehe ADR-0009).

## Was gebrochen wäre, wenn wir anders entschieden hätten

- **Mit Spring Authorization Server:** Wir hätten Login-Masken, User-DB-Tabellen, Passwort-Reset, Token-Revocation und PKCE-Consent selbst warten müssen — stattdessen wäre das Projekt ein Auth-Server-Projekt geworden und hätte trotzdem keine realistische Keycloak-Integration (inkl. Allowlist-Filterung technischer Realm-Rollen wie `default-roles-*`/`offline_access`) nachgewiesen.
- **Mit `start-dev` / H2 statt `start --optimized` + PostgreSQL:** Nicht produktionsnah, langsamerer Start und Datenverlust bei Pod-Neustart.
