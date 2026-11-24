export interface WindowLedgerConfig {
  readonly apiBaseUrl?: string;
  readonly oidcAuthority?: string;
  readonly oidcClientId?: string;
}

declare global {
  interface Window {
    __LEDGER_CONFIG__?: WindowLedgerConfig;
  }
}

export {};
