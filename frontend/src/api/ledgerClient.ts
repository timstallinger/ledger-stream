import {
  isAccountDto,
  isBookingDto,
  isRecord,
  isString,
  type AccountDto,
  type ApiErrorState,
  type BookingDto,
  type BookingResponseEnvelope,
  type CancelBookingPayload,
  type CreateAccountPayload,
  type CreateBookingPayload,
  type FieldValidationError
} from '../types/ledger';

export class LedgerApiError extends Error {
  public readonly state: ApiErrorState;

  public constructor(state: ApiErrorState) {
    super(state.message);
    this.name = 'LedgerApiError';
    this.state = state;
  }
}

export interface LedgerApiClient {
  listAccounts(): Promise<readonly AccountDto[]>;
  getAccount(accountId: string): Promise<AccountDto>;
  createAccount(payload: CreateAccountPayload): Promise<AccountDto>;
  listBookings(accountId: string): Promise<readonly BookingDto[]>;
  createBooking(
    accountId: string,
    payload: CreateBookingPayload,
    idempotencyKey: string
  ): Promise<BookingResponseEnvelope>;
  cancelBooking(
    accountId: string,
    bookingId: string,
    payload: CancelBookingPayload,
    idempotencyKey: string
  ): Promise<BookingResponseEnvelope>;
}

function parseFieldErrors(problem: Record<string, unknown>): readonly FieldValidationError[] {
  const rawErrors = problem['errors'];
  const collected: FieldValidationError[] = [];

  if (Array.isArray(rawErrors)) {
    for (const item of rawErrors) {
      if (isRecord(item) && isString(item['field']) && isString(item['message'])) {
        collected.push({ field: item['field'], message: item['message'] });
      } else if (isString(item)) {
        collected.push({ field: 'request', message: item });
      }
    }
    return collected;
  }

  if (isRecord(rawErrors)) {
    for (const [field, msg] of Object.entries(rawErrors)) {
      if (isString(msg)) {
        collected.push({ field, message: msg });
      }
    }
  }
  return collected;
}

function tryParseJson(rawText: string): unknown {
  if (rawText.trim().length === 0) {
    return null;
  }
  try {
    return JSON.parse(rawText);
  } catch {
    return null;
  }
}

function toApiErrorState(status: number, rawBody: string): ApiErrorState {
  const parsed = tryParseJson(rawBody);
  const problem = isRecord(parsed) ? parsed : null;
  const detail = problem !== null && isString(problem['detail']) ? problem['detail'] : null;
  const errorCode = problem !== null && isString(problem['errorCode']) ? problem['errorCode'] : `HTTP_${status}`;

  if (status === 401) {
    return {
      kind: 'unauthorized',
      status: 401,
      message: detail ?? 'Sitzung abgelaufen oder fehlendes Token (401). Bitte erneut anmelden.'
    };
  }

  if (status === 403) {
    return {
      kind: 'forbidden',
      status: 403,
      message: detail ?? 'Keine Berechtigung für diese Aktion (403).'
    };
  }

  if (status === 422 || status === 400) {
    const fieldErrors = problem !== null ? parseFieldErrors(problem) : [];
    return {
      kind: 'validation',
      status,
      errorCode,
      message: detail ?? 'Ungültige Eingabe oder Geschäftsregel verletzt.',
      fieldErrors
    };
  }

  if (status === 409) {
    return {
      kind: 'conflict',
      status: 409,
      errorCode,
      message: detail ?? 'Bereits gebucht — Konflikt oder identische Antwort erhalten (409).'
    };
  }

  return {
    kind: 'network',
    status,
    message: detail ?? `Serverfehler (HTTP ${status}). Bitte erneut versuchen.`
  };
}

export function createLedgerApiClient(
  apiBaseUrl: string,
  getAccessToken: () => string | null,
  fetchImpl: typeof fetch = window.fetch.bind(window)
): LedgerApiClient {
  const requestRaw = async (
    path: string,
    init: {
      readonly method: 'GET' | 'POST';
      readonly body?: string;
      readonly idempotencyKey?: string;
    }
  ): Promise<{ readonly status: number; readonly rawBody: string; readonly replayed: boolean }> => {
    const token = getAccessToken();
    const headers: Record<string, string> = {
      Accept: 'application/json',
      'X-Correlation-Id': crypto.randomUUID()
    };
    if (token !== null) {
      headers['Authorization'] = `Bearer ${token}`;
    }
    if (init.body !== undefined) {
      headers['Content-Type'] = 'application/json';
    }
    if (init.idempotencyKey !== undefined) {
      headers['Idempotency-Key'] = init.idempotencyKey;
    }

    let response: Response;
    try {
      const fetchInit: RequestInit = {
        method: init.method,
        headers,
        credentials: 'include'
      };
      if (init.body !== undefined) {
        fetchInit.body = init.body;
      }
      response = await fetchImpl(`${apiBaseUrl}${path}`, fetchInit);
    } catch {
      throw new LedgerApiError({
        kind: 'network',
        status: null,
        message: 'Netzwerkfehler: Backend nicht erreichbar. Bitte Verbindung prüfen und erneut versuchen.'
      });
    }

    const rawBody = await response.text();
    const replayed = response.headers.get('Idempotency-Replayed') === 'true';

    if (!response.ok) {
      throw new LedgerApiError(toApiErrorState(response.status, rawBody));
    }

    return {
      status: response.status,
      rawBody,
      replayed
    };
  };

  return {
    async listAccounts(): Promise<readonly AccountDto[]> {
      const { rawBody } = await requestRaw('/api/v1/accounts', { method: 'GET' });
      const parsed = tryParseJson(rawBody);
      if (!Array.isArray(parsed) || !parsed.every(isAccountDto)) {
        throw new LedgerApiError({
          kind: 'network',
          status: null,
          message: 'Unerwartetes Antwortformat für /api/v1/accounts.'
        });
      }
      return parsed;
    },

    async getAccount(accountId: string): Promise<AccountDto> {
      const { rawBody } = await requestRaw(`/api/v1/accounts/${encodeURIComponent(accountId)}`, { method: 'GET' });
      const parsed = tryParseJson(rawBody);
      if (!isAccountDto(parsed)) {
        throw new LedgerApiError({
          kind: 'network',
          status: null,
          message: 'Unerwartetes Antwortformat für Konto-Details.'
        });
      }
      return parsed;
    },

    async createAccount(payload: CreateAccountPayload): Promise<AccountDto> {
      const { rawBody } = await requestRaw('/api/v1/accounts', {
        method: 'POST',
        body: JSON.stringify(payload)
      });
      const parsed = tryParseJson(rawBody);
      if (!isAccountDto(parsed)) {
        throw new LedgerApiError({
          kind: 'network',
          status: null,
          message: 'Unerwartetes Antwortformat nach Kontoanlage.'
        });
      }
      return parsed;
    },

    async listBookings(accountId: string): Promise<readonly BookingDto[]> {
      const { rawBody } = await requestRaw(`/api/v1/accounts/${encodeURIComponent(accountId)}/bookings`, {
        method: 'GET'
      });
      const parsed = tryParseJson(rawBody);
      if (!Array.isArray(parsed) || !parsed.every(isBookingDto)) {
        throw new LedgerApiError({
          kind: 'network',
          status: null,
          message: 'Unerwartetes Antwortformat für Buchungshistorie.'
        });
      }
      return parsed;
    },

    async createBooking(
      accountId: string,
      payload: CreateBookingPayload,
      idempotencyKey: string
    ): Promise<BookingResponseEnvelope> {
      const { status, rawBody, replayed } = await requestRaw(
        `/api/v1/accounts/${encodeURIComponent(accountId)}/bookings`,
        {
          method: 'POST',
          body: JSON.stringify(payload),
          idempotencyKey
        }
      );
      const parsed = tryParseJson(rawBody);
      if (!isBookingDto(parsed)) {
        throw new LedgerApiError({
          kind: 'network',
          status: null,
          message: 'Unerwartetes Antwortformat nach Buchung.'
        });
      }
      return {
        booking: parsed,
        status,
        replayed,
        rawBody,
        idempotencyKey
      };
    },

    async cancelBooking(
      accountId: string,
      bookingId: string,
      payload: CancelBookingPayload,
      idempotencyKey: string
    ): Promise<BookingResponseEnvelope> {
      const { status, rawBody, replayed } = await requestRaw(
        `/api/v1/accounts/${encodeURIComponent(accountId)}/bookings/${encodeURIComponent(bookingId)}/cancel`,
        {
          method: 'POST',
          body: JSON.stringify(payload),
          idempotencyKey
        }
      );
      const parsed = tryParseJson(rawBody);
      if (!isBookingDto(parsed)) {
        throw new LedgerApiError({
          kind: 'network',
          status: null,
          message: 'Unerwartetes Antwortformat nach Stornierung.'
        });
      }
      return {
        booking: parsed,
        status,
        replayed,
        rawBody,
        idempotencyKey
      };
    }
  };
}
