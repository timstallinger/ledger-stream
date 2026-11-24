export type LedgerRole = 'viewer' | 'writer' | 'operator';

export type BookingType = 'CREDIT' | 'DEBIT';

export type BookingStatus = 'POSTED' | 'CANCELLED';

export interface AccountDto {
  readonly id: string;
  readonly accountNumber: string;
  /** Decimal string serialized from Java BigDecimal — never parse into JS number. */
  readonly balance: string;
  readonly currency: string;
  readonly version: number;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface BookingDto {
  readonly id: string;
  readonly accountId: string;
  readonly type: BookingType;
  /** Decimal string serialized from Java BigDecimal — never parse into JS number. */
  readonly amount: string;
  readonly currency: string;
  readonly reference: string;
  readonly status: BookingStatus;
  readonly idempotencyKey: string;
  readonly originalBookingId: string | null;
  readonly createdAt: string;
}

export interface CreateAccountPayload {
  readonly accountNumber: string;
  readonly currency: string;
}

export interface CreateBookingPayload {
  readonly type: BookingType;
  /** Decimal string sent to backend BigDecimal — never converted to float/number. */
  readonly amount: string;
  readonly currency: string;
  readonly reference: string;
}

export interface CancelBookingPayload {
  readonly reference: string;
}

export interface FieldValidationError {
  readonly field: string;
  readonly message: string;
}

export type ApiErrorState =
  | {
      readonly kind: 'unauthorized';
      readonly status: 401;
      readonly message: string;
    }
  | {
      readonly kind: 'forbidden';
      readonly status: 403;
      readonly message: string;
    }
  | {
      readonly kind: 'validation';
      readonly status: 400 | 422;
      readonly errorCode: string;
      readonly message: string;
      readonly fieldErrors: readonly FieldValidationError[];
    }
  | {
      readonly kind: 'conflict';
      readonly status: 409;
      readonly errorCode: string;
      readonly message: string;
    }
  | {
      readonly kind: 'network';
      readonly status: number | null;
      readonly message: string;
    };

export interface BookingResponseEnvelope {
  readonly booking: BookingDto;
  readonly status: number;
  readonly replayed: boolean;
  readonly rawBody: string;
  readonly idempotencyKey: string;
}

export interface DuplicateSendProtocol {
  readonly idempotencyKey: string;
  readonly executedAt: string;
  readonly first: BookingResponseEnvelope;
  readonly second: BookingResponseEnvelope;
  readonly bodiesByteIdentical: boolean;
  readonly sameBookingId: boolean;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isString(value: unknown): value is string {
  return typeof value === 'string';
}

export function isNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

export function isBookingType(value: unknown): value is BookingType {
  return value === 'CREDIT' || value === 'DEBIT';
}

export function isBookingStatus(value: unknown): value is BookingStatus {
  return value === 'POSTED' || value === 'CANCELLED';
}

export function isAccountDto(value: unknown): value is AccountDto {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isString(value['id']) &&
    isString(value['accountNumber']) &&
    isString(value['balance']) &&
    isString(value['currency']) &&
    isNumber(value['version']) &&
    isString(value['createdAt']) &&
    isString(value['updatedAt'])
  );
}

export function isBookingDto(value: unknown): value is BookingDto {
  if (!isRecord(value)) {
    return false;
  }
  const originalBookingId = value['originalBookingId'];
  return (
    isString(value['id']) &&
    isString(value['accountId']) &&
    isBookingType(value['type']) &&
    isString(value['amount']) &&
    isString(value['currency']) &&
    isString(value['reference']) &&
    isBookingStatus(value['status']) &&
    isString(value['idempotencyKey']) &&
    (originalBookingId === null || originalBookingId === undefined || isString(originalBookingId)) &&
    isString(value['createdAt'])
  );
}
