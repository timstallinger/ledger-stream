const DECIMAL_PATTERN = /^-?\d+(?:\.\d+)?$/;

export function isDecimalString(value: string): value is Intl.StringNumericLiteral {
  return DECIMAL_PATTERN.test(value.trim());
}

/**
 * Formats a BigDecimal JSON string directly using Intl.NumberFormat('de-DE')
 * without ever parsing into IEEE-754 number or performing arithmetic.
 */
export function formatMoney(amount: string, currency: string = 'EUR'): string {
  const trimmed = amount.trim();
  if (!isDecimalString(trimmed)) {
    return `${trimmed} ${currency}`;
  }
  const normalizedCurrency = currency.trim().toUpperCase() || 'EUR';
  const formatter = new Intl.NumberFormat('de-DE', {
    style: 'currency',
    currency: normalizedCurrency
  });
  return formatter.format(trimmed);
}

/**
 * Normalizes user decimal input (e.g. "12,50" -> "12.50") as a pure string
 * without numeric conversion.
 */
export function normalizeDecimalInput(raw: string): string {
  return raw.trim().replace(',', '.');
}
