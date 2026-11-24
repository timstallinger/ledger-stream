import { describe, expect, it } from 'vitest';
import { formatMoney, isDecimalString, normalizeDecimalInput } from './money';

describe('money formatting without number parsing', () => {
  it('formats standard and large BigDecimal strings in de-DE EUR without precision loss', () => {
    expect(formatMoney('1234.50', 'EUR')).toBe('1.234,50\u00A0€');
    // Beyond Number.MAX_SAFE_INTEGER (9007199254740991) — would corrupt if parsed via Number()
    expect(formatMoney('9007199254740993.45', 'EUR')).toBe('9.007.199.254.740.993,45\u00A0€');
    expect(formatMoney('0.00', 'EUR')).toBe('0,00\u00A0€');
  });

  it('validates decimal strings and normalizes German comma input as pure string', () => {
    expect(isDecimalString('100.25')).toBe(true);
    expect(isDecimalString('not-a-number')).toBe(false);
    expect(normalizeDecimalInput(' 42,75 ')).toBe('42.75');
  });
});
