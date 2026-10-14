import { DomainError } from './domain-error.js';
import { isKnownCurrency } from './currency.js';

export interface MoneyProps {
  readonly amount: string;
  readonly currency: string;
}

// O contrato fixa duas casas decimais; o registro de moedas recusa no boot qualquer escala diferente.
const maximumMinorUnits = 10n ** 20n - 1n; // A faixa acompanha a coluna numeric(20,2) do PostgreSQL.
const unsignedAmount = /^(0|[1-9][0-9]{0,17})\.[0-9]{2}$/;
const signedAmount = /^-?(0|[1-9][0-9]{0,17})\.[0-9]{2}$/;

export class Money {
  private constructor(
    private readonly minorUnits: bigint,
    public readonly currency: string,
  ) {
    if (!isKnownCurrency(currency)) throw new DomainError('UNSUPPORTED_CURRENCY');
    if (minorUnits > maximumMinorUnits || minorUnits < -maximumMinorUnits) {
      throw new DomainError('MONEY_OUT_OF_RANGE');
    }
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    return Money.parse(props, unsignedAmount);
  }

  /** A persistência confiável admite resultados internos com sinal. */
  static rehydrate(props: MoneyProps): Money {
    return Money.parse(props, signedAmount);
  }

  static zero(currency: string): Money {
    return new Money(0n, currency);
  }

  private static parse(props: MoneyProps, pattern: RegExp): Money {
    if (typeof props.amount !== 'string' || pattern.exec(props.amount)?.[0] !== props.amount) {
      throw new DomainError('INVALID_MONEY');
    }
    return new Money(BigInt(props.amount.replace('.', '')), props.currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits + other.minorUnits, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits - other.minorUnits, this.currency);
  }

  negate(): Money {
    return new Money(-this.minorUnits, this.currency);
  }

  isZero(): boolean { return this.minorUnits === 0n; }
  isPositive(): boolean { return this.minorUnits > 0n; }
  isNegative(): boolean { return this.minorUnits < 0n; }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits < other.minorUnits;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits === other.minorUnits;
  }

  toString(): string {
    const magnitude = this.minorUnits < 0n ? -this.minorUnits : this.minorUnits;
    const sign = this.minorUnits < 0n ? '-' : '';
    return `${sign}${magnitude / 100n}.${(magnitude % 100n).toString().padStart(2, '0')}`;
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) throw new DomainError('CURRENCY_MISMATCH');
  }
}
