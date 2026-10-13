// Somas históricas podem exceder a faixa de um saldo e diferenças podem
// ser negativas; são valores derivados exatos, não instâncias de Money.
// Money continua validando cada saldo persistido.
const twoDecimals = /^-?(0|[1-9][0-9]*)\.[0-9]{2}$/;

export function parseMinorUnits(text: string): bigint {
  if (typeof text !== 'string' || twoDecimals.exec(text)?.[0] !== text || text === '-0.00')
    throw new Error('Expected an exact two-decimal amount.');
  return BigInt(text.replace('.', ''));
}

export function formatMinorUnits(units: bigint): string {
  const magnitude = units < 0n ? -units : units;
  return `${units < 0n ? '-' : ''}${magnitude / 100n}.${(magnitude % 100n).toString().padStart(2, '0')}`;
}
