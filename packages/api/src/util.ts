import { Account, Near } from 'near-api-js';

import getNearConfig from '@tonic-foundation/tonic/lib/util/getNearConfig';

export function maybeDate(s: string): Date | null {
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

// is this a good idea?
let _nobody: Account | undefined;

export async function getNearNobodyAccount() {
  if (_nobody) {
    return _nobody;
  }
  const near = new Near(getNearConfig(process.env.NEAR_ENV || 'testnet'));
  _nobody = await near.account('nobody');
  return _nobody;
}
