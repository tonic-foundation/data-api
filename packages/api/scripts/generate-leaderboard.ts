/*
Generate top N trading accounts by volume.
Prints to console in CSV format.

Basic usage:
% export POSTGRES_CONNECTION="postgres://..."
% yarn generate-leaderboard \
    --limit=10 \
    --contest_start='2022-05-06 20:55:00' --contest_end='2022-05-06 21:55:00' \
    --market1=xxx --market2=yyy --market3=zzz
*/
import { BN } from 'bn.js';
import Knex from 'knex';
import { parse } from 'ts-command-line-args';
import { getDbConnectConfig } from '../src/config';

const knex = Knex(getDbConnectConfig());

export interface LeaderboardOptions {
  contest_start: string;
  contest_end: string;
  limit: number;
  market1: string;
  market2?: string;
  market3?: string;
}

export const args = parse<LeaderboardOptions>({
  contest_start: String,
  contest_end: String,
  limit: Number,
  market1: { type: String },
  market2: { type: String, optional: true },
  market3: { type: String, optional: true },
});

interface LeaderBoardEntry {
  account_id: string;
  volume_traded: string;
}

async function generateLeaderboard(): Promise<LeaderBoardEntry[]> {
  const { rows: leaderboard } = await knex.raw<{ rows: LeaderBoardEntry[] }>(
    ` with
              fills_with_maker_ids as (
                  SELECT quote_qty, account_id, order_event.market_id as market_id
                      FROM fill_event JOIN order_event ON maker_order_id = order_id
                      WHERE fill_event.created_at BETWEEN :starttime::timestamp AND :endtime::timestamp
              ),
              fills_with_taker_ids as (
                  SELECT quote_qty, account_id, order_event.market_id as market_id
                      FROM fill_event JOIN order_event ON taker_order_id = order_id
                      WHERE fill_event.created_at BETWEEN :starttime::timestamp AND :endtime::timestamp
              ),
              combined_fills as (
                  SELECT * FROM fills_with_maker_ids UNION ALL SELECT * FROM fills_with_taker_ids
              )
              SELECT account_id, SUM(quote_qty::numeric) as volume_traded
                  FROM combined_fills
                  WHERE (market_id = :market1 OR market_id = :market2 OR market_id = :market3)
                  GROUP BY account_id
                  ORDER BY volume_traded DESC
                  LIMIT :limit
            `,
    {
      starttime: args.contest_start,
      endtime: args.contest_end,
      market1: args.market1,
      market2: args.market2 || '',
      market3: args.market3 || '',
      limit: args.limit,
    }
  );

  return leaderboard;
}

async function run() {
  const quote_decimals = 6; // all quotes are in USDC for the contest;
  const quote_decimals_bn = new BN(10).pow(new BN(quote_decimals));

  const leaderboard = await generateLeaderboard();

  console.log('='.repeat(10), 'RESULTS', '='.repeat(10));
  console.log('i,account_id,vol_traded_usdc');
  leaderboard.map((x, i) =>
    console.log(`${i},${x.account_id},${new BN(x.volume_traded).div(quote_decimals_bn).toString()}`)
  );
}

run().then(() => {
  process.exit(0);
});
