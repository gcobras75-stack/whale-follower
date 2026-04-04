-- Whale Follower Bot — Sprint 2
-- Tabla de paper trades ejecutados en Bybit Testnet
-- Ejecutar en: supabase.com → SQL Editor → New query

create table if not exists paper_trades (
    id               uuid primary key default gen_random_uuid(),
    created_at       timestamptz not null default now(),

    -- Referencia a la señal que generó el trade
    signal_id        uuid references whale_signals(id) on delete set null,

    -- Datos de entrada
    side             text not null check (side in ('Buy', 'Sell')),
    entry_price      double precision not null,
    stop_loss        double precision not null,
    take_profit      double precision not null,
    size_usd         double precision not null,

    -- Estado del trade
    status           text not null default 'open'
                     check (status in ('open', 'partial', 'closed')),

    -- Resultado (nulo hasta cierre)
    exit_price       double precision,
    pnl_usd          double precision default 0,
    pnl_pct          double precision default 0,
    close_reason     text check (
                         close_reason in (
                             'take_profit', 'stop_loss',
                             'trailing_stop', 'manual', null
                         )
                     ),
    duration_seconds int
);

-- Índices
create index if not exists paper_trades_created_at_idx
    on paper_trades (created_at desc);

create index if not exists paper_trades_status_idx
    on paper_trades (status);

create index if not exists paper_trades_signal_id_idx
    on paper_trades (signal_id);

-- RLS
alter table paper_trades enable row level security;

create policy "anon can insert paper_trades" on paper_trades
    for insert to anon with check (true);

create policy "anon can update paper_trades" on paper_trades
    for update to anon using (true) with check (true);

create policy "anon can select paper_trades" on paper_trades
    for select to anon using (true);

-- Grants
grant usage on schema public to anon, authenticated;
grant select, insert, update on table paper_trades to anon;
grant select on table paper_trades to authenticated;

-- Vista de rendimiento (opcional pero útil en el dashboard)
create or replace view paper_trades_summary as
select
    count(*) filter (where status = 'closed')          as total_closed,
    count(*) filter (where pnl_usd > 0)                as winners,
    count(*) filter (where pnl_usd <= 0)               as losers,
    round(
        count(*) filter (where pnl_usd > 0)::numeric /
        nullif(count(*) filter (where status = 'closed'), 0) * 100,
    1)                                                  as win_rate_pct,
    round(sum(pnl_usd)::numeric, 2)                    as total_pnl_usd,
    round(avg(pnl_pct)::numeric, 2)                    as avg_pnl_pct,
    round(avg(duration_seconds)::numeric, 0)           as avg_duration_secs
from paper_trades;
