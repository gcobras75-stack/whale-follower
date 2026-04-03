-- Whale Follower Bot — Sprint 1
-- Run this in your Supabase SQL Editor

create table if not exists whale_signals (
    id               uuid primary key default gen_random_uuid(),
    created_at       timestamptz not null default now(),
    pair             text not null,
    score            int not null check (score between 0 and 100),
    price_entry      double precision not null,
    stop_loss        double precision not null,
    take_profit      double precision not null,
    conditions_met   jsonb not null default '{}',
    strongest_exchange text not null,
    cvd_velocity     double precision not null default 0
);

-- Index for time-based queries
create index if not exists whale_signals_created_at_idx
    on whale_signals (created_at desc);

-- Enable Row Level Security (read-only public access optional)
alter table whale_signals enable row level security;

-- Allow the service role (used by the bot's anon key) to insert
create policy "bot can insert" on whale_signals
    for insert with check (true);

-- Allow authenticated users to read
create policy "authenticated can read" on whale_signals
    for select using (auth.role() = 'authenticated');

-- Grant table-level privileges to the anon and authenticated roles
-- (Required: RLS alone is not enough — the role must also have SQL privileges)
grant usage on schema public to anon, authenticated;
grant select, insert, delete on table whale_signals to anon;
grant select on table whale_signals to authenticated;
