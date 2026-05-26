-- idx_broker_registry: canonical IDX broker code -> name + classification.
-- Source: scraped from idx.co.id/en/members-and-participants/exchange-members-profiles/<CODE>
-- via broksum/sources/idx_members.py. Refresh manually as needed
-- (`python scrape.py registry && python scrape.py upload-registry`).
--
-- Apply this DDL once via Supabase Studio SQL Editor before running upload-registry.

create table if not exists public.idx_broker_registry (
  broker_code     text primary key,
  broker_name     text not null,
  is_foreign      boolean not null default false,
  license_type    text,
  member_status   text default 'active',
  source_url      text,
  scraped_at      timestamptz default now()
);

comment on table public.idx_broker_registry is
  'Canonical IDX broker code -> name + foreign-cohort flag. Scraped quarterly from idx.co.id.';

comment on column public.idx_broker_registry.is_foreign is
  'True if broker is the Indonesian arm of a non-Indonesian parent (curated list, not scraped).';

comment on column public.idx_broker_registry.license_type is
  'Derived from page text. Common values: Broker-Dealer, Underwriter, "Broker-Dealer / Underwriter".';

-- Row Level Security: enable + allow public SELECT, deny everything else.
-- Writes only happen via the SUPABASE_KEY service-role (broksum upload step,
-- sectors_api Django ORM) which bypasses RLS automatically. Matches the
-- standard Supabase pattern for public read-only reference tables.
alter table public.idx_broker_registry enable row level security;

create policy "Public read access"
  on public.idx_broker_registry
  for select
  to anon, authenticated
  using (true);
