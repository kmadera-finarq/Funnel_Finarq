-- Extensiones
create extension if not exists pgcrypto;
create extension if not exists uuid-ossp;

-- Admins (quién ve TODO)
create table if not exists public.admins (
  user_id uuid primary key references auth.users(id) on delete cascade
);

-- Config de productos: lag (meses) para contabilización y prob. conservadora
create table if not exists public.productos_config (
  producto text primary key,
  lag_meses integer not null default 0,
  prob_default numeric not null default 0.30 check (prob_default >= 0 and prob_default <= 1)
);

insert into public.productos_config(producto, lag_meses, prob_default)
values
  ('Divisas',       0, 0.30),
  ('Inversiones',   1, 0.30),
  ('Factoraje',     1, 0.30),
  ('Arrendamiento', 2, 0.30),
  ('TPV',           0, 0.30),
  ('Créditos',      1, 0.30)
on conflict (producto) do nothing;

-- Capturas (leads/propuestas/clientes)
create table if not exists public.capturas (
  id uuid primary key default gen_random_uuid(),
  ts timestamptz not null default now(),
  user_id uuid not null references auth.users(id) on delete cascade,
  asesor text not null,
  cliente text not null,
  referencia_cliente text,               -- referencia libre
  producto text not null references public.productos_config(producto),
  monto_esperado numeric(14,2) not null,
  mes_cierre_esperado date not null,    -- usar 1º de mes
  fecha_visita date,                    -- fecha real de visita
  tipo_bau text not null default 'Nuevo' check (tipo_bau in ('BAU','Nuevo')),
  estatus text not null default 'Prospecto' check (estatus in ('Prospecto','Propuesta','Documentación','Cliente')),
  prob_ajustada numeric check (prob_ajustada >= 0 and prob_ajustada <= 1)
);

-- Metas mensuales por asesor
create table if not exists public.metas (
  id uuid primary key default gen_random_uuid(),
  asesor_user_id uuid not null references auth.users(id) on delete cascade,
  mes date not null, -- primer día del mes
  meta_monto numeric(14,2) not null,
  unique (asesor_user_id, mes)
);

-- Si vienes de v1: actualiza checks y columnas
alter table public.capturas drop constraint if exists capturas_estatus_check;
alter table public.capturas add constraint capturas_estatus_check check (estatus in ('Prospecto','Propuesta','Documentación','Cliente'));
alter table public.capturas add column if not exists referencia_cliente text;
alter table public.capturas add column if not exists fecha_visita date;
alter table public.capturas add column if not exists tipo_bau text;
update public.capturas set tipo_bau = coalesce(tipo_bau,'Nuevo');
alter table public.capturas drop constraint if exists capturas_tipo_bau_check;
alter table public.capturas add constraint capturas_tipo_bau_check check (tipo_bau in ('BAU','Nuevo'));

-- RLS
alter table public.capturas enable row level security;
alter table public.metas    enable row level security;
alter table public.productos_config enable row level security;

-- capturas: cada usuario ve/insert solo lo suyo
create policy if not exists "capturas_ins_own" on public.capturas for insert to authenticated
  with check (auth.uid() = user_id);
create policy if not exists "capturas_sel_own" on public.capturas for select to authenticated
  using (auth.uid() = user_id);
-- admins ven todo
create policy if not exists "capturas_sel_admin" on public.capturas for select to authenticated
  using (exists (select 1 from public.admins a where a.user_id = auth.uid()));

-- metas: solo admin escribe, asesor ve la suya
create policy if not exists "metas_sel_own" on public.metas for select to authenticated
  using (auth.uid() = asesor_user_id or exists (select 1 from public.admins a where a.user_id = auth.uid()));
create policy if not exists "metas_ins_admin" on public.metas for insert to authenticated
  with check (exists (select 1 from public.admins a where a.user_id = auth.uid()));
create policy if not exists "metas_upd_admin" on public.metas for update to authenticated
  using (exists (select 1 from public.admins a where a.user_id = auth.uid()))
  with check (exists (select 1 from public.admins a where a.user_id = auth.uid()));

-- productos_config: lectura para todos, escritura solo admin
create policy if not exists "prod_sel_all" on public.productos_config for select to authenticated using (true);
create policy if not exists "prod_ins_admin" on public.productos_config for insert to authenticated
  with check (exists (select 1 from public.admins a where a.user_id = auth.uid()));
create policy if not exists "prod_upd_admin" on public.productos_config for update to authenticated
  using (exists (select 1 from public.admins a where a.user_id = auth.uid()))
  with check (exists (select 1 from public.admins a where a.user_id = auth.uid()));