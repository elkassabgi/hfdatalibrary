-- M1 registry seed (AUTH_SSO_PLAN §13): the current effective origin set, so the
-- shadow-mode CORS comparison exercises the real deny->allow transitions. Applied
-- to live D1 on 2026-07-17 via `wrangler d1 execute --remote` (owner-only write
-- path per §9). Committed here for reproducibility / rebuild. Idempotent.
INSERT OR IGNORE INTO sso_clients (origin, brand_name, status, created_at) VALUES
  ('https://hfdatalibrary.com',      'HF Data Library',   'active', strftime('%s','now')),
  ('https://www.hfdatalibrary.com',  'HF Data Library',   'active', strftime('%s','now')),
  ('http://localhost:8080',          'Local Dev',         'active', strftime('%s','now')),
  ('https://econdatalibrary.com',    'Econ Data Library', 'active', strftime('%s','now')),
  ('https://www.econdatalibrary.com','Econ Data Library', 'active', strftime('%s','now')),
  ('https://elkassabgidata.com',     'ElkassabgiData',    'active', strftime('%s','now')),
  ('https://www.elkassabgidata.com', 'ElkassabgiData',    'active', strftime('%s','now')),
  -- Added at the enforcement flip: ipdatalibrary.com went live as family site #4.
  ('https://ipdatalibrary.com',      'IP Data Library',   'active', strftime('%s','now')),
  ('https://www.ipdatalibrary.com',  'IP Data Library',   'active', strftime('%s','now'));
