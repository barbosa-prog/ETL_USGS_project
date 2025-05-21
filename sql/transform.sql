-- 0) Supprimer la contrainte chk_mag qui bloquait les mags négatives
ALTER TABLE public.earthquake_event
DROP CONSTRAINT IF EXISTS chk_mag;

BEGIN;

-- 1) Remplir dim_magtype
INSERT INTO dim_magtype (magtype)
SELECT DISTINCT raw_payload->'properties'->>'magType'
FROM public.staging_usgs_raw
WHERE raw_payload->'properties'->>'magType' IS NOT NULL
ON CONFLICT (magtype) DO NOTHING;

-- 2) Remplir dim_alert
INSERT INTO dim_alert (alert_level)
SELECT DISTINCT raw_payload->'properties'->>'alert'
FROM public.staging_usgs_raw
WHERE raw_payload->'properties'->>'alert' IS NOT NULL
ON CONFLICT (alert_level) DO NOTHING;

-- 3) Remplir dim_network
INSERT INTO dim_network (network_code)
SELECT DISTINCT raw_payload->'properties'->>'net'
FROM public.staging_usgs_raw
WHERE raw_payload->'properties'->>'net' IS NOT NULL
ON CONFLICT (network_code) DO NOTHING;

-- 4) Remplir dim_event_type
INSERT INTO dim_event_type (event_type)
SELECT DISTINCT raw_payload->'properties'->>'type'
FROM public.staging_usgs_raw
WHERE raw_payload->'properties'->>'type' IS NOT NULL
ON CONFLICT (event_type) DO NOTHING;

-- 5) Charger la table de faits earthquake_event
WITH evts AS (
  SELECT
    s.event_id,
    to_timestamp((s.raw_payload->'properties'->>'time')::bigint/1000) AT TIME ZONE 'UTC'     AS time_utc,
    to_timestamp((s.raw_payload->'properties'->>'updated')::bigint/1000) AT TIME ZONE 'UTC'  AS updated_utc,
    (s.raw_payload->'properties'->>'mag')::double precision                               AS mag,
    s.raw_payload->'properties'->>'place'                                                 AS place,
    (s.raw_payload->'properties'->>'felt')::int                                           AS felt,
    (s.raw_payload->'properties'->>'cdi')::double precision                               AS cdi,
    (s.raw_payload->'properties'->>'mmi')::double precision                               AS mmi,
    (s.raw_payload->'properties'->>'tsunami')::bool                                       AS tsunami,
    s.raw_payload->'properties'->>'title'                                                AS title,
    dm.magtype_id,
    da.alert_id,
    dn.net_id,
    de.eventtype_id,
    s.raw_payload                                                                       AS properties
  FROM public.staging_usgs_raw s
  LEFT JOIN dim_magtype    dm ON dm.magtype      = s.raw_payload->'properties'->>'magType'
  LEFT JOIN dim_alert      da ON da.alert_level  = s.raw_payload->'properties'->>'alert'
  LEFT JOIN dim_network    dn ON dn.network_code = s.raw_payload->'properties'->>'net'
  LEFT JOIN dim_event_type de ON de.event_type   = s.raw_payload->'properties'->>'type'
)
INSERT INTO earthquake_event (
  event_id, time_utc, updated_utc, mag, place, felt, cdi, mmi, tsunami, title,
  magtype_id, alert_id, net_id, eventtype_id, properties
)
SELECT * FROM evts
ON CONFLICT (event_id) DO UPDATE
  SET
    updated_utc  = EXCLUDED.updated_utc,
    mag          = EXCLUDED.mag,
    place        = EXCLUDED.place,
    felt         = EXCLUDED.felt,
    cdi          = EXCLUDED.cdi,
    mmi          = EXCLUDED.mmi,
    tsunami      = EXCLUDED.tsunami,
    title        = EXCLUDED.title,
    magtype_id   = EXCLUDED.magtype_id,
    alert_id     = EXCLUDED.alert_id,
    net_id       = EXCLUDED.net_id,
    eventtype_id = EXCLUDED.eventtype_id,
    properties   = EXCLUDED.properties;

COMMIT;
