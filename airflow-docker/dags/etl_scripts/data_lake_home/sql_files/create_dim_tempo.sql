CREATE TABLE IF NOT EXISTS dim_horas (
    sk_hora_dia SERIAL PRIMARY KEY,
    hora_do_dia TIME NOT NULL
);

INSERT INTO dim_horas (hora_do_dia)
SELECT DISTINCT
    ('00:00'::TIME + MAKE_INTERVAL(mins := seq))::TIME AS hora
FROM generate_series(0, 24 * 60 - 1) AS seq;
