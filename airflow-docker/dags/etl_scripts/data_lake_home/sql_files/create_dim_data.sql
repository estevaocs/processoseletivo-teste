CREATE TABLE dim_calendario (
    sk_data SERIAL PRIMARY KEY,
    data DATE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,
    nome_dia_semana VARCHAR(20) NOT NULL,
    nome_mes VARCHAR(20) NOT NULL
);

INSERT INTO dim_calendario (data, ano, mes, dia, dia_semana, nome_dia_semana, nome_mes)
SELECT
    generate_series('2023-01-01'::DATE, '2023-12-31'::DATE, '1 day'::INTERVAL)::DATE,
    EXTRACT(YEAR FROM generate_series),
    EXTRACT(MONTH FROM generate_series),
    EXTRACT(DAY FROM generate_series),
    EXTRACT(DOW FROM generate_series) + 1,
    TO_CHAR(generate_series, 'Day'),
    TO_CHAR(generate_series, 'Month')
FROM generate_series('2023-01-01'::DATE, '2023-12-31'::DATE, '1 day'::INTERVAL);
