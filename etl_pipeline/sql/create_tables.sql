CREATE TABLE IF NOT EXISTS dim_country(
    country_id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    iso3_code CHAR(3),
    m49_code VARCHAR(10),
    region VARCHAR(50),
    sub_region VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS dim_technology(
    technology_id SERIAL PRIMARY KEY,
    technology VARCHAR(100) NOT NULL,
    sub_technology VARCHAR(100),
    group_technology VARCHAR(100),
    renewable_or_not VARCHAR(20)
);


CREATE TABLE IF NOT EXISTS dim_time(
    time_id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    decade INTEGER
);


CREATE TABLE IF NOT EXISTS dim_producer(
    producer_id SERIAL PRIMARY KEY,
    producer_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_energy_generation(
    fact_id SERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES dim_country(country_id),
    technology_id INTEGER REFERENCES dim_technology(technology_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    producer_id INTEGER REFERENCES dim_producer(producer_id),

    electricity_generation_gwh NUMERIC(12,2),
    electricity_installed_capacity_mw NUMERIC(12,2),
    heat_generation_tj NUMERIC(12,2),
    total_public_flows_usd_m NUMERIC(12,2),
    international_public_flows_usd_m NUMERIC(12,2),
    capacity_per_capita_w NUMERIC(10,2)
);
