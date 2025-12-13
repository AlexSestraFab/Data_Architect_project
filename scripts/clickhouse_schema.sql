CREATE TABLE IF NOT EXISTS dm_dashboard_main (
    declaration_hash String,
    person_hash String,
    year Int16,
    name String,
    ministry String,
    position String,
    gender String,
    married String,
    children String,
    income_official Float64,
    income_spouse Float64,
    income_child Float64,
    total_own_m2 Float64,
    own_properties_count Int32,
    foreign_properties Int32,
    total_use_m2 Float64,
    car_count Int32,
    extra String,
    coef Float64,
    income_category String,
    declaration_load_date DateTime
) ENGINE = MergeTree()
ORDER BY (year, ministry, position);