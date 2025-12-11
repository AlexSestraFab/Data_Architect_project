CREATE OR REPLACE VIEW dm_dashboard_main AS
SELECT

    d.declaration_hash, 
    p.person_hash,
    sd.year,

    sp.name, 
    sp.state_agency_short as ministry,
    sp.position, 
    sp.gender,
    sp.married,
    sp.children,

    sd.income_month_const_чиновник as income_official, 
    sd.income_month_const_супруга as income_spouse,
    sd.income_month_const_ребенок as income_child,

    COALESCE(SUM(
        CASE  
            WHEN own.meters ~ '^[0-9\.]+$' 
            THEN CAST(own.meters AS NUMERIC)
            ELSE 0
        END), 0) as total_own_m2,

    COUNT(DISTINCT own.asset_hash) as own_properties_count,

    COUNT(DISTINCT CASE 
            WHEN own.country IS NOT NULL
            AND own.country != 'Россия'
            AND own.country != 'Российская Федерация'
            THEN own.asset_hash
        END) as foreign_properties,

    COALESCE(SUM(
        CASE 
            WHEN use.use_meters ~ '^[0-9\.]+$'
            THEN CAST(use.use_meters AS NUMERIC)
            ELSE 0
        END), 0) as total_use_m2,

    COUNT(DISTINCT car.asset_hash) as car_count, 

    sd.extra, 
    sd.coef, 

    CASE 
        WHEN sd.income_month_const_чиновник > 543101 THEN 'high'
        WHEN sd.income_month_const_чиновник > 156730 THEN 'medium'
        ELSE 'low'
    END as income_category,

    d.load_date as declaration_load_date

FROM h_declaration d

JOIN l_person_declaration lpd ON d.declaration_hash = lpd.declaration_hash 
JOIN h_person p ON lpd.person_hash = p.person_hash

JOIN s_declaration sd ON d.declaration_hash = sd.declaration_hash
    AND sd.load_date = (
        SELECT MAX(load_date)
        FROM s_declaration sd2
        WHERE sd2.declaration_hash = d.declaration_hash
    )

JOIN s_person sp ON p.person_hash = sp.person_hash 
    AND sp.load_date = (
        SELECT MAX(load_date) 
        FROM s_person sp2 
        WHERE sp2.person_hash = p.person_hash 
    )

LEFT JOIN l_declaration_asset lda ON d.declaration_hash = lda.declaration_hash 
LEFT JOIN h_asset ha ON lda.asset_hash = ha.asset_hash

LEFT JOIN s_asset_own_realty own ON ha.asset_hash = own.asset_hash 
    AND own.load_date = (
        SELECT MAX(load_date)
        FROM s_asset_own_realty own2
        WHERE own2.asset_hash = ha.asset_hash
    )

LEFT JOIN s_asset_use_realty use ON ha.asset_hash = use.asset_hash 
    AND use.load_date = (
        SELECT MAX(load_date)
        FROM s_asset_use_realty use2
        WHERE use2.asset_hash = ha.asset_hash
    )

LEFT JOIN s_asset_car car ON ha.asset_hash = car.asset_hash 
    AND car.load_date = (
        SELECT MAX(load_date)
        FROM s_asset_car car2
        WHERE car2.asset_hash = ha.asset_hash
    )

GROUP BY 
    d.declaration_hash,
    p.person_hash, 
    sd.year,
    sp.name,
    sp.state_agency_short,
    sp.position,
    sp.gender, 
    sp.married,
    sp.children,
    sd.income_month_const_чиновник,
    sd.income_month_const_супруга,
    sd.income_month_const_ребенок,
    sd.extra,
    sd.coef,
    d.load_date;