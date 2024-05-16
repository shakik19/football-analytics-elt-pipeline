SELECT 
    `name`,
    `alpha-2` AS alpha_2,
    `alpha-3` AS alpha_3,
    `country-code` AS country_code,
    `iso_3166-2` AS iso_3166_2,
    `region`,
    `sub-region` AS sub_region,
    `intermediate-region` AS intermediate_region,
    `region-code` AS region_code,
    `sub-region-code` AS sub_region_code,
    `intermediate-region-code` AS intermediate_region_code  
FROM {{ ref('country_codes') }}