{{ config(materialized='view') }}

select
    location,
    -- case 
    --     when lower(location) like '%ikate%' then 'Ikate'
    --     when lower(location) like '%lekki phase 1%' then 'Lekki Phase 1'
    --     when lower(location) like '%ikoyi%' then 'Ikoyi'
    --     when lower(location) like '%ikota%' then 'Ikota'
    --     when lower(location) like '%ajah%' then 'Ajah'
    --     when lower(location) like '%ogba%' then 'Ogba'
    --     when lower(location) like '%oshodi%' then 'Oshodi'
    --     when lower(location) like '%chevron%' then 'Chevron'
    --     when lower(location) like '%agungi%' then 'Agungi'
    --     when lower(location) like '%oniru%' then 'Oniru'
    --     when lower(location) like '%victoria island%' then 'Victoria Island'
    --     when lower(location) like '%banana island%' then 'Banana Island'
    --     when lower(location) like '%yaba%' then 'Yaba'
    --     when lower(location) like '%sangotedo%' then 'Ajah'
    --     when lower(location) like '%ikeja gra%' then 'Ikeja'
    --     when lower(location) like '%isheri%' then 'Isheri'
    --     when lower(location) like '%osapa%' then 'Osapa'
    --     when lower(location) like '%ikeja%' then 'Ikeja'
    --     when lower(location) like '%maryland%' then 'Maryland'
    --     when lower(location) like '%lafiaji%' then 'Lafiaji'
    --     when lower(location) like '%orchid%' then 'Orchid'
    --     when lower(location) like '%vgc%' then 'VGC'
    --     when lower(location) like '%ologolo%' then 'Ologolo'
    --     when lower(location) like '%jakande lekki%' then 'Jakande'
    --     when lower(location) like '%gbagada%' then 'Gbagada'
    --     when lower(location) like '%conservation%' then 'Conservation'
    --     when lower(location) like '%shomolu%' then 'Shomolu'
    --     when lower(location) like '%carlton%' then 'Chevron'
    --     when lower(location) like '%egbeda%' then 'Egbeda'
    --     when lower(location) like '%admiralty%' then 'Lekki Phase 1'
    --     when lower(location) like '%lakowe%' then 'Lakowe'
    --     when lower(location) like '%magodo%' then 'Magodo'
    --     when lower(location) like '%lekki%' then 'Lekki'
    --     end as neighborhood,
    case 
        when lower(location) like '%lekki%' then 'Eti-Osa'
        when lower(location) like '%ikoyi%' then 'Eti-Osa'
        when lower(location) like '%ajah%' then 'Eti-Osa'
        when lower(location) like '%ikeja%' then 'Ikeja'
        when lower(location) like '%ojodu%' then 'Ikeja'
        when lower(location) like '%island%' then 'Eti-Osa'
        when lower(location) like '%alimosho%' then 'Alimosho'
        when lower(location) like '%gbagada%' then 'Kosofe'
        when lower(location) like '%ikosi%' then 'Epe'
        when lower(location) like '%surulere%' then 'Surulere'
        when lower(location) like '%ikorodu%' then 'Ikorodu'
        when lower(location) like '%ipaja%' then 'Alimosho'
        when lower(location) like '%yaba%' then 'Lagos Mainland'
        when lower(location) like '%igando%' then 'Alimosho'
        when lower(location) like '%isolo%' then 'Oshodi-Isolo'
        when lower(location) like '%egba%' then 'Alimosho'
        when lower(location) like '%ogba%' then 'Ifako-Ijaiye'
        when lower(location) like '%maryland%' then 'Kosofe'
        when lower(location) like '%ogudu%' then 'Kosofe'
        when lower(location) like '%okota%' then 'Oshodi-Isolo'
        when lower(location) like '%agege%' then 'Agege'
        when lower(location) like '%oshodi%' then 'Oshodi-Isolo'
        when lower(location) like '%sangotedo%' then 'Eti-Osa'
        when lower(location) like '%shomolu%' then 'Shomolu'
        when lower(location) like '%odofin%' then 'Amuwo-Odofin'
        when lower(location) like '%ilupeju%' then 'Mushin'
        when lower(location) like '%ketu%' then 'Kosofe'
        when lower(location) like '%idimu%' then 'Alimosho'
        when lower(location) like '%iju%' then 'Ifako-Ijaiye'
        when lower(location) like '%epe%' then 'Epe'
        when lower(location) like '%ojota%' then 'Kosofe'
        when lower(location) like '%ejigbo%' then 'Oshodi-Isolo'
        when lower(location) like '%bariga%' then 'Shomolu'
        when lower(location) like '%apapa%' then 'Apapa'
        when lower(location) like '%ojo%' then 'Ojo'
        when lower(location) like '%badagry%' then 'Badagry'
        when lower(location) like '%mushin%' then 'Mushin'
        when lower(location) like '%orile%' then 'Agege'
        when lower(location) like '%isheri%' then 'Kosofe'
        when lower(location) like '%magodo%' then 'Kosofe'
        when lower(location) like '%ibeju%' then 'Ibeju-Lekki'
        end as lga,
    status,
    CAST(bedrooms AS INT64) AS bedrooms,
    CAST(bathrooms AS INT64) AS bathrooms,
    CAST(toilets AS INT64) AS toilets,
    property_type,
    is_furnished,
    is_serviced,
    is_shared,
    cast(split(replace(total_area, ",", ""), " ")[offset(0)] AS float64) AS total_area, -- to get the figure before 'sqm'
    cast(split(replace(covered_area,",",""), " ")[offset(0)] AS float64) AS covered_area,
    cast(price AS float64) AS price,
    currency,
    'rent' as listing_type,
from {{ source('raw', 'lagos_for_rent_listings_raw') }}
where 
    price not like '%N/A%' 
    and 
    lower(location) like '%lagos%' 
    and 
    lower(property_type) not like '%land%' 
  