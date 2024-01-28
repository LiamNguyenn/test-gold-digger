{{ config(materialized='view', alias='_v_sub_plan_grouping') }}

select 
  *,
  -- what feature set orgs have access to
  case when id in (
    4, -- Startup Premium
    7,	-- Free (30 days)
    11,	-- Free
    17, -- Demo
    43, -- CHURN (FREE)
    52, -- Implementations Free
    53, -- Startup Standard
    55, -- ANZ Free
    144, -- International Free
    145, -- Premium Trial
    161, -- SUSPENDED (FREE) 
    162, -- SEA free
    173 -- HR Free
  ) then 'free'
  when id in (
    166 -- ATS Free
    ) then 'free ats'
  when id in (
    6,	-- Standard (6)
    13, -- Standard (8)
    19, -- Standard (5)
    35, -- Standard + YY (3)
    36, -- Implementation Standard
    38, -- CSA Standard (5)
    39, -- CSA Standard + YY (3)
    48, -- EOFY CSA Standard (4)
    50, -- Implementations Standard YY
    61, -- YY Standard (1)
    63, -- GB Free Standard
    65, -- AMP Standard (Free)
    66, -- CSA Standard HeroPay 3
    68, -- HeroPay standard 3
    72, -- Reseller Standard
    106, -- Standard (6n)
    142, -- International Standard
    149, -- Standard (6) min 99
    160, -- Zuora Standard
    164, -- UK Organic Standard
    165 -- UK CSA Standard
  ) then 'standard'  
  when id in (
    14, -- CSA (8)
    15, -- Yin Yang
    20, -- Premium (8)
    21, -- Premium + YY (6)
    22, -- CSA (1.37)
    23, -- CSA (3)
    24, -- CSA (5)
    25, -- CSA (5.5)
    26, -- CSA (6)
    27, -- CSA (6.375)
    28, -- CSA (7)
    29, -- CSA (7.2)
    30, -- CSA (7.5)
    37, -- Implementation Premium
    40, -- CSA Premium (8)
    41, -- CSA Premium + YY (6)
    44, -- EOFY Premium (5)
    45, -- EOFY Premium + YY (3)
    46, -- Premium + YY (0)
    47, -- EOFY Premium + YY (2)
    49, -- CSA Premium (4)
    51, -- Implementations Premium YY
    56, -- ANZ Premium (Free)
    58, -- ANZ Premium (5)
    60, -- YY Premium (4)
    64, -- GB Free Premium
    67, -- CSA Premium HeroPay 5
    69, -- HeroPay Premium 5
    70, -- AON Premium (5)
    71, -- AON Premium (Free)
    73, -- Reseller Premium
    107, -- Premium (9n)
    140, -- AMP Premium (3)
    141, -- AON Premium (4)
    143, -- International Premium
    147, -- CSA Frank (9.11)
    150,  -- Premium (9) min 99
    152, -- Premium (9n)
    159, -- Zuora Premium
    168 -- HR Plus
  ) then 'premium'
  when id in (
    146, -- CSA Platinum (14)
    148, -- ANZ Platinum CSA
    151, -- Platinum (14) min 199
    153, -- OE Platinum CSA
    154, -- International Platinum
    158, -- Zuora Platinum
    167,  -- Reseller Platinum
    170 -- HR Ultimate
  ) then 'platinum'
  when id in (
    5, -- Premium (L)
    9, -- Annual
    10, -- Standard (L)
    18, -- OE
    163 -- Legacy
  ) then 'legacy'
  end as pricing_tier,
  case 
    when pricing_tier in ('free', 'free ats') then 0
    when pricing_tier = 'legacy' then 1
    when pricing_tier = 'standard' then 2
    when pricing_tier = 'premium' then 3
    when pricing_tier = 'platinum' then 4
  end as pricing_hierarchy,
  case when id in (
    5, -- Premium (L)
    6, -- Standard (6)   
    9, -- Annual 
    10, -- Standard (L)
    13, -- Standard (8)
    19, -- Standard (5)
    20, -- Premium (8)
    21, -- Premium + YY (6)
    35, -- Standard + YY (3)
    46, -- Premium + YY (0)
    55, -- ANZ Free
    56, -- ANZ Premium (Free)
    58, -- ANZ Premium (5)
    60, -- YY Premium (4)
    61, -- YY Standard (1)
    68, -- HeroPay standard 3
    69, -- HeroPay Premium 5
    70, -- AON Premium (5)
    71, -- AON Premium (Free)
    72, -- Reseller Standard
    73, -- Reseller Premium
    106, -- Standard (6n)
    107, -- Premium (9n)
    140, -- AMP Premium (3)
    141, -- AON Premium (4)
    145, -- Premium Trial
    149, -- Standard (6) min 99
    150, -- Premium (9) min 99
    151,  -- Platinum (14) min 199
    152,  -- Premium 9 min 99
    158, -- Zuora Platinum
    159, -- Zuora Premium
    160, -- Zuora Standard
    162, -- SEA free
    164, -- UK Organic Standard
    166, -- ATS Free
    167, -- Reseller Platinum
    168, -- HR Plus
    170, -- HR Ultimate
    173 -- HR Free
  ) then 'organic'
  when id in (
    14, -- CSA (8)
    15, -- Yin Yang
    18, -- OE
    22, -- CSA (1.37)
    23, -- CSA (3)
    24, -- CSA (5)
    25, -- CSA (5.5)
    26, -- CSA (6)
    27, -- CSA (6.375)
    28, -- CSA (7)
    29, -- CSA (7.2)
    30, -- CSA (7.5)
    36, -- Implementation Standard
    37, -- Implementation Premium
    38, -- CSA Standard (5)
    39, -- CSA Standard + YY (3)
    40, -- CSA Premium (8)
    41, -- CSA Premium + YY (6)
    44, -- EOFY Premium (5)
    45, -- EOFY Premium + YY (3)
    47, -- EOFY Premium + YY (2)
    48, -- EOFY CSA Standard (4)
    49, -- CSA Premium (4)
    50, -- Implementations Standard YY
    51, -- Implementations Premium YY
    52, -- Implementations Free
    63, -- GB Free Standard
    64, -- GB Free Premium
    65, -- AMP Standard (Free)
    66, -- CSA Standard HeroPay 3
    67, -- CSA Premium HeroPay 5
    142, -- International Standard
    143, -- International Premium
    144, -- International Free
    146, -- CSA Platinum (14)
    147, -- CSA Frank (9.11)
    148, -- ANZ Platinum CSA
    153, -- OE Platinum CSA
    154, -- International Platinum
    163, -- Legacy
    165 -- UK CSA Standard
  ) then 'csa'
  when id in (
    4, -- Startup Premium
    7, -- Free (30 days)
    11, -- Free
    17, -- Demo
    53 -- Startup Standard
  ) then 'demo'
  when id in (
    43 -- CHURN (FREE)
  ) then 'churn'
  when id in (
    161 -- SUSPENDED (FREE)
  ) then 'free'
end as pricing_type
from {{source('postgres_public', 'subscription_plans')}}
where not _fivetran_deleted