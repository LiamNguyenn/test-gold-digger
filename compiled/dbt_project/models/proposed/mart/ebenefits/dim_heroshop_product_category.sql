select
    id                                                                       as dim_heroshop_product_category_id,
    name                                                                     as product_category,
    coalesce(name similar to 'Giftcard%|%Groceries%', FALSE)                 as is_evoucher,
    coalesce(name ~ 'Movie tickets', FALSE)                                  as is_movie_tickets,
    coalesce(name not similar to 'Movie tickets|Giftcard%|Groceries', FALSE) as is_dropship_products,
    case
        when name similar to 'Giftcard%|%Groceries%' then 'evoucher'
        when name ~ 'Movie tickets' then 'movie_tickets'
        when name not similar to 'Movie tickets|Giftcard%|Groceries' then 'dropship_products' else 'unknown'
    end                                                                      as product_type
from "dev"."staging"."stg_heroshop_db_public__product_categories"