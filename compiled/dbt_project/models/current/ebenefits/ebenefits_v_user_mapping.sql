

select distinct
    (case when json_extract_path_text(detail, 'ehUUId')= '' then null else json_extract_path_text(detail, 'ehUUId') end) as eh_user_uuid
    , (case when json_extract_path_text(detail, 'kpId')= '' then null else json_extract_path_text(detail, 'kpId') end) as kp_id
    , (case when json_extract_path_text(detail, 'eBenUUId')= '' then null else json_extract_path_text(detail, 'eBenUUId') end) as eben_uuid
    , (case when json_extract_path_text(detail, 'emailAddress')= '' then null else json_extract_path_text(detail, 'emailAddress') end) as email
from 
    "dev"."ebenefits"."user_created"