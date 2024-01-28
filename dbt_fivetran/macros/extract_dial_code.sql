{% macro extract_dial_code(phone_number) %}
    case 
        when position('+' in phone_number) > 0 then
            case 
                when position(' ' in phone_number) >= 2 then
                    substring(phone_number, 2, position(' ' in phone_number) - 2)
                when position('-' in phone_number) >= 2 then
                    substring(phone_number, 2, position('-' in phone_number) - 2)
                -- pakistan phone number
                when length(phone_number) = 13 then
                    substring(phone_number, 2, 2)
                -- australia phone number
                when length(phone_number) = 12 then
                    substring(phone_number, 2, 2)
                else null  
            end
        else null
    end 
{% endmacro %}
