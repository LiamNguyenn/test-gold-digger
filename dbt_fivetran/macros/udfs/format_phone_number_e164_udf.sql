{% macro format_phone_number_e164_udf() %}

{#
    Create a UDF to format a phone number to E164 format
#}
create schema if not exists {{ target.schema }};

create or replace function {{ target.schema }}.format_phone_number_e164 (phone_number_str text, country_code text) returns text immutable as $$ 
	import phonenumbers
 	
 	try:
 	    phone_number = phonenumbers.parse(phone_number_str, country_code)
 	    if phonenumbers.is_valid_number(phone_number):
 	    	return str(phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.E164))
 	except:
 		pass
 	return None
$$ LANGUAGE plpythonu;

{% endmacro %}
