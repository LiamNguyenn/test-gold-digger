


create schema if not exists ci;

create or replace function ci.format_phone_number_e164 (phone_number_str text, country_code text) returns text immutable as $$ 
	import phonenumbers
 	
 	try:
 	    phone_number = phonenumbers.parse(phone_number_str, country_code)
 	    if phonenumbers.is_valid_number(phone_number):
 	    	return str(phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.E164))
 	except:
 		pass
 	return None
$$ LANGUAGE plpythonu;

