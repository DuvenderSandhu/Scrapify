import phonenumbers
from phonenumbers import PhoneNumberType

def detect_number_type(phone_number_str):
    try:
        # Parse the phone number, here "US" is the region code for United States
        phone_number = phonenumbers.parse(phone_number_str, "US")
        
        # Check if the number is valid
        if not phonenumbers.is_valid_number(phone_number):
            return False # "Invalid number"

        # Determine the type of the phone number
        number_type = phonenumbers.number_type(phone_number)
        
        if number_type == PhoneNumberType.MOBILE:
            console.log("Mobile Numbe is Valid ",phone_number)
            return True # "Mobile Number"
        elif number_type == PhoneNumberType.FIXED_LINE:
            return False # "Landline Number"
        elif number_type == PhoneNumberType.FIXED_LINE_OR_MOBILE:
            return True # "Landline or Mobile Number (uncertain)"
        else:
            return False # "Other Type of Number"

    except phonenumbers.phonenumberutil.NumberParseException:
        return False # "Invalid phone number format"

def filter_valid_numbers(phone_number_list):
    # print("here")
    valid_numbers = []
    for number in phone_number_list:
        # print(number)
        response = detect_number_type(number)
        print(response)
        if response:
            valid_numbers.append(number)
    return valid_numbers