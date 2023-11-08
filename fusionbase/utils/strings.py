import re

def is_id(input_string: str) -> bool:
    """
    Check if the input string is formatted like an ID hash (32 hexadecimal characters).
    
    Args:
    input_string (str): The string to check.

    Returns:
    bool: True if the string is in MD5 format, False otherwise.
    """
    # Regular expression to match a 32-character hexadecimal string
    id_format_pattern = re.compile(r'^[a-f0-9]{32}$', re.IGNORECASE)

    # Use fullmatch to check the whole string against the pattern
    return bool(id_format_pattern.fullmatch(input_string))