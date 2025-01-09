"""
Utility functions for property case
"""


def propcase(string):
    """
    Capitalize the first letter of each word in a string
    """
    return string[0].upper() + string[1:] if string else ""
