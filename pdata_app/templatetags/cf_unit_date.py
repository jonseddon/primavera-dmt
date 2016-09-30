"""
Django filters to display the output of cf_units datetime like objects.
"""
from django import template

register = template.Library()

@register.filter
def strftime(value, arg):
    """
    Calls an object's strftime function.
    """
    if value:
        return value.strftime(arg)
    else:
        return None

