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
    return value.strftime(arg)

