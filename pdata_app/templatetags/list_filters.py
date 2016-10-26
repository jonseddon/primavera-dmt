"""
Django filters to operate on lists
"""
from django import template

register = template.Library()

@register.filter
def item_default(list_values, arg):
    """
    Loops over a list and replaces any values that are None, False or blank
    with arg.
    """
    for index, value in enumerate(list_values):
        if not value:
            list_values[index] = arg

    # remove duplicates and return
    return list(set(list_values))
