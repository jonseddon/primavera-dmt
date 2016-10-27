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
    if list_values:
        return [item for item in list_values if item ]
    else:
        return []


@register.filter
def to_comma_sep(list_values):
    """
    Removes any None, False or blank items from a list and then converts the
    list to a string of comma separated values.
    """
    default = '--'

    if list_values:
        actual_vals = [item for item in list_values if item]
        unique_vals = list(set(actual_vals))

        # remove duplicates and return
        if unique_vals:
            return ', '.join(unique_vals)
        else:
            return default
    else:
        return default