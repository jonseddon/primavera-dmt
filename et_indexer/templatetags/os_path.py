"""
Django filters to generate filters from some of  the functions in os.path.
"""
import os.path

from django import template

register = template.Library()


@register.filter
def basename(value):
    """
    Returns the basename (file name of a complete path)
    """
    return os.path.basename(value)


@register.filter
def dirname(value):
    """
    Returns the directory name of a complete path
    """
    return os.path.dirname(value)
