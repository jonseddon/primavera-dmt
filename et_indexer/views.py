from django.shortcuts import render

from .models import Datafile, VariableOccurrence
from .tables import VariableOccurrenceTable, DatafileTable
from .filters import VariableOccurrenceFilter, DatafileFilter
from .utils.table_views import PagedFilteredTableView


class VariableOccurrenceList(PagedFilteredTableView):
    model = VariableOccurrence
    table_class = VariableOccurrenceTable
    filter_class = VariableOccurrenceFilter
    page_title = 'Variable Occurrences'


class DatafileList(PagedFilteredTableView):
    model = Datafile
    table_class = DatafileTable
    filter_class = DatafileFilter
    page_title = 'Data Files'


def view_home(request):
    return render(request, 'et_indexer/home.html', {'request':request,
        'page_title': 'Elastic Tape Management System'})
