from django.shortcuts import render

from .models import Datafile, VariableOccurrence
from .tables import VariableOccurrenceTable
from .filters import VariableOccurrenceFilter
from .utils.table_views import PagedFilteredTableView


class VariableOccurrenceList(PagedFilteredTableView):
    model = VariableOccurrence
    table_class = VariableOccurrenceTable
    filter_class = VariableOccurrenceFilter
    page_title = 'Variable Occurrences'


def view_datafiles(request):
    df = Datafile.objects.all()
    return render(request, 'et_indexer/datafiles.html', {'records':df,
                                              'page_title':'Data Files'})
    

def view_home(request):
    return render(request, 'et_indexer/home.html', {'request':request,
        'page_title': 'Elastic Tape Management System'})
