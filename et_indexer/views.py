from django.shortcuts import render
from et_indexer.models import *
# Create your views here.



def view_datafiles(request):
    df = Datafile.objects.all()
    return render(request, 'et_indexer/datafiles.html', {'records':df,
                                              'page_title':'Data Files'})
    
def view_home(request):
    return render(request, 'et_indexer/home.html', {'request':request,
        'page_title': 'Elastic Tape Management System'})
    
def view_varquery(request):
    
    request_params=request.GET
    
    if not request_params:
        return render(request, 'et_indexer/variable_query.html', {
            'request':request,
            'page_title':"Variable Query"
        })
    
    var_name = request_params.get('var_name')
    
    if not var_name:
        msg="var_name not found"
        return render(request, 'et_indexer/variable_query.html', {
            'request':request,
            'page_title':"Variable Query",
            'message':msg
        })

    # there could be more than one
    n = Variable.objects.filter(var_name__icontains=var_name)

    var_occs = VariableOccurrence.objects.filter(variable=n)

    return render(request, "et_indexer/variable_query_results.html",{
        'request': request,
        'page_title': "{}: Variable Query Results".format(var_name),
        'var_occs': var_occs})
