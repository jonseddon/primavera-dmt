from django.shortcuts import render

# Create your views here.
from django.shortcuts import render_to_response

from crepe_app.models import *


def view_files(request):
    records = File.objects.all()
    return render_to_response("files.html", {'request': request,
                                            'records': records})

def view_chains(request):
    records = Chain.objects.all()
    return render_to_response("chains.html", {'request': request,
                                            'records': records})

def view_home(request):
    return render_to_response("home.html", {'request': request})