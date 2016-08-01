# primavera-dmt
PRIMAVERA Data Management Tool

Quick set-up

 1. Make a virtualenv and activate it:

 `$ virtualenv-2.7 venv`

 `$ . venv/bin/activate` 


 1. Install dependencies:

 `$ pip install -r requirements.txt`

 1. Create local settings:

 `$ cp pdata_site/settings_local.py`

 1. Django setup:

 `python manage.py makemigrations pdata_app`

 `python manage.py migrate`

 1. Run tests:

 `python manage.py test`
