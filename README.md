# crepe
CEDA REceive-to-publish PipelinE

Quick set-up

 1. Make a virtualenv and activate it:
 `$ virtualenv-2.7 venv`
 `$ . venv/bin/activate` 


 2. Install dependencies:


 2. Create local settings:
   crepe_site/settings_local.py

 1. Django setup:
  python manage.py makemigrations crepe_app
  python manage.py migrate


 3. Run tests:
   python test/test_workflows.py
