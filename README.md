[![Build Status](https://travis-ci.org/PRIMAVERA-H2020/primavera-dmt.svg?branch=master)](https://travis-ci.org/PRIMAVERA-H2020/primavera-dmt)

# primavera-dmt
PRIMAVERA Data Management Tool

### Quick set-up

1. Load a Conda or virtualenv environment containing:   
   ```  
    django  
    django-filter  
    django-solo  
    django-tables2  
    iris  
    ```  
2. Create local settings:    
   ```  
   $ cp pdata_site/settings_local.py.tmpl pdata_site/settings_local.py  
    ```  
3. Django setup:  
    ```
   python manage.py makemigrations pdata_app
   python manage.py migrate
    ```
4. Run tests:
   ```
   python manage.py test
    ```
