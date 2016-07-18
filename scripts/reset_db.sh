rm db/db.sqlite3
rm pdata_app/migrations/00*.py
python manage.py makemigrations pdata_app
python manage.py migrate
python manage.py createsuperuser
