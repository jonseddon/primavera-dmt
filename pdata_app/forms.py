from django import forms
from django.contrib.auth.forms import PasswordChangeForm

from .models import DataSubmission


class CreateSubmissionForm(forms.ModelForm):
    class Meta:
        model = DataSubmission
        fields = ('incoming_directory',)
        widgets = {'incoming_directory': forms.TextInput(attrs={
            'class': 'form-control', 'autofocus': ''})}


class PasswordChangeBootstrapForm(PasswordChangeForm):
    def __init__(self, user, *args, **kwargs):
        super(PasswordChangeBootstrapForm, self).__init__(user,*args, **kwargs)
        for field in self.fields:
            self.fields[field].widget.attrs['class'] = 'form-control'