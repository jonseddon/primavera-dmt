from django import forms

from .models import DataSubmission, RetrievalRequest


class CreateSubmissionForm(forms.ModelForm):
    class Meta:
        model = DataSubmission
        fields = ('incoming_directory',)
        widgets = {'incoming_directory': forms.TextInput(attrs={
            'class': 'form-control', 'autofocus': ''})}
