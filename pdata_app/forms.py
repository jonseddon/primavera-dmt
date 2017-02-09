from django import forms

from .models import DataSubmission, RetrievalRequest


class CreateSubmissionForm(forms.ModelForm):
    class Meta:
        model = DataSubmission
        fields = ('incoming_directory',)
        widgets = {'incoming_directory': forms.TextInput(attrs={
            'class': 'form-control', 'autofocus': ''})}


class CreateRetrievalForm(forms.ModelForm):
    class Meta:
        model = RetrievalRequest
        fields = ('data_request',)
        widgets = {'data_request': forms.TextInput(attrs={
            'class': 'form-control', 'autofocus': ''})}
