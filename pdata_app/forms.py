from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, ButtonHolder, Submit

from .models import DataSubmission, DataRequest

class CreateSubmissionForm(forms.ModelForm):

    class Meta:
        model = DataSubmission
        fields = ('incoming_directory',)
        widgets = {'incoming_directory': forms.TextInput(attrs={
            'class': 'form-control', 'autofocus': ''})}


class DataRequestFormHelper(FormHelper):
    model = DataRequest
    fields = ('project')
    form_tag = False
    layout = Layout('name', ButtonHolder(
        Submit('submit', 'Filter', css_class='button white right')
    ))


# class DataRequestFormHelper(forms.ModelForm):
#     class Meta:
#         model = DataRequest
#         fields = ('institute', 'experiment')
#
#     def __init__(self, *args, **kwargs):
#         super(DataRequestFormHelper, self).__init__(*args, **kwargs)
#         self.helper = FormHelper(self)