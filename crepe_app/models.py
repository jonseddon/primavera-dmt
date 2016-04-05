import os
from datetime import datetime
from django.utils import timezone
from django.db import models
from django.db.models.signals import post_save

from vocabs import PROCESSING_STATUS_VALUES, STATUS_VALUES, ACTION_TYPES, CHECKSUM_TYPES

# NOTES:
# - We'll need to use: on_delete=models.SET_NULL in some cases
#   to avoid cascading deletion of objects.

__all__ = ['Chain', 'Dataset', 'ProcessStage', 'Status', 'ProcessStageInChain',
           'File', 'Symlink', 'Event', 'Checksum', 'Settings']


"""
For a given controller:
 1. Set the Chain
 2. Set the ProcessStage
 3. Set the processing status of that ProcessStage

 4. In each controller
        if is_withdrawn(dataset) == False:
            if status(previous) == "DONE" and status(current) == "EMPTY":
                set_current_status("DOING")
                do(current)
            else:
                if status(next) == "EMPTY" and status(current) == "DONE":
                    set_current_status("UNDOING")
                    undo(current)




"""

class Chain(models.Model):
    # The "scheme" that is being used, e.g.: "MOHC-MASS", "SPECS", or "CMIP6-General"
    name = models.CharField(max_length=30, null=False, blank=False)
    completed_externally = models.BooleanField(null=False)  # Defines whether the

    # last Controller in chain sets is_completed on Dataset

    def __unicode__(self):
        return self.name


class Dataset(models.Model):
    incoming_dir = models.CharField(max_length=300, blank=True, null=True)
    name = models.CharField(unique=True, max_length=300, blank=False, null=False)
    chain = models.ForeignKey(Chain, null=False)
    arrival_time = models.DateTimeField(blank=False, null=False)
    processing_status = models.CharField(max_length=11, choices=PROCESSING_STATUS_VALUES.items(),
                                         null=False, blank=False)
    is_withdrawn = models.BooleanField(default=False, null=False)

    def __unicode__(self):
        return self.name

    @staticmethod
    def _set_empty_statuses(sender, instance, **kwargs):
        for psic in instance.chain.processstageinchain_set.all():
            print "DETAIL:", instance.name, psic.process_stage, STATUS_VALUES.EMPTY
            status = Status.objects.create(dataset=instance, process_stage=psic.process_stage,
                                           status_value=STATUS_VALUES.EMPTY)
            status.save()


post_save.connect(Dataset._set_empty_statuses, Dataset, dispatch_uid="crepe_app.models.Dataset")


class ProcessStage(models.Model):
    # E.g.: "QC-ceda-cc", "Create-Mapfiles", etc
    name = models.CharField(unique=True, max_length=50, null=False, blank=False)

    def __unicode__(self):
        return "Process Stage using: %s" % self.name

class Status(models.Model):
    # Connects Dataset and ProcessStage
    dataset = models.ForeignKey(Dataset, null=False)
    process_stage = models.ForeignKey(ProcessStage, null=False)
    status_value = models.CharField(max_length=7, choices=STATUS_VALUES.items(),
                                    null=False, blank=False)
    start_time = models.DateTimeField(default=timezone.now, null=False)
    end_time = models.DateTimeField(default=timezone.now, null=False)

    def __unicode__(self):
        return "%s:%s:%s" % (self.dataset.name, self.process_stage.name, self.status_value)

    class Meta:
        verbose_name_plural = 'Statuses'


class ProcessStageInChain(models.Model):
    # Mapping class to connect a set of ProcessStages to Chains
    # without binding them directly
    process_stage = models.ForeignKey(ProcessStage, null=False)
    chain = models.ForeignKey(Chain, null=False)
    position = models.IntegerField(null=False)

    class Meta:
        unique_together = ('process_stage', 'chain')
        verbose_name_plural = "Process Stages in Chain"


class File(models.Model):
    # RFKs: Symlink, Checksum
    name = models.CharField(max_length=200, null=False, blank=False)
    directory = models.CharField(max_length=300, null=False, blank=False)
    # This is where the file is NOW
    size = models.BigIntegerField(null=False)
    dataset = models.ForeignKey(Dataset, null=False)

    def __unicode__(self):
        return "%s (dir=%s)" % (self.name, self.directory)

    class Meta:
        unique_together = ('name', 'directory')
        verbose_name = "Size (in bytes)"


class Symlink(models.Model):
    location = models.CharField(unique=True, max_length=500, null=False, blank=False)
    file = models.ForeignKey(File, null=False)

    def __unicode__(self):
        dr, fname = os.path.split(self.location)
        return "Symlink: %s (dir=%s)" % (fname, dr)

    class Meta:
        verbose_name_plural = 'Symbolic Links'


class Checksum(models.Model):
    id = models.IntegerField(primary_key=True)
    file = models.ForeignKey(File, null=False, blank=False)
    checksum_value = models.CharField(max_length=200, null=False, blank=False)
    checksum_type = models.CharField(max_length=6, choices=CHECKSUM_TYPES.items(), null=False,
                                     blank=False)

    def __unicode__(self):
        return "%s: %s (%s)" % (self.checksum_type, self.checksum_value,
                                self.file.name)


class Event(models.Model):
    # Written each time a Worker completes a Task (whether successful or not)
    dataset = models.ForeignKey(Dataset, null=False)
    process_stage = models.ForeignKey(ProcessStage, null=False)
    message = models.CharField(max_length=500, null=False, blank=False)
    action_type = models.CharField(max_length=7, choices=ACTION_TYPES.items(), null=False, blank=False)
    succeeded = models.BooleanField(default=True, null=False)
    date_time = models.DateTimeField(default=timezone.now, null=False)

    def __unicode__(self):
        return "%s (%s:%s)" % (self.message[:60], self.dataset.name,
                               self.process_stage.name)


class Settings(models.Model):
    # NOTE: THIS IS NOT A GOOD WAY TO STORE A SINGLE FIELD
    is_paused = models.BooleanField(default=False, null=False)

    class Meta:
        verbose_name_plural = "Settings"
