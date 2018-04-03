from .base import CodeList

import cf_units

STATUS_VALUES = CodeList(
        (('EXPECTED', 'EXPECTED'),
         ('PENDING_PROCESSING', 'PENDING_PROCESSING'),
         ('ARRIVED', 'ARRIVED'),
         ('VALIDATED', 'VALIDATED'),
         ('ARCHIVED', 'ARCHIVED'),
         ('PUBLISHED', 'PUBLISHED'),
         ('REJECTED', 'REJECTED'))
)


ESGF_STATUSES = CodeList(
    (('CREATED', 'CREATED'),
     ('SUBMITTED', 'SUBMITTED'),
     ('AT_CEDA', 'AT_CEDA'),
     ('PUBLISHED', 'PUBLISHED'),
     ('REJECTED', 'REJECTED'),
     ('NEEDS_FIX', 'NEEDS_FIX'),
     ('FILES_MISSING', 'FILES_MISSING'))
)


FREQUENCY_VALUES = CodeList(
        (('ann', 'ann'),
         ('mon', 'mon'),
         ('day', 'day'),
         ('6hr', '6hr'),
         ('3hr', '3hr'),
         ('1hr', '1hr'),
         ('subhr', 'subhr'),
         ('fx', 'fx'))
)


ONLINE_STATUS = CodeList(
        (('online', 'online'),
         ('offline', 'offline'),
         ('partial', 'partial'))
)


CHECKSUM_TYPES = CodeList(
        (('SHA256', 'SHA256'),
         ('MD5', 'MD5'),
         ('ADLER32', 'ADLER32'))
)


VARIABLE_TYPES = CodeList(
        (('real', 'real'),
         ('float', 'float'),
         ('None', 'None'),
         ('', ''))
)


CALENDARS = CodeList(
    (('360_day', cf_units.CALENDAR_360_DAY),
     ('365_day', cf_units.CALENDAR_365_DAY),
     ('gregorian', cf_units.CALENDAR_GREGORIAN))
)
