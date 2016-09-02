from .base import CodeList

import cf_units

STATUS_VALUES = CodeList(
        (('EXPECTED', 'EXPECTED'),
         ('ARRIVED', 'ARRIVED'),
         ('VALIDATED', 'VALIDATED'),
         ('ARCHIVED', 'ARCHIVED'),
         ('PUBLISHED', 'PUBLISHED'))
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
         ('None', 'None'),
         ('', ''))
)


CALENDARS = CodeList(
    (('360_day', cf_units.CALENDAR_360_DAY),
     ('365_day', cf_units.CALENDAR_365_DAY),
     ('gregorian', cf_units.CALENDAR_GREGORIAN))
)