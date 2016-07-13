from .base import CodeList

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
         ('3hr', '3hr'))
)


ONLINE_STATUS = CodeList(
        (('online', 'online'),
         ('offline', 'offline'),
         ('partial', 'partial'))
)

CHECKSUM_TYPES = CodeList(
        (('SHA256', 'SHA256'),
         ('MD5', 'MD5'))
)

