from .base import CodeList

PROCESSING_STATUS_VALUES = CodeList(
        (('COMPLETED', 'COMPLETED'),
         ('IN_PROGRESS', 'IN_PROGRESS'),
         ('PAUSED', 'PAUSED'))
)

STATUS_VALUES = CodeList(
        (('EMPTY', 'EMPTY'),
         ('DONE', 'DONE'),
         ('PENDING_DO', 'PENDING_DO'),
         ('DOING', 'DOING'),
         ('PENDING_UNDO', 'PENDING_UNDO'),
         ('UNDOING', 'UNDOING'))
)

ACTION_TYPES = CodeList(
        (('DO', 'DO'),
         ('UNDO', 'UNDO'),
         ('CLEANUP', 'CLEANUP'))
)

CHECKSUM_TYPES = CodeList(
        (('SHA256', 'SHA256'),
         ('MD5', 'MD5'))
)