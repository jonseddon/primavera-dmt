from .base import CodeList

PROCESSING_STATUS_VALUES = CodeList(
        (('COMPLETED', 'COMPLETED'),
         ('IN_PROGRESS', 'IN_PROGRESS'),
         ('NOT_STARTED', 'NOT_STARTED'),
         ('PAUSED', 'PAUSED'))
)

STATUS_VALUES = CodeList(
        (('EMPTY', 'EMPTY'),
         ('DONE', 'DONE'),
         ('PENDING_DO', 'PENDING_DO'),
         ('DOING', 'DOING'),
         ('PENDING_UNDO', 'PENDING_UNDO'),
         ('UNDOING', 'UNDOING'),
         ('FAILED', 'FAILED'))
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