class Task(object):
    ALLOWED_TYPES = ("do", "undo")

    def __init__(self, dataset, action_type):
        self.dataset = dataset
        if action_type not in Task.ALLOWED_TYPES:
            raise Exception("Direction must be one of: %s, not '%s'." % (str(Task.ALLOWED_TYPES), action_type))

        self.action_type = action_type

    def __repr__(self):
        return self.__unicode__()

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return "TASK: Dataset=%s ; Action Type=%s ;" % (self.dataset, self.action_type)