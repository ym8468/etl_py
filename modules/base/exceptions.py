class FileConfigMissing(Exception):
     def __init__(self, value):
        self.value = value

     def __str__(self):
        return repr(self.value)

class MandatoryOptionMissing(Exception):
     def __init__(self, value):
        self.value = value

     def __str__(self):
        return repr(self.value)

# Used in modules/teradata/db.py
class ReleaseTableMissing(Exception):
     def __init__(self, value):
        self.value   = value
        self.message = value

     def __str__(self):
        return repr(self.value)
