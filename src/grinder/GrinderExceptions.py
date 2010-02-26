from exceptions import Exception

class GrinderException(Exception):
    pass

class NoChannelLabelException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "No channel label was specified"

class BadSystemIdException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "Unable to authenticate systemid, please ensure your system is registered to RHN"

class CantActivateException(GrinderException):
    def __init__(self):
        return
    def __str__(self):
        return "Your system can not be activated to synch content from RHN Hosted"