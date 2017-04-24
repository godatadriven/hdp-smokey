def logmethodcall(func):
    """
    This method will log a method call (format "Class.method (args) (kwargs)")
    """
    def wrapper(self, *args, **kwargs):
        self.logger.debug("Method call: {0}.{1}{2} {3}".format(self.__class__.__name__, func.__name__, args, kwargs))
        res = func(self, *args, **kwargs)
        return res
    return wrapper
