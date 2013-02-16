import copy


class ImmutableObject(object):
    _io_locked = False

    def __init__(self, **kwargs):
        self._io_attrs = kwargs
        self._io_locked = True

    def __getattr__(self, k):
        ## TODO: make child objects gain
        ## immutability by mutation
        if k.startswith('_io_'):
            return super(ImmutableObject, self).__getattr__(k)
        else:
            return copy.deepcopy(self._io_attrs.get(k, None))

    def __setattr__(self, k, v):
        if self._io_locked:
            raise TypeError('instance is read only')
        else:
            super(ImmutableObject, self).__setattr__(k, v)


class Request(ImmutableObject):
    pass


class Response(object):
    status_code = 200
    body = u''
    content_type = 'text/html'
    headers = {}

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class Redirect(Response):
    status_code = 301

    def __init__(self, location, *args, **kwargs):
        super(Redirect, self).__init__(*args, **kwargs)
        self.headers.update({'Location': location})


