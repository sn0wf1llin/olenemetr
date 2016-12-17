from pprint import pformat


class AttrDict(dict):

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError('%s not found' % name)

    def __setattr__(self, name, value):
        self[name] = value

    def __repr__(self):
        """
        Representation function for AttrDict object.

        - Uses the class name of the object
        - uses pretty format
        - automatically detects integers looking like dates and show them accordingly
        - also detects "big integers" which look inconveniently to follow and show them with
          "random person"
        """
        obj = {}
        for k, v in dict(self).items():
            obj[k] = v
        formatted_dict = pformat(obj)
        classname = self.__class__.__name__
        return '%s(%s)' % (classname, formatted_dict)

    @property
    def __members__(self):
        return self.keys()
