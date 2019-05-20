class NextLessRestrictiveCycleError(Exception):
    pass

class NameSqlMixin(object):
    """Mixin to provide default to_sql behavior."""

    def to_sql(self):
        """Get the SQL identifier string for the object.

        Returns:
          The name of the object
        """
        return name

class DictableMixin(object):
    """Mixin to provide marshaling to and from dict"""

    def to_dict(self):
        """Create a dictionary from this object"""
        return vars(self)

    @classmethod
    def from_dict(cls, class_dict, **kwargs):
        """Instantiate from a dictionary
        Args:
          class_dict: the dictionary containing the data for the class
          kwargs: additional keyword arguments required to instantiate this class
        """
        return cls(**{**class_dict, **kwargs})

    def __eq__(self, other):
        if issubclass(type(other), DictableMixin):
            return self.to_dict() == other.to_dict()
        else:
            return False

    def to_file(self, handler, format, overwrite=False):
        """Persist this schema to a file.

        Args:
          handler: the file handler to write to
          format: json or yaml
          overwrite:  whether this table definition should be overwritten
                  if it already exists in the existing config
        """
        # TODO
        raise NotImplementedError
        
