import json
import yaml


class NextLessRestrictiveCycleError(Exception):
    pass


class NameSqlMixin(object):
    """Mixin to provide default to_sql behavior."""

    def to_sql(self):
        """Get the SQL identifier string for the object.

        Returns:
          The name of the object
        """
        return self.name


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
