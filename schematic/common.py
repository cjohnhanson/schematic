# -*- coding: utf-8 -*-
# MIT License
#
# Copyright (c) 2019 Cody J. Hanson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
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
