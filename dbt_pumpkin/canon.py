import re
from abc import ABC, abstractmethod

from dbt_pumpkin.exception import NamingCanonError


class NamingCanon(ABC):
    _canonizable_re = re.compile("^[a-zA-Z_][a-zA-Z0-9_]*$")

    def can_canonize(self, name: str) -> bool:
        return self._canonizable_re.match(name) is not None

    @abstractmethod
    def _do_canonize(self, name: str) -> str:
        pass

    def canonize(self, name: str) -> str:
        if not self.can_canonize(name):
            raise NamingCanonError(name)
        return self._do_canonize(name)


class UppercaseCanon(NamingCanon):
    def _do_canonize(self, name: str) -> str:
        return name.upper()


class LowercaseCanon(NamingCanon):
    def _do_canonize(self, name: str) -> str:
        return name.lower()
