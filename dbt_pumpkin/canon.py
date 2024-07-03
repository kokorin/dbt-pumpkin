import re
from abc import ABC, abstractmethod

from exception import NamingCanonError


class NamingCanon(ABC):
    _convertable_re = re.compile("^[a-zA-Z_][a-zA-Z0-9_]*$")

    def _ensure_canonizeable(self, name: str):
        if not self._convertable_re.match(name):
            raise NamingCanonError(name)

    @abstractmethod
    def _do_canonize(self, name: str) -> str:
        pass

    def canonize(self, name: str) -> str:
        self._ensure_canonizeable(name)
        return self._do_canonize(name)


class UppercaseCanon(NamingCanon):
    def _do_canonize(self, name: str) -> str:
        return name.upper()


class LowercaseCanon(NamingCanon):
    def _do_canonize(self, name: str) -> str:
        return name.lower()
