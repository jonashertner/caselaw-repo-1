"""Dependency-free public research API client."""

from ._version import __version__
from .client import APIError, Client

__all__ = ["APIError", "Client", "__version__"]
