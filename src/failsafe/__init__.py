"""Fault tolerance and resilience policies for Python.

A port of failsafe-go. See PLAN.md for the design and PORT.md for differences
from the Go library.
"""

from ._async import AsyncFailsafe
from ._core import Execution, current_execution
from ._sync import Failsafe

__all__ = [
    "AsyncFailsafe",
    "Execution",
    "Failsafe",
    "current_execution",
]
