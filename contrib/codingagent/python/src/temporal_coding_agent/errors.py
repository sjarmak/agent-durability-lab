"""Typed protocol failures."""

from __future__ import annotations

from .models import Rejection


class ProtocolError(Exception):
    """Base class for protocol failures."""


class ContractValidationError(ProtocolError):
    """JSON or schema input is not v1 conformant."""


class AuthorizationError(ProtocolError):
    """The coordinator or executor was not authenticated."""


class InvalidTransitionError(ProtocolError):
    """The lifecycle does not admit the requested operation."""


class RejectedOperationError(ProtocolError):
    """A guarded operation returned a typed rejection."""

    def __init__(self, message: str, rejection: Rejection) -> None:
        super().__init__(message)
        self.rejection = rejection


class OperationConflictError(RejectedOperationError):
    """An operation or effect identity was reused with changed content."""


class StaleAuthorityError(RejectedOperationError):
    """The requested generation is no longer current."""


class RevokedAuthorityError(RejectedOperationError):
    """The supplied capability has no current authority."""


class CanceledTurnError(RejectedOperationError):
    """The turn is durably canceled."""
