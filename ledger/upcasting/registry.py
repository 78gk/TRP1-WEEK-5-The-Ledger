"""
ledger/upcasting/registry.py
============================
UpcasterRegistry — decorator-based, automatic version-chain upcasting.

Usage:
    registry = UpcasterRegistry()

    @registry.register("MyEvent", from_version=1)
    def upcast_my_event_v1_to_v2(payload: dict) -> dict:
        return {**payload, "new_field": "default"}

    # Applying a v1 event will automatically chain through v1→v2, v2→v3, etc.
    upgraded_event = registry.upcast(stored_event)

StoredEvent.copy_with() is used to produce immutable copies — the original
database row is NEVER modified.
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Awaitable, Callable

from ledger.schema.events import StoredEvent


@dataclass(slots=True)
class UpcastContext:
    load_stream: Callable[[str], Awaitable[list]]


class UpcasterRegistry:
    """
    Registry of pure payload-transform functions keyed by (event_type, from_version).

    The upcast() method applies the full version chain automatically:
    if v1→v2 and v2→v3 are registered, a v1 event gets both applied in order,
    resulting in a v3 event.  The caller never needs to know how many steps exist.
    """

    def __init__(self) -> None:
        self._upcasters: dict[tuple[str, int], Callable] = {}

    def register(self, event_type: str, from_version: int) -> Callable:
        """
        Decorator-based registration.

        @registry.register("CreditAnalysisCompleted", from_version=1)
        def upcast_credit_v1_to_v2(payload: dict) -> dict:
            ...
        """
        def decorator(fn: Callable[[dict], dict]) -> Callable[[dict], dict]:
            self._upcasters[(event_type, from_version)] = fn
            return fn

        return decorator

    async def _invoke(self, fn: Callable, payload: dict, context: UpcastContext | None) -> dict:
        parameters = inspect.signature(fn).parameters
        if len(parameters) >= 2:
            result = fn(payload, context)
        else:
            result = fn(payload)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def upcast(
        self,
        event: StoredEvent,
        context: UpcastContext | None = None,
    ) -> StoredEvent:
        """
        Apply the FULL version chain for the event's type and starting version.

        Rules:
        - If no upcaster is registered for (event_type, current_version),
          the event is returned unchanged.
        - Each step produces a NEW StoredEvent via copy_with(); the original
          object is NEVER mutated.
        - The loop walks the chain until no further upcaster is found,
          so a v1 event with v1→v2 and v2→v3 registered will reach v3.
        """
        current = event
        v = event.event_version

        while (event.event_type, v) in self._upcasters:
            fn = self._upcasters[(event.event_type, v)]
            # Pass a *copy* of the payload dict so upcasters cannot accidentally
            # mutate the object they received.
            new_payload = await self._invoke(fn, dict(current.payload), context)
            current = current.copy_with(payload=new_payload, event_version=v + 1)
            v += 1

        return current
