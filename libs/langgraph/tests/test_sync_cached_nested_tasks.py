"""Regression coverage for concurrent cached nested tasks (issue #9304)."""

import sys

from langgraph.cache.memory import InMemoryCache

from langgraph.func import entrypoint, task
from langgraph.types import CachePolicy


@task(cache_policy=CachePolicy())
def _cached_leaf(value: int) -> int:
    return value


@task(cache_policy=CachePolicy())
def _cached_parent(index: int) -> int:
    return sum(
        future.result()
        for future in [_cached_leaf(index * 1000 + offset) for offset in range(25)]
    )


def _run_nested_cached_tasks() -> int:
    @entrypoint(cache=InMemoryCache())
    def workflow(_: int) -> int:
        return sum(future.result() for future in [_cached_parent(i) for i in range(4)])

    return workflow.invoke(0)


def test_sync_cached_nested_tasks_do_not_mutate_dict_during_iteration() -> None:
    """Concurrently pushed child tasks must not invalidate cache matching."""
    expected = sum(i * 1000 + j for i in range(4) for j in range(25))
    original_switch_interval = sys.getswitchinterval()
    try:
        # Expose races between task insertion and iteration without sleeps.
        sys.setswitchinterval(1e-6)
        for _ in range(20):
            assert _run_nested_cached_tasks() == expected
    finally:
        sys.setswitchinterval(original_switch_interval)
