from __future__ import annotations

import diskcache_community as dc

from m_flow.adapters.cache.fscache.FsCacheAdapter import FSCacheAdapter

planted_calls = []


def planted():
    planted_calls.append(True)
    return "[]"


class Planted:
    def __reduce__(self):
        return (planted, ())


async def test_planted_pickle_is_a_miss(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "m_flow.adapters.cache.fscache.FsCacheAdapter.get_storage_config",
        lambda: {"data_root_directory": str(tmp_path)},
    )
    adapter = FSCacheAdapter()
    with dc.Cache(adapter.cache.directory, disk=dc.Disk) as other_writer:
        other_writer.set("agent_sessions:user-1:s1", Planted())

    assert await adapter.get_latest_qa("user-1", "s1") is None
    assert planted_calls == []
    await adapter.close()
