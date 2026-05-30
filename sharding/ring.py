import uuid
import bisect
import hashlib
from collections import Counter

class ConsistentHashRing:
    """Maps keys to nodes via consistent hashing with virtual nodes."""

    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self._ring: list[tuple[int, str]] = []
        self._positions: list[int] = []
        self._nodes: set[str] = set()

    @staticmethod
    def _hash(value: str) -> int:
        return int(hashlib.md5(value.encode()).hexdigest(), 16)

    def add_node(self, name: str) -> None:
        if name in self._nodes:
            raise ValueError(f"Node {name!r} already in ring")
        self._nodes.add(name)
        for virtual_node_index in range(self.virtual_nodes):
            position = self._hash(f"{name}:{virtual_node_index}")
            bisect.insort(self._ring, (position, name))
        self._positions = [position for position, _ in self._ring]

    def remove_node(self, name: str) -> None:
        if name not in self._nodes:
            raise ValueError(f"Node {name!r} not in ring")
        self._nodes.remove(name)
        self._ring = [(position, shard_name) for position, shard_name in self._ring if shard_name != name]
        self._positions = [position for position, _ in self._ring]

    def get_node(self, key: str) -> str:
        if not self._ring:
            raise ValueError("Ring is empty")
        position = self._hash(key)
        ring_index = bisect.bisect_right(self._positions, position)
        if ring_index == len(self._ring):
            ring_index = 0
        return self._ring[ring_index][1]

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)


if __name__ == "__main__":
    ring = ConsistentHashRing(virtual_nodes=150)
    for shard in ("shard_0", "shard_1", "shard_2"):
        ring.add_node(shard)

    keys = [str(uuid.uuid4()) for _ in range(30_000)]
    original_shards = {key: ring.get_node(key) for key in keys}

    counts = Counter(original_shards.values())
    total = sum(counts.values())
    print(f"Distribution over {total:,} keys with {ring.virtual_nodes} virtual nodes/shard:")

    for shard in sorted(counts):
        percentage = counts[shard] / total * 100
        print(f"  {shard}: {counts[shard]:>6,}  ({percentage:.2f}%)")

    print("\nResharding test — adding shard_3:")

    ring.add_node("shard_3")
    moved = sum(1 for key, original_shard in original_shards.items() if ring.get_node(key) != original_shard)

    print(f"  {moved:,}/{len(original_shards):,} keys moved ({moved / len(original_shards) * 100:.2f}%)")
    print(f"  (theoretical with hash % N: ~75% of keys would move, making consistent hashing much better than naive hashing)")
