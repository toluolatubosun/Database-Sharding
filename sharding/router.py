from typing import Literal

from sqlmodel import Session, create_engine
from sqlalchemy.engine import Engine

from configs.config import CONFIGS
from sharding.ring import ConsistentHashRing


Operation = Literal["READ", "WRITE"]


class ShardRouter:
    """Routes keys to shards and hands out database sessions."""

    GLOBAL = "global"

    # __init__ METHOD  BEFORE ADDITION OF RESHARDING LOGIC, FOR REFERENCE:
    # def __init__(
    #     self,
    #     shard_urls: dict[str, str],
    #     global_url: str,
    #     virtual_nodes: int = 150,
    # ):
    #     self._ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
    #     self._engines: dict[str, Engine] = {}

    #     for shard_name, url in shard_urls.items():
    #         self._ring.add_node(shard_name)
    #         self._engines[shard_name] = create_engine(url)

    #     self._engines[self.GLOBAL] = create_engine(global_url)

    def __init__(
        self,
        shard_urls: dict[str, str],
        global_url: str,
        virtual_nodes: int = 150,
    ):
        self._engines: dict[str, Engine] = {}
        self._ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
        # This is only used during resharding, and is None otherwise.
        self._new_ring: ConsistentHashRing | None = None 
        
        # Set up the main ring and engines for all shards
        for shard_name, url in shard_urls.items():
            self._ring.add_node(shard_name)
            self._engines[shard_name] = create_engine(url)

        # Set up the global DB engine (not part of the ring)
        self._engines[self.GLOBAL] = create_engine(global_url)
        
        reshard = CONFIGS["DB_RESHARDING"]
        resharding_phase = reshard["DB_RESHARDING_PHASE"]
    
        # ALLOW_MIGRATIONS is tooling-only (lets migrate.py see the new shard); the
        # router itself only reacts once we're actually dual-writing or cut over.
        if resharding_phase in ("DUAL_WRITE", "CUTOVER"):
            operation = reshard["OPERATION"]            # "ADD" or "REMOVE"
            target_shard = reshard["TARGET_SHARD_NAME"]

            # ADD:
            # A new shard joins; create its engine and add it to the relevant ring.
            # REMOVE:
            # An existing shard leaves; its engine is already wired up, just drop it from the relevant ring.
            if operation == "ADD":
                self._engines[target_shard] = create_engine(reshard["TARGET_SHARD_URL"])

                if resharding_phase == "DUAL_WRITE":
                    # _new_ring = current shards + the incoming target.
                    self._new_ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
                    for shard_name in shard_urls:
                        self._new_ring.add_node(shard_name)
                    self._new_ring.add_node(target_shard)

                elif resharding_phase == "CUTOVER":
                    # Target joins the main ring; reads + writes flow through it normally.
                    self._ring.add_node(target_shard)

            elif operation == "REMOVE":
                if resharding_phase == "DUAL_WRITE":
                    # _new_ring = current shards minus the doomed target.
                    self._new_ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
                    for shard_name in shard_urls:
                        if shard_name != target_shard:
                            self._new_ring.add_node(shard_name)

                elif resharding_phase == "CUTOVER":
                    # Target leaves the main ring; reads + writes naturally avoid it.
                    self._ring.remove_node(target_shard)
    
    # shard_for METHOD BEFORE ADDITION OF RESHARDING LOGIC, FOR REFERENCE:
    # def shard_for(self, key: str) -> str:
    #     return self._ring.get_node(str(key))

    def shard_for(self, key: str, operation: Operation = "READ") -> str | list[str]:
        """
        Return shard(s) for this key + operation.

        READ 
        - Only a single shard is returned, based on the original ring. 
        WRITE
        - During IDLE (no resharding happening), only the original shard is returned.
        - During DUAL_WRITE resharding, both the original shard and the new shard are returned (as a list), so the application can write to both.
        - During CUTOVER, the new shard is part of the main ring, so writes go to whichever single shard the key now maps to. i.e the new shard, as it has been added to the main ring.
        """
        current_shard = self._ring.get_node(str(key))
        
        # For reads, we always read from the original shard.
        if operation == "READ":
            return current_shard

        # If there is no new ring the resharding process is either in IDLE or CUTOVER
        # So we write to the original shard. (In CUTOVER it has already been added to the main ring) 
        if self._new_ring is None:
            return [current_shard]
        
        # If there is a new ring, we are in DUAL_WRITE, so we write to both the original shard and the new shard.
        new_shard = self._new_ring.get_node(str(key))
        if current_shard == new_shard:
            return [current_shard]
        return [current_shard, new_shard]

    def session(self, name: str) -> Session:
        if name not in self._engines:
            raise ValueError(f"Unknown DB {name!r}. Known: {sorted(self._engines)}")
        return Session(self._engines[name])

    def all_shards(self) -> list[str]:
        return self._ring.nodes
