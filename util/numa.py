import os
import sys

import numa
import psutil

from util.log import log


def set_node(numa_node: int | None):
    if numa_node is not None:
        if numa_node > numa.info.get_max_node():
            log.error("value for --numa-node (%d) exceeds maximum number of available nodes in this system (%d)" % (numa_node, numa.info.get_max_node() + 1))
            sys.exit(1)

            # bind the Python process itself, so that non-Dockerized systems are affected
        numa.schedule.run_on_nodes(numa_node)
        numa.memory.set_membind_nodes(numa_node)


def get_cpus(numa_node: int | None) -> str:
    return ",".join(str(x) for x in numa.info.node_to_cpus(numa_node)) if numa_node is not None else ""


def get_mems(numa_node: int | None) -> str:
    return str(numa_node) if numa_node is not None else ""


def get_thread_count(numa_node: int | None) -> int:
    return len(numa.info.node_to_cpus(numa_node)) if numa_node else psutil.cpu_count(logical=True)


def get_memory_size(numa_node: int | None) -> int:
    return numa.memory.node_memory_info(numa_node)[0] if numa_node else psutil.virtual_memory().total
