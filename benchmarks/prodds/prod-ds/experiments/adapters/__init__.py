from .base import EngineAdapter
from .duckdb import DuckDBAdapter
from .postgres import PostgresAdapter
from .cedardb import CedarDBAdapter
from .monetdb import MonetDBAdapter


ADAPTERS = {
    "duckdb": DuckDBAdapter,
    "postgres": PostgresAdapter,
    "cedardb": CedarDBAdapter,
    "monetdb": MonetDBAdapter,
}
