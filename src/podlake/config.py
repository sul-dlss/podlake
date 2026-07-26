import os
from dataclasses import dataclass, field

import dotenv

# The alias the DuckLake catalog is attached as; used throughout the codebase.
LAKE_ALIAS = "podlake"


@dataclass
class Config:
    """
    Resolved podlake configuration for the active profile.

    Two profiles are supported, selected with the PODLAKE_ENV environment
    variable:

    - "development" (default): a local DuckDB catalog file and a local Parquet
      DATA_PATH. No external services required.
    - "production": a Postgres catalog and an S3 DATA_PATH. AWS credentials are
      resolved by DuckDB's credential_chain (standard AWS_* env vars, shared
      config, or an assumed role).
    """

    env: str
    data_path: str
    catalog_uri: str
    pg: dict[str, str] = field(default_factory=dict)
    publish_url: str | None = None

    @property
    def is_production(self) -> bool:
        return self.env == "production"

    @property
    def is_file_catalog(self) -> bool:
        """
        True when the catalog is a local DuckDB/SQLite file (as opposed to a
        Postgres catalog). Only a file-catalog lake can be published to S3.
        """
        return not self.catalog_uri.startswith("postgres:")

    def attach_sql(self, read_only: bool = False) -> str:
        """
        Build the ATTACH statement for this profile's DuckLake catalog. The
        catalog URI (which may embed a Postgres password) and data path can't be
        passed as bind parameters, so they are inlined as single-quoted SQL
        literals with embedded quotes escaped.
        """
        options = [f"DATA_PATH '{_sql_literal(self.data_path)}'"]
        if read_only:
            options.append("READ_ONLY")
        target = _sql_literal(f"ducklake:{self.catalog_uri}")
        return f"ATTACH '{target}' AS {LAKE_ALIAS} ({', '.join(options)})"

    def describe(self) -> dict[str, str]:
        """
        A human-readable, secret-masked view of the resolved configuration,
        suitable for printing in the `config` command.
        """
        info = {
            "PODLAKE_ENV": self.env,
            "data_path": self.data_path,
        }
        if self.is_production:
            info["catalog"] = "postgres"
            info["postgres_host"] = self.pg.get("host", "")
            info["postgres_dbname"] = self.pg.get("dbname", "")
            info["postgres_user"] = self.pg.get("user", "")
            info["postgres_password"] = _mask(self.pg.get("password", ""))
        else:
            info["catalog"] = self.catalog_uri
        return info


def get_config() -> Config:
    """
    Load environment variables from a .env file (if present) and resolve the
    configuration for the active profile.
    """
    dotenv.load_dotenv()

    env = os.environ.get("PODLAKE_ENV", "development").lower()
    if env not in ("development", "production"):
        raise ValueError(
            f"PODLAKE_ENV must be 'development' or 'production', not {env!r}"
        )

    if env == "production":
        return _production_config()
    return _development_config()


def _development_config() -> Config:
    catalog = os.environ.get("PODLAKE_CATALOG", "podlake.ducklake")
    data_path = os.environ.get("PODLAKE_DATA_PATH", "./lake-data/")
    return Config(
        env="development",
        data_path=data_path,
        catalog_uri=catalog,
        publish_url=os.environ.get("PODLAKE_PUBLISH_URL"),
    )


def _production_config() -> Config:
    data_path = _require("PODLAKE_DATA_PATH")

    dsn = os.environ.get("PODLAKE_PG_DSN")
    if dsn:
        pg = _parse_dsn(dsn)
    else:
        pg = {
            "host": _require("PODLAKE_PG_HOST"),
            "port": os.environ.get("PODLAKE_PG_PORT", "5432"),
            "dbname": _require("PODLAKE_PG_DBNAME"),
            "user": _require("PODLAKE_PG_USER"),
            "password": _require("PODLAKE_PG_PASSWORD"),
        }

    # DuckLake accepts a Postgres connection string after the "postgres:" prefix.
    catalog_uri = "postgres:" + " ".join(f"{k}={v}" for k, v in pg.items())

    return Config(
        env="production",
        data_path=data_path,
        catalog_uri=catalog_uri,
        pg=pg,
        publish_url=os.environ.get("PODLAKE_PUBLISH_URL"),
    )


def _parse_dsn(dsn: str) -> dict[str, str]:
    """
    Parse a libpq-style "key=value key=value" DSN into a dict.
    """
    pg = {}
    for part in dsn.split():
        if "=" in part:
            key, value = part.split("=", 1)
            pg[key] = value
    return pg


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} environment variable must be set for the production profile"
        )
    return value


def _mask(secret: str) -> str:
    if not secret:
        return ""
    return "*" * len(secret)


def _sql_literal(value: str) -> str:
    """Escape a value for use inside a single-quoted SQL string literal."""
    return value.replace("'", "''")
