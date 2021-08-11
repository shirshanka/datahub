import glob
import importlib
import itertools
import logging
import pathlib
import re
import sys
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from dataclasses import replace
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type, cast

import pydantic
from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.methods import Looker31SDK
from looker_sdk.sdk.api31.models import DBConnection
from pydantic import root_validator, validator

from datahub.utilities.sql_parser import SQLParser

if sys.version_info >= (3, 7):
    import lkml
else:
    raise ModuleNotFoundError("The lookml plugin requires Python 3.7 or newer.")

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker import LookerAPI, LookerAPIConfig
from datahub.ingestion.source.sql.sql_types import (
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    resolve_postgres_modified_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    EnumTypeClass,
    MetadataChangeEventClass,
    SchemaMetadataClass,
)

assert sys.version_info[1] >= 7  # needed for mypy

logger = logging.getLogger(__name__)


def _get_bigquery_definition(looker_connection: DBConnection) -> Tuple[str, Optional[str], Optional[str]]:
    platform = "bigquery"
    # bigquery project ids are returned in the host field
    db = looker_connection.host
    schema = looker_connection.database
    return (platform, db, schema)


def _get_hive_definition(looker_connection: DBConnection) -> Tuple[str, Optional[str], Optional[str]]:
    # Hive is a two part system (db.table)
    platform = "hive"
    db = looker_connection.database
    schema = None
    return (platform, db, schema)


def _get_generic_definition(looker_connection: DBConnection) -> Tuple[str, Optional[str], Optional[str]]:
    dialect_name = looker_connection.dialect_name
    assert dialect_name is not None
    # generally the first part of the dialect name before _ is the name of the platform
    platform = dialect_name.split("_")[0]
    db = looker_connection.database
    schema = looker_connection.schema
    return (platform, db, schema)


class LookerConnectionDefinition(ConfigModel):
    platform: str
    default_db: str
    default_schema: str

    @validator("*")
    def lower_everything(cls, v):
        """We lower case all strings passed in to avoid casing issues later"""
        return v.lower()

    @classmethod
    def from_looker_connection(
        cls, looker_connection: DBConnection
    ) -> "LookerConnectionDefinition":

        extractors: Dict[str, Any] = {
            "bigquery": _get_bigquery_definition,
            "hive": _get_hive_definition,
            ".*": _get_generic_definition,
        }

        assert looker_connection.dialect_name is not None
        for extractor_pattern, extracting_function in extractors.items():
            if re.match(extractor_pattern, looker_connection.dialect_name):
                (platform, db, schema) = extracting_function(looker_connection)
                return cls(platform=platform, default_db=db, default_schema=schema)
        raise ConfigurationError(
            f"Could not find an appropriate platform for looker_connection: {looker_connection.name} with dialect: {looker_connection.dialect_name}"
        )


class LookMLSourceConfig(ConfigModel):
    base_folder: pydantic.DirectoryPath
    connection_to_platform_map: Optional[Dict[str, LookerConnectionDefinition]]
    platform_name: str = "looker"
    model_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    view_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    env: str = builder.DEFAULT_ENV
    parse_table_names_from_sql: bool = False
    sql_parser: str = "datahub.utilities.sql_parser.DefaultSQLParser"
    api: Optional[LookerAPIConfig]

    @validator("connection_to_platform_map", pre=True)
    def convert_string_to_connection_def(cls, conn_map):
        # Previous version of config supported strings in connection map. This upconverts strings to ConnectionMap
        for key in conn_map:
            if isinstance(conn_map[key], str):
                platform = conn_map[key]
                if "." in platform:
                    platform_db_split = conn_map[key].split(".")
                    connection = LookerConnectionDefinition(
                        platform=platform_db_split[0],
                        default_db=platform_db_split[1],
                        default_schema="",
                    )
                    conn_map[key] = connection
                else:
                    logger.warning(
                        f"Connection map for {key} provides platform {platform} but does not provide a default database name. This might result in failed resolution"
                    )
                    conn_map[key] = LookerConnectionDefinition(
                        platform=platform, default_db="", default_schema=""
                    )
        return conn_map

    @root_validator()
    def check_either_connection_map_or_connection_provided(cls, values):
        """Validate that we must either have a connection map or an api credential"""
        if not values.get("connection_to_platform_map", {}) and not values.get(
            "api", {}
        ):
            raise ConfigurationError(
                "Neither api not connection_to_platform_map config was found. LookML source requires either api credentials for Looker or a map of connection names to platform identifiers to work correctly"
            )
        return values


@dataclass
class LookMLSourceReport(SourceReport):
    models_scanned: int = 0
    views_scanned: int = 0
    explores_scanned: int = 0
    filtered_models: List[str] = dataclass_field(default_factory=list)
    filtered_views: List[str] = dataclass_field(default_factory=list)
    filtered_explores: List[str] = dataclass_field(default_factory=list)

    def report_models_scanned(self) -> None:
        self.models_scanned += 1

    def report_views_scanned(self) -> None:
        self.views_scanned += 1

    def report_explores_scanned(self) -> None:
        self.explores_scanned += 1

    def report_models_dropped(self, model: str) -> None:
        self.filtered_models.append(model)

    def report_views_dropped(self, view: str) -> None:
        self.filtered_views.append(view)

    def report_explores_dropped(self, explore: str) -> None:
        self.filtered_explores.append(explore)


@dataclass
class LookerModel:
    connection: str
    includes: List[str]
    explores: List[dict]
    resolved_includes: List[str]

    @staticmethod
    def from_looker_dict(
        looker_model_dict: dict,
        base_folder: str,
        path: str,
        reporter: LookMLSourceReport,
    ) -> "LookerModel":
        connection = looker_model_dict["connection"]
        includes = looker_model_dict.get("includes", [])
        resolved_includes = LookerModel.resolve_includes(
            includes, base_folder, path, reporter
        )
        explores = looker_model_dict.get("explores", [])

        return LookerModel(
            connection=connection,
            includes=includes,
            resolved_includes=resolved_includes,
            explores=explores,
        )

    @staticmethod
    def resolve_includes(
        includes: List[str], base_folder: str, path: str, reporter: LookMLSourceReport
    ) -> List[str]:
        """Resolve ``include`` statements in LookML model files to a list of ``.lkml`` files.

        For rules on how LookML ``include`` statements are written, see
            https://docs.looker.com/data-modeling/getting-started/ide-folders#wildcard_examples
        """
        resolved = []
        for inc in includes:
            # Filter out dashboards - we get those through the looker source.
            if (
                inc.endswith(".dashboard")
                or inc.endswith(".dashboard.lookml")
                or inc.endswith(".dashboard.lkml")
            ):
                logger.debug(f"include '{inc}' is a dashboard, skipping it")
                continue

            # Massage the looker include into a valid glob wildcard expression
            if inc.startswith("/"):
                glob_expr = f"{base_folder}{inc}"
            else:
                # Need to handle a relative path.
                glob_expr = str(pathlib.Path(path).parent / inc)
            # "**" matches an arbitrary number of directories in LookML
            outputs = sorted(
                glob.glob(glob_expr, recursive=True)
                + glob.glob(f"{glob_expr}.lkml", recursive=True)
            )
            if "*" not in inc and not outputs:
                reporter.report_failure(path, f"cannot resolve include {inc}")
            elif not outputs:
                reporter.report_failure(
                    path, f"did not resolve anything for wildcard include {inc}"
                )

            resolved.extend(outputs)
        return resolved


@dataclass
class LookerViewFile:
    absolute_file_path: str
    connection: Optional[str]
    includes: List[str]
    resolved_includes: List[str]
    views: List[Dict]
    raw_file_content: str

    @staticmethod
    def from_looker_dict(
        absolute_file_path: str,
        looker_view_file_dict: dict,
        base_folder: str,
        raw_file_content: str,
        reporter: LookMLSourceReport,
    ) -> "LookerViewFile":
        includes = looker_view_file_dict.get("includes", [])
        resolved_includes = LookerModel.resolve_includes(
            includes, base_folder, absolute_file_path, reporter
        )
        views = looker_view_file_dict.get("views", [])

        return LookerViewFile(
            absolute_file_path=absolute_file_path,
            connection=None,
            includes=includes,
            resolved_includes=resolved_includes,
            views=views,
            raw_file_content=raw_file_content,
        )


class LookerViewFileLoader:
    """
    Loads the looker viewfile at a :path and caches the LookerViewFile in memory
    This is to avoid reloading the same file off of disk many times during the recursive include resolution process
    """

    def __init__(self, base_folder: str, reporter: LookMLSourceReport) -> None:
        self.viewfile_cache: Dict[str, LookerViewFile] = {}
        self._base_folder = base_folder
        self.reporter = reporter

    def is_view_seen(self, path: str) -> bool:
        return path in self.viewfile_cache

    def _load_viewfile(
        self, path: str, reporter: LookMLSourceReport
    ) -> Optional[LookerViewFile]:
        if self.is_view_seen(path):
            return self.viewfile_cache[path]

        try:
            with open(path, "r") as file:
                raw_file_content = file.read()
        except Exception as e:
            self.reporter.report_failure(path, f"failed to load view file: {e}")
            return None
        try:
            with open(path, "r") as file:
                parsed = lkml.load(file)
                looker_viewfile = LookerViewFile.from_looker_dict(
                    absolute_file_path=path,
                    looker_view_file_dict=parsed,
                    base_folder=self._base_folder,
                    raw_file_content=raw_file_content,
                    reporter=reporter,
                )
                logger.debug(f"adding viewfile for path {path} to the cache")
                self.viewfile_cache[path] = looker_viewfile
                return looker_viewfile
        except Exception as e:
            self.reporter.report_failure(path, f"failed to load view file: {e}")
            return None

    def load_viewfile(
        self,
        path: str,
        connection: LookerConnectionDefinition,
        reporter: LookMLSourceReport,
    ) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(path, reporter)
        if viewfile is None:
            return None

        return replace(viewfile, connection=connection)


class ViewFieldType(Enum):
    DIMENSION = "Dimension"
    DIMENSION_GROUP = "Dimension Group"
    MEASURE = "Measure"


@dataclass
class ViewField:
    name: str
    type: str
    description: str
    field_type: ViewFieldType
    is_primary_key: bool = False


@dataclass
class LookerView:
    absolute_file_path: str
    connection: LookerConnectionDefinition
    view_name: str
    sql_table_names: List[str]
    fields: List[ViewField]
    raw_file_content: str

    @classmethod
    def _import_sql_parser_cls(cls, sql_parser_path: str) -> Type[SQLParser]:
        assert "." in sql_parser_path, "sql_parser-path must contain a ."
        module_name, cls_name = sql_parser_path.rsplit(".", 1)
        import sys

        logger.info(sys.path)
        parser_cls = getattr(importlib.import_module(module_name), cls_name)
        if not issubclass(parser_cls, SQLParser):
            raise ValueError(f"must be derived from {SQLParser}; got {parser_cls}")

        return parser_cls

    @classmethod
    def _get_sql_table_names(cls, sql: str, sql_parser_path: str) -> List[str]:
        parser_cls = cls._import_sql_parser_cls(sql_parser_path)

        sql_table_names: List[str] = parser_cls(sql).get_tables()

        # Remove quotes from table names
        sql_table_names = [t.replace('"', "") for t in sql_table_names]
        sql_table_names = [t.replace("`", "") for t in sql_table_names]

        return sql_table_names

    @classmethod
    def _get_fields(
        cls, field_list: List[Dict], type_cls: ViewFieldType
    ) -> List[ViewField]:
        fields = []
        for field_dict in field_list:
            is_primary_key = field_dict.get("primary_key", "no") == "yes"
            name = field_dict["name"]
            native_type = field_dict.get("type", "string")
            description = field_dict.get("description", "")
            field = ViewField(
                name=name,
                type=native_type,
                description=description,
                is_primary_key=is_primary_key,
                field_type=type_cls,
            )
            fields.append(field)
        return fields

    @classmethod
    def from_looker_dict(
        cls,
        looker_view: dict,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        reporter: LookMLSourceReport,
        parse_table_names_from_sql: bool = False,
        sql_parser_path: str = "datahub.utilities.sql_parser.DefaultSQLParser",
    ) -> Optional["LookerView"]:
        view_name = looker_view["name"]
        logger.debug(f"Handling view {view_name}")

        # The sql_table_name might be defined in another view and this view is extending that view,
        # so we resolve this field while taking that into account.
        sql_table_name: Optional[str] = LookerView.get_including_extends(
            view_name,
            looker_view,
            connection,
            looker_viewfile,
            looker_viewfile_loader,
            "sql_table_name",
            reporter,
        )

        # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
        sql_table_name = (
            sql_table_name.replace('"', "").replace("`", "")
            if sql_table_name is not None
            else None
        )
        derived_table = looker_view.get("derived_table", None)

        dimensions = cls._get_fields(
            looker_view.get("dimensions", []), ViewFieldType.DIMENSION
        )
        dimension_groups = cls._get_fields(
            looker_view.get("dimension_groups", []), ViewFieldType.DIMENSION_GROUP
        )
        measures = cls._get_fields(
            looker_view.get("measures", []), ViewFieldType.MEASURE
        )
        fields: List[ViewField] = dimensions + dimension_groups + measures

        # Parse SQL from derived tables to extract dependencies
        if derived_table is not None:
            sql_table_names = []
            if parse_table_names_from_sql and "sql" in derived_table:
                logger.debug(
                    f"Parsing sql from derived table section of view: {view_name}"
                )
                # Get the list of tables in the query
                sql_table_names = cls._get_sql_table_names(
                    derived_table["sql"], sql_parser_path
                )

            return LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=sql_table_names,
                fields=fields,
                raw_file_content=looker_viewfile.raw_file_content,
            )

        # If not a derived table, then this view essentially wraps an existing
        # object in the database.
        if sql_table_name is not None:
            # If sql_table_name is set, there is a single dependency in the view, on the sql_table_name.
            sql_table_names = [sql_table_name]
        else:
            # Otherwise, default to the view name as per the docs:
            # https://docs.looker.com/reference/view-params/sql_table_name-for-view
            sql_table_names = [view_name]

        output_looker_view = LookerView(
            absolute_file_path=looker_viewfile.absolute_file_path,
            connection=connection,
            view_name=view_name,
            sql_table_names=sql_table_names,
            fields=fields,
            raw_file_content=looker_viewfile.raw_file_content,
        )
        return output_looker_view

    @classmethod
    def resolve_extends_view_name(
        cls,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        target_view_name: str,
        reporter: LookMLSourceReport,
    ) -> Optional[dict]:
        # The view could live in the same file.
        for raw_view in looker_viewfile.views:
            raw_view_name = raw_view["name"]
            if raw_view_name == target_view_name:
                return raw_view

        # Or it could live in one of the included files. We do not know which file the base view
        # lives in, so we try them all!
        for include in looker_viewfile.resolved_includes:
            included_looker_viewfile = looker_viewfile_loader.load_viewfile(
                include, connection, reporter
            )
            if not included_looker_viewfile:
                logger.warning(
                    f"unable to load {include} (included from {looker_viewfile.absolute_file_path})"
                )
                continue
            for raw_view in included_looker_viewfile.views:
                raw_view_name = raw_view["name"]
                # Make sure to skip loading view we are currently trying to resolve
                if raw_view_name == target_view_name:
                    return raw_view

        return None

    @classmethod
    def get_including_extends(
        cls,
        view_name: str,
        looker_view: dict,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        field: str,
        reporter: LookMLSourceReport,
    ) -> Optional[Any]:
        extends = list(
            itertools.chain.from_iterable(
                looker_view.get("extends", looker_view.get("extends__all", []))
            )
        )

        # First, check the current view.
        if field in looker_view:
            return looker_view[field]

        # Then, check the views this extends, following Looker's precedence rules.
        for extend in reversed(extends):
            assert extend != view_name, "a view cannot extend itself"
            extend_view = LookerView.resolve_extends_view_name(
                connection, looker_viewfile, looker_viewfile_loader, extend, reporter
            )
            if not extend_view:
                raise NameError(
                    f"failed to resolve extends view {extend} in view {view_name} of file {looker_viewfile.absolute_file_path}"
                )
            if field in extend_view:
                return extend_view[field]

        return None


field_type_mapping = {
    **POSTGRES_TYPES_MAP,
    **SNOWFLAKE_TYPES_MAP,
    "date": DateTypeClass,
    "date_day_of_month": NumberTypeClass,
    "date_day_of_week": EnumTypeClass,
    "date_day_of_week_index": EnumTypeClass,
    "date_fiscal_month_num": NumberTypeClass,
    "date_fiscal_quarter": DateTypeClass,
    "date_fiscal_quarter_of_year": EnumTypeClass,
    "date_hour": TimeTypeClass,
    "date_hour_of_day": NumberTypeClass,
    "date_month": DateTypeClass,
    "date_month_num": NumberTypeClass,
    "date_month_name": EnumTypeClass,
    "date_quarter": DateTypeClass,
    "date_quarter_of_year": EnumTypeClass,
    "date_time": TimeTypeClass,
    "date_time_of_day": TimeTypeClass,
    "date_microsecond": TimeTypeClass,
    "date_millisecond": TimeTypeClass,
    "date_minute": TimeTypeClass,
    "date_raw": TimeTypeClass,
    "date_second": TimeTypeClass,
    "date_week": TimeTypeClass,
    "date_year": DateTypeClass,
    "date_day_of_year": NumberTypeClass,
    "date_week_of_year": NumberTypeClass,
    "date_fiscal_year": DateTypeClass,
    "duration_day": StringTypeClass,
    "duration_hour": StringTypeClass,
    "duration_minute": StringTypeClass,
    "duration_month": StringTypeClass,
    "duration_quarter": StringTypeClass,
    "duration_second": StringTypeClass,
    "duration_week": StringTypeClass,
    "duration_year": StringTypeClass,
    "distance": NumberTypeClass,
    "duration": NumberTypeClass,
    "location": UnionTypeClass,
    "number": NumberTypeClass,
    "string": StringTypeClass,
    "tier": EnumTypeClass,
    "time": TimeTypeClass,
    "unquoted": StringTypeClass,
    "yesno": BooleanTypeClass,
    "zipcode": EnumTypeClass,
    "int": NumberTypeClass,
    "average": NumberTypeClass,
    "average_distinct": NumberTypeClass,
    "count": NumberTypeClass,
    "count_distinct": NumberTypeClass,
    "list": ArrayTypeClass,
    "max": NumberTypeClass,
    "median": NumberTypeClass,
    "median_distinct": NumberTypeClass,
    "min": NumberTypeClass,
    "percent_of_previous": NumberTypeClass,
    "percent_of_total": NumberTypeClass,
    "percentile": NumberTypeClass,
    "percentile_distinct": NumberTypeClass,
    "running_total": NumberTypeClass,
    "sum": NumberTypeClass,
    "sum_distinct": NumberTypeClass,
}


@dataclass
class LookerExplore:
    name: str
    from_: Optional[str]
    description: Optional[str]
    joins: Optional[List[str]] = None
    field_match = re.compile(r"\${([^}]+)}")

    def _has_different_metadata_from_underlying_view(self) -> bool:
        """
        We only handle alias explores for now, because not handling them breaks lineage.
        Join based explores end up showing up in Chart lineage correctly through to the
        underlying views without needing any special handling.
        """
        if self.from_ is not None and self.name != self.from_ and self.joins is None:
            return True
        return False

    def _get_fields_from_sql_equality(self, sql_fragment: str) -> List[str]:
        return self.field_match.findall(sql_fragment)

    def __init__(self, dict: Dict):
        self.name = dict["name"]
        self.from_ = dict.get("from")
        self.description = dict.get("description")
        self.view_name = dict.get("view_name")
        for join in dict.get("joins", {}):
            sql_on = join.get("sql_on", None)
            if sql_on is not None:
                fields = self._get_fields_from_sql_equality(sql_on)
                self.joins = fields


class LookMLSource(Source):
    source_config: LookMLSourceConfig
    reporter: LookMLSourceReport
    looker_client: Optional[Looker31SDK] = None

    def __init__(self, config: LookMLSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.reporter = LookMLSourceReport()
        if self.source_config.api:
            looker_api = LookerAPI(self.source_config.api)
            self.looker_client = looker_api.get_client()

    @classmethod
    def create(cls, config_dict, ctx):
        config = LookMLSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _load_model(self, path: str) -> LookerModel:
        with open(path, "r") as file:
            parsed = lkml.load(file)
            looker_model = LookerModel.from_looker_dict(
                parsed, str(self.source_config.base_folder), path, self.reporter
            )
        return looker_model

    def _generate_fully_qualified_name(
        self, sql_table_name: str, connection_def: LookerConnectionDefinition
    ) -> str:
        """Returns a fully qualified dataset name, resolved through a connection definition.
        Input sql_table_name can be in three forms: table, db.table, db.schema.table"""
        # TODO: This function should be extracted out into a Platform specific naming class since name translations are required across all connectors

        # Bigquery has "project.db.table" which can be mapped to db.schema.table form
        # All other relational db's follow "db.schema.table"
        # With the exception of mysql, hive which are "db.table"

        # first detect which one we have
        parts = len(sql_table_name.split("."))

        if parts == 3:
            # fully qualified
            return sql_table_name.lower()

        if parts == 1:
            # Bare table form
            dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
            return dataset_name

        if parts == 2:
            # if this is a 2 part platform, we are fine
            if connection_def.platform == "mysql" or connection_def.platform == "hive":
                return sql_table_name
            # otherwise we attach the default top-level container
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
            return dataset_name

        self.reporter.report_warning(
            key=sql_table_name, reason=f"{sql_table_name} has more than 3 parts."
        )
        return sql_table_name.lower()

    def _construct_datalineage_urn(
        self, sql_table_name: str, connection_def: LookerConnectionDefinition
    ) -> str:
        logger.debug(f"sql_table_name={sql_table_name}")

        # Check if table name matches cascading derived tables pattern
        # derived tables can be referred to using aliases that look like table_name.SQL_TABLE_NAME
        # See https://docs.looker.com/data-modeling/learning-lookml/derived-tables#syntax_for_referencing_a_derived_table
        if re.fullmatch(r"\w+\.SQL_TABLE_NAME", sql_table_name):
            sql_table_name = sql_table_name.lower().split(".")[0]

        # Ensure sql_table_name is in canonical form (add in db, schema names)
        sql_table_name = self._generate_fully_qualified_name(
            sql_table_name, connection_def
        )

        return builder.make_dataset_urn(
            connection_def.platform, sql_table_name.lower(), self.source_config.env
        )

    def _get_connection_def_based_on_connection_string(
        self, connection: str
    ) -> LookerConnectionDefinition:
        if self.source_config.connection_to_platform_map is None:
            self.source_config.connection_to_platform_map = {}
        assert self.source_config.connection_to_platform_map is not None

        if connection in self.source_config.connection_to_platform_map:
            return self.source_config.connection_to_platform_map[connection]
        elif self.looker_client:
            try:
                looker_connection: DBConnection = self.looker_client.connection(
                    connection
                )
            except SDKError as e:
                logger.error(f"Failed to retrieve connection {connection} from Looker")
                raise e
            if looker_connection:
                connection_def: LookerConnectionDefinition = (
                    LookerConnectionDefinition.from_looker_connection(looker_connection)
                )

                # Populate the cache (using the config map) to avoid calling looker again for this connection
                self.source_config.connection_to_platform_map[
                    connection
                ] = connection_def
                return connection_def

        raise Exception(f"Could not find a matching looker connection {connection}")

    def _get_upstream_lineage(self, looker_view: LookerView) -> UpstreamLineage:
        upstreams = []
        for sql_table_name in looker_view.sql_table_names:

            sql_table_name = sql_table_name.replace('"', "").replace("`", "")

            upstream = UpstreamClass(
                dataset=self._construct_datalineage_urn(
                    sql_table_name, looker_view.connection
                ),
                type=DatasetLineageTypeClass.VIEW,
            )
            upstreams.append(upstream)

        upstream_lineage = UpstreamLineage(upstreams=upstreams)

        return upstream_lineage

    def _get_field_type(self, native_type: str) -> SchemaFieldDataType:

        type_class = field_type_mapping.get(native_type)

        if type_class is None:

            # attempt Postgres modified type
            type_class = resolve_postgres_modified_type(native_type)

        # if still not found, report a warning
        if type_class is None:
            self.reporter.report_warning(
                native_type,
                f"The type '{native_type}' is not recognized for field type, setting as NullTypeClass.",
            )
            type_class = NullTypeClass

        data_type = SchemaFieldDataType(type=type_class())
        return data_type

    def _get_fields_and_primary_keys(
        self, looker_view: LookerView
    ) -> Tuple[List[SchemaField], List[str]]:
        fields: List[SchemaField] = []
        primary_keys: List = []
        for field in looker_view.fields:
            schema_field = SchemaField(
                fieldPath=field.name,
                type=self._get_field_type(field.type),
                nativeDataType=field.type,
                description=f"{field.field_type.value}. {field.description}",
            )
            fields.append(schema_field)
            if field.is_primary_key:
                primary_keys.append(schema_field.fieldPath)
        return fields, primary_keys

    def _get_schema(self, looker_view: LookerView) -> SchemaMetadataClass:
        fields, primary_keys = self._get_fields_and_primary_keys(looker_view)
        schema_metadata = SchemaMetadata(
            schemaName=looker_view.view_name,
            platform=f"urn:li:dataPlatform:{self.source_config.platform_name}",
            version=0,
            fields=fields,
            primaryKeys=primary_keys,
            hash="",
            platformSchema=OtherSchema(rawSchema="looker-view"),
        )
        return schema_metadata

    def _get_custom_properties(self, looker_view: LookerView) -> DatasetPropertiesClass:
        dataset_props = DatasetPropertiesClass(
            customProperties={"looker.file_content": looker_view.raw_file_content}
        )
        return dataset_props

    def _build_dataset_mce(self, looker_view: LookerView) -> MetadataChangeEvent:
        """
        Creates MetadataChangeEvent for the dataset, creating upstream lineage links
        """
        logger.debug(f"looker_view = {looker_view.view_name}")
        dataset_name = looker_view.view_name

        # Sanitize the urn creation.
        dataset_name = dataset_name.replace('"', "").replace("`", "")
        dataset_snapshot = DatasetSnapshot(
            urn=builder.make_dataset_urn(
                self.source_config.platform_name, dataset_name, self.source_config.env
            ),
            aspects=[],  # we append to this list later on
        )
        dataset_snapshot.aspects.append(Status(removed=False))
        dataset_snapshot.aspects.append(self._get_upstream_lineage(looker_view))
        dataset_snapshot.aspects.append(self._get_schema(looker_view))
        dataset_snapshot.aspects.append(self._get_custom_properties(looker_view))

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

        return mce

    def _build_mce_for_explore(  # noqa: C901
        self, explore: LookerExplore, views_with_workunits: Dict[str, MetadataWorkUnit]
    ) -> Optional[MetadataChangeEvent]:
        # We only generate MCE-s for explores that contain from clauses and do NOT contain joins
        # All other explores (passthrough explores and joins) end in correct resolution of lineage, and don't need additional nodes in the graph.
        if explore.from_ is not None and explore.joins is None:
            # this is an alias explore (explore name different from view name)
            dataset_name = explore.name

            # Sanitize the urn creation.
            dataset_name = dataset_name.replace('"', "").replace("`", "")
            dataset_snapshot = DatasetSnapshot(
                urn=builder.make_dataset_urn(
                    self.source_config.platform_name,
                    dataset_name,
                    self.source_config.env,
                ),
                aspects=[],  # we append to this list later on
            )
            dataset_snapshot.aspects.append(Status(removed=False))
            if explore.from_ in views_with_workunits:
                metadata = views_with_workunits[explore.from_].metadata
                if isinstance(metadata, MetadataChangeEventClass):
                    mce = cast(MetadataChangeEventClass, metadata)
                    for x in metadata.proposedSnapshot.aspects:
                        if isinstance(x, SchemaMetadataClass):
                            logger.info("Found schema metadata")
                            dataset_snapshot.aspects.append(x)
            upstreams = []
            upstream = UpstreamClass(
                dataset=builder.make_dataset_urn(
                    self.source_config.platform_name,
                    explore.from_,
                    self.source_config.env,
                ),
                type=DatasetLineageTypeClass.VIEW,
            )
            upstreams.append(upstream)
            upstream_lineage = UpstreamLineage(upstreams=upstreams)
            dataset_snapshot.aspects.append(upstream_lineage)
            dataset_props = DatasetPropertiesClass(
                customProperties={
                    "looker.type": "explore",
                    "looker.explore.from": str(explore.from_),
                },
                description=explore.description,
            )
            dataset_snapshot.aspects.append(dataset_props)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            return mce
        else:
            self.reporter.report_explores_dropped(explore.name)
            return None

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        viewfile_loader = LookerViewFileLoader(
            str(self.source_config.base_folder), self.reporter
        )

        # some views can be mentioned by multiple 'include' statements, so this set is used to prevent
        # creating duplicate MCE messages
        processed_view_files: Set[str] = set()
        # We cache the entire workunit, because aliased explores copy their schemas from the underlying view
        views_with_workunits: Dict[str, MetadataWorkUnit] = {}

        # The ** means "this directory and all subdirectories", and hence should
        # include all the files we want.
        model_files = sorted(self.source_config.base_folder.glob("**/*.model.lkml"))

        # We collect a list of explores that need to be emitted, after emitting all the underlying views
        explores_to_emit: List[LookerExplore] = []

        for file_path in model_files:
            self.reporter.report_models_scanned()
            model_name = file_path.stem

            if not self.source_config.model_pattern.allowed(model_name):
                self.reporter.report_models_dropped(model_name)
                continue
            try:
                logger.debug(f"Attempting to load model: {file_path}")
                model = self._load_model(str(file_path))
            except Exception as e:
                self.reporter.report_warning(
                    model_name, f"unable to load Looker model at {file_path}: {repr(e)}"
                )
                continue

            assert model.connection is not None
            connectionDefinition = self._get_connection_def_based_on_connection_string(
                model.connection
            )

            for include in model.resolved_includes:
                if include in processed_view_files:
                    logger.debug(f"view '{include}' already processed, skipping it")
                    continue

                logger.debug(f"Attempting to load view file: {include}")
                looker_viewfile = viewfile_loader.load_viewfile(
                    include, connectionDefinition, self.reporter
                )
                if looker_viewfile is not None:
                    for raw_view in looker_viewfile.views:
                        self.reporter.report_views_scanned()
                        try:
                            maybe_looker_view = LookerView.from_looker_dict(
                                raw_view,
                                connectionDefinition,
                                looker_viewfile,
                                viewfile_loader,
                                self.reporter,
                                self.source_config.parse_table_names_from_sql,
                                self.source_config.sql_parser,
                            )
                        except Exception as e:
                            self.reporter.report_warning(
                                include,
                                f"unable to load Looker view {raw_view}: {repr(e)}",
                            )
                            continue
                        if maybe_looker_view:
                            if self.source_config.view_pattern.allowed(
                                maybe_looker_view.view_name
                            ):
                                mce = self._build_dataset_mce(maybe_looker_view)
                                workunit = MetadataWorkUnit(
                                    id=f"lookml-{maybe_looker_view.view_name}", mce=mce
                                )
                                self.reporter.report_workunit(workunit)
                                processed_view_files.add(include)
                                views_with_workunits[
                                    maybe_looker_view.view_name
                                ] = workunit
                                yield workunit
                            else:
                                self.reporter.report_views_dropped(
                                    maybe_looker_view.view_name
                                )
            for explore in model.explores:
                maybe_looker_explore: LookerExplore = LookerExplore(explore)
                self.reporter.report_explores_scanned()
                if maybe_looker_explore._has_different_metadata_from_underlying_view():
                    # we need to emit metadata for this explore
                    explores_to_emit.append(maybe_looker_explore)
                else:
                    self.reporter.report_explores_dropped(maybe_looker_explore.name)

        for candidate_explore in explores_to_emit:
            explore_mce = self._build_mce_for_explore(
                explore=candidate_explore, views_with_workunits=views_with_workunits
            )
            if explore_mce is not None:
                workunit = MetadataWorkUnit(
                    id=f"lookml-{maybe_looker_explore.name}", mce=explore_mce
                )
                self.reporter.report_workunit(workunit)
                yield workunit

    def get_report(self):
        return self.reporter

    def close(self):
        pass
