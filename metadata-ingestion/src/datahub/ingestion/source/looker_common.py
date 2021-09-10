import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple

from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.methods import Looker31SDK

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import (
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    resolve_postgres_modified_type,
)
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
    BrowsePathsClass,
    DatasetPropertiesClass,
    EnumTypeClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    StatusClass,
    TagAssociationClass,
    TagPropertiesClass,
    TagSnapshotClass,
)

# from pydantic import root_validator, validator


logger = logging.getLogger(__name__)


class LookerCommonConfig(ConfigModel):
    tag_measures_and_dimensions: bool = True


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


class LookerUtil:
    field_type_mapping = {
        **POSTGRES_TYPES_MAP,
        **SNOWFLAKE_TYPES_MAP,
        "date": DateTypeClass,
        "date_date": DateTypeClass,
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

    @staticmethod
    def _extract_view_from_field(field: str) -> str:
        assert (
            field.count(".") == 1
        ), f"Error: A field must be prefixed by a view name, field is: {field}"
        view_name = field.split(".")[0]
        return view_name

    @staticmethod
    def _get_field_type(
        native_type: str, reporter: SourceReport
    ) -> SchemaFieldDataType:

        type_class = LookerUtil.field_type_mapping.get(native_type)

        if type_class is None:

            # attempt Postgres modified type
            type_class = resolve_postgres_modified_type(native_type)

        # if still not found, report a warning
        if type_class is None:
            reporter.report_warning(
                native_type,
                f"The type '{native_type}' is not recognized for field type, setting as NullTypeClass.",
            )
            type_class = NullTypeClass

        data_type = SchemaFieldDataType(type=type_class())
        return data_type

    @staticmethod
    def _get_schema(
        platform_name: str,
        schema_name: str,
        view_fields: List[ViewField],
        reporter: SourceReport,
    ) -> SchemaMetadataClass:
        fields, primary_keys = LookerUtil._get_fields_and_primary_keys(
            view_fields=view_fields, reporter=reporter
        )
        schema_metadata = SchemaMetadata(
            schemaName=schema_name,
            platform=f"urn:li:dataPlatform:{platform_name}",
            version=0,
            fields=fields,
            primaryKeys=primary_keys,
            hash="",
            platformSchema=OtherSchema(rawSchema="looker-view"),
        )
        return schema_metadata

    type_to_tag_map: Dict[ViewFieldType, List[str]] = {
        ViewFieldType.DIMENSION: ["urn:li:tag:datahub.dimension"],
        ViewFieldType.DIMENSION_GROUP: [
            "urn:li:tag:datahub.dimension",
            "urn:li:tag:datahub.temporal",
        ],
        ViewFieldType.MEASURE: ["urn:li:tag:datahub.measure"],
    }

    tag_definitions: Dict[str, TagPropertiesClass] = {
        "urn:li:tag:Dimension": TagPropertiesClass(
            name="Dimension",
            description="A tag that is applied to all dimension fields.",
        ),
        "urn:li:tag:Temporal": TagPropertiesClass(
            name="Temporal",
            description="A tag that is applied to all time-based (temporal) fields such as timestamps or durations.",
        ),
        "urn:li:tag:Measure": TagPropertiesClass(
            name="Measure",
            description="A tag that is applied to all measures (metrics). Measures are typically the columns that you aggregate on",
        ),
    }

    @staticmethod
    def _get_tag_mce_for_urn(tag_urn: str) -> MetadataChangeEvent:
        assert tag_urn in LookerUtil.tag_definitions
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:datahub",
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeEvent(
            proposedSnapshot=TagSnapshotClass(
                urn=tag_urn, aspects=[ownership, LookerUtil.tag_definitions[tag_urn]]
            )
        )

    @staticmethod
    def _get_tags_from_field_type(
        field_type: ViewFieldType, reporter: SourceReport
    ) -> Optional[GlobalTagsClass]:
        if field_type in LookerUtil.type_to_tag_map:
            return GlobalTagsClass(
                tags=[
                    TagAssociationClass(tag=tag_name)
                    for tag_name in LookerUtil.type_to_tag_map[field_type]
                ]
            )
        else:
            reporter.report_warning(
                "lookml",
                "Failed to map view field type {field_type}. Won't emit tags for it",
            )
            return None

    @staticmethod
    def get_tag_mces() -> Iterable[MetadataChangeEvent]:
        # Emit tag MCEs for measures and dimensions:
        return [
            LookerUtil._get_tag_mce_for_urn(tag_urn)
            for tag_urn in LookerUtil.tag_definitions
        ]

    @staticmethod
    def _get_fields_and_primary_keys(
        view_fields: List[ViewField],
        reporter: SourceReport,
        tag_measures_and_dimensions: bool = True,
    ) -> Tuple[List[SchemaField], List[str]]:
        primary_keys: List = []
        fields = []
        for field in view_fields:
            schema_field = SchemaField(
                fieldPath=field.name,
                type=LookerUtil._get_field_type(field.type, reporter),
                nativeDataType=field.type,
                description=f"{field.description}"
                if tag_measures_and_dimensions is True
                else f"{field.field_type.value}. {field.description}",
                globalTags=LookerUtil._get_tags_from_field_type(
                    field.field_type, reporter
                )
                if tag_measures_and_dimensions is True
                else None,
            )
            fields.append(schema_field)
            if field.is_primary_key:
                primary_keys.append(schema_field.fieldPath)
        return fields, primary_keys


@dataclass
class LookerExplore:
    name: str
    model_name: str
    project_name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    upstream_views: Optional[
        List[str]
    ] = None  # captures the view name(s) this explore is derived from
    joins: Optional[List[str]] = None
    fields: Optional[List[ViewField]] = None  # the fields exposed in this explore
    shell_explore: bool = False  # set to true when a shell class is created

    def make_explore_urn(self, env: str, platform: str) -> str:
        dataset_name = f"{self.model_name}.explore.{self.name}"
        dataset_name = dataset_name.replace('"', "").replace("`", "")
        return builder.make_dataset_urn(platform, dataset_name, env)

    @staticmethod
    def _get_fields_from_sql_equality(sql_fragment: str) -> List[str]:
        field_match = re.compile(r"\${([^}]+)}")
        return field_match.findall(sql_fragment)

    @classmethod
    def __from_dict(cls, model_name: str, dict: Dict) -> "LookerExplore":
        if dict.get("joins", {}) != {}:
            assert "joins" in dict
            view_names = set()
            for join in dict["joins"]:
                sql_on = join.get("sql_on", None)
                if sql_on is not None:
                    fields = cls._get_fields_from_sql_equality(sql_on)
                    joins = fields
                    for f in fields:
                        view_names.add(LookerUtil._extract_view_from_field(f))
        else:
            # non-join explore, get view_name from `from` field if possible, default to explore name
            view_names = set(dict.get("from", dict.get("name")))
        return LookerExplore(
            model_name=model_name,
            name=dict["name"],
            label=dict.get("label"),
            description=dict.get("description"),
            upstream_views=list(view_names),
            joins=joins,
        )

    @classmethod
    def from_api(  # noqa: C901
        cls,
        model: str,
        explore_name: str,
        client: Looker31SDK,
        reporter: SourceReport,
    ) -> Optional["LookerExplore"]:
        try:
            explore = client.lookml_model_explore(model, explore_name)
            views = set()
            if explore.joins is not None and explore.joins != []:
                for e_join in [
                    e for e in explore.joins if e.dependent_fields is not None
                ]:
                    assert e_join.dependent_fields is not None
                    for field_name in e_join.dependent_fields:
                        try:
                            view_name = LookerUtil._extract_view_from_field(field_name)
                            views.add(view_name)
                        except AssertionError:
                            reporter.report_warning(
                                key=f"chart-field-{field_name}",
                                reason="The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
                            )
                            continue
            else:
                assert explore.view_name is not None
                views.add(explore.view_name)

            view_fields: List[ViewField] = []
            if explore.fields is not None:
                if explore.fields.dimensions is not None:
                    for dim_field in explore.fields.dimensions:
                        if dim_field.name is None:
                            continue
                        else:
                            view_fields.append(
                                ViewField(
                                    name=dim_field.name,
                                    description=dim_field.description
                                    if dim_field.description
                                    else "",
                                    type=dim_field.type
                                    if dim_field.type is not None
                                    else "",
                                    field_type=ViewFieldType.DIMENSION_GROUP
                                    if dim_field.dimension_group is not None
                                    else ViewFieldType.DIMENSION,
                                    is_primary_key=dim_field.primary_key
                                    if dim_field.primary_key
                                    else False,
                                )
                            )
                if explore.fields.measures is not None:
                    for measure_field in explore.fields.measures:
                        if measure_field.name is None:
                            continue
                        else:
                            view_fields.append(
                                ViewField(
                                    name=measure_field.name,
                                    description=measure_field.description
                                    if measure_field.description
                                    else "",
                                    type=measure_field.type
                                    if measure_field.type is not None
                                    else "",
                                    field_type=ViewFieldType.MEASURE,
                                    is_primary_key=measure_field.primary_key
                                    if measure_field.primary_key
                                    else False,
                                )
                            )

            return cls(
                name=explore_name,
                model_name=model,
                project_name=explore.project_name,
                label=explore.label,
                description=explore.description,
                fields=view_fields,
                upstream_views=list(views),
            )
        except SDKError:
            logger.warn(
                "Failed to extract explore {} from model {}.".format(
                    explore_name, model
                )
            )
            # raise ValueError(
            #    "Failed to extract explore {} from model {}.".format(
            #        explore_name, model
            #    )
            # )
        except AssertionError:
            reporter.report_warning(
                key="chart-",
                reason="Was unable to find dependent views for this chart",
            )
        return None

    def _to_mce(  # noqa: C901
        self,
        env: str,
        platform: str,
        reporter: SourceReport,
        views_with_workunits: Dict[str, MetadataWorkUnit],
    ) -> Optional[MetadataChangeEvent]:
        # We only generate MCE-s for explores that contain from clauses and do NOT contain joins
        # All other explores (passthrough explores and joins) end in correct resolution of lineage, and don't need additional nodes in the graph.

        dataset_snapshot = DatasetSnapshot(
            urn=self.make_explore_urn(env, platform),
            aspects=[],  # we append to this list later on
        )
        browse_paths = BrowsePathsClass(
            paths=[
                f"/{env.lower()}/looker/{self.project_name}/explores/{self.model_name}.{self.name}"
            ]
        )
        dataset_snapshot.aspects.append(browse_paths)
        dataset_snapshot.aspects.append(StatusClass(removed=False))

        custom_properties = {"looker.type": "explore"}
        if self.label is not None:
            custom_properties["looker.explore.label"] = str(self.label)
        dataset_props = DatasetPropertiesClass(
            description=self.description,
            customProperties=custom_properties,
        )
        dataset_snapshot.aspects.append(dataset_props)

        if self.upstream_views is not None:
            upstreams = [
                UpstreamClass(
                    dataset=builder.make_dataset_urn(
                        platform,
                        f"{self.project_name}.{view_name}",  # view names are prefixed with project name
                        env,
                    ),
                    type=DatasetLineageTypeClass.VIEW,
                )
                for view_name in self.upstream_views
            ]
            upstream_lineage = UpstreamLineage(upstreams=upstreams)
            dataset_snapshot.aspects.append(upstream_lineage)
        if self.fields is not None:
            schema_metadata = LookerUtil._get_schema(
                platform_name=platform,
                schema_name=self.name,
                view_fields=self.fields,
                reporter=reporter,
            )
            dataset_snapshot.aspects.append(schema_metadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return mce
