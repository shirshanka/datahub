# LookML

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[lookml]'`.

Note! This plugin uses a package that requires Python 3.7+!

## Capabilities

This plugin extracts the following:

- LookML views from model files
- Name, upstream table names, metadata for dimensions, measures, and dimension groups attached as tags
- If API integration is enabled (recommended), resolves table and view names by calling the Looker API, otherwise supports offline resolution of these names. 

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "lookml"
  config:
    # Coordinates
    base_folder: /path/to/model/files

    # Options
    api:
      # Coordinates for your looker instance
      base_url: https://YOUR_INSTANCE.cloud.looker.com

      # Credentials for your Looker connection (https://docs.looker.com/reference/api-and-integration/api-auth)
      client_id: client_id_from_looker 
      client_secret: client_secret_from_looker
      
    # Alternative to API section above if you want a purely file-based ingestion with no api calls to Looker
    # connection_to_platform_map:
    #   connection_name:
    #     platform: snowflake # bigquery, hive, etc
    #     default_db: DEFAULT_DATABASE. # the default database configured for this connection
    #     default_schema: DEFAULT_SCHEMA # the default schema configured for this connection
          
    
sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                          | Required | Default    | Description                                                             |
| ---------------------------------------------- | -------- | ---------- | ----------------------------------------------------------------------- |
| `base_folder`                                  | ✅       |            | Where the `*.model.lkml` and `*.view.lkml` files are stored.            |
| `api.base_url`                                 | ❓ if providing api creds |            | Url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar. | 
| `api.client_id`                                | ❓ if providing api creds |            | Looker API3 client ID.                                 |
| `api.client_secret`                            | ❓ if providing api creds	|            | Looker API3 client secret. | 
| `connection_to_platform_map.<connection_name>` |          |            | Mappings between connection names in the model files to platform, database and schema values |
| `connection_to_platform_map.<connection_name>.platform` | ❓ if not using api         |           | Mappings between connection name in the model files to platform name (e.g. snowflake, bigquery, etc) |
| `connection_to_platform_map.<connection_name>.default_db` | ❓ if not using api         |           | Mappings between connection name in the model files to default database configured for this platform on Looker |
| `connection_to_platform_map.<connection_name>.default_schema` | ❓ if not using api         |           | Mappings between connection name in the model files to default schema configured for this platform on Looker |
| `platform_name`                                |          | `"looker"` | Platform to use in namespace when constructing URNs.                    |
| `model_pattern.allow`                          |          |            | List of regex patterns for models to include in ingestion.                       |
| `model_pattern.deny`                           |          |            | List of regex patterns for models to exclude from ingestion.                     |
| `model_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `view_pattern.allow`                           |          |            | List of regex patterns for views to include in ingestion.                        |
| `view_pattern.deny`                            |          |            | List of regex patterns for views to exclude from ingestion.                      |
| `view_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `env`                                          |          | `"PROD"`   | Environment to use in namespace when constructing URNs.                 |
| `parse_table_names_from_sql`                   |          | `False`    | See note below.                                                         |
| `sql_parser`                                   |          | `datahub.utilities.sql_parser.DefaultSQLParser`    | See note below.                                                         |
| `tag_measures_and_dimensions`   |          | `True`    | When enabled, attaches tags to measures, dimensions and dimension_groups to make them more discoverable. When disabled, adds this information to the description of the column. |

Note! The integration can use an SQL parser to try to parse the tables the views depends on. This parsing is disabled by default, 
but can be enabled by setting `parse_table_names_from_sql: True`.  The default parser is based on the [`sql-metadata`](https://pypi.org/project/sql-metadata/) package. 
As this package doesn't officially support all the SQL dialects that Looker supports, the result might not be correct. You can, however, implement a
custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
