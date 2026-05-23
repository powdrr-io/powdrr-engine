use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use powdrr_service_lib::data_contract::{
    CreateTable, DynamoDbTableConfig, RedisTableConfig, ServingPattern, ServingTableConfig,
    SupportDynamoDbTableConfig, SupportKeySchemaConfig, SupportRedisTableConfig,
    SupportTableConfig, TableDescription,
};

use crate::service_impl_provider::{ServiceImplError, SERVICE_IMPL};

const SUPPORT_SURFACES_CONFIG_PATH_ENV: &str = "SUPPORT_SURFACES_CONFIG_PATH";
const SUPPORT_ES_PATTERN_PREFIX: &str = "_support_es_";
const SUPPORT_REDIS_PATTERN_PREFIX: &str = "_support_redis_";
const DYNAMODB_CONFIG_PATTERN_PREFIX: &str = "_dynamodb_";

#[derive(serde::Deserialize, Debug, Clone)]
pub(crate) struct SupportSurfaceBootstrapConfig {
    #[serde(default)]
    pub(crate) tables: Vec<SupportSurfaceBootstrapTable>,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub(crate) struct SupportSurfaceBootstrapTable {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) tags: Option<HashMap<String, String>>,
    pub(crate) support: SupportTableConfig,
}

pub(crate) async fn apply_support_surface_config_from_env() -> Result<(), ServiceImplError> {
    let Some(path) = std::env::var(SUPPORT_SURFACES_CONFIG_PATH_ENV).ok() else {
        return Ok(());
    };

    let config = load_support_surface_config(Path::new(&path)).map_err(ServiceImplError::new)?;
    apply_support_surface_config(config).await
}

fn load_support_surface_config(path: &Path) -> Result<SupportSurfaceBootstrapConfig, String> {
    let body = fs::read_to_string(path)
        .map_err(|error| format!("Failed to read support surfaces config {path:?}: {error}"))?;
    serde_yaml::from_str(&body)
        .map_err(|error| format!("Failed to parse support surfaces config {path:?}: {error}"))
}

async fn apply_support_surface_config(
    config: SupportSurfaceBootstrapConfig,
) -> Result<(), ServiceImplError> {
    for table in config.tables.iter() {
        let existing = SERVICE_IMPL.describe_table(&table.name).await?;
        let create_table = build_create_table_request(existing.as_ref(), table)?;
        SERVICE_IMPL.upsert_table_metadata(&create_table).await?;
    }

    Ok(())
}

fn build_create_table_request(
    existing: Option<&TableDescription>,
    table: &SupportSurfaceBootstrapTable,
) -> Result<CreateTable, ServiceImplError> {
    validate_support_config(&table.support)?;

    let dynamodb = table
        .support
        .dynamodb
        .as_ref()
        .map(|config| derive_support_dynamodb_config(&table.support.key_schema, config));
    let redis = table
        .support
        .redis
        .as_ref()
        .map(|config| derive_support_redis_config(&table.support.key_schema, config));
    let serving = derive_support_serving_config(
        existing.and_then(|description| description.serving.clone()),
        &table.support.key_schema,
        table.support.elasticsearch.is_some(),
        dynamodb.as_ref(),
        redis.as_ref(),
    );

    Ok(CreateTable {
        name: table.name.clone(),
        tags: table.tags.clone().unwrap_or_else(|| {
            existing
                .map(|description| description.tags.clone())
                .unwrap_or_default()
        }),
        serving: Some(serving),
        support: Some(table.support.clone()),
        dynamodb,
        mongodb: existing.and_then(|description| description.mongodb.clone()),
        redis,
    })
}

fn validate_support_config(config: &SupportTableConfig) -> Result<(), ServiceImplError> {
    if config.key_schema.primary_key.trim().is_empty() {
        return Err(ServiceImplError::new(
            "Support primary_key must be a non-empty string".to_string(),
        ));
    }
    if let Some(range_key) = config.key_schema.range_key.as_ref() {
        if range_key.trim().is_empty() {
            return Err(ServiceImplError::new(
                "Support range_key must be a non-empty string when set".to_string(),
            ));
        }
        if range_key == &config.key_schema.primary_key {
            return Err(ServiceImplError::new(
                "Support primary_key and range_key must be different fields".to_string(),
            ));
        }
    }
    if config.elasticsearch.is_none() && config.dynamodb.is_none() && config.redis.is_none() {
        return Err(ServiceImplError::new(
            "Support config must expose at least one surface".to_string(),
        ));
    }

    if let Some(redis) = config.redis.as_ref() {
        if config.key_schema.range_key.is_some() {
            return Err(ServiceImplError::new(
                "Redis support config does not support range_key yet".to_string(),
            ));
        }
        if redis.value_field.trim().is_empty() {
            return Err(ServiceImplError::new(
                "Support Redis value_field must be a non-empty string".to_string(),
            ));
        }
    }

    if let Some(dynamodb) = config.dynamodb.as_ref() {
        validate_support_index_names(dynamodb)?;
    }

    Ok(())
}

fn validate_support_index_names(
    config: &SupportDynamoDbTableConfig,
) -> Result<(), ServiceImplError> {
    let mut seen = HashSet::new();
    for index in config.local_secondary_indexes.iter() {
        if !seen.insert(index.name.clone()) {
            return Err(ServiceImplError::new(format!(
                "Duplicate local secondary index name {}",
                index.name
            )));
        }
    }
    for index in config.global_secondary_indexes.iter() {
        if !seen.insert(index.name.clone()) {
            return Err(ServiceImplError::new(format!(
                "Duplicate global secondary index name {}",
                index.name
            )));
        }
    }
    Ok(())
}

fn derive_support_dynamodb_config(
    key_schema: &SupportKeySchemaConfig,
    config: &SupportDynamoDbTableConfig,
) -> DynamoDbTableConfig {
    DynamoDbTableConfig {
        partition_key: key_schema.primary_key.clone(),
        sort_key: key_schema.range_key.clone(),
        local_secondary_indexes: config.local_secondary_indexes.clone(),
        global_secondary_indexes: config.global_secondary_indexes.clone(),
    }
}

fn derive_support_redis_config(
    key_schema: &SupportKeySchemaConfig,
    config: &SupportRedisTableConfig,
) -> RedisTableConfig {
    RedisTableConfig {
        enabled: true,
        database: config.database,
        key_field: key_schema.primary_key.clone(),
        value_field: Some(config.value_field.clone()),
    }
}

fn derive_support_serving_config(
    existing: Option<ServingTableConfig>,
    key_schema: &SupportKeySchemaConfig,
    expose_elasticsearch: bool,
    dynamodb: Option<&DynamoDbTableConfig>,
    redis: Option<&RedisTableConfig>,
) -> ServingTableConfig {
    let mut patterns = existing.unwrap_or_default();
    patterns.patterns.retain(|pattern| {
        !pattern.name.starts_with(SUPPORT_ES_PATTERN_PREFIX)
            && !pattern.name.starts_with(SUPPORT_REDIS_PATTERN_PREFIX)
            && !pattern.name.starts_with(DYNAMODB_CONFIG_PATTERN_PREFIX)
    });

    if expose_elasticsearch {
        patterns.patterns.extend(derived_support_key_patterns(
            SUPPORT_ES_PATTERN_PREFIX,
            key_schema,
            true,
            true,
        ));
    }

    if redis.is_some() {
        patterns.patterns.push(ServingPattern {
            name: format!("{SUPPORT_REDIS_PATTERN_PREFIX}get"),
            eq_fields: vec![key_schema.primary_key.clone()],
            range_field: None,
            order_field: None,
            descending: false,
            max_limit: Some(1),
            projection: None,
            aggregate: None,
        });
    }

    if let Some(dynamodb) = dynamodb {
        patterns
            .patterns
            .extend(derived_dynamodb_serving_patterns(dynamodb));
    }

    patterns
}

fn derived_support_key_patterns(
    prefix: &str,
    key_schema: &SupportKeySchemaConfig,
    include_get_item_pattern: bool,
    include_exact_query_pattern: bool,
) -> Vec<ServingPattern> {
    let mut patterns = vec![];
    if include_get_item_pattern {
        patterns.push(ServingPattern {
            name: format!("{prefix}get_item"),
            eq_fields: match key_schema.range_key.as_ref() {
                Some(range_key) => vec![key_schema.primary_key.clone(), range_key.clone()],
                None => vec![key_schema.primary_key.clone()],
            },
            range_field: None,
            order_field: None,
            descending: false,
            max_limit: Some(1),
            projection: None,
            aggregate: None,
        });
    }
    if include_exact_query_pattern {
        if let Some(range_key) = key_schema.range_key.as_ref() {
            patterns.push(ServingPattern {
                name: format!("{prefix}exact_query"),
                eq_fields: vec![key_schema.primary_key.clone(), range_key.clone()],
                range_field: None,
                order_field: None,
                descending: false,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
        }
    }
    if let Some(range_key) = key_schema.range_key.as_ref() {
        for descending in [false, true] {
            patterns.push(ServingPattern {
                name: format!(
                    "{prefix}partition_query_{}",
                    if descending { "desc" } else { "asc" }
                ),
                eq_fields: vec![key_schema.primary_key.clone()],
                range_field: None,
                order_field: Some(range_key.clone()),
                descending,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
            patterns.push(ServingPattern {
                name: format!(
                    "{prefix}range_query_{}",
                    if descending { "desc" } else { "asc" }
                ),
                eq_fields: vec![key_schema.primary_key.clone()],
                range_field: Some(range_key.clone()),
                order_field: Some(range_key.clone()),
                descending,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
        }
    }
    patterns
}

fn sanitize_serving_pattern_suffix(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn derived_dynamodb_serving_patterns(config: &DynamoDbTableConfig) -> Vec<ServingPattern> {
    let mut patterns = derived_support_key_patterns(
        DYNAMODB_CONFIG_PATTERN_PREFIX,
        &SupportKeySchemaConfig {
            primary_key: config.partition_key.clone(),
            range_key: config.sort_key.clone(),
        },
        true,
        false,
    );
    for index in config.local_secondary_indexes.iter() {
        let prefix = format!(
            "{}lsi_{}_",
            DYNAMODB_CONFIG_PATTERN_PREFIX,
            sanitize_serving_pattern_suffix(&index.name)
        );
        patterns.extend(derived_support_key_patterns(
            &prefix,
            &SupportKeySchemaConfig {
                primary_key: config.partition_key.clone(),
                range_key: Some(index.sort_key.clone()),
            },
            false,
            true,
        ));
    }
    for index in config.global_secondary_indexes.iter() {
        let prefix = format!(
            "{}gsi_{}_",
            DYNAMODB_CONFIG_PATTERN_PREFIX,
            sanitize_serving_pattern_suffix(&index.name)
        );
        patterns.extend(derived_support_key_patterns(
            &prefix,
            &SupportKeySchemaConfig {
                primary_key: index.partition_key.clone(),
                range_key: index.sort_key.clone(),
            },
            false,
            true,
        ));
    }
    patterns
}

#[cfg(test)]
mod tests {
    use super::{
        build_create_table_request, load_support_surface_config, SupportSurfaceBootstrapTable,
    };
    use powdrr_service_lib::data_contract::{
        SupportDynamoDbTableConfig, SupportElasticSearchTableConfig, SupportKeySchemaConfig,
        SupportRedisTableConfig, SupportTableConfig,
    };
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    #[test]
    fn builds_create_table_from_support_config() {
        let request = build_create_table_request(
            None,
            &SupportSurfaceBootstrapTable {
                name: "events".to_string(),
                tags: Some(HashMap::from([("team".to_string(), "serving".to_string())])),
                support: SupportTableConfig {
                    key_schema: SupportKeySchemaConfig {
                        primary_key: "tenant".to_string(),
                        range_key: Some("event_id".to_string()),
                    },
                    elasticsearch: Some(SupportElasticSearchTableConfig::default()),
                    dynamodb: Some(SupportDynamoDbTableConfig::default()),
                    redis: None,
                },
            },
        )
        .unwrap();

        assert_eq!(request.name, "events");
        assert_eq!(
            request.support.as_ref().unwrap().key_schema.primary_key,
            "tenant"
        );
        assert_eq!(
            request.dynamodb.as_ref().unwrap().partition_key,
            "tenant".to_string()
        );
        let pattern_names = request
            .serving
            .as_ref()
            .unwrap()
            .patterns
            .iter()
            .map(|pattern| pattern.name.clone())
            .collect::<Vec<_>>();
        assert!(pattern_names.contains(&"_support_es_get_item".to_string()));
        assert!(pattern_names.contains(&"_support_es_exact_query".to_string()));
        assert!(pattern_names.contains(&"_dynamodb_get_item".to_string()));
    }

    #[test]
    fn parses_yaml_support_surface_config() {
        let file = NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"
tables:
  - name: sessions
    support:
      key_schema:
        primary_key: session_id
      redis:
        database: 3
        value_field: payload_json
"#,
        )
        .unwrap();

        let config = load_support_surface_config(file.path()).unwrap();
        assert_eq!(config.tables.len(), 1);
        assert_eq!(config.tables[0].name, "sessions");
        assert_eq!(
            config.tables[0].support.redis.as_ref().unwrap().value_field,
            "payload_json"
        );
    }

    #[test]
    fn rejects_redis_range_key_support() {
        let result = build_create_table_request(
            None,
            &SupportSurfaceBootstrapTable {
                name: "sessions".to_string(),
                tags: None,
                support: SupportTableConfig {
                    key_schema: SupportKeySchemaConfig {
                        primary_key: "tenant".to_string(),
                        range_key: Some("event_id".to_string()),
                    },
                    elasticsearch: None,
                    dynamodb: None,
                    redis: Some(SupportRedisTableConfig {
                        database: 1,
                        value_field: "payload".to_string(),
                    }),
                },
            },
        );

        assert!(result.unwrap_err().message.contains("range_key"));
    }
}
