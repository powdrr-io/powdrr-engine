use crate::data_contract::{
    FileDescriptor, IcebergColumnStats, IcebergFileStats, IcebergRowGroupStats,
};
use serde_json::Value;
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryPredicate {
    pub field: String,
    pub eq: Option<Value>,
    pub in_values: Option<Vec<Value>>,
    pub gt: Option<Value>,
    pub gte: Option<Value>,
    pub lt: Option<Value>,
    pub lte: Option<Value>,
}

pub fn group_files_by_schema(files: &[FileDescriptor]) -> Vec<Vec<FileDescriptor>> {
    let mut groups: Vec<Vec<FileDescriptor>> = vec![];

    for file in files.iter().cloned() {
        if let Some(existing_group) = groups.iter_mut().find(|group| {
            group
                .first()
                .map(|existing| existing.schema == file.schema)
                .unwrap_or(false)
        }) {
            existing_group.push(file);
        } else {
            groups.push(vec![file]);
        }
    }

    groups
}

pub fn file_may_match_predicates(
    file_stats: &IcebergFileStats,
    predicates: &[QueryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| predicate_may_match_file(file_stats, predicate))
}

pub fn row_group_may_match_predicates(
    row_group_stats: &IcebergRowGroupStats,
    predicates: &[QueryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| predicate_may_match_row_group(row_group_stats, predicate))
}

fn predicate_may_match_file(file_stats: &IcebergFileStats, predicate: &QueryPredicate) -> bool {
    let Some(column_stats) = file_stats
        .columns
        .iter()
        .find(|stats| stats.field_name == predicate.field)
    else {
        return true;
    };

    predicate_may_match_stats(column_stats, file_stats.record_count, predicate)
}

fn predicate_may_match_row_group(
    row_group_stats: &IcebergRowGroupStats,
    predicate: &QueryPredicate,
) -> bool {
    let Some(column_stats) = row_group_stats
        .columns
        .iter()
        .find(|stats| stats.field_name == predicate.field)
    else {
        return true;
    };

    predicate_may_match_stats(column_stats, row_group_stats.record_count, predicate)
}

fn predicate_may_match_stats(
    column_stats: &IcebergColumnStats,
    record_count: Option<u64>,
    predicate: &QueryPredicate,
) -> bool {
    if let Some(eq) = predicate.eq.as_ref() {
        return equality_may_match(column_stats, record_count, eq);
    }
    if let Some(values) = predicate.in_values.as_ref() {
        return values
            .iter()
            .any(|value| equality_may_match(column_stats, record_count, value));
    }

    range_may_match(column_stats, record_count, predicate)
}

fn equality_may_match(
    column_stats: &IcebergColumnStats,
    record_count: Option<u64>,
    value: &Value,
) -> bool {
    if column_is_all_null(column_stats, record_count) {
        return false;
    }

    if let Some(lower_bound) = column_stats.lower_bound.as_ref() {
        if matches!(
            compare_scalar_values(value, lower_bound),
            Some(Ordering::Less)
        ) {
            return false;
        }
    }
    if let Some(upper_bound) = column_stats.upper_bound.as_ref() {
        if matches!(
            compare_scalar_values(value, upper_bound),
            Some(Ordering::Greater)
        ) {
            return false;
        }
    }

    true
}

fn range_may_match(
    column_stats: &IcebergColumnStats,
    record_count: Option<u64>,
    predicate: &QueryPredicate,
) -> bool {
    if column_is_all_null(column_stats, record_count) {
        return false;
    }

    if let Some(value) = predicate.gt.as_ref() {
        if let Some(upper_bound) = column_stats.upper_bound.as_ref() {
            if matches!(
                compare_scalar_values(upper_bound, value),
                Some(Ordering::Less | Ordering::Equal)
            ) {
                return false;
            }
        }
    }
    if let Some(value) = predicate.gte.as_ref() {
        if let Some(upper_bound) = column_stats.upper_bound.as_ref() {
            if matches!(
                compare_scalar_values(upper_bound, value),
                Some(Ordering::Less)
            ) {
                return false;
            }
        }
    }
    if let Some(value) = predicate.lt.as_ref() {
        if let Some(lower_bound) = column_stats.lower_bound.as_ref() {
            if matches!(
                compare_scalar_values(lower_bound, value),
                Some(Ordering::Greater | Ordering::Equal)
            ) {
                return false;
            }
        }
    }
    if let Some(value) = predicate.lte.as_ref() {
        if let Some(lower_bound) = column_stats.lower_bound.as_ref() {
            if matches!(
                compare_scalar_values(lower_bound, value),
                Some(Ordering::Greater)
            ) {
                return false;
            }
        }
    }

    true
}

pub fn column_is_all_null(column_stats: &IcebergColumnStats, record_count: Option<u64>) -> bool {
    match (column_stats.null_count, record_count) {
        (Some(null_count), Some(record_count)) => null_count >= record_count,
        _ => false,
    }
}

pub fn compare_scalar_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64()?.partial_cmp(&right.as_f64()?),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}
