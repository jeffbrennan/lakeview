use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use test_case::test_case;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum DeltaAction {
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    Add(FileAdd),
    Remove(FileRemove),
    CommitInfo(CommitInfo),
}

#[derive(Debug, Default, Serialize)]
struct TransactionLog {
    path: String,
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    adds: Vec<FileAdd>,
    removes: Vec<FileRemove>,
    commit_info: Option<CommitInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Protocol {
    min_reader_version: u8,
    min_writer_version: u8,
}

#[derive(Debug, Deserialize, Serialize)]
struct StorageFormat {
    provider: String,
    options: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileStats {
    num_records: u64,
    min_values: HashMap<String, StatValue>,
    max_values: HashMap<String, StatValue>,
    null_count: HashMap<String, NullCount>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileAdd {
    path: String,
    partition_values: HashMap<String, String>,
    size: u64,
    modification_time: u64,
    data_change: bool,
    #[serde(deserialize_with = "deserialize_stats")]
    stats: FileStats,
    tags: Option<String>,
    base_row_id: Option<String>,
    default_row_commit_version: Option<String>,
    clustering_provider: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileRemove {
    path: String,
    data_change: bool,
    deletion_timestamp: u64,
    extended_file_metadata: Option<bool>,
    partition_values: HashMap<String, String>,
    size: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum StatValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Struct(HashMap<String, StatValue>),
    Array(Vec<StatValue>),
}

/// null counts can be a number or nested for struct fields
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum NullCount {
    Count(u64),
    Nested(HashMap<String, NullCount>),
}

#[derive(Debug, Deserialize, Serialize)]
struct MetricsWrite {
    execution_time_ms: u64,
    num_added_files: u16,
    num_added_rows: u64,
    num_partitions: u16,
    num_removed_files: u16,
}

#[derive(Debug, Deserialize, Serialize)]
struct MetricsDelete {
    execution_time_ms: u64,
    num_added_files: u16,
    num_copied_rows: u64,
    num_deleted_rows: u64,
    num_removed_files: u16,
    rewrite_time_ms: u32,
    scan_time_ms: u32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetricsOptimize {
    files_added: String,
    files_removed: String,
    num_batches: u16,
    num_files_added: u16,
    num_files_removed: u16,
    partitions_optimized: u16,
    preserve_insertion_order: bool,
    total_considered_files: u16,
    total_files_skipped: u16,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetricsVacuumStart {
    num_files_to_delete: u16,
    size_of_data_to_delete: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetricsVacuumEnd {
    num_deleted_files: u16,
    num_vacuumed_directories: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum OperationMetrics {
    Add(MetricsWrite),
    Remove(MetricsDelete),
    Optimize(MetricsOptimize),
    VacuumStart(MetricsVacuumStart),
    VacuumEnd(MetricsVacuumEnd),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommitInfo {
    timestamp: u128,
    operation: String,
    operation_parameters: HashMap<String, String>,
    engine_info: String,
    operation_metrics: OperationMetrics,
    read_version: Option<u32>,
    client_version: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: StorageFormat,
    #[serde(rename = "schemaString", deserialize_with = "deserialize_schema")]
    schema: Schema,
    partition_columns: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Schema {
    #[serde(rename = "type")]
    type_name: String,
    fields: Vec<Field>,
}

/// recursive enum to handle all possible field types
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum FieldType {
    Primitive(String),
    Struct(StructType),
    Array(ArrayType),
    Map(MapType),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StructType {
    #[serde(rename = "type")]
    type_name: String,
    fields: Vec<Field>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArrayType {
    #[serde(rename = "type")]
    type_name: String,
    element_type: Box<FieldType>,
    contains_null: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MapType {
    #[serde(rename = "type")]
    type_name: String,
    key_type: Box<FieldType>,
    value_type: Box<FieldType>,
    value_contains_null: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Field {
    name: String,
    #[serde(rename = "type")]
    field_type: FieldType,
    nullable: bool,
    metadata: HashMap<String, String>,
}

// primary funcs
fn read_lines(path: &Path) -> io::Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let lines = reader.lines().collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(lines)
}

fn parse_delta_log(path: &Path) -> io::Result<TransactionLog> {
    let lines = read_lines(path)?;
    let mut result = TransactionLog {
        path: path.to_string_lossy().into_owned(),
        ..Default::default()
    };

    for (line_num, line) in lines.iter().enumerate() {
        let action: DeltaAction = serde_json::from_str(line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse line {}: {}", line_num + 1, e),
            )
        })?;

        match action {
            DeltaAction::Protocol(p) => result.protocol = Some(p),
            DeltaAction::Metadata(m) => result.metadata = Some(m),
            DeltaAction::Add(a) => result.adds.push(a),
            DeltaAction::Remove(r) => result.removes.push(r),
            DeltaAction::CommitInfo(c) => result.commit_info = Some(c),
        }
    }

    Ok(result)
}

// json string deserializers
fn deserialize_stats<'de, D: Deserializer<'de>>(deserializer: D) -> Result<FileStats, D::Error> {
    let s: String = Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

fn deserialize_schema<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Schema, D::Error> {
    let s: String = Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

#[test_case("tests/data/delta/uniform/_delta_log/00000000000000000000.json" ; "add")]
#[test_case("tests/data/delta/fragmented/_delta_log/00000000000000000052.json" ; "remove")]
#[test_case("tests/data/delta/compacted/_delta_log/00000000000000000063.json" ; "optimize")]
#[test_case("tests/data/delta/compacted/_delta_log/00000000000000000064.json" ; "vacuum_start")]
#[test_case("tests/data/delta/compacted/_delta_log/00000000000000000065.json" ; "vacuum_end")]
fn test_parse_delta_log(path: &str) {
    let test_path = Path::new(path);
    let result = parse_delta_log(test_path);

    match result {
        Ok(metadata) => println!("{}", serde_json::to_string_pretty(&metadata).unwrap()),
        Err(e) => panic!("Failed to parse: {}", e),
    }
}
