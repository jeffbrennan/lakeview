use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use test_case::test_case;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataDeltaProtocol {
    min_reader_version: u8,
    min_writer_version: u8,
}

#[derive(Debug, Deserialize, Serialize)]
struct MetadataDeltaFormat {
    provider: String,
    options: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataDeltaAdd {
    path: String,
    partition_values: HashMap<String, String>,
    size: u64,
    modification_time: u64,
    data_change: bool,
    stats: String,
    tags: Option<String>,
    base_row_id: Option<String>,
    default_row_commit_version: Option<String>,
    clustering_provider: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataDeltaRemove {
    path: String,
    data_change: bool,
    deletion_timestamp: u64,
    extended_file_metadata: Option<bool>,
    partition_values: HashMap<String, String>,
    size: u64,
}

// #[derive(Debug, Deserialize, Serialize)]
// #[serde(rename_all = "lowercase")]
// enum MetadataDeltaOperation {
//     Add(MetadataDeltaOperationAdd),
//     Remove(MetadataDeltaOperationRemove),
// }

#[derive(Debug, Deserialize, Serialize)]
struct MetadataDeltaOperationMetricsAdd {
    execution_time_ms: u64,
    num_added_files: u16,
    num_added_rows: u64,
    num_partitions: u16,
    num_removed_files: u16,
}

#[derive(Debug, Deserialize, Serialize)]
struct MetadataDeltaOperationMetricsRemove {
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
struct MetadataDeltaOperationMetricsOptimize {
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
struct MetadataDeltaOperationMetricsVacuumStart {
    num_files_to_delete: u16,
    size_of_data_to_delete: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum MetadataDeltaOperationMetrics {
    Add(MetadataDeltaOperationMetricsAdd),
    Remove(MetadataDeltaOperationMetricsRemove),
    Optimize(MetadataDeltaOperationMetricsOptimize),
    VacuumStart(MetadataDeltaOperationMetricsVacuumStart),
}

// TODO parse schema string, stats
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataDeltaCommit {
    timestamp: u128,
    operation: String,
    operation_parameters: HashMap<String, String>,
    engine_info: String,
    operation_metrics: MetadataDeltaOperationMetrics,
    read_version: Option<u32>,
    client_version: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataDeltaMetadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: MetadataDeltaFormat,
    schema_string: String,
    partition_columns: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum DeltaAction {
    Protocol(MetadataDeltaProtocol),
    #[serde(rename = "metaData")]
    Metadata(MetadataDeltaMetadata),
    Add(MetadataDeltaAdd),
    Remove(MetadataDeltaRemove),
    CommitInfo(MetadataDeltaCommit),
}

#[derive(Debug, Default, Serialize)]
struct DeltaLogFile {
    protocol: Option<MetadataDeltaProtocol>,
    metadata: Option<MetadataDeltaMetadata>,
    adds: Vec<MetadataDeltaAdd>,
    removes: Vec<MetadataDeltaRemove>,
    commit_info: Option<MetadataDeltaCommit>,
}

fn read_lines(path: &Path) -> io::Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let lines = reader.lines().collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(lines)
}

fn parse_delta_log(path: &Path) -> io::Result<DeltaLogFile> {
    let lines = read_lines(path)?;
    let mut result = DeltaLogFile::default();

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
