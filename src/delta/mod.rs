use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

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
struct MetadataDeltaOperationAdd {
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
struct MetadataDeltaOperationRemove {
    path: String,
    data_change: bool,
    deletion_timestamp: u64,
    extended_file_metadata: bool,
    partition_values: HashMap<String, String>,
    size: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum MetadataDeltaOperation {
    Add(MetadataDeltaOperationAdd),
    Remove(MetadataDeltaOperationRemove),
}

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
#[serde(untagged)]
enum MetadataDeltaOperationMetrics {
    Add(MetadataDeltaOperationMetricsAdd),
    Remove(MetadataDeltaOperationMetricsRemove),
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

#[derive(Deserialize)]
struct ProtocolLine {
    protocol: MetadataDeltaProtocol,
}

#[derive(Deserialize)]
struct MetadataLine {
    #[serde(rename = "metaData")]
    metadata: MetadataDeltaMetadata,
}

#[derive(Deserialize)]
struct CommitLine {
    #[serde(rename = "commitInfo")]
    commit: MetadataDeltaCommit,
}

#[derive(Debug, Deserialize, Serialize)]
struct MetadataDelta {
    protocol: Option<MetadataDeltaProtocol>,
    metadata: Option<MetadataDeltaMetadata>,
    operation: MetadataDeltaOperation,
    commit: MetadataDeltaCommit,
}

fn read_lines(path: &Path) -> io::Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let lines = reader.lines().collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(lines)
}

fn parse_delta_metadata(path: &Path) -> io::Result<MetadataDelta> {
    let lines = read_lines(path)?;
    if lines.len() == 2 {
        let operation: MetadataDeltaOperation = serde_json::from_str(&lines[0])?;
        let commit_line: CommitLine = serde_json::from_str(&lines[1])?;

        Ok(MetadataDelta {
            protocol: None,
            metadata: None,
            operation: operation,
            commit: commit_line.commit,
        })
    } else if lines.len() == 4 {
        let protocol_line: ProtocolLine = serde_json::from_str(&lines[0])?;
        let metadata_line: MetadataLine = serde_json::from_str(&lines[1])?;
        let operation: MetadataDeltaOperation = serde_json::from_str(&lines[2])?;
        let commit_line: CommitLine = serde_json::from_str(&lines[3])?;

        Ok(MetadataDelta {
            protocol: Some(protocol_line.protocol),
            metadata: Some(metadata_line.metadata),
            operation: operation,
            commit: commit_line.commit,
        })
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Expected 2 or 4 lines, got {}", lines.len()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delta_metadata_add() {
        let test_path = Path::new("tests/data/delta/uniform/_delta_log/00000000000000000000.json");
        let result = parse_delta_metadata(test_path);

        match result {
            Ok(metadata) => println!("{}", serde_json::to_string_pretty(&metadata).unwrap()),
            Err(e) => panic!("Failed to parse: {}", e),
        }
    }

    #[test]
    fn test_parse_delta_metadata_remove() {
        let test_path =
            Path::new("tests/data/delta/fragmented/_delta_log/00000000000000000052.json");
        let result = parse_delta_metadata(test_path);

        match result {
            Ok(metadata) => println!("{}", serde_json::to_string_pretty(&metadata).unwrap()),
            Err(e) => panic!("Failed to parse: {}", e),
        }
    }
    #[test]
    fn test_parse_delta_metadata_optimize() {
        // TODO: handle optimize case, could be any number of files
        let test_path =
            Path::new("tests/data/delta/compacted/_delta_log/00000000000000000063.json");
        let result = parse_delta_metadata(test_path);

        match result {
            Ok(metadata) => println!("{}", serde_json::to_string_pretty(&metadata).unwrap()),
            Err(e) => panic!("Failed to parse: {}", e),
        }
    }
    #[test]
    fn test_parse_delta_metadata_vacuum_start() {
        // TODO
        let test_path =
            Path::new("tests/data/delta/compacted/_delta_log/00000000000000000064.json");
        let result = parse_delta_metadata(test_path);

        match result {
            Ok(metadata) => println!("{}", serde_json::to_string_pretty(&metadata).unwrap()),
            Err(e) => panic!("Failed to parse: {}", e),
        }
    }
    #[test]
    fn test_parse_delta_metadata_vacuum_end() {
        // TODO
        let test_path =
            Path::new("tests/data/delta/compacted/_delta_log/00000000000000000065.json");
        let result = parse_delta_metadata(test_path);

        match result {
            Ok(metadata) => println!("{}", serde_json::to_string_pretty(&metadata).unwrap()),
            Err(e) => panic!("Failed to parse: {}", e),
        }
    }
}
