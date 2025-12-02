use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

#[pyfunction]
fn hello_from_bin() -> String {
    "Hello from lakeview!".to_string()
}

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

// TODO
#[derive(Debug, Deserialize, Serialize)]
struct MetadataDeltaOperationRemove;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum MetadataDeltaOperation {
    Add(MetadataDeltaOperationAdd),
    Remove(MetadataDeltaOperationRemove),
}

#[derive(Debug, Deserialize, Serialize)]
struct MetadataDeltaOperationMetrics {
    execution_time_ms: u64,
    num_added_files: u16,
    num_added_rows: u64,
    num_partitions: u16,
    num_removed_files: u16,
}

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
    protocol: MetadataDeltaProtocol,
    metadata: MetadataDeltaMetadata,
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

    let protocol_line: ProtocolLine = serde_json::from_str(&lines[0])?;
    let metadata_line: MetadataLine = serde_json::from_str(&lines[1])?;
    let operation: MetadataDeltaOperation = serde_json::from_str(&lines[2])?;
    let commit_line: CommitLine = serde_json::from_str(&lines[3])?;

    Ok(MetadataDelta {
        protocol: protocol_line.protocol,
        metadata: metadata_line.metadata,
        operation: operation,
        commit: commit_line.commit,
    })
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_from_bin, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delta_metadata() {
        let test_path = Path::new("tests/data/delta/uniform/_delta_log/00000000000000000000.json");
        let result = parse_delta_metadata(test_path);

        match result {
            Ok(metadata) => println!("{}", serde_json::to_string_pretty(&metadata).unwrap()),
            Err(e) => panic!("Failed to parse: {}", e),
        }
    }
}
