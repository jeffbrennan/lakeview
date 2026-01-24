use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pyo3::{pyclass, pymethods};
use rayon::prelude::*;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct CheckpointMetadata {
    version: u64,
    size: Option<u64>,
    size_in_bytes: Option<u64>,
    num_of_add_files: Option<u64>,
}

fn read_checkpoint_metadata(delta_log: &Path) -> io::Result<Option<CheckpointMetadata>> {
    let checkpoint_file = delta_log.join("_last_checkpoint");
    if !checkpoint_file.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(checkpoint_file)?;
    let metadata: CheckpointMetadata = serde_json::from_str(&content)?;
    Ok(Some(metadata))
}

fn read_checkpoint_parquet(delta_log: &Path, version: u64) -> io::Result<HashMap<String, FileAdd>> {
    let checkpoint_path = delta_log.join(format!("{:020}.checkpoint.parquet", version));

    if !checkpoint_path.exists() {
        return Ok(HashMap::new());
    }

    let file = fs::File::open(&checkpoint_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut reader = builder
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut active_files = HashMap::new();

    while let Some(batch_result) = reader.next() {
        let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        if let Err(e) = process_checkpoint_batch(&batch, &mut active_files) {
            eprintln!("Warning: failed to process checkpoint batch: {}", e);
        }
    }

    Ok(active_files)
}

fn process_checkpoint_batch(
    batch: &RecordBatch,
    active_files: &mut HashMap<String, FileAdd>,
) -> io::Result<()> {
    let schema = batch.schema();

    let add_column_idx = schema.column_with_name("add").map(|(idx, _)| idx);
    let remove_column_idx = schema.column_with_name("remove").map(|(idx, _)| idx);

    if let Some(idx) = add_column_idx {
        if let Some(add_array) = batch.column(idx).as_struct_opt() {
            let path_array = add_array
                .column_by_name("path")
                .and_then(|col| col.as_string_opt::<i32>());
            let size_array = add_array
                .column_by_name("size")
                .and_then(|col| col.as_primitive_opt::<Int64Type>());
            let modification_time_array = add_array
                .column_by_name("modificationTime")
                .and_then(|col| col.as_primitive_opt::<Int64Type>());

            if let (Some(paths), Some(sizes), Some(mod_times)) =
                (path_array, size_array, modification_time_array)
            {
                for i in 0..batch.num_rows() {
                    if paths.is_null(i) {
                        continue;
                    }

                    let path = paths.value(i).to_string();
                    let size = if sizes.is_null(i) {
                        0
                    } else {
                        sizes.value(i) as u64
                    };
                    let mod_time = if mod_times.is_null(i) {
                        0
                    } else {
                        mod_times.value(i) as u64
                    };

                    let stats_json = add_array
                        .column_by_name("stats")
                        .and_then(|col| col.as_string_opt::<i32>())
                        .and_then(|stats| {
                            if stats.is_null(i) {
                                None
                            } else {
                                Some(stats.value(i))
                            }
                        });

                    let num_records = if let Some(stats_str) = stats_json {
                        serde_json::from_str::<serde_json::Value>(stats_str)
                            .ok()
                            .and_then(|v| v.get("numRecords")?.as_u64())
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    active_files.insert(
                        path.clone(),
                        FileAdd {
                            path,
                            partition_values: HashMap::new(),
                            size,
                            modification_time: mod_time,
                            data_change: true,
                            stats: FileStats {
                                num_records,
                                min_values: HashMap::new(),
                                max_values: HashMap::new(),
                                null_count: HashMap::new(),
                            },
                            tags: None,
                            base_row_id: None,
                            default_row_commit_version: None,
                            clustering_provider: None,
                        },
                    );
                }
            }
        }
    }

    if let Some(idx) = remove_column_idx {
        if let Some(remove_array) = batch.column(idx).as_struct_opt() {
            if let Some(path_array) = remove_array
                .column_by_name("path")
                .and_then(|col| col.as_string_opt::<i32>())
            {
                for i in 0..batch.num_rows() {
                    if !path_array.is_null(i) {
                        let path = path_array.value(i).to_string();
                        active_files.remove(&path);
                    }
                }
            }
        }
    }

    Ok(())
}

fn read_lines(path: &Path) -> io::Result<Vec<String>> {
    let file = fs::File::open(path)?;
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

fn find_delta_logs(path: impl AsRef<Path>, recursive: bool) -> std::io::Result<Vec<PathBuf>> {
    find_delta_logs_with_limit(path, recursive, None)
}

fn find_delta_logs_with_limit(
    path: impl AsRef<Path>,
    recursive: bool,
    limit: Option<usize>,
) -> std::io::Result<Vec<PathBuf>> {
    let path = path.as_ref();
    let mut delta_logs = vec![];

    if path.is_dir() {
        if let Some(name) = path.file_name() {
            if name == "_delta_log" {
                delta_logs.push(path.to_path_buf());
                return Ok(delta_logs);
            }
        }
    }

    let entries = std::fs::read_dir(path)?;
    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();

        if entry_path.is_dir() {
            if let Some(name) = entry_path.file_name() {
                if name == "_delta_log" {
                    delta_logs.push(entry_path);
                    if let Some(lim) = limit {
                        if delta_logs.len() >= lim {
                            return Ok(delta_logs);
                        }
                    }
                } else if recursive {
                    if let Ok(mut sub_logs) = find_delta_logs_with_limit(
                        &entry_path,
                        recursive,
                        limit.map(|lim| lim.saturating_sub(delta_logs.len())),
                    ) {
                        delta_logs.append(&mut sub_logs);
                        if let Some(lim) = limit {
                            if delta_logs.len() >= lim {
                                return Ok(delta_logs);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(delta_logs)
}

/// Collects all JSON files from the given `_delta_log` directories
fn get_json_files_from_delta_logs(delta_logs: &[PathBuf]) -> std::io::Result<Vec<PathBuf>> {
    let files: Vec<PathBuf> = delta_logs
        .par_iter()
        .filter_map(|delta_log| {
            let entries = std::fs::read_dir(delta_log).ok()?;
            let json_files: Vec<PathBuf> = entries
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let path = entry.path();
                    if entry.metadata().ok()?.is_file() {
                        if path.extension()? == "json" {
                            return Some(path);
                        }
                    }
                    None
                })
                .collect();
            Some(json_files)
        })
        .flatten()
        .collect();

    Ok(files)
}

fn parse_directory(
    path: &Path,
    recursive: bool,
) -> HashMap<String, Result<TransactionLog, io::Error>> {
    let delta_logs = find_delta_logs(path, recursive).unwrap_or_default();
    let entries = get_json_files_from_delta_logs(&delta_logs).unwrap_or_default();

    let mut results = HashMap::new();
    for entry in entries {
        let result = parse_delta_log(&entry);
        results.insert(entry.to_string_lossy().into_owned(), result);
    }
    results
}

#[derive(Debug)]
pub struct FileSizeStats {
    pub min: u64,
    pub p25: u64,
    pub median: u64,
    pub p75: u64,
    pub max: u64,
    pub mean: f64,
}

#[derive(Debug)]
pub struct TableSummary {
    pub path: String,
    pub num_files: usize,
    pub version: u64,
    pub last_modified: u64,
    pub total_rows: u64,
    pub total_size: u64,
    pub file_size_stats: Option<FileSizeStats>,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn compute_file_size_stats(sizes: &mut Vec<u64>) -> Option<FileSizeStats> {
    if sizes.is_empty() {
        return None;
    }
    sizes.sort();
    let sum: u64 = sizes.iter().sum();
    Some(FileSizeStats {
        min: sizes[0],
        p25: percentile(sizes, 0.25),
        median: percentile(sizes, 0.5),
        p75: percentile(sizes, 0.75),
        max: sizes[sizes.len() - 1],
        mean: sum as f64 / sizes.len() as f64,
    })
}

pub fn summarize_tables(path: &Path, recursive: bool) -> io::Result<Vec<TableSummary>> {
    summarize_tables_paginated(path, recursive, None, None)
}

pub fn summarize_tables_paginated(
    path: &Path,
    recursive: bool,
    page_size: Option<usize>,
    page: Option<usize>,
) -> io::Result<Vec<TableSummary>> {
    let delta_logs = if let (Some(size), Some(pg)) = (page_size, page) {
        let skip = (pg.saturating_sub(1)) * size;
        let limit = skip + size;
        let all_logs = find_delta_logs_with_limit(path, recursive, Some(limit))?;
        all_logs.into_iter().skip(skip).take(size).collect()
    } else {
        find_delta_logs(path, recursive)?
    };

    if delta_logs.is_empty() {
        return Ok(Vec::new());
    }

    let mut summaries = Vec::new();

    for delta_log in delta_logs {
        let table_path = delta_log
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| delta_log.to_string_lossy().into_owned());

        let checkpoint_metadata = read_checkpoint_metadata(&delta_log).ok().flatten();

        let all_adds = if let Some(ref checkpoint) = checkpoint_metadata {
            read_checkpoint_parquet(&delta_log, checkpoint.version).unwrap_or_default()
        } else {
            HashMap::new()
        };

        let checkpoint_version = checkpoint_metadata.as_ref().map(|c| c.version).unwrap_or(0);

        let json_files = get_json_files_from_delta_logs(&[delta_log.clone()])?;

        let json_files_to_read: Vec<_> = json_files
            .into_iter()
            .filter(|p| {
                p.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|v| v > checkpoint_version)
                    .unwrap_or(true)
            })
            .collect();

        let version = json_files_to_read
            .iter()
            .filter_map(|p| {
                p.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(checkpoint_version);

        let all_adds_mutex = Mutex::new(all_adds);
        let removed_paths_mutex = Mutex::new(std::collections::HashSet::new());

        json_files_to_read.par_iter().for_each(|json_file| {
            if let Ok(log) = parse_delta_log(json_file) {
                if !log.adds.is_empty() {
                    let mut all_adds = all_adds_mutex.lock().unwrap();
                    for add in log.adds {
                        all_adds.insert(add.path.clone(), add);
                    }
                }
                if !log.removes.is_empty() {
                    let mut removed_paths = removed_paths_mutex.lock().unwrap();
                    for remove in log.removes {
                        removed_paths.insert(remove.path);
                    }
                }
            }
        });

        let mut all_adds = all_adds_mutex.into_inner().unwrap();
        let removed_paths = removed_paths_mutex.into_inner().unwrap();

        // Remove deleted files from adds
        for removed in &removed_paths {
            all_adds.remove(removed);
        }

        let active_files: Vec<_> = all_adds.into_values().collect();
        let num_files = active_files.len();
        let last_modified = active_files
            .iter()
            .map(|f| f.modification_time)
            .max()
            .unwrap_or(0);

        let total_rows: u64 = active_files.iter().map(|f| f.stats.num_records).sum();
        let total_size: u64 = active_files.iter().map(|f| f.size).sum();

        let mut sizes: Vec<u64> = active_files.iter().map(|f| f.size).collect();
        let file_size_stats = compute_file_size_stats(&mut sizes);

        summaries.push(TableSummary {
            path: table_path,
            num_files,
            version,
            last_modified,
            total_rows,
            total_size,
            file_size_stats,
        });
    }

    Ok(summaries)
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct OperationRecord {
    #[pyo3(get)]
    pub version: u64,
    #[pyo3(get)]
    pub timestamp: u128,
    #[pyo3(get)]
    pub operation: String,
    #[pyo3(get)]
    pub rows_added: Option<u64>,
    #[pyo3(get)]
    pub rows_deleted: Option<u64>,
    #[pyo3(get)]
    pub rows_copied: Option<u64>,
    #[pyo3(get)]
    pub files_added: Option<u16>,
    #[pyo3(get)]
    pub files_removed: Option<u16>,
    #[pyo3(get)]
    pub bytes_added: u64,
    #[pyo3(get)]
    pub bytes_removed: u64,
    #[pyo3(get)]
    pub total_rows: i64,
    #[pyo3(get)]
    pub total_bytes: i64,
}

#[pymethods]
impl OperationRecord {
    fn __repr__(&self) -> String {
        format!(
            "OperationRecord(version={}, operation='{}', total_rows={})",
            self.version, self.operation, self.total_rows
        )
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct TableHistory {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub operations: Vec<OperationRecord>,
}

#[pymethods]
impl TableHistory {
    fn to_dict(&self, py: pyo3::Python) -> Vec<HashMap<String, pyo3::PyObject>> {
        use pyo3::ToPyObject;

        self.operations
            .iter()
            .map(|op| {
                let mut row = HashMap::new();
                row.insert("table_path".to_string(), self.path.to_object(py));
                row.insert("version".to_string(), op.version.to_object(py));
                row.insert("timestamp".to_string(), (op.timestamp as u64).to_object(py));
                row.insert("operation".to_string(), op.operation.to_object(py));
                row.insert("rows_added".to_string(), op.rows_added.to_object(py));
                row.insert("rows_deleted".to_string(), op.rows_deleted.to_object(py));
                row.insert("rows_copied".to_string(), op.rows_copied.to_object(py));
                row.insert("files_added".to_string(), op.files_added.to_object(py));
                row.insert("files_removed".to_string(), op.files_removed.to_object(py));
                row.insert("bytes_added".to_string(), op.bytes_added.to_object(py));
                row.insert("bytes_removed".to_string(), op.bytes_removed.to_object(py));
                row.insert("total_rows".to_string(), op.total_rows.to_object(py));
                row.insert("total_bytes".to_string(), op.total_bytes.to_object(py));
                row
            })
            .collect()
    }
}

pub fn get_table_history(
    path: &Path,
    recursive: bool,
    limit: Option<usize>,
) -> io::Result<Vec<TableHistory>> {
    get_table_history_paginated(path, recursive, limit, None, None)
}

pub fn get_table_history_paginated(
    path: &Path,
    recursive: bool,
    limit: Option<usize>,
    page_size: Option<usize>,
    page: Option<usize>,
) -> io::Result<Vec<TableHistory>> {
    let delta_logs_to_process = if let (Some(size), Some(pg)) = (page_size, page) {
        let skip = (pg.saturating_sub(1)) * size;
        let max_logs = skip + size;
        let all_logs = find_delta_logs_with_limit(path, recursive, Some(max_logs))?;
        all_logs.into_iter().skip(skip).take(size).collect()
    } else {
        find_delta_logs(path, recursive)?
    };

    let mut histories = Vec::new();

    for delta_log in delta_logs_to_process {
        let table_path = delta_log
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| delta_log.to_string_lossy().into_owned());

        let mut json_files = get_json_files_from_delta_logs(&[delta_log.clone()])?;

        json_files.sort_by(|a, b| {
            let va = a
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let vb = b
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            va.cmp(&vb)
        });

        // Parse logs in parallel, but preserve order
        let parsed_logs: Vec<_> = json_files
            .par_iter()
            .filter_map(|json_file| {
                let version = json_file
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let log = parse_delta_log(json_file).ok()?;
                let bytes_added: u64 = log.adds.iter().map(|f| f.size).sum();
                let bytes_removed: u64 = log.removes.iter().map(|f| f.size).sum();

                let commit = log.commit_info?;
                Some((version, bytes_added, bytes_removed, commit))
            })
            .collect();

        // Compute running totals sequentially
        let mut operations = Vec::new();
        let mut running_rows: i64 = 0;
        let mut running_bytes: i64 = 0;

        for (version, bytes_added, bytes_removed, commit) in parsed_logs {
            let (rows_added, rows_deleted, rows_copied, files_added, files_removed) =
                match commit.operation_metrics {
                    OperationMetrics::Add(m) => (
                        Some(m.num_added_rows),
                        None,
                        None,
                        Some(m.num_added_files),
                        Some(m.num_removed_files),
                    ),
                    OperationMetrics::Remove(m) => (
                        None,
                        Some(m.num_deleted_rows),
                        Some(m.num_copied_rows),
                        Some(m.num_added_files),
                        Some(m.num_removed_files),
                    ),
                    OperationMetrics::Optimize(m) => (
                        None,
                        None,
                        None,
                        Some(m.num_files_added),
                        Some(m.num_files_removed),
                    ),
                    OperationMetrics::VacuumStart(_) => (None, None, None, None, None),
                    OperationMetrics::VacuumEnd(m) => {
                        (None, None, None, None, Some(m.num_deleted_files))
                    }
                };

            running_rows += rows_added.unwrap_or(0) as i64;
            running_rows -= rows_deleted.unwrap_or(0) as i64;
            running_bytes += bytes_added as i64;
            running_bytes -= bytes_removed as i64;

            operations.push(OperationRecord {
                version,
                timestamp: commit.timestamp,
                operation: commit.operation,
                rows_added,
                rows_deleted,
                rows_copied,
                files_added,
                files_removed,
                bytes_added,
                bytes_removed,
                total_rows: running_rows,
                total_bytes: running_bytes,
            });
        }

        operations.reverse();
        if let Some(n) = limit {
            operations.truncate(n);
        }

        histories.push(TableHistory {
            path: table_path,
            operations,
        });
    }

    Ok(histories)
}

fn deserialize_stats<'de, D: Deserializer<'de>>(deserializer: D) -> Result<FileStats, D::Error> {
    let s: String = Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

fn deserialize_schema<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Schema, D::Error> {
    let s: String = Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

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

    #[test_case("tests/data/delta/", true; "multidir-recursive")]
    #[test_case("tests/data/delta/", false; "multidir-non-recursive")]
    fn test_parse_directory(path: &str, recursive: bool) {
        let test_path = Path::new(path);
        let _result = parse_directory(test_path, recursive);
    }

    #[test_case("tests/data/delta/", true, 4; "parent-dir-recursive")]
    #[test_case("tests/data/delta/", false, 0; "parent-dir-non-recursive")]
    #[test_case("tests/data/delta/uniform/", false, 1; "table-dir")]
    #[test_case("tests/data/delta/uniform/_delta_log/", false, 1; "delta-log-dir-direct")]
    fn test_find_delta_logs(path: &str, recursive: bool, expected_count: usize) {
        let test_path = Path::new(path);
        let result = find_delta_logs(test_path, recursive).unwrap();
        assert_eq!(
            result.len(),
            expected_count,
            "Expected {} delta logs, found {}: {:?}",
            expected_count,
            result.len(),
            result
        );
    }
}
