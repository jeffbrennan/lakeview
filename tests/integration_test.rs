use std::path::Path;
use _core::delta::{get_table_history, summarize_tables};

#[test]
fn test_summarize_tables_delta_big() {
    let test_path = Path::new("tests/data/delta_big");
    let result = summarize_tables(test_path, true);

    assert!(result.is_ok(), "Failed to summarize tables: {:?}", result.err());
    let summaries = result.unwrap();

    assert!(!summaries.is_empty(), "Expected at least one table summary");
    println!("Found {} tables", summaries.len());

    for summary in summaries.iter().take(3) {
        println!("Table: {}, Files: {}, Version: {}",
                 summary.path, summary.num_files, summary.version);
    }
}

#[test]
fn test_get_table_history_delta_big() {
    let test_path = Path::new("tests/data/delta_big");
    let result = get_table_history(test_path, true, Some(5));

    assert!(result.is_ok(), "Failed to get table history: {:?}", result.err());
    let histories = result.unwrap();

    assert!(!histories.is_empty(), "Expected at least one table history");
    println!("Found {} table histories", histories.len());

    for history in histories.iter().take(2) {
        println!("Table: {}, Operations: {}",
                 history.path, history.operations.len());
    }
}
