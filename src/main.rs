use clap::Parser;
use std::path::PathBuf;

use _core::delta::summarize_tables;

#[derive(Parser)]
#[command(name = "lakeview")]
#[command(about = "A tool for inspecting Delta Lake tables")]
struct Cli {
    /// Path to scan for Delta tables
    path: PathBuf,

    /// Print summary information for each table
    #[arg(long)]
    summary: bool,

    /// Recursively scan directories
    #[arg(short, long)]
    recursive: bool,
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_timestamp(ts_ms: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let d = UNIX_EPOCH + Duration::from_millis(ts_ms);
    let datetime: chrono::DateTime<chrono::Utc> = d.into();
    datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn main() {
    let cli = Cli::parse();

    if cli.summary {
        match summarize_tables(&cli.path, cli.recursive) {
            Ok(summaries) => {
                if summaries.is_empty() {
                    eprintln!("No Delta tables found in {:?}", cli.path);
                    return;
                }

                for summary in summaries {
                    println!("Table: {}", summary.path);
                    println!("  Version:       {}", summary.version);
                    println!("  Files:         {}", summary.num_files);
                    if summary.last_modified > 0 {
                        println!("  Last Modified: {}", format_timestamp(summary.last_modified));
                    }
                    if let Some(stats) = summary.file_size_stats {
                        println!("  File Sizes:");
                        println!("    Min:    {}", format_bytes(stats.min));
                        println!("    25%:    {}", format_bytes(stats.p25));
                        println!("    Median: {}", format_bytes(stats.median));
                        println!("    75%:    {}", format_bytes(stats.p75));
                        println!("    Max:    {}", format_bytes(stats.max));
                        println!("    Mean:   {}", format_bytes(stats.mean as u64));
                    }
                    println!();
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("Please specify --summary to view table information");
    }
}
