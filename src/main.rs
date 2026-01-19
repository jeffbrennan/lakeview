use clap::{Parser, ValueEnum};
use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Axis, Block, Borders, Chart, Dataset, Paragraph},
};
use std::io::stdout;
use std::path::PathBuf;

use _core::delta::{get_table_history_paginated, summarize_tables_paginated};

#[derive(Debug, Clone, ValueEnum)]
enum OperationType {
    Write,
    Delete,
    Optimize,
    Vacuum,
}

#[derive(Parser)]
#[command(name = "lakeview")]
#[command(about = "A tool for inspecting Delta Lake tables")]
struct Cli {
    path: PathBuf,

    #[arg(long)]
    summary: bool,

    #[arg(long)]
    history: bool,

    #[arg(long)]
    graph: bool,

    #[arg(short, long)]
    recursive: bool,

    #[arg(long, default_value = "20")]
    n_operations: usize,

    #[arg(short, long, default_value = "5")]
    n_tables: usize,

    #[arg(short, long, default_value = "1")]
    page: usize,

    #[arg(short, long, value_delimiter = ',')]
    operations: Vec<OperationType>,
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

fn format_number(n: i64) -> String {
    let s = n.abs().to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    if n < 0 {
        result.push('-');
    }
    result.chars().rev().collect()
}

fn run_graph(histories: Vec<_core::delta::TableHistory>) -> std::io::Result<()> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let colors = [
        Color::Cyan,
        Color::Yellow,
        Color::Green,
        Color::Magenta,
        Color::Red,
        Color::Blue,
    ];

    let mut all_data: Vec<(String, Vec<(f64, f64)>, Color)> = Vec::new();
    let mut min_version = f64::MAX;
    let mut max_version = f64::MIN;
    let mut max_rows = f64::MIN;

    for (i, history) in histories.iter().enumerate() {
        let mut points: Vec<(f64, f64)> = history
            .operations
            .iter()
            .map(|op| (op.version as f64, op.total_rows as f64))
            .collect();
        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        for (v, r) in &points {
            min_version = min_version.min(*v);
            max_version = max_version.max(*v);
            max_rows = max_rows.max(*r);
        }

        let color = colors[i % colors.len()];
        let name = history
            .path
            .split('/')
            .last()
            .unwrap_or(&history.path)
            .to_string();
        all_data.push((name, points, color));
    }

    // Add some padding to bounds
    let version_padding = (max_version - min_version).max(1.0) * 0.05;
    let rows_padding = max_rows.max(1.0) * 0.05;

    loop {
        terminal.draw(|frame| {
            let area = frame.area();

            let datasets: Vec<Dataset> = all_data
                .iter()
                .map(|(name, points, color)| {
                    Dataset::default()
                        .name(name.clone())
                        .marker(symbols::Marker::Braille)
                        .graph_type(ratatui::widgets::GraphType::Line)
                        .style(Style::default().fg(*color))
                        .data(points)
                })
                .collect();

            let chart = Chart::new(datasets)
                .block(
                    Block::default()
                        .title(" Row Count Over Versions (press 'q' to quit) ")
                        .borders(Borders::ALL),
                )
                .x_axis(
                    Axis::default()
                        .title("Version")
                        .style(Style::default().fg(Color::Gray))
                        .bounds([min_version - version_padding, max_version + version_padding])
                        .labels(vec![
                            Line::from(format!("{:.0}", min_version)),
                            Line::from(format!("{:.0}", (min_version + max_version) / 2.0)),
                            Line::from(format!("{:.0}", max_version)),
                        ]),
                )
                .y_axis(
                    Axis::default()
                        .title("Rows")
                        .style(Style::default().fg(Color::Gray))
                        .bounds([0.0, max_rows + rows_padding])
                        .labels(vec![
                            Line::from("0"),
                            Line::from(format_number((max_rows / 2.0) as i64)),
                            Line::from(format_number(max_rows as i64)),
                        ]),
                );

            let legend_text: String = all_data
                .iter()
                .map(|(name, _, _)| name.clone())
                .collect::<Vec<_>>()
                .join(" | ");

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(10), Constraint::Length(1)])
                .split(area);

            frame.render_widget(chart, chunks[0]);
            frame.render_widget(
                Paragraph::new(legend_text).style(Style::default().fg(Color::DarkGray)),
                chunks[1],
            );
        })?;

        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                    break;
                }
            }
        }
    }

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

fn main() {
    let cli = Cli::parse();

    if cli.summary {
        match summarize_tables_paginated(
            &cli.path,
            cli.recursive,
            Some(cli.n_tables),
            Some(cli.page),
        ) {
            Ok(summaries) => {
                if summaries.is_empty() {
                    eprintln!("No Delta tables found in {:?}", cli.path);
                    return;
                }

                const BOLD: &str = "\x1b[1m";
                const RESET: &str = "\x1b[0m";

                // Print header
                println!(
                    "{BOLD}table                          | version |  files | total_rows |  total_size |        min |        p25 |     median |        p75 |        max |       mean{RESET}"
                );
                println!(
                    "-------------------------------|---------|--------|------------|-------------|------------|------------|------------|------------|------------|----------"
                );

                for summary in summaries {
                    let (min, p25, median, p75, max, mean) =
                        if let Some(stats) = &summary.file_size_stats {
                            (
                                format_bytes(stats.min),
                                format_bytes(stats.p25),
                                format_bytes(stats.median),
                                format_bytes(stats.p75),
                                format_bytes(stats.max),
                                format_bytes(stats.mean as u64),
                            )
                        } else {
                            (
                                String::new(),
                                String::new(),
                                String::new(),
                                String::new(),
                                String::new(),
                                String::new(),
                            )
                        };

                    let table_name = if summary.path.len() > 30 {
                        format!("...{}", &summary.path[summary.path.len() - 27..])
                    } else {
                        summary.path.clone()
                    };

                    println!(
                        "{:<30} | {:>7} | {:>6} | {:>10} | {:>11} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
                        table_name,
                        summary.version,
                        summary.num_files,
                        format_number(summary.total_rows as i64),
                        format_bytes(summary.total_size),
                        min,
                        p25,
                        median,
                        p75,
                        max,
                        mean
                    );
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    } else if cli.history {
        match get_table_history_paginated(
            &cli.path,
            cli.recursive,
            Some(cli.n_operations),
            Some(cli.n_tables),
            Some(cli.page),
        ) {
            Ok(histories) => {
                if histories.is_empty() {
                    eprintln!("No Delta tables found in {:?}", cli.path);
                    return;
                }

                const BOLD: &str = "\x1b[1m";
                const RESET: &str = "\x1b[0m";

                let mut tables_shown = 0;

                for history in histories.iter() {
                    let filtered_ops = if cli.operations.is_empty() {
                        history.operations.iter().collect::<Vec<_>>()
                    } else {
                        history
                            .operations
                            .iter()
                            .filter(|op| {
                                let op_lower = op.operation.to_lowercase();
                                cli.operations.iter().any(|filter_op| match filter_op {
                                    OperationType::Write => {
                                        op_lower.contains("write") || op_lower == "add"
                                    }
                                    OperationType::Delete => {
                                        op_lower.contains("delete") || op_lower == "remove"
                                    }
                                    OperationType::Optimize => op_lower.contains("optimize"),
                                    OperationType::Vacuum => op_lower.contains("vacuum"),
                                })
                            })
                            .collect::<Vec<_>>()
                    };

                    if filtered_ops.is_empty() {
                        continue;
                    }

                    if tables_shown > 0 {
                        println!();
                    }
                    tables_shown += 1;

                    println!("{BOLD}{}{RESET}", history.path);
                    println!(
                        "{BOLD}version | timestamp               | operation    | rows_added | rows_deleted | total_rows | total_size{RESET}"
                    );
                    println!(
                        "--------|-------------------------|--------------|------------|--------------|------------|------------"
                    );

                    for op in filtered_ops {
                        let ts = format_timestamp(op.timestamp as u64);
                        let rows_added = op
                            .rows_added
                            .map(|n| format_number(n as i64))
                            .unwrap_or_default();
                        let rows_deleted = op
                            .rows_deleted
                            .map(|n| format_number(n as i64))
                            .unwrap_or_default();

                        println!(
                            "{:>7} | {} | {:>12} | {:>10} | {:>12} | {:>10} | {:>10}",
                            op.version,
                            ts,
                            op.operation,
                            rows_added,
                            rows_deleted,
                            format_number(op.total_rows),
                            format_bytes(op.total_bytes.max(0) as u64)
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    } else if cli.graph {
        match get_table_history_paginated(&cli.path, cli.recursive, None, None, None) {
            Ok(histories) => {
                if histories.is_empty() {
                    eprintln!("No Delta tables found in {:?}", cli.path);
                    return;
                }
                if let Err(e) = run_graph(histories) {
                    eprintln!("Error running graph: {}", e);
                    std::process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("Please specify --summary, --history, or --graph to view table information");
    }
}
