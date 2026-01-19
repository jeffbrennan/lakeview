use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::path::Path;
use _core::delta::{get_table_history, summarize_tables};

fn bench_summarize_tables(c: &mut Criterion) {
    let test_path = Path::new("tests/data/delta_big");

    c.bench_function("summarize_tables_delta_big", |b| {
        b.iter(|| {
            let result = summarize_tables(black_box(test_path), black_box(true));
            black_box(result)
        });
    });
}

fn bench_get_table_history(c: &mut Criterion) {
    let test_path = Path::new("tests/data/delta_big");

    c.bench_function("get_table_history_delta_big", |b| {
        b.iter(|| {
            let result = get_table_history(black_box(test_path), black_box(true), black_box(Some(10)));
            black_box(result)
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_summarize_tables, bench_get_table_history
}
criterion_main!(benches);
