# Начало работы с ExpenseKV

Этот репозиторий содержит самостоятельный сервер на Rust, тесты, Dockerfile,
скрипты сравнения с Redis и результаты экспериментов.

- [README.md](README.md) — сборка, запуск, команды и конфигурация.
- [ARCHITECTURE.md](ARCHITECTURE.md) — обработка запросов, WAL, TTL и компактация.
- [benchmarks/MEMORY_REPORT.md](benchmarks/MEMORY_REPORT.md) — актуальные замеры скорости и памяти.

Быстрая проверка и сборка:

```sh
cargo test --locked
cargo build --release --locked
./target/release/expense-kv
```

Нужны Rust 1.89+ и Linux/macOS. Для Python-тестов и скриптов измерения нужен Python 3.9+;
для сравнения с Redis — redis-server и redis-benchmark в PATH.

Интеграция с Django описана в архитектурном документе как внешний проект.
Его исходники, интеграционные патчи, готовые бинарники и локальная база данных
в этот репозиторий не включены.
