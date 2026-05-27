// Под-крейт `y2kexplorer` экспортирует часть TUI как библиотеку (для y2k-probe и тестов);
// часть экранов/тем отключена в нынешнем UI и сохранена под фиче-флагами/будущие экраны.
#![allow(dead_code)]

pub mod config;
pub mod kafka;
pub mod labels;
pub mod schema_registry;
pub mod kafka_connect;
