use crate::datasource::{WatchableDefinition, WatchableValue};
use std::collections::HashMap;

/// An entry in the in-memory watchable registry.
#[derive(Debug, Clone)]
pub struct DatastoreEntry {
    pub definition: WatchableDefinition,
    /// 16-bit wire ID assigned automatically at startup for RPV entries.
    /// Sequential, starting at 0. Meaningless for Var/Alias entries.
    pub rpv_id: u16,
}

/// Shared, read-only metadata store. Built once at startup from the datasource.
/// Keyed by `path` — the unique identifier for every watchable.
/// Current values are stored separately in `ValueStore`.
#[derive(Debug, Default)]
pub struct Datastore {
    entries: HashMap<String, DatastoreEntry>,
}

impl Datastore {
    pub fn populate(&mut self, defs: Vec<WatchableDefinition>) {
        let mut next_rpv_id: u16 = 0;
        for def in defs {
            let path = def.path.clone();
            let rpv_id = if def.kind == crate::datasource::WatchableKind::Rpv {
                let assigned = next_rpv_id;
                next_rpv_id = next_rpv_id
                    .checked_add(1)
                    .expect("RPV count exceeds u16 limit (65535)");
                assigned
            } else {
                0
            };
            self.entries.insert(
                path,
                DatastoreEntry {
                    definition: def,
                    rpv_id,
                },
            );
        }
    }

    pub fn get(&self, path: &str) -> Option<&DatastoreEntry> {
        self.entries.get(path)
    }

    pub fn all_entries(&self) -> impl Iterator<Item = &DatastoreEntry> {
        self.entries.values()
    }

    pub fn var_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.definition.kind == crate::datasource::WatchableKind::Var)
            .count()
    }

    pub fn alias_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.definition.kind == crate::datasource::WatchableKind::Alias)
            .count()
    }

    pub fn rpv_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.definition.kind == crate::datasource::WatchableKind::Rpv)
            .count()
    }
}

/// Holds the latest known value for each watchable.
#[derive(Debug, Default)]
pub struct ValueStore {
    values: HashMap<String, WatchableValue>,
}

impl ValueStore {
    pub fn set(&mut self, id: &str, value: WatchableValue) {
        self.values.insert(id.to_owned(), value);
    }

    pub fn get(&self, id: &str) -> Option<&WatchableValue> {
        self.values.get(id)
    }
}
