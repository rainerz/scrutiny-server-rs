use std::collections::HashMap;
use crate::datasource::{WatchableDefinition, WatchableValue};

/// An entry in the in-memory watchable registry.
#[derive(Debug, Clone)]
pub struct DatastoreEntry {
    pub definition: WatchableDefinition,
}

/// Shared, read-only metadata store. Built once at startup from the datasource.
/// Current values are stored separately in `ValueStore`.
#[derive(Debug, Default)]
pub struct Datastore {
    /// Keyed by watchable ID.
    entries: HashMap<String, DatastoreEntry>,
    /// Reverse lookup: display path → ID.
    by_path: HashMap<String, String>,
}

impl Datastore {
    pub fn populate(&mut self, defs: Vec<WatchableDefinition>) {
        for def in defs {
            let id = def.id.clone();
            let path = def.path.clone();
            self.entries.insert(id.clone(), DatastoreEntry { definition: def });
            self.by_path.insert(path, id);
        }
    }

    pub fn get_by_id(&self, id: &str) -> Option<&DatastoreEntry> {
        self.entries.get(id)
    }

    pub fn get_by_path(&self, path: &str) -> Option<&DatastoreEntry> {
        self.by_path.get(path).and_then(|id| self.entries.get(id))
    }

    pub fn id_for_path(&self, path: &str) -> Option<&str> {
        self.by_path.get(path).map(String::as_str)
    }

    pub fn all_entries(&self) -> impl Iterator<Item = &DatastoreEntry> {
        self.entries.values()
    }

    pub fn var_count(&self) -> usize {
        self.entries.values().filter(|e| e.definition.kind == crate::datasource::WatchableKind::Var).count()
    }

    pub fn alias_count(&self) -> usize {
        self.entries.values().filter(|e| e.definition.kind == crate::datasource::WatchableKind::Alias).count()
    }

    pub fn rpv_count(&self) -> usize {
        self.entries.values().filter(|e| e.definition.kind == crate::datasource::WatchableKind::Rpv).count()
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
