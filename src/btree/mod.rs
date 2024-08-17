mod in_memory_node;
mod node;

use crate::btree::node::{Node, NodeRef};
use crate::error::DbResult;
use std::mem::replace;

struct BTree<T: Node> {
    root: T,
}

impl<T: Node> BTree<T> {
    fn set(&mut self, key: &str, value: &str) -> DbResult<()> {
        if let Some(entry_data) = self.root.set(key, value)? {
            let old_root = replace(&mut self.root, T::new()?);
            self.root.insert_entry(entry_data, 0)?;
            self.root.set_tail(Some(NodeRef::new(old_root)))?;
        };
        Ok(())
    }
    fn get(&self, key: &str) -> DbResult<Option<String>> {
        self.root.get(key)
    }
}
