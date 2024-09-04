pub mod in_memory_node;
mod node;

use crate::btree::node::Node;
use crate::error::DbResult;
use crate::kvdb::KVDb;
use std::cell::RefCell;
use std::mem::replace;
use std::rc::Rc;

pub struct BTree<T: Node> {
    branching_factor: usize,
    root: T,
}

impl<T: Node> KVDb for BTree<T> {
    fn description(&self) -> String {
        format!("B-tree with {} nodes", T::description())
    }
    fn set(&mut self, key: &str, value: &str) -> DbResult<()> {
        if let Some(entry_data) = self.root.set(key, value)? {
            let old_root = replace(&mut self.root, T::new(self.branching_factor)?);
            self.root.insert_entry(0, entry_data)?;
            self.root.set_tail(Some(Rc::new(RefCell::new(old_root))))?;
        };
        Ok(())
    }
    fn delete(&mut self, key: &str) -> DbResult<()> {
        todo!()
    }
    fn get(&mut self, key: &str) -> DbResult<Option<String>> {
        self.root.get(key)
    }
}

impl<T: Node> BTree<T> {
    pub fn new(branching_factor: usize) -> DbResult<Self> {
        Ok(BTree {
            branching_factor,
            root: T::new(branching_factor)?,
        })
    }
}
