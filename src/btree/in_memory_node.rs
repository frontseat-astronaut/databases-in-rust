use crate::btree::node::{Entry, Node, NodeRef};
use crate::error::DbResult;
use std::collections::VecDeque;

pub struct InMemoryNode {
    is_leaf: bool,
    entries: VecDeque<Entry<Self>>,
    tail: Option<NodeRef<Self>>,
}

impl Node for InMemoryNode {
    fn branching_factor(&self) -> usize {
        // TODO
        return 3;
    }

    fn is_leaf(&self) -> DbResult<bool> {
        Ok(self.is_leaf)
    }

    fn num_entries(&self) -> DbResult<usize> {
        Ok(self.entries.len())
    }

    fn get_tail(&self) -> DbResult<&Option<NodeRef<Self>>> {
        Ok(&self.tail)
    }

    fn get_entry_data(&self, idx: usize) -> DbResult<Entry<Self>>
    where
        Self: Sized,
    {
        todo!()
    }

    fn set_tail(&mut self, child: Option<NodeRef<Self>>) -> DbResult<()> {
        todo!()
    }

    fn change_value(&mut self, idx: usize, value: &str) -> DbResult<()> {
        todo!()
    }

    fn insert_entry(&mut self, data: Entry<Self>, idx: usize) -> DbResult<()> {
        todo!()
    }

    fn remove_entry(&mut self, idx: usize) -> DbResult<Entry<Self>> {
        todo!()
    }

    fn split(&mut self) -> DbResult<Self> {
        todo!()
    }

    fn new() -> DbResult<Self> {
        todo!()
    }
}
