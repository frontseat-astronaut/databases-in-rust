use crate::btree::node::{Entry, Node};
use crate::error::DbResult;
use crate::error::Error::IndexOutOfBounds;
use std::cell::RefCell;
use std::mem::replace;
use std::rc::Rc;

#[derive(Clone)]
pub struct InMemoryNode {
    branching_factor: usize,
    entries: Vec<Entry<Self>>,
    tail: Option<Rc<RefCell<Self>>>,
}

impl Node for InMemoryNode {
    fn branching_factor(&self) -> usize {
        self.branching_factor
    }

    fn num_entries(&self) -> DbResult<usize> {
        Ok(self.entries.len())
    }

    fn get_tail(&self) -> DbResult<Option<Rc<RefCell<Self>>>> {
        Ok(Self::copy_pointer(&self.tail))
    }

    fn get_entry_data(&self, idx: usize) -> DbResult<Entry<Self>>
    where
        Self: Sized,
    {
        match self.entries.get(idx) {
            Some(entry) => Ok(Entry {
                key: entry.key.to_owned(),
                value: entry.value.to_owned(),
                child: Self::copy_pointer(&entry.child),
            }),
            None => Err(IndexOutOfBounds(idx)),
        }
    }

    fn set_tail(&mut self, child: Option<Rc<RefCell<Self>>>) -> DbResult<()> {
        self.tail = child;
        Ok(())
    }

    fn change_value(&mut self, idx: usize, value: &str) -> DbResult<()> {
        if idx >= self.entries.len() {
            return Err(IndexOutOfBounds(idx));
        }
        self.entries[idx].value = value.to_string();
        Ok(())
    }

    fn insert_entry(&mut self, idx: usize, data: Entry<Self>) -> DbResult<()> {
        self.entries.insert(idx, data);
        Ok(())
    }

    fn remove_entry(&mut self, idx: usize) -> DbResult<Entry<Self>> {
        if idx >= self.entries.len() {
            return Err(IndexOutOfBounds(idx));
        }
        Ok(self.entries.remove(idx))
    }

    fn split_half(&mut self) -> DbResult<Self> {
        let length = self.entries.len();
        let right_half_entries = self.entries.split_off(length / 2);
        let left_half_entries = replace(&mut self.entries, right_half_entries);
        Ok(Self {
            branching_factor: self.branching_factor,
            entries: left_half_entries,
            tail: None,
        })
    }

    fn description() -> String {
        "In-Memory".to_string()
    }

    fn new(branching_factor: usize) -> DbResult<Self> {
        Ok(Self {
            branching_factor,
            entries: Vec::new(),
            tail: None,
        })
    }
}

impl InMemoryNode {
    fn copy_pointer(ptr: &Option<Rc<RefCell<Self>>>) -> Option<Rc<RefCell<Self>>> {
        match ptr {
            None => None,
            Some(node_ref) => Some(Rc::clone(node_ref)),
        }
    }
}
