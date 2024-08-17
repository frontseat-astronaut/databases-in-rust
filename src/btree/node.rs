use crate::error::DbResult;
use std::cell::RefCell;
use std::cmp::Ordering;

pub type NodeRef<T> = RefCell<T>;

pub struct Entry<T: Node> {
    key: String,
    value: String,
    child: Option<NodeRef<T>>,
}

pub trait Node
where
    Self: Sized,
{
    fn set(&mut self, key: &str, value: &str) -> DbResult<Option<Entry<Self>>>
    where
        Self: Sized,
    {
        let (maybe_child, maybe_key_value, idx) = self.lower_bound(key)?;
        if let Some((entry_key, _)) = maybe_key_value {
            if key == entry_key {
                self.change_value(idx, value)?;
                return Ok(None);
            }
        }
        match maybe_child {
            Some(child) => match child.borrow_mut().set(key, value)? {
                Some(bubbled_up_entry) => self.insert_and_split(bubbled_up_entry, idx),
                None => Ok(None),
            },
            None => self.insert_and_split(
                Entry {
                    child: None,
                    key: key.to_string(),
                    value: value.to_string(),
                },
                idx,
            ),
        }
    }
    fn get(&self, key: &str) -> DbResult<Option<String>>
    where
        Self: Sized,
    {
        let (maybe_child, maybe_key_value, idx) = self.lower_bound(key)?;
        if let Some((entry_key, entry_value)) = maybe_key_value {
            if entry_key == key {
                return Ok(Some(entry_value));
            }
        }
        if let Some(child) = maybe_child {
            return child.borrow().get(key);
        }
        Ok(None)
    }
    fn lower_bound(
        &self,
        key: &str,
    ) -> DbResult<(Option<NodeRef<Self>>, Option<(String, String)>, usize)>
    where
        Self: Sized,
    {
        let length = self.num_entries()?;
        if length == 0 {
            panic!("no entry in the node");
        }
        let mut low_idx = 0;
        let mut high_idx = length - 1;
        let mut ans = (self.get_tail()?, None, length);
        while low_idx <= high_idx {
            let mid_idx = (low_idx + high_idx) / 2;
            let entry_data = self.get_entry_data(mid_idx)?;
            match key.cmp(&entry_data.key) {
                Ordering::Greater => {
                    low_idx = mid_idx + 1;
                }
                _ => {
                    ans = (
                        entry_data.child,
                        Some((entry_data.key, entry_data.value)),
                        mid_idx,
                    );
                    high_idx = mid_idx - 1;
                }
            };
        }
        Ok(ans)
    }
    fn insert_and_split(
        &mut self,
        entry: Entry<Self>,
        idx: usize,
    ) -> DbResult<Option<Entry<Self>>> {
        let length = self.num_entries()?;
        if length < self.branching_factor() {
            self.insert_entry(entry, idx)?;
            return Ok(None);
        }

        let mid_idx = length / 2;
        let replacement_indices = match idx.cmp(&mid_idx) {
            Ordering::Less => Some((idx, mid_idx - 1)),
            Ordering::Equal => None,
            Ordering::Greater => Some((idx - 1, mid_idx)),
        };
        let Entry {
            child: mid_child,
            key,
            value,
        } = match replacement_indices {
            Some((addition_idx, removal_idx)) => {
                let removed_entry = self.remove_entry(removal_idx)?;
                self.insert_entry(entry, addition_idx)?;
                removed_entry
            }
            None => entry,
        };
        let mut left_node = self.split()?;
        left_node.set_tail(mid_child)?;
        Ok(Some(Entry {
            child: Some(NodeRef::new(left_node)),
            key,
            value,
        }))
    }
    fn branching_factor(&self) -> usize;
    fn is_leaf(&self) -> DbResult<bool>;
    fn num_entries(&self) -> DbResult<usize>;
    fn get_tail(&self) -> DbResult<&Option<NodeRef<Self>>>;
    fn get_entry_data(&self, idx: usize) -> DbResult<Entry<Self>>
    where
        Self: Sized;
    fn set_tail(&mut self, child: Option<NodeRef<Self>>) -> DbResult<()>;
    fn change_value(&mut self, idx: usize, value: &str) -> DbResult<()>;
    fn insert_entry(&mut self, data: Entry<Self>, idx: usize) -> DbResult<()>;
    fn remove_entry(&mut self, idx: usize) -> DbResult<Entry<Self>>;
    fn split(&mut self) -> DbResult<Self>;
    fn new() -> DbResult<Self>;
}
