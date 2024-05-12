use std::mem::replace;

use crate::{
    error::Error,
    kv_file::KVFile,
    kvdb::KVEntry,
    segmented_files_db::segment_file::{SegmentFile, SegmentFileFactory},
};

const TMP_FILE_NAME: &str = "merged_tmp_file.txt";

pub struct File {
    dir_path: String,
    sparsity: u64,
    kvfile: KVFile,
    sparse_index: Vec<(String, u64)>,
    last_indexed_offset: u64,
}

impl SegmentFile for File {
    fn add_entry(&mut self, key: &str, entry: &KVEntry<String>) -> Result<(), Error> {
        self.kvfile.append_line(key, entry).and_then(|offset| {
            if self.sparse_index.is_empty() || offset - self.last_indexed_offset > self.sparsity {
                self.sparse_index.push((key.to_owned(), offset));
                self.last_indexed_offset = offset;
            }
            Ok(())
        })
    }
    fn get_entry(&self, key: &str) -> Result<Option<KVEntry<String>>, Error> {
        let index = match self
            .sparse_index
            .binary_search_by(|(this_key, _)| this_key.cmp(&key.to_string()))
        {
            Ok(index) => index,
            Err(0) => return Ok(None),
            Err(index) => index - 1,
        };
        let (_, start_offset) = self.sparse_index.get(index).unwrap();

        let mut value = None;
        self.kvfile.read_lines_from_offset(
            &mut |this_key, this_entry, _| {
                if this_key.as_str() > key {
                    return Ok(true);
                }
                if this_key == key {
                    value = Some(this_entry)
                }
                Ok(false)
            },
            *start_offset,
        )?;
        Ok(value)
    }
    fn absorb(&mut self, other: &Self) -> Result<(), Error> {
        let mut new_file = KVFile::new(&self.dir_path, TMP_FILE_NAME);

        let mut this_reader = self.kvfile.get_reader()?;
        let mut this_buf = this_reader.read_line()?;

        let mut other_reader = other.kvfile.get_reader()?;
        let mut other_buf = other_reader.read_line()?;

        let mut writer_buf = None;

        loop {
            let use_other = match &this_buf {
                None => true,
                Some((this_key, _)) => match &other_buf {
                    None => false,
                    Some((other_key, _)) => this_key >= other_key,
                },
            };
            let (buf, reader) = match use_other {
                true => (&mut other_buf, &mut other_reader),
                false => (&mut this_buf, &mut this_reader),
            };

            let prev_writer_buf = replace(&mut writer_buf, replace(buf, reader.read_line()?));
            match prev_writer_buf {
                None => {
                    if writer_buf.is_none() {
                        break;
                    }
                }
                Some((ref prev_key, ref prev_entry)) => {
                    let should_write = match writer_buf {
                        None => true,
                        Some((ref current_key, _)) => current_key > prev_key,
                    };
                    if should_write {
                        new_file.append_line(prev_key, prev_entry)?;
                    }
                }
            };
        }

        let old_file = replace(&mut self.kvfile, new_file);
        let file_name = old_file.file_name.clone();
        old_file.delete()?;
        self.kvfile.rename(&file_name)?;

        Ok(())
    }
    fn rename(&mut self, new_file_name: &str) -> Result<(), Error> {
        self.kvfile.rename(new_file_name)
    }
    fn delete(self) -> Result<(), Error> {
        self.kvfile.delete()
    }
}

pub struct Factory {
    pub dir_path: String,
    pub sparsity: u64,
}

impl SegmentFileFactory<File> for Factory {
    fn new(&self, file_name: &str) -> Result<File, Error> {
        let kvfile = KVFile::new(&self.dir_path, file_name);
        Ok(File {
            dir_path: self.dir_path.clone(),
            sparsity: self.sparsity,
            kvfile,
            sparse_index: vec![],
            last_indexed_offset: 0,
        })
    }
    fn from_disk(&self, file_name: &str) -> Result<File, Error> {
        let kvfile = KVFile::new(&self.dir_path, file_name);

        let mut last_indexed_offset = 0;
        let mut sparse_index = vec![];
        kvfile.read_lines(&mut |key, _, offset| {
            if sparse_index.is_empty() || offset - last_indexed_offset > self.sparsity {
                sparse_index.push((key.to_owned(), offset));
                last_indexed_offset = offset;
            }
            Ok(false)
        })?;

        Ok(File {
            dir_path: self.dir_path.clone(),
            sparsity: self.sparsity,
            kvfile,
            sparse_index,
            last_indexed_offset,
        })
    }
}
