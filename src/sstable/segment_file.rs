use std::collections::VecDeque;
use std::mem::replace;

use crate::error::DbResult;
use crate::tmp_file_names::{TMP_COMPACTION_FILE_NAME, TMP_MERGING_FILE_NAME};
use crate::{
    kv_file::{KVFile, KVLine},
    kvdb::KeyStatus,
    segmented_files_db::segment_file::{
        SegmentFile, SegmentFileFactory, SegmentReader, SegmentReaderFactory,
    },
};

pub struct Reader<'a> {
    kvfile: KVFile,
    sparse_index: &'a Vec<(String, u64)>,
}

impl<'a> SegmentReader<'a> for Reader<'a> {
    fn get_status(&mut self, key: &str) -> DbResult<Option<KeyStatus<String>>> {
        get_status(self.sparse_index, &mut self.kvfile, key)
    }
}

pub struct File {
    sparsity: u64,
    kvfile: KVFile,
    sparse_index: Vec<(String, u64)>,
    last_indexed_offset: u64,
}

impl SegmentFile for File {
    type Reader<'a> = Reader<'a>;
    fn set_status(&mut self, key: &str, status: &KeyStatus<String>) -> DbResult<()> {
        set_status(
            &mut self.sparse_index,
            &mut self.last_indexed_offset,
            self.sparsity,
            &mut self.kvfile,
            key,
            status,
        )
    }
    fn get_status(&mut self, key: &str) -> DbResult<Option<KeyStatus<String>>> {
        get_status(&self.sparse_index, &mut self.kvfile, key)
    }
    fn absorb<'a>(&mut self, other: &mut Reader<'a>) -> DbResult<()> {
        let mut new_file = KVFile::new(&self.kvfile.dir_path, TMP_MERGING_FILE_NAME)?;
        let mut new_index = vec![];
        let mut last_indexed_offset = 0;

        let mut this_iter = self.kvfile.iter()?;
        let mut this_buf = this_iter.try_next()?;

        let mut other_iter = other.kvfile.iter()?;
        let mut other_buf = other_iter.try_next()?;

        let mut writer_buf = None;

        loop {
            let use_other = match &this_buf {
                None => true,
                Some(KVLine { key: this_key, .. }) => match &other_buf {
                    None => false,
                    Some(KVLine { key: other_key, .. }) => this_key >= other_key,
                },
            };
            let (buf, iter) = match use_other {
                true => (&mut other_buf, &mut other_iter),
                false => (&mut this_buf, &mut this_iter),
            };

            let prev_writer_buf = replace(&mut writer_buf, replace(buf, iter.try_next()?));
            match prev_writer_buf {
                None => {
                    if writer_buf.is_none() {
                        break;
                    }
                }
                Some(KVLine {
                    key: ref prev_key,
                    status: prev_status,
                    ..
                }) => {
                    let should_write = match writer_buf {
                        None => true,
                        Some(KVLine {
                            key: ref current_key,
                            ..
                        }) => current_key > prev_key,
                    };
                    if should_write {
                        let offset = new_file.append_line(&prev_key, &prev_status)?;
                        if should_create_new_index_entry(
                            &new_index,
                            offset,
                            last_indexed_offset,
                            self.sparsity,
                        ) {
                            new_index.push((prev_key.to_owned(), offset));
                            last_indexed_offset = offset;
                        }
                    }
                }
            };
        }

        self.replace(new_file, new_index, last_indexed_offset)?;
        Ok(())
    }
    fn rename(&mut self, new_file_name: &str) -> DbResult<()> {
        self.kvfile.rename(new_file_name)
    }
    fn compact(&mut self) -> DbResult<()> {
        let mut new_file = KVFile::new(&self.kvfile.dir_path, TMP_COMPACTION_FILE_NAME)?;
        let mut new_index = vec![];
        let mut last_indexed_offset = 0;

        let mut flush_lines_buf = |lines_buf: &mut VecDeque<KVLine>| -> DbResult<()> {
            while !lines_buf.is_empty() {
                let line_to_add = lines_buf.pop_front().unwrap();
                set_status(
                    &mut new_index,
                    &mut last_indexed_offset,
                    self.sparsity,
                    &mut new_file,
                    &line_to_add.key,
                    &line_to_add.status,
                )?;
            }
            Ok(())
        };

        let mut index_iter = self.sparse_index.iter().peekable();
        let mut file_iter = self.kvfile.iter()?;
        let mut lines_buf = VecDeque::<KVLine>::new();
        loop {
            let maybe_next_index_entry = index_iter.peek();
            let Some(line) = file_iter.try_next()? else {
                flush_lines_buf(&mut lines_buf)?;
                break;
            };
            if let Some((_, next_index_offset)) = maybe_next_index_entry {
                if line.offset.eq(next_index_offset) {
                    flush_lines_buf(&mut lines_buf)?;
                    index_iter.next();
                }
            }
            // skip deleted entries
            if let KeyStatus::Present(_) = line.status {
                lines_buf.push_back(line);
            }
        }

        self.replace(new_file, new_index, last_indexed_offset)?;
        Ok(())
    }
    fn delete(mut self) -> DbResult<()> {
        self.kvfile.delete()
    }
}

impl File {
    fn replace(
        &mut self,
        new_file: KVFile,
        new_index: Vec<(String, u64)>,
        last_indexed_offset: u64,
    ) -> DbResult<()> {
        let mut old_file = replace(&mut self.kvfile, new_file);
        let file_name = old_file.file_name.clone();
        old_file.delete()?;
        self.kvfile.rename(&file_name)?;
        self.sparse_index = new_index;
        self.last_indexed_offset = last_indexed_offset;
        Ok(())
    }
}

pub struct ReaderFactory {}

impl SegmentReaderFactory<File> for ReaderFactory {
    fn new<'a>(&self, file: &'a File) -> DbResult<<File as SegmentFile>::Reader<'a>> {
        return Ok(Reader {
            kvfile: KVFile::copy(&file.kvfile)?,
            sparse_index: &file.sparse_index,
        });
    }
}

pub struct Factory {
    pub dir_path: String,
    pub sparsity: u64,
}

impl SegmentFileFactory<File> for Factory {
    fn new(&self, file_name: &str) -> DbResult<File> {
        let kvfile = KVFile::new(&self.dir_path, file_name)?;
        Ok(File {
            sparsity: self.sparsity,
            kvfile,
            sparse_index: vec![],
            last_indexed_offset: 0,
        })
    }
    fn from_disk(&self, file_name: &str) -> DbResult<File> {
        let mut kvfile = KVFile::new(&self.dir_path, file_name)?;

        let mut last_indexed_offset = 0;
        let mut sparse_index = vec![];
        for line_result in kvfile.iter()? {
            let KVLine { key, offset, .. } = line_result?;
            if should_create_new_index_entry(
                &sparse_index,
                offset,
                last_indexed_offset,
                self.sparsity,
            ) {
                sparse_index.push((key.to_owned(), offset));
                last_indexed_offset = offset;
            }
        }

        Ok(File {
            sparsity: self.sparsity,
            kvfile,
            sparse_index,
            last_indexed_offset,
        })
    }
}

fn get_status(
    sparse_index: &Vec<(String, u64)>,
    kvfile: &mut KVFile,
    key: &str,
) -> DbResult<Option<KeyStatus<String>>> {
    let index = match sparse_index.binary_search_by(|(this_key, _)| this_key.cmp(&key.to_string()))
    {
        Ok(index) => index,
        Err(0) => return Ok(None),
        Err(index) => index - 1,
    };
    let (_, start_offset) = sparse_index.get(index).unwrap();

    let mut status = None;
    for line_result in kvfile.iter_from_offset(*start_offset)? {
        let line = line_result?;
        if line.key.as_str() > key {
            break;
        }
        if line.key == key {
            status = Some(line.status)
        }
    }
    Ok(status)
}

fn set_status(
    sparse_index: &mut Vec<(String, u64)>,
    last_indexed_offset: &mut u64,
    sparsity: u64,
    kvfile: &mut KVFile,
    key: &str,
    status: &KeyStatus<String>,
) -> DbResult<()> {
    kvfile.append_line(key, status).and_then(|offset| {
        if should_create_new_index_entry(&sparse_index, offset, *last_indexed_offset, sparsity) {
            sparse_index.push((key.to_owned(), offset));
            *last_indexed_offset = offset;
        }
        Ok(())
    })
}

fn should_create_new_index_entry<T>(
    index: &Vec<T>,
    offset: u64,
    last_indexed_offset: u64,
    sparsity: u64,
) -> bool {
    index.is_empty() || offset - last_indexed_offset > sparsity
}
