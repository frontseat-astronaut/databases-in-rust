use std::path::PathBuf;

use crate::error::Error;

use super::segment_file::{SegmentFile, SegmentFileFactory};

pub struct Segment<T>
where
    T: SegmentFile,
{
    pub id: usize,
    pub file: T,
}

impl<T> Segment<T>
where
    T: SegmentFile,
{
    pub fn new<U: SegmentFileFactory<T>>(id: usize, file_factory: &U) -> Result<Self, Error> {
        Ok(Segment {
            id,
            file: file_factory.new(get_segment_file_name(id).as_str())?,
        })
    }
    pub fn try_from_disk<U: SegmentFileFactory<T>>(
        path: &PathBuf,
        file_factory: &U,
    ) -> Result<Option<Self>, Error> {
        if let Some(file_name_os_str) = path.file_name() {
            if let Some(file_name) = file_name_os_str.to_str() {
                if let Some(file_stem_os_str) = path.file_stem() {
                    if let Some(file_stem) = file_stem_os_str.to_str() {
                        if let Ok(id) = file_stem.parse::<usize>() {
                            return Ok(Some(Segment {
                                id,
                                file: file_factory.from_disk(file_name)?,
                            }));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
    pub fn from_file(file: T, id: usize) -> Result<Self, Error> {
        let mut segment = Segment { id: 0, file };
        segment.change_id(id)?;
        Ok(segment)
    }
    pub fn change_id(&mut self, id: usize) -> Result<(), Error> {
        self.id = id;
        self.file.rename(get_segment_file_name(id).as_str())
    }
}

fn get_segment_file_name(id: usize) -> String {
    format!("{}.txt", id)
}
