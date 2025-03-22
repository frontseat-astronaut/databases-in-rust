use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec};
use std::io::{BufReader, BufWriter, Read, Write};

use super::Serializer;
use crate::error::DbResult;

pub struct JsonSerializer;

impl JsonSerializer {
    pub fn new() -> Self {
        JsonSerializer
    }
}

impl Serializer for JsonSerializer {
    fn write<T: Serialize, W: Write>(&self, data: T, writer: &mut BufWriter<W>) -> DbResult<()> {
        let encoded = to_vec(&data)?;

        let length = encoded.len() as u32;
        writer.write_all(&length.to_le_bytes())?;
        writer.write_all(&encoded)?;
        writer.flush()?;
        Ok(())
    }

    fn read<T: for<'de> Deserialize<'de>, R: Read>(
        &self,
        reader: &mut BufReader<R>,
    ) -> DbResult<Option<T>> {
        let mut length_bytes = [0u8; 4];
        reader.read_exact(&mut length_bytes)?;
        let length = u32::from_le_bytes(length_bytes) as usize;

        let mut buffer = vec![0u8; length];
        reader.read_exact(&mut buffer)?;

        let data: T = from_slice(&buffer)?;
        Ok(Some(data))
    }

    fn copy(&self) -> DbResult<Self> {
        Ok(Self::new())
    }
}
