use apache_avro::{from_value, to_value, Reader, Schema, Writer};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

use crate::error::{DbResult, Error};

use super::Serializer;

pub struct AvroSerializer {
    schema: Schema,
}

impl AvroSerializer {
    pub fn new(schema_path: &str) -> DbResult<Self> {
        let mut schema_file = File::open(schema_path)?;
        let mut schema_content = String::new();
        schema_file.read_to_string(&mut schema_content)?;
        let schema = Schema::parse_str(&schema_content)
            .map_err(|e| Error::wrap("error in parsing avro schema file", e.into()))?;

        Ok(AvroSerializer { schema })
    }
}

impl Serializer for AvroSerializer {
    fn write<T: Serialize, W: Write>(&self, data: T, writer: &mut BufWriter<W>) -> DbResult<()> {
        let mut writer = Writer::new(&self.schema, writer);
        writer.append(to_value(data)?)?;
        writer.flush()?;
        Ok(())
    }

    fn read<T: for<'de> Deserialize<'de>, R: Read>(
        &self,
        reader: &mut BufReader<R>,
    ) -> DbResult<Option<T>> {
        let mut reader = Reader::new(reader)
            .map_err(|e| Error::wrap("error in creating avro reader", e.into()))?;
        match reader.next() {
            Some(result) => {
                let value = result?;
                Ok(Some(from_value::<T>(&value)?))
            }
            None => Ok(None),
        }
    }

    fn copy(&self) -> DbResult<Self> {
        Ok(AvroSerializer {
            schema: self.schema.clone(),
        })
    }
}
