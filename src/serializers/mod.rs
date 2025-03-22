use std::io::{BufReader, BufWriter, Read, Write};

use serde::{Deserialize, Serialize};

use crate::error::DbResult;

pub mod avro;
pub mod json;
pub mod message_pack;

pub use avro::AvroSerializer;
pub use json::JsonSerializer;
pub use message_pack::MessagePackSerializer;

pub enum SerializerEnum {
    Avro(AvroSerializer),
    MessagePack(MessagePackSerializer),
    JSON(JsonSerializer),
}

impl Serializer for SerializerEnum {
    fn write<T: Serialize, W: Write>(&self, data: T, writer: &mut BufWriter<W>) -> DbResult<()> {
        match self {
            SerializerEnum::Avro(serializer) => serializer.write(data, writer),
            SerializerEnum::MessagePack(serializer) => serializer.write(data, writer),
            SerializerEnum::JSON(serializer) => serializer.write(data, writer),
        }
    }

    fn read<T: for<'de> Deserialize<'de>, R: Read>(
        &self,
        reader: &mut BufReader<R>,
    ) -> DbResult<Option<T>> {
        match self {
            SerializerEnum::Avro(serializer) => serializer.read(reader),
            SerializerEnum::MessagePack(serializer) => serializer.read(reader),
            SerializerEnum::JSON(serializer) => serializer.read(reader),
        }
    }

    fn copy(&self) -> DbResult<Self>
    where
        Self: Sized,
    {
        Ok(match self {
            SerializerEnum::Avro(serializer) => SerializerEnum::Avro(serializer.copy()?),
            SerializerEnum::MessagePack(serializer) => {
                SerializerEnum::MessagePack(serializer.copy()?)
            }
            SerializerEnum::JSON(serializer) => SerializerEnum::JSON(serializer.copy()?),
        })
    }
}

pub trait Serializer {
    fn write<T: Serialize, W: Write>(&self, data: T, writer: &mut BufWriter<W>) -> DbResult<()>;

    fn read<T: for<'de> Deserialize<'de>, R: Read>(
        &self,
        reader: &mut BufReader<R>,
    ) -> DbResult<Option<T>>;

    fn copy(&self) -> DbResult<Self>
    where
        Self: Sized;
}
