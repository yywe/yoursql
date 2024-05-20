use crate::common::record_batch::RecordBatch;
use crate::common::types::DataType;
use crate::common::types::DataValue;
use tokio::runtime::Runtime;
use crate::common::schema::Schema;
use crate::session::SessionContext;
use std::sync::Arc;
use std::ffi::CString;
use std::ffi::CStr;
use anyhow::Result;
use log::info;
use futures::TryStreamExt;

/// We will have rust code and LLVM IR and they will need to interact with each other.
/// Thus, we need to serialize and deserialize the data between them. Specifically, for Rust
/// and LLVM IR, they both need to have their own implementation of the serialization.
/// Also note that Rust string is not null-terminated and support UTF-8 encoding, 
/// while C string is null-terminated and support ASCII encoding.
/// Currently UTF-8 is not supported here.
/// The serialization format is as follows:
/// given RecordBatch, serialize its rows into vec of bytes based on its schema.
/// format: header: total bytes, number of rows, row offsets; rows: data bytes, null bitmap
pub fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = vec![];
    let mut row_offsets = vec![];
    for row in batch.rows.iter() {
        // the serialization need also consider the NULL bitmap
        // for at the end of each row bytes we append a NULL bitmap in the form of 
        // index id: flag, 0 means NULL, 1 means not NULL
        let mut data_bytes=vec![];
        let mut null_bitmap=vec![];
        for i in 0..batch.schema.fields.len() {
            let field = &batch.schema.fields[i];
            let value = row[i].clone();
            //assert that the field type matches the value type
            assert_eq!(*field.data_type(), value.get_datatype());
            match field.data_type() {
                // For null and bollean, we serialize them as 1 byte. for null type it is actually not used
                DataType::Null =>{
                    data_bytes.push(0 as u8);
                    null_bitmap.push(true as u8);
                }
                DataType::Boolean => {
                    if value.is_none() {
                        data_bytes.push(0 as u8);
                        null_bitmap.push(true as u8);
                    } else {
                        let value: bool = value.try_into()?;
                        data_bytes.push(value as u8);
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::Int8 => {
                    if value.is_none() {
                        data_bytes.push(0 as u8);
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i8 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::Int16 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i16.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i16 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::Int32 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i32 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::Int64 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i64.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i64 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::UInt8 => {
                    if value.is_none() {
                        data_bytes.push(0 as u8);
                        null_bitmap.push(true as u8);
                    } else {
                        let value: u8 = value.try_into()?;
                        data_bytes.push(value);
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::UInt16 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_u16.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: u16 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::UInt32 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_u32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: u32 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::UInt64 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_u64.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: u64 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::Float32 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_f32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: f32 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                },
                DataType::Float64 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_f64.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: f64 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Utf8 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_u32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: String = value.try_into()?;
                        let c_string = CString::new(value)?;
                        let str_bytes = c_string.as_bytes_with_nul();
                        let str_len = str_bytes.len() as u32;
                        data_bytes.extend_from_slice(&str_len.to_le_bytes());
                        data_bytes.extend_from_slice(str_bytes);
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Binary => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_u32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: Vec<u8> = value.try_into()?;
                        let len = value.len() as u32;
                        data_bytes.extend_from_slice(&len.to_le_bytes());
                        data_bytes.extend_from_slice(&value);
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Date32 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i32 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Date64 => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i64.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i64 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Time32Second => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i32 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Time32Millisecond => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i32.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i32 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Time64Microsecond => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i64.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i64 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                }
                DataType::Time64Nanosecond => {
                    if value.is_none() {
                        data_bytes.extend_from_slice(&0_i64.to_le_bytes());
                        null_bitmap.push(true as u8);
                    } else {
                        let value: i64 = value.try_into()?;
                        data_bytes.extend_from_slice(&value.to_le_bytes());
                        null_bitmap.push(false as u8);
                    }
                }
            }
        }
        row_offsets.push(bytes.len() as u32);
        //merge the data bytes and null bitmap and append to the bytes
        let row_bytes = data_bytes.iter().chain(null_bitmap.iter());
        bytes.extend(row_bytes);
    }
    //add a header, total bytes, number of rows: row offsets. note the original row_offset need to plus the header size
    let header_size =4 + 4 + 4 * batch.rows.len() as u32;
    let total_bytes = bytes.len() as u32 + header_size;
    let mut header = total_bytes.to_le_bytes().to_vec();
    info!("serialized total bytes: {}, number of rows: {}", total_bytes, batch.rows.len() as u32);
    header.extend_from_slice(&(batch.rows.len() as u32).to_le_bytes());
    for offset in row_offsets {
        header.extend_from_slice(&(offset + header_size as u32).to_le_bytes());
    }
    bytes = header.iter().chain(bytes.iter()).cloned().collect();
    Ok(bytes)
}

///given the schema and the bytes, deserialize the bytes into RecordBatch
pub fn deserialize_batch(schema: Arc<Schema>, bytes: &[u8]) -> Result<RecordBatch> {
    let num_rows = u32::from_le_bytes(bytes[4..8].try_into()?);
    let mut row_offsets = vec![];
    for i in 0..num_rows {
        let offset = u32::from_le_bytes(bytes[8 + 4 * i as usize..8 + 4 * (i + 1) as usize].try_into()?);
        row_offsets.push(offset);
    }
    let mut rows = vec![];
    for offset  in row_offsets {
        let mut index = offset as usize;
        let mut row = vec![];
        for field in schema.fields.iter() {
            let value = match field.data_type() {
                DataType::Null => {
                    index += 1;
                    DataValue::Null
                }
                DataType::Boolean => {
                    let value = bytes[index];
                    index += 1;
                    DataValue::Boolean(Some(value != 0))
                }
                DataType::Int8 => {
                    let value = i8::from_le_bytes(bytes[index..index + 1].try_into()?);
                    index += 1;
                    DataValue::Int8(Some(value))
                }
                DataType::Int16 => {
                    let value = i16::from_le_bytes(bytes[index..index + 2].try_into()?);
                    index += 2;
                    DataValue::Int16(Some(value))
                }
                DataType::Int32 => {
                    let value = i32::from_le_bytes(bytes[index..index + 4].try_into()?);
                    index += 4;
                    DataValue::Int32(Some(value))
                }
                DataType::Int64 => {
                    let value = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                    index += 8;
                    DataValue::Int64(Some(value))
                }
                DataType::UInt8 => {
                    let value = bytes[index];
                    index += 1;
                    DataValue::UInt8(Some(value))
                }
                DataType::UInt16 => {
                    let value = u16::from_le_bytes(bytes[index..index + 2].try_into()?);
                    index += 2;
                    DataValue::UInt16(Some(value))
                }
                DataType::UInt32 => {
                    let value = u32::from_le_bytes(bytes[index..index + 4].try_into()?);
                    index += 4;
                    DataValue::UInt32(Some(value))
                }
                DataType::UInt64 => {
                    let value = u64::from_le_bytes(bytes[index..index + 8].try_into()?);
                    index += 8;
                    DataValue::UInt64(Some(value))
                }
                DataType::Float32 => {
                    let value = f32::from_le_bytes(bytes[index..index+ 4].try_into()?);
                    index += 4;
                    DataValue::Float32(Some(value))
                }
                DataType::Float64 => {
                    let value = f64::from_le_bytes(bytes[index..index+ 8].try_into()?);
                    index += 8;
                    DataValue::Float64(Some(value))
                }
                DataType::Utf8 => {
                    let len = u32::from_le_bytes(bytes[index..index + 4].try_into()?);
                    index += 4;
                    // note the len include the null-terminated character
                    if len == 0 {
                        DataValue::Utf8(None)
                    } else {
                        // deserialize the string, note it is a C string with null-terminated, i.e, has 0 at end
                        let end = index + len as usize;
                        // note we should not include the null-terminated character when construct CString
                        let value = CString::new(&bytes[index..end-1])?.to_str()?.to_string();
                        index = end;
                        DataValue::Utf8(Some(value))
                    }
                }
                DataType::Binary => {
                    let len = u32::from_le_bytes(bytes[index..index + 4].try_into()?);
                    index += 4;
                    if len == 0 {
                        DataValue::Binary(None)
                    } else {
                        let end = index + len as usize;
                        let value = bytes[index..end].to_vec();
                        index = end;
                        DataValue::Binary(Some(value))
                    }
                }
                DataType::Date32 => {
                    let value = i32::from_le_bytes(bytes[index..index + 4].try_into()?);
                    index += 4;
                    DataValue::Date32(Some(value))
                }
                DataType::Date64 => {
                    let value = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                    index += 8;
                    DataValue::Date64(Some(value))
                }
                DataType::Time32Second => {
                    let value = i32::from_le_bytes(bytes[index..index + 4].try_into()?);
                    index += 4;
                    DataValue::Time32Second(Some(value))
                }
                DataType::Time32Millisecond => {
                    let value = i32::from_le_bytes(bytes[index..index + 4].try_into()?);
                    index += 4;
                    DataValue::Time32Millisecond(Some(value))
                }
                DataType::Time64Microsecond => {
                    let value = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                    index += 8;
                    DataValue::Time64Microsecond(Some(value))
                }
                DataType::Time64Nanosecond => {
                    let value = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                    index += 8;
                    DataValue::Time64Nanosecond(Some(value))
                }
            }; 
            row.push(value);
        }
        // now read the null bitmap and patch for the row in case some fields are NULL
        for i in 0..schema.fields.len() {
            let flag = bytes[index];
            index += 1;
            if flag == 1 {
                row[i] = DataValue::try_from(&row[i].get_datatype())?;
            }
        }
        rows.push(row);
    }
    Ok(RecordBatch {
        schema: schema.clone(),
        rows: rows,
    })
}


/// Important node: when interact Rust with LLVM, it is inevitable to exchange data.
/// However. the way Rust manage memory and LLVM are quite different.  Here I found two 
/// ideas regards data exchange.  1. use Box<Vec<_>> to allocate memory in heap and then 
/// forget it and return the raw pointer to LLVM (although the content of vec is in heap,
/// the vec itself is in stack, when it goes out ot scope, the vec will be dropped, thus 
/// the content will be freed, thus we need to box it to heap). However, I am not sure if 
/// this will cause memory leak. 2. use libc::malloc to allocate memory in heap and then 
/// copy the data to the heap memory and return the raw pointer to LLVM.  After used, then
/// manually free the memory.
/// Here use the 2nd appraoch. 1st approach is commented
pub fn load_table_data(name: *const libc::c_char, database: *const libc::c_char, session_ptr: *const SessionContext )->*const i8 {
    let name = unsafe { CStr::from_ptr(name) }.to_str().unwrap();
    let database = unsafe { CStr::from_ptr(database) }.to_str().unwrap();
    let database_name = database.to_string();
    let table_name = name.to_string();
    info!("load data from table {}, database {}", database_name, table_name);
    let session: &SessionContext = unsafe { &*session_ptr };
    let rt = Runtime::new().unwrap();
    let data = rt.block_on(async {
        let db = session.database(&database_name).unwrap();
        let student_table = db.get_table(table_name.as_str()).await.unwrap();
        let exec = student_table.scan(&session.state(), None, &[]).await.unwrap();
        let it = exec.execute(&session.state()).unwrap();
        it.try_collect::<Vec<_>>().await
    }).unwrap();
    //just ensure only 1 batch in the table
    //TODO: for multiple record batch, we can flat it here to a single batch.
    assert_eq!(data.len(), 1, "all data should be in a single batch");
    //let buffer= Box::new(serialize_batch(&data[0]).unwrap()); // 1st approach to exchange data
    let buffer= serialize_batch(&data[0]).unwrap();
    
    // Test code, try deserialize it
    //let schema = data[0].schema.clone();
    //let ans = deserialize_batch(schema, &buffer).unwrap();

    //let ptr = buffer.as_ptr() as *const i8; // 1st approach to exchange data
    // 2nd approach, call libc::malloc and copy the data to the heap memory, 
    // the original buffer will be dropped when out of scope
    let data_ptr = unsafe {
        let data_ptr = libc::malloc(buffer.len()) as *mut u8;
        if data_ptr.is_null() {
            panic!("Memory allocation failed");
        }
        data_ptr
    };
    unsafe {
        std::ptr::copy_nonoverlapping(buffer.as_ptr(), data_ptr, buffer.len());
    }
    //std::mem::forget(buffer); // 1st approach to exchange data
    //return ptr // 1st approach to exchange data
    data_ptr as *const i8
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record_batch::RecordBatch;
    use crate::common::schema::Schema;
    use crate::common::schema::Field;
    use crate::common::types::DataType;
    use crate::common::types::DataValue;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_serialize_batch() {
        let table1_def = Schema::new(
            vec![
                Field::new("id", DataType::Int64, false,  None),
                Field::new("name", DataType::Utf8, false, None),
                Field::new("age", DataType::Int8, false, None),
                Field::new("address", DataType::Utf8, false,None),
                Field::new("marks", DataType::Float32, false, None)
            ],
            HashMap::new(),
        );
        let table1_ref = Arc::new(table1_def);
        let row_batch1 = vec![
            vec![
                DataValue::Int64(Some(1)),
                DataValue::Utf8(Some("John".into())),
                DataValue::Int8(Some(20)),
                DataValue::Utf8(Some("100 bay street".into())),
                DataValue::Float32(Some(99.9)),
            ],
            vec![
                DataValue::Int64(Some(2)),
                DataValue::Utf8(Some("Andy".into())),
                DataValue::Int8(Some(21)),
                DataValue::Utf8(Some("121 hunter street".into())),
                DataValue::Float32(Some(88.8)),
            ],
        ];
        let batch1 = RecordBatch {
            schema: table1_ref.clone(),
            rows: row_batch1.clone(),
        };
        let bytes = serialize_batch(&batch1).unwrap();
        let batch2 = deserialize_batch(table1_ref.clone(), &bytes).unwrap();
        println!("original batch is: {:?}", batch1.rows);
        println!("deserizlied batch is: {:?}", batch2.rows);
        assert_eq!(batch1, batch2);
    }    
}
