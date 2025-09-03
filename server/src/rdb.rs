use std::{error::Error, path::Path, fs::{File, create_dir_all, write}, io::prelude::*};
use crc64::crc64;
use crate::db::{Database, Value, ValueType, ExpiryType, ExpiryLen};
use crate::config::{DEF_DB_DIR, DEF_DB_FILE};

static DEF_HEADER: &str = "REDIS0011";
static DEF_META_ATTR_KEY: &str = "redis-ver";
static DEF_META_ATTR_VAL: &str = "7.2.0";
static ENC_INT8:  u8 = 0;
static ENC_INT16: u8 = 1;
static ENC_INT32: u8 = 2;

fn try_int_encode(to_encode: &str) -> Option<Vec<u8>> {
    // Reject non-decimal forms
    if to_encode.is_empty()
       || to_encode.len() > 1 && to_encode.starts_with('0')
       || to_encode.len() > 2 && to_encode.starts_with("-0")
    {
        return None;
    }

    let parsed: i64 = to_encode.parse().ok()?;
    let mut encoded = Vec::new();

    if parsed >= i8::MIN as i64 && parsed <= i8::MAX as i64 { // C0-[B]
        encoded.push(0xC0 | ENC_INT8);
        encoded.push(parsed as i8 as u8); // 2-complement
    } else if parsed >= i16::MIN as i64 && parsed <= i16::MAX as i64 { // C1-[B]-[B]
        encoded.push(0xC0 | ENC_INT16);
        encoded.extend_from_slice(&(parsed as i16).to_le_bytes()); // 2-complement, LE
    } else if parsed >= i32::MIN as i64 && parsed <= i32::MAX as i64 { // C2-[B]-[B]-[B]-[B]
        encoded.push(0xC0 | ENC_INT32);
        encoded.extend_from_slice(&(parsed as i32).to_le_bytes()); // 2-complement, LE
    } else { todo!() } // LZF-compressed

    Some(encoded)
}

fn _usize_encode(to_encode: usize, num_bytes: Option<usize>) -> Vec<u8> {
    let encoded = to_encode.to_le_bytes(); // LE
    encoded.into_iter().take(num_bytes.unwrap_or(usize::MAX)).collect()
}

fn string_encode(to_encode: &str) -> Vec<u8> {
    if let Some(int_enc) = try_int_encode(to_encode) {
        return int_enc;
    }

    let mut encoded = Vec::new();
    let string = to_encode.as_bytes().to_vec();
    let size = size_encode(string.len());

    encoded.extend_from_slice(&size);
    encoded.extend_from_slice(&string);

    encoded
}

fn size_encode(to_encode: usize) -> Vec<u8> {
    let mut encoded = Vec::new();

    match to_encode {
        size if size < (1 << 6) => { // 00[bbbbbb]
            encoded.push(size as u8)
        }, size if size < (1 << 14) => { // 01[bbbbbb]-[B]
            encoded.push(0b01000000 | ((size >> 8) & 0x3F) as u8);
            encoded.push((size & 0xFF) as u8);
        }, size if size < (1 << 32) => { // 10000000-[B]-[B]-[B]-[B]
            encoded.push(0b10000000);
            encoded.extend_from_slice(&(size as u32).to_be_bytes()); // BE
        }, _ => todo!() // Special format
    }

    encoded
}

fn string_decode<'a>(to_decode: &'a [u8]) -> Result<(&'a [u8], usize), Box<dyn Error + Send + Sync>> {
    let err_msg = "Failed to decode string";

    // Extract string's start + size by matching the 2 MSBs
    let (str_size, str_start) = match to_decode.get(0).ok_or(err_msg)? >> 6 {
        0x00 => ((to_decode[0] & 0x3F) as usize, 1usize), // Remaining 6 bits
        0b01 => {
            let next_byte = to_decode.get(1).ok_or(err_msg)?;
            (((to_decode[0] & 0x3F) + next_byte) as usize, 2usize) // Remaining 6 bits + next byte
        }, 0b10 => {
            let byte_2 = to_decode.get(1).ok_or(err_msg)?;
            let byte_3 = to_decode.get(2).ok_or(err_msg)?;
            let byte_4 = to_decode.get(3).ok_or(err_msg)?;
            let byte_5 = to_decode.get(4).ok_or(err_msg)?;
            ((byte_2 + byte_3 + byte_4 + byte_5) as usize, 6usize) // Next 4 bytes
        }, 0b11 => todo!(), // Special format
        _ => unreachable!()
    };

    // Return string + end of string (for chained strings)
    Ok((&to_decode[str_start..str_start+str_size], str_start + str_size))
}

fn entry_decode(to_decode: &Vec<u8>) -> Result<(String, Value, usize), Box<dyn Error + Send + Sync>> {
    let err_msg = "Failed to decode entry";
    if to_decode.is_empty() {
        return Err(err_msg.into());
    }
    
    let mut pos = 0;
    // Extract expire time (optional)
    let exp = match to_decode[pos] {
        0xFC => { // Milliseconds
            pos += 1;
            let bytes = to_decode.get(pos..pos+8).ok_or(err_msg)?;
            pos += 8;
            Some(ExpiryType::Milliseconds(u64::from_le_bytes(bytes.try_into()?) as usize))
        }, 0xFD => { // Seconds
            pos += 1;
            let bytes = to_decode.get(pos..pos+4).ok_or(err_msg)?;
            pos += 4;
            Some(ExpiryType::Seconds(u32::from_le_bytes(bytes.try_into()?) as usize))
        }, _ => None
    };

    // Extract + parse key and value
    let (key, val_rel_start) = string_decode(&to_decode.get(pos+1..).ok_or(err_msg)?)?;
    let (val, val_end) = match to_decode[pos] {
        0 => { // String value type
            let res = string_decode(&to_decode.get(pos+1+val_rel_start..).ok_or(err_msg)?)?;
            (
                ValueType::String(String::from_utf8(res.0.to_vec())?), // Parsed value
                pos + 1 + val_rel_start + res.1 // Absolute end of value
            )
        }, _ => todo!()
    };
    let parsed_key = String::from_utf8(key.to_vec())?;
    let parsed_val = Value { val, exp };

    // Return key + value + end of entry (for chained entries)
    Ok((parsed_key, parsed_val, val_end))
}

pub struct RDBFile {
    pub sect_header: Vec<u8>,
    pub sect_metadata: Vec<u8>,
    pub sect_database: Vec<u8>,
    pub sect_eof: Vec<u8>
}

impl Default for RDBFile {
    fn default() -> Self {
        // Generate an empty RDB
        let mut file = Self {
            sect_header: Vec::from(DEF_HEADER),
            sect_metadata: RDBFile::gen_metadata(vec![
                (DEF_META_ATTR_KEY, DEF_META_ATTR_VAL)
            ]),
            sect_database: vec![0xFE, 0x00],
            sect_eof: vec![] // Dummy
        };
        file.gen_eof();

        file
    }
}

impl RDBFile {
    pub fn from(dir: &String, name: &String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut path = Path::new(dir).join(name);
        if !path.exists() {
            // Create empty file in path
            let dir_path = path.parent().unwrap_or(Path::new(DEF_DB_DIR));
            let name_path = if !name.ends_with(".rdb") || name.len() < 4 { DEF_DB_FILE } else { name };
            create_dir_all(dir_path)?;
            write(&dir_path.join(name_path), RDBFile::default().to_vec())?;
            path = Path::new(dir_path).join(name_path);
        }

        // Open + read file
        let mut file = File::open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        
        Self::from_vec(bytes)
    }

    pub fn from_vec(bytes: Vec<u8>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Parse to sections
        let metadata_start = bytes.iter().position(
            |&c| c == 0xFA
        ).ok_or("Missing metadata section marker")?;
        let database_start = bytes.iter().position(
            |&c| c == 0xFE
        ).ok_or("Missing database section marker")?;
        let eof_start = bytes.iter().rposition(
            |&c| c == 0xFF
        ).ok_or("Missing EOF section marker")?;

        // TODO: Check content validity

        Ok(Self {
            sect_header: bytes[..metadata_start].to_vec(),
            sect_metadata: bytes[metadata_start..database_start].to_vec(),
            sect_database: bytes[database_start..eof_start].to_vec(),
            sect_eof: bytes[eof_start..].to_vec()
        })
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut vec = self.sect_header.clone();
        vec.extend_from_slice(&self.sect_metadata);
        vec.extend_from_slice(&self.sect_database);
        vec.extend_from_slice(&self.sect_eof);

        vec
    }

    pub async fn to_dbs(&self) -> Result<Vec<Database>, Box<dyn Error + Send + Sync>> {
        let sect_db = &self.sect_database;
        let mut dbs = Vec::new();

        if let Some(fb_mark) = sect_db.iter().position(|&c| c == 0xFB) {
            let db = Database::default();
            let (tab_size, _exp_size) = (
                *sect_db.get(fb_mark + 1).ok_or("Couldn't extract table size")? as usize,
                *sect_db.get(fb_mark + 2).ok_or("Couldn't extract # of expire keys")? as usize
            );
            // Parse entries
            let mut pos = fb_mark + 3;
            {
                let mut guard = db.lock().await;
                for _ in 0..tab_size {
                    let (key, val, size) = entry_decode(&sect_db[pos..].to_vec())?;
                    guard.insert(key, val);
                    pos += size;
                }
            }
            dbs.push(db);
        }

        Ok(dbs)
    }

    pub fn gen_metadata(subsects: Vec<(&str, &str)>) -> Vec<u8> {
        let mut metadata = vec![0xFA];

        for subsect in subsects {
            metadata.extend_from_slice(&string_encode(subsect.0));
            metadata.extend_from_slice(&string_encode(subsect.1));
        }

        metadata
    }

    pub async fn _gen_database(&mut self, dbs: Option<Vec<Database>>) {
        let mut database = vec![0xFE];

        if dbs.is_some() {
            for (i, db) in dbs.unwrap().iter().enumerate() {
                let guard = db.lock().await;
                database.extend_from_slice(&size_encode(i)); // Index
                database.push(0xFB); // Divider
                database.extend_from_slice(&size_encode(guard.len())); // # of keys
                database.extend_from_slice(&size_encode(guard.elen())); // # of keys with expiry

                for (key, val) in guard.iter() {
                    if let Some(exp) = &val.exp { // Key has an expire
                        match exp {
                            ExpiryType::Milliseconds(millis) => {
                                database.push(0xFC); // Divider
                                database.extend_from_slice(&_usize_encode(*millis, Some(8)));
                            }, ExpiryType::Seconds(secs) => {
                                database.push(0xFD); // Divider
                                database.extend_from_slice(&_usize_encode(*secs, Some(4)));
                            }
                        }
                    }

                    match &val.val {
                        ValueType::String(val) => {
                            database.push(0); // Type
                            database.extend_from_slice(&string_encode(key));
                            database.extend_from_slice(&string_encode(val));
                        }, _ => todo!()
                    }
                }
            }
        } else { database.push(0x00); }

        self.sect_database = database;
    }

    pub fn gen_eof(&mut self) {
        let mut eof = vec![0xFF];
        
        // Concatenate all file data
        let mut file_data = self.sect_header.clone();
        file_data.extend_from_slice(&self.sect_metadata);
        file_data.extend_from_slice(&self.sect_database);
        // Calculate checksum
        let checksum = crc64(0, &file_data) as usize;
        eof.extend_from_slice(&checksum.to_le_bytes()); // LE

        self.sect_eof = eof;
    }
}
