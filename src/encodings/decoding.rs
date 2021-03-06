// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains all supported decoders for Parquet.

use std::cmp;
use std::marker::PhantomData;
use std::mem;
use std::slice::from_raw_parts_mut;

use super::rle::RleDecoder;
use basic::*;
use data_type::*;
use errors::{ParquetError, Result};
use schema::types::ColumnDescPtr;
use util::bit_util::BitReader;
use util::memory::{ByteBuffer, ByteBufferPtr};

// ----------------------------------------------------------------------
// Decoders

/// A Parquet decoder for the data type `T`.
pub trait Decoder<T: DataType> {
  /// Sets the data to decode to be `data`, which should contain `num_values` of values
  /// to decode.
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()>;

  /// Consumes values from this decoder and write the results to `buffer`. This will try
  /// to fill up `buffer`.
  ///
  /// Returns the actual number of values decoded, which should be equal to `buffer.len()`
  /// unless the remaining number of values is less than `buffer.len()`.
  fn get(&mut self, buffer: &mut [T::T]) -> Result<usize>;

  /// Returns the number of values left in this decoder stream.
  fn values_left(&self) -> usize;

  /// Returns the encoding for this decoder.
  fn encoding(&self) -> Encoding;
}

/// Gets a decoder for the column descriptor `descr` and encoding type `encoding`.
///
/// NOTE: the primitive type in `descr` MUST match the data type `T`, otherwise
/// disastrous consequence could occur.
pub fn get_decoder<T: DataType>(
  descr: ColumnDescPtr,
  encoding: Encoding
) -> Result<Box<Decoder<T>>> where T: 'static {
  let decoder: Box<Decoder<T>> = match encoding {
    Encoding::PLAIN => {
      Box::new(PlainDecoder::new(descr.type_length()))
    },
    Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
      return Err(general_err!("Cannot initialize this encoding through this function"))
    },
    Encoding::RLE => {
      Box::new(RleValueDecoder::new())
    },
    Encoding::DELTA_BINARY_PACKED => {
      Box::new(DeltaBitPackDecoder::new())
    },
    Encoding::DELTA_LENGTH_BYTE_ARRAY => {
      Box::new(DeltaLengthByteArrayDecoder::new())
    },
    Encoding::DELTA_BYTE_ARRAY => {
      Box::new(DeltaByteArrayDecoder::new())
    },
    e => return Err(nyi_err!("Encoding {} is not supported", e))
  };
  Ok(decoder)
}

// ----------------------------------------------------------------------
// PLAIN Decoding

/// Plain decoding that supports all types.
/// Values are encoded back to back. For native types, data is encoded as little endian.
/// Floating point types are encoded in IEEE.
/// See [`PlainDecoder`](`::encoding::PlainEncoder`) for more information.
pub struct PlainDecoder<T: DataType> {
  // The remaining number of values in the byte array
  num_values: usize,

  // The current starting index in the byte array.
  start: usize,

  // The length for the type `T`. Only used when `T` is `FixedLenByteArrayType`
  type_length: i32,

  // The byte array to decode from. Not set if `T` is bool.
  data: Option<ByteBufferPtr>,

  // Read `data` bit by bit. Only set if `T` is bool.
  bit_reader: Option<BitReader>,

  // To allow `T` in the generic parameter for this struct. This doesn't take any space.
  _phantom: PhantomData<T>
}

impl<T: DataType> PlainDecoder<T> {
  /// Creates new plain decoder.
  pub fn new(type_length: i32) -> Self {
    PlainDecoder {
      data: None,
      bit_reader: None,
      type_length: type_length,
      num_values: 0,
      start: 0,
      _phantom: PhantomData
    }
  }
}

impl<T: DataType> Decoder<T> for PlainDecoder<T> {
  #[inline]
  default fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    self.num_values = num_values;
    self.start = 0;
    self.data = Some(data);
    Ok(())
  }

  #[inline]
  fn values_left(&self) -> usize {
    self.num_values
  }

  #[inline]
  fn encoding(&self) -> Encoding {
    Encoding::PLAIN
  }

  #[inline]
  default fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = mem::size_of::<T::T>() * num_values;
    if bytes_left < bytes_to_decode {
      return Err(eof_err!("Not enough bytes to decode"));
    }
    let raw_buffer: &mut [u8] = unsafe {
      from_raw_parts_mut(buffer.as_ptr() as *mut u8, bytes_to_decode)
    };
    raw_buffer.copy_from_slice(data.range(self.start, bytes_to_decode).as_ref());
    self.start += bytes_to_decode;
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl Decoder<Int96Type> for PlainDecoder<Int96Type> {
  fn get(&mut self, buffer: &mut [Int96]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = 12 * num_values;
    if bytes_left < bytes_to_decode {
      return Err(eof_err!("Not enough bytes to decode"));
    }
    for i in 0..num_values {
      buffer[i].set_data(
        unsafe {
          // TODO: avoid this copying
          let slice = ::std::slice::from_raw_parts(
            data.range(self.start, 12).as_ref().as_ptr() as *mut u32, 3);
          Vec::from(slice)
        }
      );
      self.start += 12;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl Decoder<BoolType> for PlainDecoder<BoolType> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    self.num_values = num_values;
    self.bit_reader = Some(BitReader::new(data));
    Ok(())
  }

  fn get(&mut self, buffer: &mut [bool]) -> Result<usize> {
    assert!(self.bit_reader.is_some());

    let bit_reader = self.bit_reader.as_mut().unwrap();
    let values_read = bit_reader.get_batch::<bool>(buffer, 1);
    self.num_values -= values_read;

    Ok(values_read)
  }
}

impl Decoder<ByteArrayType> for PlainDecoder<ByteArrayType> {
  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      let len: usize = read_num_bytes!(
        u32, 4, data.start_from(self.start).as_ref()) as usize;
      self.start += mem::size_of::<u32>();
      if data.len() < self.start + len {
        return Err(eof_err!("Not enough bytes to decode"));
      }
      buffer[i].set_data(data.range(self.start, len));
      self.start += len;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl Decoder<FixedLenByteArrayType> for PlainDecoder<FixedLenByteArrayType> {
  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.data.is_some());
    assert!(self.type_length > 0);

    let data = self.data.as_mut().unwrap();
    let type_length = self.type_length as usize;
    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      if data.len() < self.start + type_length {
        return Err(eof_err!("Not enough bytes to decode"));
      }
      buffer[i].set_data(data.range(self.start, type_length));
      self.start += type_length;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

// ----------------------------------------------------------------------
// RLE_DICTIONARY/PLAIN_DICTIONARY Decoding

/// Dictionary decoder.
/// The dictionary encoding builds a dictionary of values encountered in a given column.
/// The dictionary is be stored in a dictionary page per column chunk.
/// See [`DictEncoder`](`::encoding::DictEncoder`) for more information.
pub struct DictDecoder<T: DataType> {
  // The dictionary, which maps ids to the values
  dictionary: Vec<T::T>,

  // Whether `dictionary` has been initialized
  has_dictionary: bool,

  // The decoder for the value ids
  rle_decoder: Option<RleDecoder>,

  // Number of values left in the data stream
  num_values: usize
}

impl<T: DataType> DictDecoder<T> {
  /// Creates new dictionary decoder.
  pub fn new() -> Self {
    Self {
      dictionary: vec![],
      has_dictionary: false,
      rle_decoder: None,
      num_values: 0
    }
  }

  /// Decodes and sets values for dictionary using `decoder` decoder.
  pub fn set_dict(&mut self, mut decoder: Box<Decoder<T>>) -> Result<()> {
    let num_values = decoder.values_left();
    self.dictionary.resize(num_values, T::T::default());
    let _ = decoder.get(&mut self.dictionary)?;
    self.has_dictionary = true;
    Ok(())
  }
}

impl<T: DataType> Decoder<T> for DictDecoder<T> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    // First byte in `data` is bit width
    let bit_width = data.as_ref()[0];
    let mut rle_decoder = RleDecoder::new(bit_width);
    rle_decoder.set_data(data.start_from(1));
    self.num_values = num_values;
    self.rle_decoder = Some(rle_decoder);
    Ok(())
  }

  fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
    assert!(self.rle_decoder.is_some());
    assert!(self.has_dictionary, "Must call set_dict() first!");

    let rle = self.rle_decoder.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    rle.get_batch_with_dict(&self.dictionary[..], buffer, num_values)
  }

  /// Number of values left in this decoder stream
  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::RLE_DICTIONARY
  }
}

// ----------------------------------------------------------------------
// RLE Decoding

/// RLE/Bit-Packing hybrid decoding for values.
/// Currently is used only for data pages v2 and supports boolean types.
/// See [`RleValueEncoder`](`::encoding::RleValueEncoder`) for more information.
pub struct RleValueDecoder<T: DataType> {
  values_left: usize,
  decoder: Option<RleDecoder>,
  _phantom: PhantomData<T>
}

impl<T: DataType> RleValueDecoder<T> {
  pub fn new() -> Self {
    Self {
      values_left: 0,
      decoder: None,
      _phantom: PhantomData
    }
  }

  #[inline]
  fn set_data_internal(
    &mut self, data: ByteBufferPtr,
    num_values: usize
  ) -> Result<()> {
    // We still need to remove prefix of i32 from the stream.
    let i32_size = mem::size_of::<i32>();
    let data_size = read_num_bytes!(i32, i32_size, data.as_ref()) as usize;
    let rle_decoder = self.decoder.as_mut().expect("RLE decoder is not initialized");
    rle_decoder.set_data(data.range(i32_size, data_size));
    self.values_left = num_values;
    Ok(())
  }
}

impl<T: DataType> Decoder<T> for RleValueDecoder<T> {
  #[inline]
  default fn set_data(
    &mut self,
    _data: ByteBufferPtr,
    _num_values: usize
  ) -> Result<()> {
    panic!("RleValueDecoder only supports BoolType");
  }

  #[inline]
  fn values_left(&self) -> usize {
    self.values_left
  }

  #[inline]
  fn encoding(&self) -> Encoding {
    Encoding::RLE
  }

  #[inline]
  fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
    let rle_decoder = self.decoder.as_mut().expect("RLE decoder is not initialized");
    let values_read = rle_decoder.get_batch(buffer)?;
    self.values_left -= values_read;
    Ok(values_read)
  }
}

impl Decoder<BoolType> for RleValueDecoder<BoolType> {
  #[inline]
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    // Only support RLE value reader for boolean values with bit width of 1.
    self.decoder = Some(RleDecoder::new(1));
    self.set_data_internal(data, num_values)
  }
}

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED Decoding

/// Delta binary packed decoder.
/// Supports INT32 and INT64 types.
/// See [`DeltaBitPackEncoder`](`::encoding::DeltaBitPackEncoder`) for more information.
pub struct DeltaBitPackDecoder<T: DataType> {
  bit_reader: BitReader,
  initialized: bool,

  // Header info
  num_values: usize,
  num_mini_blocks: i64,
  values_per_mini_block: usize,
  values_current_mini_block: usize,
  first_value: i64,
  first_value_read: bool,

  // Per block info
  min_delta: i64,
  mini_block_idx: usize,
  delta_bit_width: u8,
  delta_bit_widths: ByteBuffer,
  deltas_in_mini_block: Vec<T::T>, // eagerly loaded deltas for a mini block
  use_batch: bool,

  current_value: i64,

  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaBitPackDecoder<T> {
  /// Creates new delta bit packed decoder.
  pub fn new() -> Self {
    Self {
      bit_reader: BitReader::from(vec![]),
      initialized: false,
      num_values: 0,
      num_mini_blocks: 0,
      values_per_mini_block: 0,
      values_current_mini_block: 0,
      first_value: 0,
      first_value_read: false,
      min_delta: 0,
      mini_block_idx: 0,
      delta_bit_width: 0,
      delta_bit_widths: ByteBuffer::new(),
      deltas_in_mini_block: vec![],
      use_batch: mem::size_of::<T::T>() == 4,
      current_value: 0,
      _phantom: PhantomData
    }
  }

  /// Returns underlying bit reader offset.
  pub fn get_offset(&self) -> usize {
    assert!(self.initialized, "Bit reader is not initialized");
    self.bit_reader.get_byte_offset()
  }

  /// Initializes new mini block.
  #[inline]
  fn init_block(&mut self) -> Result<()> {
    self.min_delta = self.bit_reader
      .get_zigzag_vlq_int()
      .ok_or(eof_err!("Not enough data to decode 'min_delta'"))?;

    let mut widths = vec![];
    for _ in 0..self.num_mini_blocks {
      let w = self.bit_reader
        .get_aligned::<u8>(1)
        .ok_or(eof_err!("Not enough data to decode 'width'"))?;
      widths.push(w);
    }

    self.delta_bit_widths.set_data(widths);
    self.mini_block_idx = 0;
    self.delta_bit_width = self.delta_bit_widths.data()[0];
    self.values_current_mini_block = self.values_per_mini_block;
    Ok(())
  }

  /// Loads delta into mini block.
  #[inline]
  fn load_deltas_in_mini_block(&mut self) -> Result<()> {
    self.deltas_in_mini_block.clear();
    if self.use_batch {
      self.deltas_in_mini_block.resize(self.values_current_mini_block, T::T::default());
      let loaded = self.bit_reader.get_batch::<T::T>(
        &mut self.deltas_in_mini_block[..], self.delta_bit_width as usize
      );
      assert!(loaded == self.values_current_mini_block);
    } else {
      for _ in 0..self.values_current_mini_block {
        // TODO: load one batch at a time similar to int32
        let delta = self.bit_reader
          .get_value::<T::T>(self.delta_bit_width as usize)
          .ok_or(eof_err!("Not enough data to decode 'delta'"))?;
        self.deltas_in_mini_block.push(delta);
      }
    }

    Ok(())
  }
}

impl<T: DataType> Decoder<T> for DeltaBitPackDecoder<T> {
  // # of total values is derived from encoding
  #[inline]
  default fn set_data(&mut self, data: ByteBufferPtr, _: usize) -> Result<()> {
    self.bit_reader = BitReader::new(data);
    self.initialized = true;

    let block_size = self.bit_reader
      .get_vlq_int()
      .ok_or(eof_err!("Not enough data to decode 'block_size'"))?;
    self.num_mini_blocks = self.bit_reader
      .get_vlq_int()
      .ok_or(eof_err!("Not enough data to decode 'num_mini_blocks'"))?;
    self.num_values = self.bit_reader
      .get_vlq_int()
      .ok_or(eof_err!("Not enough data to decode 'num_values'"))? as usize;
    self.first_value = self.bit_reader
      .get_zigzag_vlq_int()
      .ok_or(eof_err!("Not enough data to decode 'first_value'"))?;

    // Reset decoding state
    self.first_value_read = false;
    self.mini_block_idx = 0;
    self.delta_bit_widths.clear();
    self.values_current_mini_block = 0;

    self.values_per_mini_block = (block_size / self.num_mini_blocks) as usize;
    assert!(self.values_per_mini_block % 8 == 0);

    Ok(())
  }

  default fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
    assert!(self.initialized, "Bit reader is not initialized");

    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      if !self.first_value_read {
        self.set_decoded_value(buffer, i, self.first_value);
        self.current_value = self.first_value;
        self.first_value_read = true;
        continue;
      }

      if self.values_current_mini_block == 0 {
        self.mini_block_idx += 1;
        if self.mini_block_idx < self.delta_bit_widths.size() {
          self.delta_bit_width = self.delta_bit_widths.data()[self.mini_block_idx];
          self.values_current_mini_block = self.values_per_mini_block;
        } else {
          self.init_block()?;
        }
        self.load_deltas_in_mini_block()?;
      }

      // we decrement values in current mini block, so we need to invert index for delta
      let delta = self.get_delta(
        self.deltas_in_mini_block.len() - self.values_current_mini_block
      );
      // It is OK for deltas to contain "overflowed" values after encoding,
      // e.g. i64::MAX - i64::MIN, so we use `wrapping_add` to "overflow" again and
      // restore original value.
      self.current_value = self.current_value.wrapping_add(self.min_delta);
      self.current_value = self.current_value.wrapping_add(delta as i64);
      self.set_decoded_value(buffer, i, self.current_value);
      self.values_current_mini_block -= 1;
    }

    self.num_values -= num_values;
    Ok(num_values)
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_BINARY_PACKED
  }
}

/// Helper trait to define specific conversions when decoding values
trait DeltaBitPackDecoderConversion<T: DataType> {
  /// Sets decoded value based on type `T`.
  #[inline]
  fn get_delta(&self, index: usize) -> i64;

  #[inline]
  fn set_decoded_value(&self, buffer: &mut [T::T], index: usize, value: i64);
}

impl<T: DataType> DeltaBitPackDecoderConversion<T> for DeltaBitPackDecoder<T> {
  #[inline]
  default fn get_delta(&self, _: usize) -> i64 {
    panic!("DeltaBitPackDecoder only supports Int32Type and Int64Type")
  }

  #[inline]
  default fn set_decoded_value(&self, _: &mut [T::T], _: usize, _: i64) {
    panic!("DeltaBitPackDecoder only supports Int32Type and Int64Type")
  }
}

impl DeltaBitPackDecoderConversion<Int32Type> for DeltaBitPackDecoder<Int32Type> {
  #[inline]
  fn get_delta(&self, index: usize) -> i64 {
    self.deltas_in_mini_block[index] as i64
  }

  #[inline]
  fn set_decoded_value(&self, buffer: &mut [i32], index: usize, value: i64) {
    buffer[index] = value as i32;
  }
}

impl DeltaBitPackDecoderConversion<Int64Type> for DeltaBitPackDecoder<Int64Type> {
  #[inline]
  fn get_delta(&self, index: usize) -> i64 {
    self.deltas_in_mini_block[index]
  }

  #[inline]
  fn set_decoded_value(&self, buffer: &mut [i64], index: usize, value: i64) {
    buffer[index] = value;
  }
}


// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY Decoding

/// Delta length byte array decoder.
/// Only applied to byte arrays to separate the length values and the data, the lengths
/// are encoded using DELTA_BINARY_PACKED encoding.
/// See [`DeltaLengthByteArrayEncoder`](`::encoding::DeltaLengthByteArrayEncoder`)
/// for more information.
pub struct DeltaLengthByteArrayDecoder<T: DataType> {
  // Lengths for each byte array in `data`
  // TODO: add memory tracker to this
  lengths: Vec<i32>,

  // Current index into `lengths`
  current_idx: usize,

  // Concatenated byte array data
  data: Option<ByteBufferPtr>,

  // Offset into `data`, always point to the beginning of next byte array.
  offset: usize,

  // Number of values left in this decoder stream
  num_values: usize,

  // Placeholder to allow `T` as generic parameter
  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaLengthByteArrayDecoder<T> {
  /// Creates new delta length byte array decoder.
  pub fn new() -> Self {
    Self {
      lengths: vec![],
      current_idx: 0,
      data: None,
      offset: 0,
      num_values: 0,
      _phantom: PhantomData
    }
  }
}

impl<T: DataType> Decoder<T> for DeltaLengthByteArrayDecoder<T> {
  default fn set_data(&mut self, _: ByteBufferPtr, _: usize) -> Result<()> {
    Err(general_err!("DeltaLengthByteArrayDecoder only support ByteArrayType"))
  }

  default fn get(&mut self, _: &mut [T::T]) -> Result<usize> {
    Err(general_err!("DeltaLengthByteArrayDecoder only support ByteArrayType"))
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_LENGTH_BYTE_ARRAY
  }
}

impl Decoder<ByteArrayType> for DeltaLengthByteArrayDecoder<ByteArrayType> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    let mut len_decoder = DeltaBitPackDecoder::<Int32Type>::new();
    len_decoder.set_data(data.all(), num_values)?;
    let num_lengths = len_decoder.values_left();
    self.lengths.resize(num_lengths, 0);
    len_decoder.get(&mut self.lengths[..])?;

    self.data = Some(data.start_from(len_decoder.get_offset()));
    self.offset = 0;
    self.current_idx = 0;
    self.num_values = num_lengths;
    Ok(())
  }

  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_ref().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      let len = self.lengths[self.current_idx] as usize;
      buffer[i].set_data(data.range(self.offset, len));
      self.offset += len;
      self.current_idx += 1;
    }

    self.num_values -= num_values;
    Ok(num_values)
  }
}

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY Decoding

/// Delta byte array decoder.
/// Prefix lengths are encoded using `DELTA_BINARY_PACKED` encoding, Suffixes are stored
/// using `DELTA_LENGTH_BYTE_ARRAY` encoding.
/// See [`DeltaByteArrayEncoder`](`::encoding::DeltaByteArrayEncoder`) for more
/// information.
pub struct DeltaByteArrayDecoder<T: DataType> {
  // Prefix lengths for each byte array
  // TODO: add memory tracker to this
  prefix_lengths: Vec<i32>,

  // The current index into `prefix_lengths`,
  current_idx: usize,

  // Decoder for all suffixes, the # of which should be the same as `prefix_lengths.len()`
  suffix_decoder: Option<DeltaLengthByteArrayDecoder<ByteArrayType>>,

  // The last byte array, used to derive the current prefix
  previous_value: Vec<u8>,

  // Number of values left
  num_values: usize,

  // Placeholder to allow `T` as generic parameter
  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaByteArrayDecoder<T> {
  /// Creates new delta byte array decoder.
  pub fn new() -> Self {
    Self {
      prefix_lengths: vec![],
      current_idx: 0,
      suffix_decoder: None,
      previous_value: vec![],
      num_values: 0,
      _phantom: PhantomData
    }
  }
}

impl<'m, T: DataType> Decoder<T> for DeltaByteArrayDecoder<T> {
  default fn set_data(&mut self, _: ByteBufferPtr, _: usize) -> Result<()> {
    Err(general_err!("DeltaByteArrayDecoder only support ByteArrayType"))
  }

  default fn get(&mut self, _: &mut [T::T]) -> Result<usize> {
    Err(general_err!("DeltaByteArrayDecoder only support ByteArrayType"))
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_BYTE_ARRAY
  }
}

impl<> Decoder<ByteArrayType> for DeltaByteArrayDecoder<ByteArrayType> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    let mut prefix_len_decoder = DeltaBitPackDecoder::<Int32Type>::new();
    prefix_len_decoder.set_data(data.all(), num_values)?;
    let num_prefixes = prefix_len_decoder.values_left();
    self.prefix_lengths.resize(num_prefixes, 0);
    prefix_len_decoder.get(&mut self.prefix_lengths[..])?;

    let mut suffix_decoder = DeltaLengthByteArrayDecoder::new();
    suffix_decoder.set_data(
      data.start_from(prefix_len_decoder.get_offset()), num_values)?;
    self.suffix_decoder = Some(suffix_decoder);
    self.num_values = num_prefixes;
    self.current_idx = 0;
    self.previous_value.clear();
    Ok(())
  }

  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.suffix_decoder.is_some());

    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      // Process suffix
      // TODO: this is awkward - maybe we should add a non-vectorized API?
      let mut suffix = vec![ByteArray::new(); 1];
      let suffix_decoder = self.suffix_decoder.as_mut().unwrap();
      suffix_decoder.get(&mut suffix[..])?;
      let suffix = suffix[0].data();

      // Extract current prefix length, can be 0
      let prefix_len = self.prefix_lengths[self.current_idx] as usize;

      // Concatenate prefix with suffix
      let mut result = Vec::new();
      result.extend_from_slice(&self.previous_value[0..prefix_len]);
      result.extend_from_slice(suffix);

      let data = ByteBufferPtr::new(result.clone());
      buffer[i].set_data(data);
      self.previous_value = result;
      self.current_idx += 1;
    }

    self.num_values -= num_values;
    Ok(num_values)
  }
}


#[cfg(test)]
mod tests {
  use super::super::encoding::*;
  use super::*;
  use schema::types::{ColumnDescriptor, ColumnPath, Type as Tpe};
  use std::mem;
  use std::rc::Rc;
  use util::bit_util::set_array_bit;
  use util::memory::MemTracker;
  use util::test_common::RandGen;

  #[test]
  fn test_get_decoders() {
    // supported encodings
    test_get_decoder::<Int32Type>(Encoding::PLAIN, None);
    test_get_decoder::<Int32Type>(Encoding::DELTA_BINARY_PACKED, None);
    test_get_decoder::<Int32Type>(Encoding::DELTA_LENGTH_BYTE_ARRAY, None);
    test_get_decoder::<Int32Type>(Encoding::DELTA_BYTE_ARRAY, None);
    test_get_decoder::<BoolType>(Encoding::RLE, None);

    // error when initializing
    test_get_decoder::<Int32Type>(
      Encoding::RLE_DICTIONARY,
      Some(general_err!("Cannot initialize this encoding through this function"))
    );
    test_get_decoder::<Int32Type>(
      Encoding::PLAIN_DICTIONARY,
      Some(general_err!("Cannot initialize this encoding through this function"))
    );

    // unsupported
    test_get_decoder::<Int32Type>(
      Encoding::BIT_PACKED,
      Some(nyi_err!("Encoding BIT_PACKED is not supported"))
    );
  }

  #[test]
  fn test_plain_decode_int32() {
    let data = vec![42, 18, 52];
    let data_bytes = Int32Type::to_byte_array(&data[..]);
    let mut buffer = vec![0; 3];
    test_plain_decode::<Int32Type>(
      ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]
    );
  }

  #[test]
  fn test_plain_decode_int64() {
    let data = vec![42, 18, 52];
    let data_bytes = Int64Type::to_byte_array(&data[..]);
    let mut buffer = vec![0; 3];
    test_plain_decode::<Int64Type>(
      ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]
    );
  }

  #[test]
  fn test_plain_decode_float() {
    let data = vec![3.14, 2.414, 12.51];
    let data_bytes = FloatType::to_byte_array(&data[..]);
    let mut buffer = vec![0.0; 3];
    test_plain_decode::<FloatType>(
      ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]
    );
  }

  #[test]
  fn test_plain_decode_double() {
    let data = vec![3.14f64, 2.414f64, 12.51f64];
    let data_bytes = DoubleType::to_byte_array(&data[..]);
    let mut buffer = vec![0.0f64; 3];
    test_plain_decode::<DoubleType>(
      ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]
    );
  }

  #[test]
  fn test_plain_decode_int96() {
    let v0 = vec![11, 22, 33];
    let v1 = vec![44, 55, 66];
    let v2 = vec![10, 20, 30];
    let v3 = vec![40, 50, 60];
    let mut data = vec![Int96::new(); 4];
    data[0].set_data(v0);
    data[1].set_data(v1);
    data[2].set_data(v2);
    data[3].set_data(v3);
    let data_bytes = Int96Type::to_byte_array(&data[..]);
    let mut buffer = vec![Int96::new(); 4];
    test_plain_decode::<Int96Type>(
      ByteBufferPtr::new(data_bytes), 4, -1, &mut buffer[..], &data[..]
    );
  }

  #[test]
  fn test_plain_decode_bool() {
    let data = vec![false, true, false, false, true, false, true, true, false, true];
    let data_bytes = BoolType::to_byte_array(&data[..]);
    let mut buffer = vec![false; 10];
    test_plain_decode::<BoolType>(
      ByteBufferPtr::new(data_bytes), 10, -1, &mut buffer[..], &data[..]
    );
  }

  #[test]
  fn test_plain_decode_byte_array() {
    let mut data = vec![ByteArray::new(); 2];
    data[0].set_data(ByteBufferPtr::new(String::from("hello").into_bytes()));
    data[1].set_data(ByteBufferPtr::new(String::from("parquet").into_bytes()));
    let data_bytes = ByteArrayType::to_byte_array(&data[..]);
    let mut buffer = vec![ByteArray::new(); 2];
    test_plain_decode::<ByteArrayType>(
      ByteBufferPtr::new(data_bytes), 2, -1, &mut buffer[..], &data[..]
    );
  }

  #[test]
  fn test_plain_decode_fixed_len_byte_array() {
    let mut data = vec![ByteArray::default(); 3];
    data[0].set_data(ByteBufferPtr::new(String::from("bird").into_bytes()));
    data[1].set_data(ByteBufferPtr::new(String::from("come").into_bytes()));
    data[2].set_data(ByteBufferPtr::new(String::from("flow").into_bytes()));
    let data_bytes = FixedLenByteArrayType::to_byte_array(&data[..]);
    let mut buffer = vec![ByteArray::default(); 3];
    test_plain_decode::<FixedLenByteArrayType>(
      ByteBufferPtr::new(data_bytes), 3, 4, &mut buffer[..], &data[..]
    );
  }

  #[test]
  #[should_panic(expected = "RleValueEncoder only supports BoolType")]
  fn test_rle_value_encode_int32_not_supported() {
    let mut encoder = RleValueEncoder::<Int32Type>::new();
    encoder.put(&vec![1, 2, 3, 4]).unwrap();
  }

  #[test]
  #[should_panic(expected = "RleValueDecoder only supports BoolType")]
  fn test_rle_value_decode_int32_not_supported() {
    let mut decoder = RleValueDecoder::<Int32Type>::new();
    decoder.set_data(ByteBufferPtr::new(vec![5, 0, 0, 0]), 1).unwrap();
  }

  #[test]
  fn test_rle_value_decode_bool_decode() {
    // Test multiple 'put' calls on the same encoder
    let data = vec![
      BoolType::gen_vec(-1, 256),
      BoolType::gen_vec(-1, 257),
      BoolType::gen_vec(-1, 126)
    ];
    test_rle_value_decode::<BoolType>(data);
  }

  #[test]
  #[should_panic(expected = "Bit reader is not initialized")]
  fn test_delta_bit_packed_not_initialized_offset() {
    // Fail if set_data() is not called before get_offset()
    let decoder = DeltaBitPackDecoder::<Int32Type>::new();
    decoder.get_offset();
  }

  #[test]
  #[should_panic(expected = "Bit reader is not initialized")]
  fn test_delta_bit_packed_not_initialized_get() {
    // Fail if set_data() is not called before get()
    let mut decoder = DeltaBitPackDecoder::<Int32Type>::new();
    let mut buffer = vec![];
    decoder.get(&mut buffer).unwrap();
  }

  #[test]
  fn test_delta_bit_packed_int32_empty() {
    let data = vec![vec![0; 0]];
    test_delta_bit_packed_decode::<Int32Type>(data);
  }

  #[test]
  fn test_delta_bit_packed_int32_repeat() {
    let block_data = vec![
      1, 2, 3, 4, 5, 6, 7, 8,
      1, 2, 3, 4, 5, 6, 7, 8,
      1, 2, 3, 4, 5, 6, 7, 8,
      1, 2, 3, 4, 5, 6, 7, 8
    ];
    test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
  }

  #[test]
  fn test_delta_bit_packed_int32_uneven() {
    let block_data = vec![1, -2, 3, -4, 5, 6, 7, 8, 9, 10, 11];
    test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
  }

  #[test]
  fn test_delta_bit_packed_int32_same_values() {
    let block_data = vec![
      127, 127, 127, 127, 127, 127, 127, 127,
      127, 127, 127, 127, 127, 127, 127, 127
    ];
    test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);

    let block_data = vec![
      -127, -127, -127, -127, -127, -127, -127, -127,
      -127, -127, -127, -127, -127, -127, -127, -127
    ];
    test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
  }

  #[test]
  fn test_delta_bit_packed_int32_min_max() {
    let block_data = vec![
      i32::min_value(), i32::max_value(),
      i32::min_value(), i32::max_value(),
      i32::min_value(), i32::max_value(),
      i32::min_value(), i32::max_value()
    ];
    test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
  }

  #[test]
  fn test_delta_bit_packed_int32_multiple_blocks() {
    // Test multiple 'put' calls on the same encoder
    let data = vec![
      Int32Type::gen_vec(-1, 64),
      Int32Type::gen_vec(-1, 128),
      Int32Type::gen_vec(-1, 64)
    ];
    test_delta_bit_packed_decode::<Int32Type>(data);
  }

  #[test]
  fn test_delta_bit_packed_int32_data_across_blocks() {
    // Test multiple 'put' calls on the same encoder
    let data = vec![
      Int32Type::gen_vec(-1, 256),
      Int32Type::gen_vec(-1, 257)
    ];
    test_delta_bit_packed_decode::<Int32Type>(data);
  }

  #[test]
  fn test_delta_bit_packed_int32_with_empty_blocks() {
    let data = vec![
      Int32Type::gen_vec(-1, 128),
      vec![0; 0],
      Int32Type::gen_vec(-1, 64)
    ];
    test_delta_bit_packed_decode::<Int32Type>(data);
  }

  #[test]
  fn test_delta_bit_packed_int64_empty() {
    let data = vec![vec![0; 0]];
    test_delta_bit_packed_decode::<Int64Type>(data);
  }

  #[test]
  fn test_delta_bit_packed_int64_min_max() {
    let block_data = vec![
      i64::min_value(), i64::max_value(),
      i64::min_value(), i64::max_value(),
      i64::min_value(), i64::max_value(),
      i64::min_value(), i64::max_value()
    ];
    test_delta_bit_packed_decode::<Int64Type>(vec![block_data]);
  }

  #[test]
  fn test_delta_bit_packed_int64_multiple_blocks() {
    // Test multiple 'put' calls on the same encoder
    let data = vec![
      Int64Type::gen_vec(-1, 64),
      Int64Type::gen_vec(-1, 128),
      Int64Type::gen_vec(-1, 64)
    ];
    test_delta_bit_packed_decode::<Int64Type>(data);
  }

  #[test]
  fn test_delta_bit_packed_decoder_sample() {
    let data_bytes = vec![
      128, 1, 4, 3, 58, 28, 6, 0,
      0, 0, 0, 8, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0
    ];
    let buffer = ByteBufferPtr::new(data_bytes);
    let mut decoder: DeltaBitPackDecoder<Int32Type> = DeltaBitPackDecoder::new();
    decoder.set_data(buffer, 3).unwrap();
    // check exact offsets, because when reading partial values we end up with
    // some data not being read from bit reader
    assert_eq!(decoder.get_offset(), 5);
    let mut result = vec![0, 0, 0];
    decoder.get(&mut result).unwrap();
    assert_eq!(decoder.get_offset(), 34);
    assert_eq!(result, vec![29, 43, 89]);
  }

  #[test]
  fn test_delta_byte_array_same_arrays() {
    let data = vec![
      vec![ByteArray::from(vec![1, 2, 3, 4, 5, 6])],
      vec![
        ByteArray::from(vec![1, 2, 3, 4, 5, 6]),
        ByteArray::from(vec![1, 2, 3, 4, 5, 6])
      ],
      vec![
        ByteArray::from(vec![1, 2, 3, 4, 5, 6]),
        ByteArray::from(vec![1, 2, 3, 4, 5, 6])
      ]
    ];
    test_delta_byte_array_decode(data);
  }

  #[test]
  fn test_delta_byte_array_unique_arrays() {
    let data = vec![
      vec![ByteArray::from(vec![1])],
      vec![ByteArray::from(vec![2, 3]), ByteArray::from(vec![4, 5, 6])],
      vec![ByteArray::from(vec![7, 8]), ByteArray::from(vec![9, 0, 1, 2])]
    ];
    test_delta_byte_array_decode(data);
  }

  #[test]
  fn test_delta_byte_array_single_array() {
    let data = vec![
      vec![ByteArray::from(vec![1, 2, 3, 4, 5, 6])]
    ];
    test_delta_byte_array_decode(data);
  }

  // Test column descriptor for the column.
  // Used for testing of Int32Type decoders and as a placeholder for delta encodings.
  fn get_test_column_desc_ptr() -> ColumnDescPtr {
    let type_ptr =
      Rc::new(Tpe::primitive_type_builder("col", Type::INT32).build().unwrap());
    Rc::new(ColumnDescriptor::new(type_ptr, None, 0, 0, ColumnPath::from("col")))
  }

  fn test_get_decoder<T: 'static + DataType>(
    encoding: Encoding, err: Option<ParquetError>
  ) {
    let descr = get_test_column_desc_ptr();
    let decoder = get_decoder::<T>(descr, encoding);
    match err {
      Some(parquet_error) => {
        assert!(decoder.is_err());
        assert_eq!(decoder.err().unwrap(), parquet_error);
      },
      None => {
        assert!(decoder.is_ok());
        assert_eq!(decoder.unwrap().encoding(), encoding);
      }
    }
  }

  fn test_plain_decode<T: DataType>(
    data: ByteBufferPtr,
    num_values: usize,
    type_length: i32,
    buffer: &mut [T::T],
    expected: &[T::T]
  ) {
    let mut decoder: PlainDecoder<T> = PlainDecoder::new(type_length);
    let result = decoder.set_data(data, num_values);
    assert!(result.is_ok());
    let result = decoder.get(&mut buffer[..]);
    assert!(result.is_ok());
    assert_eq!(decoder.values_left(), 0);
    assert_eq!(buffer, expected);
  }

  fn test_rle_value_decode<T: 'static + DataType>(data: Vec<Vec<T::T>>) {
    test_encode_decode::<T>(data, Encoding::RLE);
  }

  fn test_delta_bit_packed_decode<T: 'static + DataType>(data: Vec<Vec<T::T>>) {
    test_encode_decode::<T>(data, Encoding::DELTA_BINARY_PACKED);
  }

  fn test_delta_byte_array_decode(data: Vec<Vec<ByteArray>>) {
    test_encode_decode::<ByteArrayType>(data, Encoding::DELTA_BYTE_ARRAY);
  }

  // Input data represents vector of data slices to write (test multiple `put()` calls)
  // For example,
  //   vec![vec![1, 2, 3]] invokes `put()` once and writes {1, 2, 3}
  //   vec![vec![1, 2], vec![3]] invokes `put()` twice and writes {1, 2, 3}
  fn test_encode_decode<T: 'static + DataType>(
    data: Vec<Vec<T::T>>,
    encoding: Encoding
  ) {
    // Encode data
    let mut encoder = get_encoder::<T>(get_test_column_desc_ptr(), encoding,
      Rc::new(MemTracker::new())).expect("get encoder");

    for v in &data[..] {
      encoder.put(&v[..]).expect("ok to encode");
    }
    let bytes = encoder.flush_buffer().expect("ok to flush buffer");

    // Flatten expected data as contiguous array of values
    let expected: Vec<T::T> = data.iter().flat_map(|s| s.clone()).collect();

    // Decode data and compare with original
    let mut decoder = get_decoder::<T>(get_test_column_desc_ptr(), encoding)
      .expect("get decoder");

    let mut result = vec![T::T::default(); expected.len()];
    decoder.set_data(bytes, expected.len()).expect("ok to set data");
    let mut result_num_values = 0;
    while decoder.values_left() > 0 {
      result_num_values += decoder.get(&mut result[result_num_values..])
        .expect("ok to decode");
    }
    assert_eq!(result_num_values, expected.len());
    assert_eq!(result, expected);
  }

  fn usize_to_bytes(v: usize) -> [u8; 4] {
    unsafe { mem::transmute::<u32, [u8; 4]>(v as u32) }
  }

  /// A util trait to convert slices of different types to byte arrays
  trait ToByteArray<T: DataType> {
    fn to_byte_array(data: &[T::T]) -> Vec<u8>;
  }

  impl<T> ToByteArray<T> for T where T: DataType {
    default fn to_byte_array(data: &[T::T]) -> Vec<u8> {
      let mut v = vec![];
      let type_len = ::std::mem::size_of::<T::T>();
      v.extend_from_slice(
        unsafe {
          ::std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * type_len)
        }
      );
      v
    }
  }

  impl ToByteArray<BoolType> for BoolType {
    fn to_byte_array(data: &[bool]) -> Vec<u8> {
      let mut v = vec![];
      for i in 0..data.len() {
        if i % 8 == 0 {
          v.push(0);
        }
        if data[i] {
          set_array_bit(&mut v[..], i);
        }
      }
      v
    }
  }

  impl ToByteArray<Int96Type> for Int96Type {
    fn to_byte_array(data: &[Int96]) -> Vec<u8> {
      let mut v = vec![];
      for d in data {
        unsafe {
          let copy = ::std::slice::from_raw_parts(d.data().as_ptr() as *const u8, 12);
          v.extend_from_slice(copy);
        };
      }
      v
    }
  }

  impl ToByteArray<ByteArrayType> for ByteArrayType {
    fn to_byte_array(data: &[ByteArray]) -> Vec<u8> {
      let mut v = vec![];
      for d in data {
        let buf = d.data();
        let len = &usize_to_bytes(buf.len());
        v.extend_from_slice(len);
        v.extend(buf);
      }
      v
    }
  }

  impl ToByteArray<FixedLenByteArrayType> for FixedLenByteArrayType {
    fn to_byte_array(data: &[ByteArray]) -> Vec<u8> {
      let mut v = vec![];
      for d in data {
        let buf = d.data();
        v.extend(buf);
      }
      v
    }
  }
}
