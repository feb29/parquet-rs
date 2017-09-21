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

use std::mem;
use std::collections::HashMap;

use basic::*;
use data_type::*;
use schema::types::ColumnDescPtr;
use encodings::decoding::{get_decoder, Decoder, PlainDecoder, DictDecoder};
use encodings::levels::LevelDecoder;
use errors::{Result, ParquetError};
use super::page::{Page, PageReader};

pub enum ColumnReader<'a> {
  BoolColumnReader(ColumnReaderImpl<'a, BoolType>),
  Int32ColumnReader(ColumnReaderImpl<'a, Int32Type>),
  Int64ColumnReader(ColumnReaderImpl<'a, Int64Type>),
  Int96ColumnReader(ColumnReaderImpl<'a, Int96Type>),
  FloatColumnReader(ColumnReaderImpl<'a, FloatType>),
  DoubleColumnReader(ColumnReaderImpl<'a, DoubleType>),
  ByteArrayColumnReader(ColumnReaderImpl<'a, ByteArrayType>),
  FixedLenByteArrayColumnReader(ColumnReaderImpl<'a, FixedLenByteArrayType>),
}

/// Get a specific column reader corresponding to `col_descr`. The column
/// reader will read from pages in `col_page_reader`.
pub fn get_column_reader<'a>(col_descr: ColumnDescPtr,
                             col_page_reader: Box<PageReader + 'a>)
                             -> ColumnReader<'a> {
  match col_descr.physical_type() {
    Type::BOOLEAN => ColumnReader::BoolColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader)),
    Type::INT32 => ColumnReader::Int32ColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader)),
    Type::INT64 => ColumnReader::Int64ColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader)),
    Type::INT96 => ColumnReader::Int96ColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader)),
    Type::FLOAT => ColumnReader::FloatColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader)),
    Type::DOUBLE => ColumnReader::DoubleColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader)),
    Type::BYTE_ARRAY => ColumnReader::ByteArrayColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader)),
    Type::FIXED_LEN_BYTE_ARRAY => ColumnReader::FixedLenByteArrayColumnReader(
      ColumnReaderImpl::new(col_descr, col_page_reader))
  }
}

/// Get a typed column reader for the specific type `T`, by casting `col_reader`.
/// NOTE: the caller MUST guarantee that the actual enum value for `col_reader`
/// matches the type `T`. Otherwise, disastrous consequence could happen.
pub fn get_typed_column_reader<'a, T: DataType>(
    col_reader: ColumnReader<'a>) -> ColumnReaderImpl<'a, T> {
  match col_reader {
    ColumnReader::BoolColumnReader(r) => unsafe { mem::transmute(r) },
    ColumnReader::Int32ColumnReader(r) => unsafe { mem::transmute(r) },
    ColumnReader::Int64ColumnReader(r) => unsafe { mem::transmute(r) },
    ColumnReader::Int96ColumnReader(r) => unsafe { mem::transmute(r) },
    ColumnReader::FloatColumnReader(r) => unsafe { mem::transmute(r) },
    ColumnReader::DoubleColumnReader(r) => unsafe { mem::transmute(r) },
    ColumnReader::ByteArrayColumnReader(r) => unsafe { mem::transmute(r) },
    ColumnReader::FixedLenByteArrayColumnReader(r) => unsafe { mem::transmute(r) }
  }
}

/// A value reader for a particular primitive column.
/// The lifetime parameter `'a` denotes the lifetime of the page reader
pub struct ColumnReaderImpl<'a, T: DataType> {
  descr: ColumnDescPtr,
  def_level_decoder: Option<LevelDecoder>,
  rep_level_decoder: Option<LevelDecoder>,
  page_reader: Box<PageReader + 'a>,
  current_encoding: Option<Encoding>,

  // The total number of values stored in the data page.
  num_buffered_values: u32,

  // The number of values from the current data page that has been
  // decoded into memory so far.
  num_decoded_values: u32,

  // Cache of decoders for existing encodings
  decoders: HashMap<Encoding, Box<Decoder<T>>>
}

impl<'a, T: DataType> ColumnReaderImpl<'a, T> where T: 'static {
  pub fn new(descr: ColumnDescPtr, page_reader: Box<PageReader + 'a>) -> Self {
    Self { descr: descr, def_level_decoder: None, rep_level_decoder: None,
           page_reader: page_reader, current_encoding: None,
           num_buffered_values: 0, num_decoded_values: 0, decoders: HashMap::new() }
  }

  /// Read a batch of values of at most `batch_size`.
  /// This will try to read from the row group, and fills up at most
  /// `batch_size` values for `def_levels`, `rep_levels` and `values`.
  /// It will stop either when the row group is depleted or `batch_size` values
  /// has been read.
  ///
  /// Note that in case the field being read is not required, `values` could
  /// contain less values than `def_levels`. Also note that this will skip
  /// reading def/rep levels if the field is not required / not repeated, respectively.
  ///
  /// If `def_levels` or `rep_levels` is `None`, this will also skip reading the respective
  /// levels. This is useful when the caller of this function knows in advance that
  /// the field is required and non-repeated, therefore can avoid allocating memory for
  /// the levels data.
  ///
  /// Returns a tuple where the first element is the actual number of values read,
  /// and the second element is the actual number of levels read.
  #[inline]
  pub fn read_batch(&mut self, batch_size: usize,
                    mut def_levels: Option<&mut [i16]>,
                    mut rep_levels: Option<&mut [i16]>,
                    values: &mut [T::T]) -> Result<(usize, usize)> {
    let mut values_read = 0;
    let mut levels_read = 0;

    while values_read < batch_size {
      if !self.has_next()? {
        break;
      }

      let mut values_to_read = 0;
      let mut num_def_levels = 0;
      let num_rep_levels;

      let next_levels_read = levels_read + ::std::cmp::min(
        batch_size, (self.num_buffered_values - self.num_decoded_values) as usize);

      // if the field is required and non-repeated, there are no definition levels
      if self.descr.max_def_level() > 0 && def_levels.as_ref().is_some() {
        if let Some(ref mut levels) = def_levels {
          assert!(levels.len() >= next_levels_read,
              "def_levels.len() ({}) must be at least {}", levels.len(), next_levels_read);
          num_def_levels = self.read_def_levels(&mut levels[levels_read..next_levels_read])?;
          for i in levels_read..levels_read + num_def_levels {
            if levels[i] == self.descr.max_def_level() {
              values_to_read += 1;
            }
          }
        }
      } else {
        // required field, read all values
        values_to_read = batch_size;
      }

      if self.descr.max_rep_level() > 0 && rep_levels.is_some() {
        if let Some(ref mut levels) = rep_levels {
          assert!(levels.len() >= next_levels_read, "rep_levels.len() must be at least {}", next_levels_read);
          num_rep_levels = self.read_rep_levels(&mut levels[levels_read..next_levels_read])?;
          assert_eq!(num_def_levels, num_rep_levels, "Number of decoded rep / def levels did not match");
          levels_read += num_rep_levels;
        }
      }

      assert!(values.len() >= values_read + values_to_read,
          "values.len() must be at least {}", values_read + values_to_read);
      let curr_values_read = self.read_values(&mut values[values_read..values_read + values_to_read])?;
      self.num_decoded_values += ::std::cmp::max(num_def_levels, curr_values_read) as u32;
      values_read += curr_values_read;
    }

    Ok((values_read, levels_read))
  }

  /// Read a new page and set up the decoders for levels, values or dictionary.
  /// Returns false if there's no page left.
  fn read_new_page(&mut self) -> Result<bool> {
    #[allow(while_true)]
    while true {
      match self.page_reader.get_next_page()? {
        // No more page to read
        None => {
          return Ok(false)
        },
        Some(current_page) => {
          match current_page {
            // 1. dictionary page: configure dictionary for this page.
            p @ Page::DictionaryPage { .. } => {
              self.configure_dictionary(p)?;
              continue;
            },
            // 2. data page v1
            Page::DataPage { buf, num_values, mut encoding, def_level_encoding, rep_level_encoding } => {
              self.num_buffered_values = num_values;
              self.num_decoded_values = 0;

              let mut buffer_ptr = buf;

              if self.descr.max_rep_level() > 0 {
                let mut rep_decoder = LevelDecoder::new(rep_level_encoding, self.descr.max_rep_level());
                let total_bytes = rep_decoder.set_data(buffer_ptr.all());
                buffer_ptr = buffer_ptr.start_from(total_bytes);
                self.rep_level_decoder = Some(rep_decoder);
              }

              if self.descr.max_def_level() > 0 {
                let mut def_decoder = LevelDecoder::new(def_level_encoding, self.descr.max_def_level());
                let total_bytes = def_decoder.set_data(buffer_ptr.all());
                buffer_ptr = buffer_ptr.start_from(total_bytes);
                self.def_level_decoder = Some(def_decoder);
              }

              if encoding == Encoding::PLAIN_DICTIONARY {
                encoding = Encoding::RLE_DICTIONARY;
              }

              let decoder =
                if encoding == Encoding::RLE_DICTIONARY {
                  self.decoders.get_mut(&encoding).expect("Decoder for dict should have been set")
                } else {
                  // Search cache for data page decoder
                  if !self.decoders.contains_key(&encoding) {
                    // Initialize decoder for this page
                    // TODO: support other types of encodings
                    let data_decoder = match encoding {
                      Encoding::PLAIN => get_decoder::<T>(self.descr.clone(), encoding)?,
                      en => return Err(nyi_err!("Unsupported encoding {}", en))
                    };
                    self.decoders.insert(encoding, data_decoder);
                  }
                  self.decoders.get_mut(&encoding).unwrap()
                };

              decoder.set_data(buffer_ptr, num_values as usize)?;
              self.current_encoding = Some(encoding);

              return Ok(true)
            },
            // TODO: support other page types (e.g., data page v2)
            _ => {
              // Skip this page and continue.
              continue;
            }
          };
        }
      }
    }

    Ok(true)
  }

  #[inline]
  fn has_next(&mut self) -> Result<bool> {
    if self.num_buffered_values == 0 || self.num_buffered_values == self.num_decoded_values {
      // TODO: should we return false if read_new_page() = true and num_buffered_values = 0?
      if !self.read_new_page()? {
        Ok(false)
      } else {
        Ok(self.num_buffered_values != 0)
      }
    } else { Ok(true) }
  }

  #[inline]
  fn read_rep_levels(&mut self, buffer: &mut [i16]) -> Result<usize> {
    let level_decoder = self.rep_level_decoder.as_mut().expect("rep_level_decoder be set");
    level_decoder.get(buffer)
  }

  #[inline]
  fn read_def_levels(&mut self, buffer: &mut [i16]) -> Result<usize> {
    let level_decoder = self.def_level_decoder.as_mut().expect("def_level_decoder be set");
    level_decoder.get(buffer)
  }

  #[inline]
  fn read_values(&mut self, buffer: &mut [T::T]) -> Result<usize> {
    let encoding = self.current_encoding.expect("current_encoding should be set");
    let current_decoder = self.decoders
      .get_mut(&encoding)
      .expect(format!("decoder for encoding {} should be set", encoding).as_str());
    current_decoder.get(buffer)
  }

  #[inline]
  fn configure_dictionary(&mut self, page: Page) -> Result<bool> {
    let mut encoding = page.encoding();
    if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY {
      encoding = Encoding::RLE_DICTIONARY
    }

    if self.decoders.contains_key(&encoding) {
      return Err(general_err!("Column cannot have more than one dictionary"))
    }

    if encoding == Encoding::RLE_DICTIONARY {
      let mut dictionary = PlainDecoder::<T>::new(self.descr.type_length());
      let num_values = page.num_values();
      dictionary.set_data(page.buffer().clone(), num_values as usize)?;

      let mut decoder = DictDecoder::new();
      decoder.set_dict(Box::new(dictionary))?;
      self.decoders.insert(encoding, Box::new(decoder));
      Ok(true)
    } else {
      Err(nyi_err!("Invalid/Unsupported encoding type for dictionary: {}", encoding))
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use std::rc::Rc;
  use std::collections::VecDeque;
  use std::vec::IntoIter;
  use rand::distributions::range::SampleRange;

  use basic::Type as PhysicalType;
  use column::page::Page;
  use encodings::encoding::{get_encoder, Encoder, DictEncoder};
  use encodings::levels::LevelEncoder;
  use schema::types::{Type as SchemaType, ColumnDescriptor, ColumnPath};
  use util::memory::{ByteBufferPtr, MemTracker, MemTrackerPtr};
  use util::test_common::random_numbers_range;

  const NUM_LEVELS: usize = 128;
  const NUM_PAGES: usize = 2;
  const MAX_DEF_LEVEL: i16 = 5;
  const MAX_REP_LEVEL: i16 = 5;

  #[test]
  fn test_read_plain_int32() {
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = SchemaType::primitive_type_builder("a", PhysicalType::INT32)
      .with_repetition(Repetition::REQUIRED)
      .with_logical_type(LogicalType::INT_32)
      .with_length(-1)
      .build()
      .expect("new_primitive_type() should be OK");
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 16, ::std::i32::MIN, ::std::i32::MAX);
  }

  #[test]
  fn test_read_plain_int32_uneven() {
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = SchemaType::primitive_type_builder("a", PhysicalType::INT32)
      .with_repetition(Repetition::REQUIRED)
      .with_logical_type(LogicalType::INT_32)
      .with_length(-1)
      .build()
      .expect("new_primitive_type() should be OK");
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 17, ::std::i32::MIN, ::std::i32::MAX);
  }

  #[test]
  fn test_read_plain_int32_multi_page() {
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = get_test_int32_type();
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 512, ::std::i32::MIN, ::std::i32::MAX);
  }

  #[test]
  fn test_read_plain_int32_required_non_repeated() {
    // test case when column descriptor has MAX_DEF_LEVEL = 0 and MAX_REP_LEVEL = 0
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = get_test_int32_type();
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, 0, 0, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 16, ::std::i32::MIN, ::std::i32::MAX);
  }

  #[test]
  fn test_read_plain_int64() {
    let mut tester = ColumnReaderTester::<Int64Type>::new();
    let primitive_type = get_test_int64_type();
    let descr = ColumnDescriptor::new(Rc::new(primitive_type), None, 1, 1, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 16, ::std::i64::MIN, ::std::i64::MAX);
  }

  #[test]
  fn test_read_plain_int64_uneven() {
    let mut tester = ColumnReaderTester::<Int64Type>::new();
    let primitive_type = get_test_int64_type();
    let descr = ColumnDescriptor::new(Rc::new(primitive_type), None, 1, 1, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 17, ::std::i64::MIN, ::std::i64::MAX);
  }

  #[test]
  fn test_read_plain_int64_multi_page() {
    let mut tester = ColumnReaderTester::<Int64Type>::new();
    let primitive_type = get_test_int64_type();
    let descr = ColumnDescriptor::new(Rc::new(primitive_type), None, 1, 1, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 512, ::std::i64::MIN, ::std::i64::MAX);
  }

  #[test]
  fn test_read_plain_int64_required_non_repeated() {
    // test case when column descriptor has MAX_DEF_LEVEL = 0 and MAX_REP_LEVEL = 0
    let mut tester = ColumnReaderTester::<Int64Type>::new();
    let primitive_type = get_test_int64_type();
    let descr = ColumnDescriptor::new(Rc::new(primitive_type), None, 0, 0, ColumnPath::new(Vec::new()));
    tester.test_plain(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 16, ::std::i64::MIN, ::std::i64::MAX);
  }

  #[test]
  fn test_read_dict_int32_small() {
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = get_test_int32_type();
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_dict(Rc::new(descr), 2, 2, 16, 0, 3);
  }

  #[test]
  fn test_read_dict_int32() {
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = get_test_int32_type();
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_dict(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 16, 0, 3);
  }

  #[test]
  fn test_read_dict_int32_uneven() {
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = get_test_int32_type();
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_dict(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 17, 0, 3);
  }

  #[test]
  fn test_read_dict_int32_multi_page() {
    let mut tester = ColumnReaderTester::<Int32Type>::new();
    let primitive_type = get_test_int32_type();
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_dict(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 512, 0, 3);
  }

  #[test]
  fn test_read_dict_int64() {
    let mut tester = ColumnReaderTester::<Int64Type>::new();
    let primitive_type = get_test_int64_type();
    let descr = ColumnDescriptor::new(
      Rc::new(primitive_type), None, MAX_DEF_LEVEL, MAX_REP_LEVEL, ColumnPath::new(Vec::new()));
    tester.test_dict(Rc::new(descr), NUM_PAGES, NUM_LEVELS, 16, 0, 3);
  }

  fn get_test_int32_type() -> SchemaType {
    SchemaType::primitive_type_builder("a", PhysicalType::INT32)
      .with_repetition(Repetition::REQUIRED)
      .with_logical_type(LogicalType::INT_32)
      .with_length(-1)
      .build()
      .expect("build() should be OK")
  }

  fn get_test_int64_type() -> SchemaType {
    SchemaType::primitive_type_builder("a", PhysicalType::INT64)
      .with_repetition(Repetition::REQUIRED)
      .with_logical_type(LogicalType::INT_64)
      .with_length(-1)
      .build()
      .expect("build() should be OK")
  }

  struct ColumnReaderTester<T: DataType> where T::T: PartialOrd + SampleRange + Copy, T: 'static {
    rep_levels: Vec<i16>,
    def_levels: Vec<i16>,
    values: Vec<T::T>
  }

  impl<T: DataType> ColumnReaderTester<T> where T::T: PartialOrd + SampleRange + Copy, T: 'static  {
    pub fn new() -> Self {
      Self { rep_levels: Vec::new(), def_levels: Vec::new(), values: Vec::new() }
    }

    pub fn test_plain(&mut self, desc: ColumnDescPtr, num_pages: usize,
                      num_levels: usize, batch_size: usize, min: T::T, max: T::T) {
      let mut pages = VecDeque::new();
      make_pages::<T>(
        desc.clone(), Encoding::PLAIN, num_pages, num_levels, min, max,
        &mut self.def_levels, &mut self.rep_levels, &mut self.values, &mut pages);

      let page_reader = TestPageReader::new(Vec::from(pages));
      let column_reader: ColumnReader = get_column_reader(desc, Box::new(page_reader));
      let mut typed_column_reader = get_typed_column_reader::<T>(column_reader);
      let mut actual_rep_levels = vec![0; num_levels * num_pages];
      let mut actual_def_levels = vec![0; num_levels * num_pages];
      let mut actual_values = vec![T::T::default(); num_levels * num_pages];

      let mut curr_values_read = 0;
      let mut curr_levels_read = 0;
      let mut done = false;
      while !done {
        let (values_read, levels_read) = typed_column_reader.read_batch(
          batch_size,
          Some(&mut actual_def_levels[curr_levels_read..]),
          Some(&mut actual_rep_levels[curr_levels_read..]),
          &mut actual_values[curr_values_read..])
        .expect("read_batch() should be OK");

        if values_read == 0 {
          done = true;
        }

        curr_values_read += values_read;
        curr_levels_read += levels_read;
      }

      assert_eq!(&actual_rep_levels[..curr_levels_read], &self.rep_levels[..]);
      assert_eq!(&actual_def_levels[..curr_levels_read], &self.def_levels[..]);
      assert_eq!(&actual_values[..curr_values_read], &self.values[..]);
    }

    pub fn test_dict(&mut self, desc: ColumnDescPtr, num_pages: usize,
                      num_levels: usize, batch_size: usize, min: T::T, max: T::T) {
      let mut pages = VecDeque::new();
      make_pages::<T>(
        desc.clone(), Encoding::RLE_DICTIONARY, num_pages, num_levels, min, max,
        &mut self.def_levels, &mut self.rep_levels, &mut self.values, &mut pages);

      let page_reader = TestPageReader::new(Vec::from(pages));
      let column_reader: ColumnReader = get_column_reader(desc, Box::new(page_reader));
      let mut typed_column_reader = get_typed_column_reader::<T>(column_reader);
      let mut actual_rep_levels = vec![0; num_levels * num_pages];
      let mut actual_def_levels = vec![0; num_levels * num_pages];
      let mut actual_values = vec![T::T::default(); num_levels * num_pages];

      let mut curr_values_read = 0;
      let mut curr_levels_read = 0;
      let mut done = false;
      while !done {
        let (values_read, levels_read) = typed_column_reader.read_batch(
          batch_size,
          Some(&mut actual_def_levels[curr_levels_read..]),
          Some(&mut actual_rep_levels[curr_levels_read..]),
          &mut actual_values[curr_values_read..])
        .expect("read_batch() should be OK");

        if values_read == 0 {
          done = true;
        }

        curr_values_read += values_read;
        curr_levels_read += levels_read;
      }

      assert_eq!(actual_rep_levels.len(), self.rep_levels.len(), "rep len doesn't match");
      assert_eq!(&actual_rep_levels[..curr_levels_read], &self.rep_levels[..], "rep content doesn't match");
      assert_eq!(actual_def_levels.len(), self.def_levels.len(), "def len doesn't match");
      assert_eq!(&actual_def_levels[..curr_levels_read], &self.def_levels[..], "def content doesn't match");
      assert_eq!(&actual_values[..curr_values_read], &self.values[..], "values doesn't match");
    }
  }

  struct TestPageReader {
    pages: IntoIter<Page>,
  }

  impl TestPageReader {
    pub fn new(pages: Vec<Page>) -> Self {
      Self { pages: pages.into_iter() }
    }
  }

  impl PageReader for TestPageReader {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
      Ok(self.pages.next())
    }
  }

  // ----------------------------------------------------------------------
  // Utility functions for generating testing pages

  trait DataPageBuilder {
    fn add_rep_levels(&mut self, max_level: i16, rep_levels: &[i16]);
    fn add_def_levels(&mut self, max_level: i16, def_levels: &[i16]);
    fn add_values<T: DataType>(&mut self, encoding: Encoding, values: &[T::T]) where T: 'static;
    fn add_indices(&mut self, indices: ByteBufferPtr);
    fn consume(self) -> Page;
  }

  /// A utility struct for building data pages. Callers must call:
  ///   - add_rep_levels()
  ///   - add_def_levels()
  ///   - add_values() for normal data page / add_indices() for dictionary data page
  ///   - consume()
  /// in order to populate and obtain a data page.
  struct DataPageBuilderImpl {
    desc: ColumnDescPtr,
    encoding: Option<Encoding>,
    mem_tracker: MemTrackerPtr,
    num_values: u32,
    buffer: Vec<u8>
  }

  impl DataPageBuilderImpl {
    // Create new DataPageBuilder
    // when number of values is not explicitly provided, set it to 0
    fn new(desc: ColumnDescPtr, num_values: u32) -> Self {
      DataPageBuilderImpl {
        desc: desc,
        encoding: None,
        mem_tracker: MemTracker::new_ptr(None).expect("new_ptr() should be OK"),
        num_values: num_values,
        buffer: vec!()
      }
    }

    fn add_levels(&mut self, max_level: i16, levels: &[i16]) {
      let max_buffer_size = LevelEncoder::max_buffer_size(Encoding::RLE, max_level, levels.len());
      let mut level_encoder = LevelEncoder::new(Encoding::RLE, max_level, vec![0; max_buffer_size]);
      level_encoder.put(levels).expect("put() should be OK");
      let encoded_levels = level_encoder.consume().expect("consume() should be OK");
      self.buffer.extend_from_slice(encoded_levels.as_slice());
    }
  }

  impl DataPageBuilder for DataPageBuilderImpl {
    fn add_rep_levels(&mut self, max_levels: i16, rep_levels: &[i16]) {
      self.num_values = rep_levels.len() as u32;
      self.add_levels(max_levels, rep_levels);
    }

    fn add_def_levels(&mut self, max_levels: i16, def_levels: &[i16]) {
      assert!(self.num_values == def_levels.len() as u32, "Must call `add_rep_levels() first!`");
      self.add_levels(max_levels, def_levels);
    }

    fn add_values<T: DataType>(&mut self, encoding: Encoding, values: &[T::T]) where T: 'static {
      assert!(self.num_values >= values.len() as u32, "num_values: {}, values.len(): {}",
              self.num_values, values.len());
      self.encoding = Some(encoding);
      let mut encoder: Box<Encoder<T>> = get_encoder::<T>(
        self.desc.clone(), encoding, self.mem_tracker.clone()).expect("get_encoder() should be OK");
      encoder.put(values).expect("put() should be OK");
      let encoded_values = encoder.flush_buffer().expect("consume_buffer() should be OK");
      self.buffer.extend_from_slice(encoded_values.data());
    }

    fn add_indices(&mut self, indices: ByteBufferPtr) {
      self.encoding = Some(Encoding::RLE_DICTIONARY);
      self.buffer.extend_from_slice(indices.data());
    }

    fn consume(self) -> Page {
      Page::DataPage {
        buf: ByteBufferPtr::new(self.buffer),
        num_values: self.num_values,
        encoding: self.encoding.unwrap(),
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE
      }
    }

  }

  fn make_pages<T: DataType>(desc: ColumnDescPtr,
                             encoding: Encoding,
                             num_pages: usize,
                             levels_per_page: usize,
                             min: T::T,
                             max: T::T,
                             def_levels: &mut Vec<i16>,
                             rep_levels: &mut Vec<i16>,
                             values: &mut Vec<T::T>,
                             pages: &mut VecDeque<Page>)
      where T::T: PartialOrd + SampleRange + Copy, T: 'static {
    let mut num_values = 0;
    let max_def_level = desc.max_def_level();
    let max_rep_level = desc.max_rep_level();

    let mem_tracker = MemTracker::new_ptr(None).expect("new_ptr() should be OK");
    let mut dict_encoder = DictEncoder::<T>::new(desc.clone(), mem_tracker);

    for i in 0..num_pages {
      let mut num_values_cur_page = 0;
      let level_range = i * levels_per_page .. (i+1) * levels_per_page;

      if max_def_level > 0 {
        random_numbers_range(levels_per_page, 0, max_def_level + 1, def_levels);
        for dl in &def_levels[level_range.clone()] {
          if *dl == max_def_level {
            num_values_cur_page += 1;
          }
        }
      } else {
        num_values_cur_page = levels_per_page;
      }
      if max_rep_level > 0 {
        random_numbers_range(levels_per_page, 0, max_rep_level + 1, rep_levels);
      }
      random_numbers_range(num_values_cur_page, min, max, values);

      // Generate the current page

      let mut page_builder = DataPageBuilderImpl::new(desc.clone(), levels_per_page as u32);
      if max_rep_level > 0 {
        page_builder.add_rep_levels(max_rep_level, &rep_levels[level_range.clone()]);
      }
      if max_def_level > 0 {
        page_builder.add_def_levels(max_def_level, &def_levels[level_range]);
      }

      let value_range = num_values .. num_values + num_values_cur_page;
      match encoding {
        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
          let _ = dict_encoder.put(&values[value_range.clone()]);
          let indices = dict_encoder.write_indices().expect("write_indices() should be OK");
          page_builder.add_indices(indices);
        },

        Encoding::PLAIN => {
          page_builder.add_values::<T>(encoding, &values[value_range]);
        },

        enc @ _ => panic!("Unexpected encoding {}", enc)
      }

      let data_page = page_builder.consume();
      pages.push_back(data_page);
      num_values += num_values_cur_page;
    }

    if encoding == Encoding::PLAIN_DICTIONARY || encoding == Encoding::RLE_DICTIONARY {
      let dict = dict_encoder.write_dict().expect("write_dict() should be OK");
      let dict_page = Page::DictionaryPage {
        buf: dict,
        num_values: dict_encoder.num_entries() as u32,
        encoding: Encoding::RLE_DICTIONARY,
        is_sorted: false
      };
      pages.push_front(dict_page);
    }
  }
}
