#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// crossbeam-skiplist is lockfree datastruct
/// so it is safe to use in a multi-threaded environment
/// and don't worry about the lock
pub struct MemTable {
    /// bytes::Byte is similar to Arc<[u8]>
    /// clone the Bytes, or get a slice of Bytes, the underlying data will not be copied
    map: Arc<SkipMap<Bytes, Bytes>>,

    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    pub fn create(_id: usize) -> Self {
        Self {
            id: _id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        self.map.get(_key).map(|e| e.value().clone())
    }

    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let key_and_value_size = _key.len() + _value.len();
        self.map
            .insert(Bytes::copy_from_slice(_key), Bytes::copy_from_slice(_value));
        self.approximate_size
            .fetch_add(key_and_value_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        let (lower, upper) = (map_bound(_lower), map_bound(_upper));

        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();

        let entry = iter.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        iter.with_mut(|x| *x.item = entry);
        iter
    }

    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        unimplemented!()
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(&self.borrow_item().0[..])
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_get() {
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
        assert_eq!(
            &memtable.for_testing_get_slice(b"key1").unwrap()[..],
            b"value1"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key2").unwrap()[..],
            b"value2"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key3").unwrap()[..],
            b"value3"
        );
    }

    #[test]
    fn test_memtable_overwrite() {
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
        memtable.for_testing_put_slice(b"key1", b"value11").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value22").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value33").unwrap();
        assert_eq!(
            &memtable.for_testing_get_slice(b"key1").unwrap()[..],
            b"value11"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key2").unwrap()[..],
            b"value22"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key3").unwrap()[..],
            b"value33"
        );
    }

    #[test]
    fn test_memtable_iter() {
        use std::ops::Bound;
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").unwrap();

        {
            let mut iter = memtable.for_testing_scan_slice(Bound::Unbounded, Bound::Unbounded);
            assert_eq!(iter.key().for_testing_key_ref(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key().for_testing_key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key().for_testing_key_ref(), b"key3");
            assert_eq!(iter.value(), b"value3");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter =
                memtable.for_testing_scan_slice(Bound::Included(b"key1"), Bound::Included(b"key2"));
            assert_eq!(iter.key().for_testing_key_ref(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert_eq!(iter.key().for_testing_key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter =
                memtable.for_testing_scan_slice(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
            assert_eq!(iter.key().for_testing_key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter.next().unwrap();
            assert!(!iter.is_valid());
        }
    }

    #[test]
    fn test_empty_memtable_iter() {
        use std::ops::Bound;
        let memtable = MemTable::create(0);
        {
            let iter =
                memtable.for_testing_scan_slice(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
            assert!(!iter.is_valid());
        }
        {
            let iter =
                memtable.for_testing_scan_slice(Bound::Included(b"key1"), Bound::Included(b"key2"));
            assert!(!iter.is_valid());
        }
        {
            let iter = memtable.for_testing_scan_slice(Bound::Unbounded, Bound::Unbounded);
            assert!(!iter.is_valid());
        }
    }
}
