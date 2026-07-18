use crate::sstable::sstable_builder::SSTableBuilder;

mod error;
mod memtable;
mod model;
mod sstable;
mod wal;

fn main() {
    let mut builder = SSTableBuilder::new("1.sst");
    builder
        .build(
            vec![
                (b"key1".to_vec(), Some(b"value1".to_vec())),
                (b"key2".to_vec(), None),
                (b"key3".to_vec(), Some(b"value3".to_vec())),
            ]
            .into_iter(),
        )
        .unwrap();
}
