use std::collections::BTreeMap;

#[derive(Default)]
pub struct Index(BTreeMap<u64, u64>);

impl Index {
    pub fn insert(&mut self, offset: u64, byte_offset: u64) {
        assert!(self.0.last_entry().is_none_or(|v| offset > *v.key()));

        self.0.insert(offset, byte_offset);
    }

    pub fn get(&mut self, offset: u64) -> Option<&u64> {
        self.0.get(&offset)
    }
}

#[cfg(test)]
mod test {
    use crate::dur::index::Index;

    #[test]
    fn insert_and_get() {
        let mut index = Index::default();

        index.insert(0, 1);
        index.insert(1, 2);

        assert_eq!(index.get(0), Some(&1));
        assert_eq!(index.get(1), Some(&2));
    }

    #[test]
    #[should_panic]
    fn can_only_insert_at_the_end() {
        let mut index = Index::default();

        index.insert(10, 10);
        index.insert(0, 0);
    }
}
