use std::io::Write;

use crate::{
    bin_ser::BinarySerialize,
    data::{record::Record, record_set_header::RecordSetHeader},
};

pub struct RecordSet {}

impl RecordSet {
    pub fn write_to_buf(records: &[Record], writer: &mut impl Write) -> std::io::Result<()> {
        let header = RecordSetHeader::new();

        // TODO convert to serialize straight into writer
        let mut buf = Vec::new();
        header.serialize(&mut buf);

        writer.write_all(&buf)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::RecordSet;

    #[test]
    fn test_write_to_buf_empty() {
        let mut buf = vec![];
        RecordSet::write_to_buf(&[], &mut buf).expect("write_to_buf failed");

        assert_eq!(buf.len(), 28);
    }
}
