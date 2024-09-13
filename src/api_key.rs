use strum_macros::FromRepr;

#[derive(FromRepr, Debug, PartialEq)]
#[repr(i16)]
pub enum ApiKey {
    CreatePartition = 1,
}
