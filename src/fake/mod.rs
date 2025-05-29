use bytes::Bytes;
use fake::{Dummy, Fake, Faker};

pub struct BytesFake;

impl Dummy<BytesFake> for Bytes {
    fn dummy_with_rng<R: fake::Rng + ?Sized>(_config: &BytesFake, rng: &mut R) -> Self {
        let len: usize = (10..50).fake_with_rng(rng);
        let data: Vec<u8> = (0..len).map(|_| Faker.fake_with_rng(rng)).collect();

        data.into()
    }
}
