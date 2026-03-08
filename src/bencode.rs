pub struct Bencoding {}

pub trait Bencodable {
    fn bencode(&self) -> Bencoding;
    fn bdecode(encoding: Bencoding) -> Option<Self>
    where
        Self: Sized;
}

// impl Bencodable for Vec<u32> {
//     fn bencode(&self) -> Bencoding {
//         Bencoding {}
//     }

//     fn bdecode(_: Bencoding) -> Option<Self> {
//         None
//     }
// }
