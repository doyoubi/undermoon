use std::vec::Vec;
use std::boxed::Box;

pub type BinSafeStr = Vec<u8>;

#[derive(Debug, PartialEq)]
pub enum BulkStr {
    Str(BinSafeStr),
    Nil,
}

pub enum Array {
    Arr(Vec<Resp>),
    Nil,
}

pub enum Resp {
    Error(BinSafeStr),
    Simple(BinSafeStr),
    Bulk(BulkStr),
    Integer(BinSafeStr),
    Arr(Array),
}