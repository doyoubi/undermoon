use std::vec::Vec;

pub type BinSafeStr = Vec<u8>;

#[derive(Debug, PartialEq, Clone)]
pub enum BulkStr {
    Str(BinSafeStr),
    Nil,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Array {
    Arr(Vec<Resp>),
    Nil,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Resp {
    Error(BinSafeStr),
    Simple(BinSafeStr),
    Bulk(BulkStr),
    Integer(BinSafeStr),
    Arr(Array),
}