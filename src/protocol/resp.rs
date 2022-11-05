use super::encoder::get_resp_size_hint;
use super::fp::Functor;
use super::packet::PacketSizeHint;
use bytes::Bytes;
use std::ops::Range;
use std::vec::Vec;

pub type BinSafeStr = Vec<u8>;

pub type BulkStrVec = BulkStr<BinSafeStr>;
pub type ArrayVec = Array<BinSafeStr>;
pub type RespVec = Resp<BinSafeStr>;

pub type BulkStrBytes = BulkStr<Bytes>;
pub type ArrayBytes = Array<Bytes>;
pub type RespBytes = Resp<Bytes>;

pub type BulkStrSlice<'a> = BulkStr<&'a [u8]>;
pub type ArraySlice<'a> = Array<&'a [u8]>;
pub type RespSlice<'a> = Resp<&'a [u8]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataIndex(pub usize, pub usize);

pub type BulkStrIndex = BulkStr<DataIndex>;
pub type ArrayIndex = Array<DataIndex>;
pub type RespIndex = Resp<DataIndex>;

#[derive(Debug, Clone)]
pub struct IndexedResp {
    resp: RespIndex,
    data: Bytes,
}

impl IndexedResp {
    pub fn new(resp: RespIndex, data: Bytes) -> Self {
        Self { resp, data }
    }

    pub fn get_array_element(&self, index: usize) -> Option<&[u8]> {
        match self.resp {
            RespIndex::Arr(ArrayIndex::Arr(ref resps)) => {
                resps.get(index).and_then(|resp| match resp {
                    RespIndex::Bulk(BulkStrIndex::Str(s)) => Some(
                        self.data
                            .get(s.to_range())
                            .expect("IndexedResp::get_array_element"),
                    ),
                    _ => None,
                })
            }
            _ => None,
        }
    }

    pub fn get_array_len(&self) -> Option<usize> {
        match self.resp {
            RespIndex::Arr(ArrayIndex::Arr(ref resps)) => Some(resps.len()),
            _ => None,
        }
    }

    pub fn to_resp_slice(&self) -> RespSlice {
        self.resp.map_to_slice(&self.data)
    }

    pub fn to_resp_vec(&self) -> RespVec {
        self.resp.as_ref().map(|DataIndex(s, e)| {
            self.data
                .get(*s..*e)
                .expect("IndexedResp::to_resp_vec")
                .to_vec()
        })
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BulkStr<T> {
    Str(T),
    Nil,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Array<T> {
    Arr(Vec<Resp<T>>),
    Nil,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Resp<T> {
    Error(T),
    Simple(T),
    Bulk(BulkStr<T>),
    Integer(T),
    Arr(Array<T>),
}

impl<T> Functor for BulkStr<T> {
    type A = T;
    type Wrap<A> = BulkStr<A>;

    fn map<B, F>(self, f: F) -> Self::Wrap<B>
    where
        F: Fn(Self::A) -> B + Copy,
    {
        match self {
            Self::Str(t) => BulkStr::Str(f(t)),
            Self::Nil => BulkStr::Nil,
        }
    }

    fn as_ref(&self) -> Self::Wrap<&Self::A> {
        match *self {
            Self::Str(ref t) => BulkStr::Str(t),
            Self::Nil => BulkStr::Nil,
        }
    }

    fn as_mut(&mut self) -> Self::Wrap<&mut Self::A> {
        match *self {
            Self::Str(ref mut t) => BulkStr::Str(t),
            Self::Nil => BulkStr::Nil,
        }
    }

    fn map_in_place<F>(&mut self, f: F)
    where
        F: Fn(&mut Self::A) + Copy,
    {
        match *self {
            Self::Str(ref mut t) => f(t),
            Self::Nil => (),
        }
    }
}

impl<T> BulkStr<T> {
    pub fn map_str<U, F: FnOnce(T) -> U>(self, f: F) -> Option<U> {
        match self {
            Self::Str(t) => Some(f(t)),
            Self::Nil => None,
        }
    }
}

impl<T> Functor for Array<T> {
    type A = T;
    type Wrap<A> = Array<A>;

    fn map<B, F>(self, f: F) -> Self::Wrap<B>
    where
        F: Fn(Self::A) -> B + Copy,
    {
        match self {
            Self::Arr(t) => Array::Arr(t.into_iter().map(move |e| e.map(f)).collect()),
            Self::Nil => Array::Nil,
        }
    }

    fn as_ref(&self) -> Self::Wrap<&Self::A> {
        match *self {
            Self::Arr(ref t) => Array::Arr(t.iter().map(|e| e.as_ref()).collect()),
            Self::Nil => Array::Nil,
        }
    }

    fn as_mut(&mut self) -> Self::Wrap<&mut Self::A> {
        match *self {
            Self::Arr(ref mut t) => Array::Arr(t.iter_mut().map(|e| e.as_mut()).collect()),
            Self::Nil => Array::Nil,
        }
    }

    fn map_in_place<F>(&mut self, f: F)
    where
        F: Fn(&mut Self::A) + Copy,
    {
        match *self {
            Self::Arr(ref mut arr) => {
                for resp in arr.iter_mut() {
                    resp.map_in_place(f)
                }
            }
            Self::Nil => (),
        }
    }
}

impl<T> Functor for Resp<T> {
    type A = T;
    type Wrap<A> = Resp<A>;

    fn map<B, F>(self, f: F) -> Self::Wrap<B>
    where
        F: Fn(Self::A) -> B + Copy,
    {
        match self {
            Self::Error(t) => Resp::Error(f(t)),
            Self::Simple(t) => Resp::Simple(f(t)),
            Self::Bulk(bulk_str) => Resp::Bulk(bulk_str.map(f)),
            Self::Integer(t) => Resp::Integer(f(t)),
            Self::Arr(arr) => Resp::Arr(arr.map(f)),
        }
    }

    fn as_ref(&self) -> Self::Wrap<&Self::A> {
        match *self {
            Self::Error(ref t) => Resp::Error(t),
            Self::Simple(ref t) => Resp::Simple(t),
            Self::Bulk(ref bulk_str) => Resp::Bulk(bulk_str.as_ref()),
            Self::Integer(ref t) => Resp::Integer(t),
            Self::Arr(ref arr) => Resp::Arr(arr.as_ref()),
        }
    }

    fn as_mut(&mut self) -> Self::Wrap<&mut Self::A> {
        match *self {
            Self::Error(ref mut t) => Resp::Error(t),
            Self::Simple(ref mut t) => Resp::Simple(t),
            Self::Bulk(ref mut bulk_str) => Resp::Bulk(bulk_str.as_mut()),
            Self::Integer(ref mut t) => Resp::Integer(t),
            Self::Arr(ref mut arr) => Resp::Arr(arr.as_mut()),
        }
    }

    fn map_in_place<F>(&mut self, f: F)
    where
        F: Fn(&mut Self::A) + Copy,
    {
        match *self {
            Self::Error(ref mut t) => f(t),
            Self::Simple(ref mut t) => f(t),
            Self::Bulk(ref mut bulk_str) => bulk_str.map_in_place(f),
            Self::Integer(ref mut t) => f(t),
            Self::Arr(ref mut arr) => arr.map_in_place(f),
        }
    }
}

impl DataIndex {
    pub fn map<F: FnOnce(usize) -> usize + Copy>(self, f: F) -> Self {
        let DataIndex(s, e) = self;
        Self(f(s), f(e))
    }

    pub fn advance(&mut self, count: usize) {
        self.0 += count;
        self.1 += count;
    }

    pub fn to_range(&self) -> Range<usize> {
        self.0..self.1
    }
}

impl BulkStrIndex {
    pub fn try_to_range(&self) -> Option<Range<usize>> {
        self.as_ref().map_str(|DataIndex(s, e)| *s..*e)
    }
}

impl ArrayIndex {
    pub fn map_to_slice<'a>(&self, data: &'a [u8]) -> ArraySlice<'a> {
        self.as_ref()
            .map(|DataIndex(s, e)| data.get(*s..*e).expect("ArrayIndex::map_to_slice"))
    }
}

impl RespIndex {
    pub fn map_to_slice<'a>(&self, data: &'a [u8]) -> RespSlice<'a> {
        self.as_ref()
            .map(|DataIndex(s, e)| data.get(*s..*e).expect("RespIndex::map_to_slice"))
    }
}

impl PacketSizeHint for RespVec {
    fn get_size_hint(&self) -> Option<usize> {
        match get_resp_size_hint(self) {
            Ok(n) => Some(n),
            Err(_) => {
                error!("FATAL: failed to get size hint");
                None
            }
        }
    }
}

pub trait AdvanceIndex: Functor<A = DataIndex> {
    fn advance(&mut self, count: usize);
}

impl<T> AdvanceIndex for T
where
    T: Functor<A = DataIndex>,
{
    fn advance(&mut self, count: usize) {
        self.map_in_place(move |DataIndex(s, e)| {
            *s += count;
            *e += count;
        })
    }
}
