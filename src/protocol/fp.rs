pub trait Functor {
    type A; // Concrete inner type of Wrap<A>
    type Wrap<T>: Functor; // In the form of Wrap<A>

    fn map<B, F>(self, f: F) -> Self::Wrap<B>
    where
        F: Fn(Self::A) -> B + Copy;
    fn as_ref(&self) -> Self::Wrap<&Self::A>;
    fn as_mut(&mut self) -> Self::Wrap<&mut Self::A>;
    fn map_in_place<F>(&mut self, f: F)
    where
        F: Fn(&mut Self::A) + Copy;
}
