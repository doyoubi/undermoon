// This is a small utils to implement higher kinded types for using functor.
// The following codes are based on:
// - https://gist.github.com/edmundsmith/855fcf0cb35dd467c29a9350481f0ecf
// - https://gist.github.com/burjui/4ec90b395975709b79f999839dcf9674

// Placeholder for any type.
pub struct ForAll;

pub trait Unplug {
    type F; // In the form of Functor<ForAll>
    type A; // Concrete inner type of Functor<A>
}

pub trait Plug<A> {
    type Result;
}

// Functor takes self value.
pub trait VFunctor: Unplug + Plug<<Self as Unplug>::A> {
    fn map<B, F>(self, f: F) -> <Self as Plug<B>>::Result
    where
        Self: Plug<B>,
        F: Fn(<Self as Unplug>::A) -> B + Copy;
}

// Functor takes self reference.
pub trait RFunctor<'a>: Unplug + Plug<<Self as Unplug>::A> {
    fn as_ref(&'a self) -> <Self as Plug<&'a <Self as Unplug>::A>>::Result
    where
        Self: Plug<&'a <Self as Unplug>::A>;
    fn as_mut(&'a mut self) -> <Self as Plug<&'a mut <Self as Unplug>::A>>::Result
    where
        Self: Plug<&'a mut <Self as Unplug>::A>;
    fn map_in_place<F>(&'a mut self, f: F)
    where
        F: Fn(&'a mut <Self as Unplug>::A) + Copy;
}
