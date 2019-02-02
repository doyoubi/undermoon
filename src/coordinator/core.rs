use std::io;
use std::fmt;
use std::error::Error;
use futures::{future, Future};

pub struct ProxyFailure {
    proxy_address: String,
    report_id: String,
}

pub trait FailureChecker {
    fn check(&self, address: String) -> Box<dyn Future<Item = Option<ProxyFailure>, Error = CheckError> + Send>;
}

pub trait FailureReporter {
    fn report(&self, failure: ProxyFailure) -> Box<dyn Future<Item = Option<ProxyFailure>, Error = ReportError> + Send>;
}

pub trait FailureHandler {
    fn handle_failure(&self, failure: ProxyFailure) -> Box<dyn Future<Item = (), Error = FailureHandlerError> + Send>;
}

#[derive(Debug)]
pub enum ReportError {
    Io(io::Error),
    Rejected,
}

impl fmt::Display for ReportError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ReportError {
    fn description(&self) -> &str {
        "report error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            ReportError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum CheckError {
    Io(io::Error),
    InvalidReply,
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for CheckError {
    fn description(&self) -> &str {
        "check error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            CheckError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum FailureHandlerError {
    Io(io::Error),
    InvalidReply,
}

impl fmt::Display for FailureHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for FailureHandlerError {
    fn description(&self) -> &str {
        "failure handler error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            FailureHandlerError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyChecker {}

    impl FailureChecker for DummyChecker {
        fn check(&self, address: String) -> Box<dyn Future<Item = Option<ProxyFailure>, Error = CheckError> + Send> {
            Box::new(future::ok(None))
        }
    }

    fn check<C: FailureChecker>(checker: C) {
        checker.check("".to_string()).wait();
    }

    #[test]
    fn test_reporter() {
        let checker = DummyChecker{};
        check(checker);
    }
}

