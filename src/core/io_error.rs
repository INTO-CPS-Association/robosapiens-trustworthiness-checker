use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IoErrorKind {
    Closed,
    Backend,
    Source,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorDetails {
    kind: IoErrorKind,
    message: Option<String>,
    causes: Vec<String>,
    cleanup: Vec<String>,
}

impl ErrorDetails {
    pub const fn closed() -> Self {
        Self {
            kind: IoErrorKind::Closed,
            message: None,
            causes: Vec::new(),
            cleanup: Vec::new(),
        }
    }
    pub fn new(kind: IoErrorKind, message: impl fmt::Display) -> Self {
        Self {
            kind,
            message: Some(message.to_string()),
            causes: Vec::new(),
            cleanup: Vec::new(),
        }
    }
    pub fn from_anyhow(kind: IoErrorKind, error: anyhow::Error) -> Self {
        let mut chain = error.chain();
        let message = chain.next().map(ToString::to_string);
        let causes = chain.map(ToString::to_string).collect();
        Self {
            kind,
            message,
            causes,
            cleanup: Vec::new(),
        }
    }
    pub fn kind(&self) -> IoErrorKind {
        self.kind
    }
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
    pub fn cleanup_causes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.cleanup.iter().map(String::as_str)
    }
    pub fn causes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.causes.iter().map(String::as_str)
    }
    pub fn with_cleanup(mut self, cleanup: impl fmt::Display) -> Self {
        self.cleanup.push(cleanup.to_string());
        self
    }
    pub fn context(mut self, context: impl fmt::Display) -> Self {
        if let Some(message) = self.message.replace(context.to_string()) {
            self.causes.insert(0, message);
        }
        self
    }
    pub(crate) fn fmt_direction(&self, direction: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.kind, self.message()) {
            (IoErrorKind::Closed, _) => write!(f, "{direction} is closed")?,
            (IoErrorKind::Backend, Some(message)) => {
                write!(f, "{direction} backend error: {message}")?
            }
            (IoErrorKind::Source, Some(message)) => {
                write!(f, "{direction} source error: {message}")?
            }
            (IoErrorKind::Invalid, Some(message)) => write!(f, "invalid {direction}: {message}")?,
            (_, None) => write!(f, "{direction} operation failed")?,
        }
        for cleanup in &self.cleanup {
            write!(f, "; additionally: {cleanup}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputError(ErrorDetails);
impl InputError {
    pub const fn closed() -> Self {
        Self(ErrorDetails::closed())
    }
    pub fn backend(error: impl fmt::Display) -> Self {
        Self(ErrorDetails::new(IoErrorKind::Backend, error))
    }
    pub fn source(error: impl fmt::Display) -> Self {
        Self(ErrorDetails::new(IoErrorKind::Source, error))
    }
    pub fn invalid(error: impl fmt::Display) -> Self {
        Self(ErrorDetails::new(IoErrorKind::Invalid, error))
    }
    pub fn kind(&self) -> IoErrorKind {
        self.0.kind()
    }
    pub fn message(&self) -> Option<&str> {
        self.0.message()
    }
    pub fn details(&self) -> &ErrorDetails {
        &self.0
    }
    pub fn causes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.causes()
    }
    pub fn cleanup_causes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.cleanup_causes()
    }
    pub fn is_closed(&self) -> bool {
        self.kind() == IoErrorKind::Closed
    }
    pub fn is_backend(&self) -> bool {
        self.kind() == IoErrorKind::Backend
    }
    pub fn is_source(&self) -> bool {
        self.kind() == IoErrorKind::Source
    }
    pub fn is_invalid(&self) -> bool {
        self.kind() == IoErrorKind::Invalid
    }
    pub fn with_cleanup(self, cleanup: impl fmt::Display) -> Self {
        Self(self.0.with_cleanup(cleanup))
    }
    pub fn context(self, context: impl fmt::Display) -> Self {
        Self(self.0.context(context))
    }
}
impl fmt::Display for InputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_direction("input", f)
    }
}
impl std::error::Error for InputError {}
impl From<anyhow::Error> for InputError {
    fn from(error: anyhow::Error) -> Self {
        if error.chain().count() == 1 && error.is::<Self>() {
            return error.downcast::<Self>().expect("checked input error type");
        }
        Self(ErrorDetails::from_anyhow(IoErrorKind::Source, error))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputError(ErrorDetails);
impl OutputError {
    pub const fn closed() -> Self {
        Self(ErrorDetails::closed())
    }
    pub fn backend(error: impl fmt::Display) -> Self {
        Self(ErrorDetails::new(IoErrorKind::Backend, error))
    }
    pub fn source(error: impl fmt::Display) -> Self {
        Self(ErrorDetails::new(IoErrorKind::Source, error))
    }
    pub fn invalid(error: impl fmt::Display) -> Self {
        Self(ErrorDetails::new(IoErrorKind::Invalid, error))
    }
    pub fn kind(&self) -> IoErrorKind {
        self.0.kind()
    }
    pub fn message(&self) -> Option<&str> {
        self.0.message()
    }
    pub fn details(&self) -> &ErrorDetails {
        &self.0
    }
    pub fn causes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.causes()
    }
    pub fn cleanup_causes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.cleanup_causes()
    }
    pub fn is_closed(&self) -> bool {
        self.kind() == IoErrorKind::Closed
    }
    pub fn is_backend(&self) -> bool {
        self.kind() == IoErrorKind::Backend
    }
    pub fn is_source(&self) -> bool {
        self.kind() == IoErrorKind::Source
    }
    pub fn is_invalid(&self) -> bool {
        self.kind() == IoErrorKind::Invalid
    }
    pub fn with_cleanup(self, cleanup: impl fmt::Display) -> Self {
        Self(self.0.with_cleanup(cleanup))
    }
    pub fn context(self, context: impl fmt::Display) -> Self {
        Self(self.0.context(context))
    }
}
impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_direction("output", f)
    }
}
impl std::error::Error for OutputError {}
impl From<anyhow::Error> for OutputError {
    fn from(error: anyhow::Error) -> Self {
        if error.chain().count() == 1 && error.is::<Self>() {
            return error.downcast::<Self>().expect("checked output error type");
        }
        Self(ErrorDetails::from_anyhow(IoErrorKind::Invalid, error))
    }
}
impl From<String> for OutputError {
    fn from(error: String) -> Self {
        Self::invalid(error)
    }
}
impl From<&str> for OutputError {
    fn from(error: &str) -> Self {
        Self::invalid(error)
    }
}
