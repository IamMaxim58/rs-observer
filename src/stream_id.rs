use std::fmt;
use std::str::FromStr;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub millis: u64,
    pub sequence: u64,
}

impl StreamId {
    pub const ZERO: Self = Self {
        millis: 0,
        sequence: 0,
    };

    pub fn is_after(self, baseline: Self) -> bool {
        self > baseline
    }
}

#[derive(Debug, Error)]
pub enum StreamIdParseError {
    #[error("stream id must have `<millis>-<sequence>` format")]
    InvalidFormat,
    #[error("stream id contains an invalid integer")]
    InvalidInteger,
}

impl FromStr for StreamId {
    type Err = StreamIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (millis, sequence) = value
            .split_once('-')
            .ok_or(StreamIdParseError::InvalidFormat)?;
        if millis.is_empty() || sequence.is_empty() || sequence.contains('-') {
            return Err(StreamIdParseError::InvalidFormat);
        }

        Ok(Self {
            millis: millis
                .parse()
                .map_err(|_| StreamIdParseError::InvalidInteger)?,
            sequence: sequence
                .parse()
                .map_err(|_| StreamIdParseError::InvalidInteger)?,
        })
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.millis, self.sequence)
    }
}
