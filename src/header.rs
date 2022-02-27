use headers::Header;
use once_cell::sync::Lazy;

static HEADER_TAG: Lazy<headers::HeaderName> =
    Lazy::new(|| headers::HeaderName::from_static("presents-tag"));
static HEADER_TOPIC: Lazy<headers::HeaderName> =
    Lazy::new(|| headers::HeaderName::from_static("presents-topic"));

pub struct Tag(pub String);

impl Header for Tag {
    fn name() -> &'static headers::HeaderName {
        &HEADER_TAG
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i headers::HeaderValue>,
    {
        // take the last tag only
        // decode wouldn't be called with no values, so unwrap is OK
        let value = values.last().unwrap();
        value
            .to_str()
            .map_err(|_| headers::Error::invalid())
            .map(|value| Self(value.to_owned()))
    }

    fn encode<E: Extend<headers::HeaderValue>>(&self, values: &mut E) {
        // we only construct `Tag` from headers, so unwrap is OK
        values.extend(Some((&self.0).try_into().unwrap()))
    }
}

pub struct Topic(pub String);

impl Header for Topic {
    fn name() -> &'static headers::HeaderName {
        &HEADER_TOPIC
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i headers::HeaderValue>,
    {
        // take the last topic only
        // decode wouldn't be called with no values, so unwrap is OK
        let value = values.last().unwrap();
        value
            .to_str()
            .map_err(|_| headers::Error::invalid())
            .map(|value| Self(value.to_owned()))
    }

    fn encode<E: Extend<headers::HeaderValue>>(&self, values: &mut E) {
        // we only construct `Tag` from headers, so unwrap is OK
        values.extend(Some((&self.0).try_into().unwrap()))
    }
}
