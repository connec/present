use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{self, Poll},
};

use futures::{Stream, StreamExt};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

const BROADCAST_CAPACITY: usize = 100;

#[derive(Clone, Debug)]
pub enum Event {
    Joined(String),
    Left(String),
}

#[derive(Clone, Default)]
pub struct Presence(Arc<Mutex<PresenceState>>);

impl Presence {
    pub fn join(&self, tag: String, topic: String) -> (BTreeSet<String>, Joiners, Present) {
        let (members, joiners) = self.0.lock().unwrap().insert(&topic, tag.clone());
        (
            members,
            joiners,
            Present {
                topic,
                tag,
                state: self.0.clone(),
            },
        )
    }
}

#[derive(Default)]
struct PresenceState {
    memberships: BTreeMap<String, Membership>,
}

impl PresenceState {
    fn insert(&mut self, topic: &str, tag: String) -> (BTreeSet<String>, Joiners) {
        if self.memberships.contains_key(topic) {
            let membership = self.memberships.get_mut(topic).unwrap();
            membership.insert(tag)
        } else {
            let membership = self.memberships.entry(topic.to_owned()).or_default();
            membership.insert(tag)
        }
    }

    fn remove(&mut self, topic: &str, tag: String) {
        if let Some(membership) = self.memberships.get_mut(topic) {
            membership.remove(tag);
            if membership.is_empty() {
                self.memberships.remove(topic);
            }
        }
    }
}

struct Membership {
    tx: broadcast::Sender<Event>,
    members: BTreeSet<String>,
}

impl Membership {
    fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn insert(&mut self, tag: String) -> (BTreeSet<String>, Joiners) {
        self.members.insert(tag.clone());

        // ignore errors – either there are no listeners or the buffer is full
        let _ = self.tx.send(Event::Joined(tag));

        (self.members.clone(), Joiners::new(self.tx.subscribe()))
    }

    fn remove(&mut self, tag: String) {
        self.members.remove(&tag);

        // ignore errors – either there are no listeners or the buffer is full
        let _ = self.tx.send(Event::Left(tag));
    }
}

impl Default for Membership {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tx,
            members: Default::default(),
        }
    }
}

pub struct Present {
    topic: String,
    tag: String,
    state: Arc<Mutex<PresenceState>>,
}

impl Drop for Present {
    fn drop(&mut self) {
        let tag = mem::take(&mut self.tag);
        self.state.lock().unwrap().remove(&self.topic, tag);
    }
}

pub struct Joiners(BroadcastStream<Event>);

impl Joiners {
    fn new(rx: broadcast::Receiver<Event>) -> Self {
        Self(BroadcastStream::new(rx))
    }
}

impl Stream for Joiners {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        match self.0.poll_next_unpin(cx) {
            Poll::Pending | Poll::Ready(Some(Err(_))) => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(event)),
        }
    }
}
