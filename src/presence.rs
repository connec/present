use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, Weak},
    task::Poll,
};

use futures::{
    stream::{self, FuturesUnordered},
    Stream, StreamExt,
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::{BroadcastStream, UnboundedReceiverStream};
use tracing::trace;

const BROADCAST_BUFFER: usize = 10;

/// The API for the presence internals.
#[derive(Clone, Debug)]
pub struct Presence {
    joins: mpsc::UnboundedSender<Join>,
}

impl Presence {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(handle_joins(tx.clone(), rx));
        Self { joins: tx }
    }

    pub async fn join(
        &self,
        topic: &str,
        tag: &str,
    ) -> Result<(Vec<Arc<str>>, ParticipantEvents), Unavailable> {
        let (tx, rx) = oneshot::channel();
        let join = Join {
            topic: topic.into(),
            tag: tag.into(),
            tx,
        };

        if self.joins.send(join).is_err() {
            // The receiver has closed, meaning we're no longer handling joins – this is an
            // unrecoverable error, but we want to handle it gracefully.
            return Err(Unavailable);
        }

        let mut joined = rx.await.map_err(|_| {
            // the join never completed, so the service is likely unavailable
            Unavailable
        })?;

        Ok((
            std::mem::take(&mut joined.members),
            ParticipantEvents::new(joined),
        ))
    }
}

pub struct Unavailable;

// A stream that disseminates presence events.
#[must_use]
pub struct ParticipantEvents {
    tag: Arc<str>,
    topic_tx: mpsc::UnboundedSender<TopicMsg>,
    msg_rx: BroadcastStream<ParticipantMsg>,
    _alive: Arc<()>,
}

impl Drop for ParticipantEvents {
    fn drop(&mut self) {
        if self
            .topic_tx
            .send(TopicMsg::Leave(self.tag.clone()))
            .is_err()
        {
            tracing::warn!(%self.tag, "topic handler is gone");
        }
    }
}

impl ParticipantEvents {
    fn new(joined: Joined) -> Self {
        Self {
            tag: joined.tag,
            topic_tx: joined.topic_tx,
            msg_rx: BroadcastStream::new(joined.msg_rx),
            _alive: joined.alive,
        }
    }
}

impl Stream for ParticipantEvents {
    type Item = Result<Event, Unavailable>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let item = self
            .msg_rx
            .poll_next_unpin(cx)
            .map_ok(|msg| match msg {
                ParticipantMsg::Join(tag) if tag != self.tag => Some(Event::Join(tag)),
                ParticipantMsg::Leave(tag) if tag != self.tag => Some(Event::Leave(tag)),
                _ => None,
            })
            .map_err(|_| Unavailable)?;

        match item {
            Poll::Ready(Some(None)) => self.poll_next(cx),
            item => item.map(|opt| opt.flatten().map(Ok)),
        }
    }
}

pub enum Event {
    Join(Arc<str>),
    Leave(Arc<str>),
}

struct Joined {
    tag: Arc<str>,
    topic_tx: mpsc::UnboundedSender<TopicMsg>,
    members: Vec<Arc<str>>,
    msg_rx: broadcast::Receiver<ParticipantMsg>,
    alive: Arc<()>,
}

#[derive(Clone, Debug)]
enum ParticipantMsg {
    Join(Arc<str>),
    Leave(Arc<str>),
}

#[derive(Debug)]
enum JoinMsg {
    Join(Join),
    Close(Arc<str>),
}

struct Join {
    topic: Arc<str>,
    tag: Arc<str>,
    tx: oneshot::Sender<Joined>,
}

impl fmt::Debug for Join {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Join")
            .field("topic", &self.topic.as_ref())
            .field("tag", &self.tag.as_ref())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum TopicMsg {
    Ping,
    Join(Join),
    Leave(Arc<str>),
}

struct TopicClose {
    // INVARIANT: must be Some until drop
    topic: Option<Arc<str>>,
    tx: Option<oneshot::Sender<Arc<str>>>,
}

impl Drop for TopicClose {
    fn drop(&mut self) {
        trace!(topic = %self.topic.as_ref().unwrap(), "closed");
        if let Err(topic) = self.tx.take().unwrap().send(self.topic.take().unwrap()) {
            // the join task must be dead, so we're broken
            tracing::warn!(%topic, "couldn't close topic, join handling has closed");
        }
    }
}

// All joins are handled by the same task to simplify state maagement.
#[tracing::instrument(skip_all)]
async fn handle_joins(retry: mpsc::UnboundedSender<Join>, joins: mpsc::UnboundedReceiver<Join>) {
    trace!("started");

    let mut topics = HashMap::new();
    let mut msgs = stream::select(
        UnboundedReceiverStream::new(joins).map(JoinMsg::Join),
        FuturesUnordered::new()
            .filter_map(|res: Result<_, _>| Box::pin(async move { res.ok() }))
            .map(JoinMsg::Close),
    );

    while let Some(msg) = msgs.next().await {
        trace!(?msg);
        match msg {
            JoinMsg::Join(join) => {
                // Manually hash the topic so that `topics` doesn't store a copy of the strings
                let mut hasher = DefaultHasher::new();
                join.topic.hash(&mut hasher);
                let topic_key = hasher.finish();

                let tx = topics.entry(topic_key).or_insert_with(|| {
                    trace!(topic = %join.topic, "new");
                    let (tx, rx) = mpsc::unbounded_channel();
                    let (close_tx, close_rx) = oneshot::channel();

                    msgs.get_ref().1.get_ref().get_ref().push(close_rx);

                    tokio::spawn(handle_topic(
                        join.topic.clone(),
                        tx.clone(),
                        rx,
                        TopicClose {
                            topic: Some(join.topic.clone()),
                            tx: Some(close_tx),
                        },
                    ));
                    tx
                });

                let (s1, s2) = msgs.into_inner();
                msgs = stream::select(s1, s2);

                if let Err(mpsc::error::SendError(TopicMsg::Join(join))) =
                    tx.send(TopicMsg::Join(join))
                {
                    // the topic handler has gone, remove it from the state and retry the join
                    trace!(?join, "retrying due to dropped topic");
                    topics.remove(&topic_key);

                    #[allow(clippy::ok_expect)] // Err is not Debug
                    retry
                        .send(join)
                        .ok()
                        .expect("BUG: joins receiver gone, in loop over joins receiver?");
                }
            }
            JoinMsg::Close(topic) => {
                // check that the sender is still gone
                let mut hasher = DefaultHasher::new();
                topic.hash(&mut hasher);
                let topic_key = hasher.finish();

                if let Some(tx) = topics.get(&topic_key) {
                    if tx.send(TopicMsg::Ping).is_err() {
                        trace!(%topic, "dropped");
                        topics.remove(&topic_key);
                    }
                }
            }
        }
    }
}

// Each topic is handled in its own task.
#[tracing::instrument(skip(topic_tx, topic_rx, _close))]
async fn handle_topic(
    topic: Arc<str>,
    topic_tx: mpsc::UnboundedSender<TopicMsg>,
    mut topic_rx: mpsc::UnboundedReceiver<TopicMsg>,
    _close: TopicClose,
) {
    trace!("started");

    let (participants, _) = broadcast::channel(BROADCAST_BUFFER);
    let mut tags = HashMap::new();

    while let Some(msg) = topic_rx.recv().await {
        trace!(?msg);
        match msg {
            TopicMsg::Ping => {}
            TopicMsg::Join(join) => {
                let (tag, alive) = if let Some(res) = tags
                    .get_key_value(&join.tag)
                    .and_then(|(k, v): (&Arc<str>, &Weak<()>)| Some((k.clone(), v.upgrade()?)))
                {
                    trace!(tag = %join.tag, "exists");
                    res
                } else {
                    trace!(tag = %join.tag, "new");
                    let alive = Arc::new(());
                    tags.insert(join.tag.clone(), Arc::downgrade(&alive));
                    (join.tag, alive)
                };

                if participants
                    .send(ParticipantMsg::Join(tag.clone()))
                    .is_err()
                {
                    // all other participants have gone, but we're adding a new one, so we don't terminate
                    trace!("no participants to notify");
                }

                let joined = Joined {
                    tag: tag.clone(),
                    topic_tx: topic_tx.clone(),
                    members: tags.keys().filter(|t| *t != &tag).cloned().collect(),
                    msg_rx: participants.subscribe(),
                    alive,
                };

                trace!(tag = %tag, "joined");
                if join.tx.send(joined).is_err() {
                    // the participant already disconnected, this will be cleaned up automatically
                    // sine the participant was dropped
                    trace!(tag = %tag, "already dropped");
                }
            }
            TopicMsg::Leave(tag) => {
                if tags.get(&tag).and_then(Weak::upgrade).is_some() {
                    trace!(%tag, "has other connections");
                } else {
                    trace!(%tag, "no more connections");
                    tags.remove(&tag);
                    if participants.send(ParticipantMsg::Leave(tag)).is_err() {
                        // all participants have gone, so the topic no longer needs handling
                        trace!("no participants to notify, terminating");
                        break;
                    }
                }
            }
        }
    }
}
