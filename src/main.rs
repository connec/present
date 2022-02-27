mod header;
mod presence;

use std::sync::Arc;

use axum::{
    extract::{ws::WebSocket, Extension, TypedHeader, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    AddExtensionLayer, Router,
};
use futures::{stream, SinkExt, StreamExt, TryStreamExt};
use tokio::sync::oneshot;
use tower_http::trace::TraceLayer;
use tracing::trace;
use tracing_subscriber::EnvFilter;

use self::{
    header::{Tag, Topic},
    presence::Presence,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let app = Router::new()
        .route("/", get(connect))
        .layer(AddExtensionLayer::new(Presence::new()))
        .layer(TraceLayer::new_for_http());
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn connect(
    Extension(presence): Extension<Presence>,
    TypedHeader(Tag(tag)): TypedHeader<Tag>,
    TypedHeader(Topic(topic)): TypedHeader<Topic>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move { connection(presence, tag, topic, socket).await })
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Members { members: Vec<Arc<str>> },
    Join { tag: Arc<str> },
    Leave { tag: Arc<str> },
}

impl From<Message> for axum::extract::ws::Message {
    fn from(msg: Message) -> Self {
        Self::Text(serde_json::to_string(&msg).expect("BUG: unserializable message"))
    }
}

enum Event {
    Closed,
    Event(presence::Event),
}

async fn connection(presence: Presence, tag: String, topic: String, socket: WebSocket) {
    let (members, events) = if let Ok(res) = presence.join(&topic, &tag).await {
        res
    } else {
        return;
    };

    let (mut socket_tx, mut socket_rx) = socket.split();
    let (close_tx, close_rx) = oneshot::channel::<()>();
    let mut events = stream::select(
        events.map_ok(Event::Event),
        stream::once(close_rx).map(|_| Ok(Event::Closed)),
    );

    tokio::spawn(async move {
        // drain any incoming messages, when the stream ends the close_tx is dropped, which will end
        // the read loop
        let _close_tx = close_tx;
        while socket_rx.next().await.is_some() {}
    });

    socket_tx
        .send(Message::Members { members }.into())
        .await
        .ok();

    while let Some(Ok(event)) = events.next().await {
        match event {
            Event::Event(presence::Event::Join(tag)) => {
                socket_tx.send(Message::Join { tag }.into()).await.ok();
            }
            Event::Event(presence::Event::Leave(tag)) => {
                socket_tx.send(Message::Leave { tag }.into()).await.ok();
            }
            Event::Closed => break,
        }
    }

    trace!("connection closed");
}
