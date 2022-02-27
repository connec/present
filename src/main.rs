mod header;
mod presence;

use std::collections::BTreeSet;

use axum::{
    extract::{ws::WebSocket, Extension, TypedHeader, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    AddExtensionLayer, Router,
};
use futures::{SinkExt, StreamExt};
use tower_http::trace::TraceLayer;
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
        .layer(AddExtensionLayer::new(Presence::default()))
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
enum Message {
    Members(BTreeSet<String>),
    Joined(String),
    Left(String),
}

impl From<presence::Event> for Message {
    fn from(event: presence::Event) -> Self {
        match event {
            presence::Event::Joined(tag) => Self::Joined(tag),
            presence::Event::Left(tag) => Self::Left(tag),
        }
    }
}

impl From<Message> for axum::extract::ws::Message {
    fn from(msg: Message) -> Self {
        Self::Text(serde_json::to_string(&msg).expect("BUG: unserializable message"))
    }
}

async fn connection(presence: Presence, tag: String, topic: String, socket: WebSocket) {
    let (mut tx, mut rx) = socket.split();

    let (members, mut joiners, present) = presence.join(tag, topic);

    tokio::spawn(async move {
        let _present = present;
        while rx.next().await.is_some() {}
    });

    if tx.send(Message::Members(members).into()).await.is_err() {
        // client has already gone
        return;
    }

    while let Some(event) = joiners.next().await {
        if tx.send(Message::from(event).into()).await.is_err() {
            // client has gone
            return;
        }
    }
}
