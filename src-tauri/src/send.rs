use std::{
    path::{Component, Path},
    time::Duration,
};

use anyhow::{Context, anyhow};
use async_walkdir::WalkDir;
use futures_util::{StreamExt, stream::FuturesUnordered};
use iroh::{protocol::ProtocolHandler, Endpoint, Watcher};
use iroh_blobs::{
    BlobFormat, BlobsProtocol,
    api::blobs::{AddPathOptions, AddProgressItem, ImportMode},
    format::collection::Collection,
    provider::{
        self,
        events::{ConnectMode, EventMask, EventSender, ProviderMessage, RequestUpdate},
    },
    store::fs::FsStore,
    ticket::BlobTicket,
};
use itertools::Itertools;
use rand::Rng;
use tauri::{AppHandle, Emitter, Manager};
use tauri_plugin_dialog::DialogExt;
use tokio::{fs::canonicalize, sync::{mpsc, oneshot}, task::JoinHandle};
use tracing::trace;

use crate::coupon::{CouponMachineConfig, provide_coupon};

struct AbortOnDropHandle<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn per_request_progress(
    app: AppHandle,
    connection_id: u64,
    request_id: u64,
    mut rx: irpc::channel::mpsc::Receiver<RequestUpdate>,
) {
    while let Ok(Some(msg)) = rx.recv().await {
        match msg {
            RequestUpdate::Started(msg) => {
                _ = app.emit("request-start", (connection_id, request_id, msg.size));
            }
            RequestUpdate::Progress(msg) => {
                _ = app.emit("request-progress", (connection_id, request_id, msg.end_offset));
            }
            RequestUpdate::Completed(_) => {
                _ = app.emit("request-complete", (connection_id, request_id));
            }
            RequestUpdate::Aborted(_) => {
                _ = app.emit("request-abort", (connection_id, request_id));
            }
        }
    }
}

async fn show_provide_progress(
    app: AppHandle,
    mut recv: mpsc::Receiver<ProviderMessage>,
) -> anyhow::Result<()> {
    let mut tasks = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            item = recv.recv() => {
                let Some(item) = item else {
                    break;
                };

                trace!("got event {item:?}");
                if let ProviderMessage::GetRequestReceivedNotify(msg) = item {
                    let request_id = msg.request_id;
                    let connection_id = msg.connection_id;
                    let app = app.clone();
                    tasks.push(per_request_progress(app, connection_id, request_id, msg.rx));
                }
            }
            Some(_) = tasks.next(), if !tasks.is_empty() => {}
        }
    }
    while tasks.next().await.is_some() {}
    Ok(())
}

#[tauri::command]
pub async fn send_file(app: AppHandle) -> Result<(), String> {
    let (send, recv) = oneshot::channel();
    app.dialog().file().pick_file(|root| {
        _ = send.send(root);
    });
    let path = recv
        .await
        .map_err(|e| e.to_string())?
        .ok_or("Send cancelled!".to_string())?
        .into_path()
        .map_err(|e| e.to_string())?;
    send_inner(app, &path).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn send_folder(app: AppHandle) -> Result<(), String> {
    let (send, recv) = oneshot::channel();
    app.dialog().file().pick_folder(|root| {
        _ = send.send(root);
    });
    let path = recv
        .await
        .map_err(|e| e.to_string())?
        .ok_or("Send cancelled!".to_string())?
        .into_path()
        .map_err(|e| e.to_string())?;
    send_inner(app, &path).await.map_err(|e| e.to_string())
}

async fn send_inner(app: AppHandle, path: &Path) -> anyhow::Result<()> {
    let endpoint = Endpoint::builder()
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .bind()
        .await?;

    let suffix = rand::thread_rng().r#gen::<[u8; 16]>();
    let cwd = canonicalize(app.path().temp_dir()?).await?;
    let mut blobs_data_dir = cwd.join(format!(".halfcopy-send-{}", hex::encode(suffix)));
    while blobs_data_dir.exists() {
        let suffix = rand::thread_rng().r#gen::<[u8; 16]>();
        blobs_data_dir = cwd.join(format!(".halfcopy-send-{}", hex::encode(suffix)));
    }

    let canonicalized = canonicalize(path).await?;
    if canonicalized == cwd {
        return Err(anyhow!("can not share from the current directory"));
    }

    let (progress_tx, progress_rx) = mpsc::channel(32);

    let store = FsStore::load(&blobs_data_dir).await?;
    let blobs = BlobsProtocol::new(
        &store,
        endpoint.clone(),
        Some(EventSender::new(
            progress_tx,
            EventMask {
                connected: ConnectMode::Notify,
                get: provider::events::RequestMode::NotifyLog,
                ..EventMask::DEFAULT
            },
        )),
    );

    let mut name_and_tags = vec![];

    let root = canonicalized
        .parent()
        .context("Shared path has no parent! Are you trying to share root?")?;
    if canonicalized.is_file() {
        let relative = canonicalized.strip_prefix(root)?;
        if relative.to_str().is_none() {
            anyhow::bail!("Path {} is invalid for sharing!", relative.display());
        }
        let name = relative
            .components()
            .filter_map(|component| {
                if let Component::Normal(component) = component {
                    component.to_str()
                } else {
                    None
                }
            })
            .join("/");
        let import = store.add_path_with_opts(AddPathOptions {
            path: canonicalized.clone(),
            mode: ImportMode::TryReference,
            format: BlobFormat::Raw,
        });
        let mut stream = import.stream().await;
        let mut item_size = 0;
        let temp_tag = loop {
            let item = stream
                .next()
                .await
                .context("import stream ended without a tag")?;
            trace!("importing {} {item:?}", relative.display());
            match item {
                AddProgressItem::Size(size) => {
                    item_size = size;
                }
                AddProgressItem::Error(cause) => {
                    anyhow::bail!("error importing {}: {}", relative.display(), cause);
                }
                AddProgressItem::Done(tt) => {
                    break tt;
                }
                _ => {}
            }
        };
        name_and_tags.push((name, temp_tag, item_size));
    } else {
        let mut data_sources = vec![];
        let mut entries = WalkDir::new(&canonicalized);
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            if entry.file_type().await?.is_file() {
                data_sources.push(entry.path());
            }
        }

        app.emit("import-start", data_sources.len())?;
        for (i, path) in data_sources.into_iter().enumerate() {
            app.emit("import-progress", i)?;
            let relative = path.strip_prefix(root)?;
            if relative.to_str().is_none() {
                anyhow::bail!("Path {} is invalid for sharing!", relative.display());
            }
            let name = relative
                .components()
                .filter_map(|component| {
                    if let Component::Normal(component) = component {
                        component.to_str()
                    } else {
                        None
                    }
                })
                .join("/");
            let import = store.add_path_with_opts(AddPathOptions {
                path: path.clone(),
                mode: ImportMode::TryReference,
                format: BlobFormat::Raw,
            });
            let mut stream = import.stream().await;
            let mut item_size = 0;
            let temp_tag = loop {
                let item = stream
                    .next()
                    .await
                    .context("import stream ended without a tag")?;
                trace!("importing {} {item:?}", relative.display());
                match item {
                    AddProgressItem::Size(size) => {
                        item_size = size;
                    }
                    AddProgressItem::Error(cause) => {
                        anyhow::bail!("error importing {}: {}", relative.display(), cause);
                    }
                    AddProgressItem::Done(tt) => {
                        break tt;
                    }
                    _ => {}
                }
            };
            name_and_tags.push((name, temp_tag, item_size));
        }
    }
    name_and_tags.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));

    let (collection, tags) = name_and_tags
        .into_iter()
        .map(|(name, tag, _)| ((name, *tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let temp_tag = collection.clone().store(&store).await?;
    drop(tags);

    let progress = AbortOnDropHandle(tokio::spawn(show_provide_progress(
        app.clone(),
        progress_rx,
    )));

    app.emit("coupon-start", ())?;

    let app2 = &app;
    let remote = provide_coupon(
        &CouponMachineConfig {
            url: "wss://couponmachine.skye.vg".to_owned(),
            realm: "halfcopy".to_owned(),
            password_length: 3,
        },
        |coupon| async move {
            app2.emit("coupon-available", coupon)?;
            Ok(())
        },
        async {
            let addr = endpoint.node_addr().initialized().await;
            let ticket = BlobTicket::new(addr, *temp_tag.hash(), BlobFormat::HashSeq);
            Ok(ticket)
        },
    )
    .await?;

    app.emit("connection-wait", ())?;
    while let Some(conn) = endpoint.accept().await {
        let Ok(conn) = conn.await else {continue;};
        if conn.remote_node_id()? != remote {
            continue;
        }
        app.emit("connection-got", ())?;
        blobs.accept(conn).await?;
        break;
    }

    drop(temp_tag);
    drop(progress);

    app.emit("teardown", ())?;
    tokio::time::timeout(Duration::from_secs(5), blobs.shutdown()).await?;
    tokio::fs::remove_dir_all(blobs_data_dir).await?;
    Ok(())
}
