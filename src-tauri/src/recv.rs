use std::path::{Path, PathBuf};
use futures_util::StreamExt;
use iroh::Endpoint;
use iroh_blobs::{
    api::{
        blobs::{ExportMode, ExportOptions},
        remote::GetProgressItem,
    },
    format::collection::Collection,
    get::request::get_hash_seq_and_sizes,
    store::fs::FsStore,
    ticket::BlobTicket,
};
use tauri::{AppHandle, Emitter, Manager};
use tauri_plugin_dialog::DialogExt;
use tokio::sync::oneshot;
use tracing::trace;

use crate::coupon::{CouponMachineConfig, receive_coupon};

#[tauri::command]
pub async fn recv(app: AppHandle, coupon: String) -> Result<(), String> {
    recv_inner(app, coupon).await.map_err(|e| e.to_string())
}

async fn recv_inner(app: AppHandle, coupon: String) -> anyhow::Result<()> {
    let endpoint = Endpoint::builder()
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .bind()
        .await?;

    let ticket: BlobTicket = receive_coupon(
        &CouponMachineConfig {
            url: "wss://couponmachine.skye.vg".to_owned(),
            realm: "halfcopy".to_owned(),
            password_length: 3,
        },
        &coupon,
        endpoint.node_id(),
    )
    .await?;
    let addr = ticket.node_addr().clone();

    let dir_name = format!(".halfcopy-recv-{}", ticket.hash().to_hex());
    let iroh_data_dir = app.path().app_cache_dir()?.join(dir_name);
    let db = FsStore::load(&iroh_data_dir).await?;

    let hash_and_format = ticket.hash_and_format();
    let local = db.remote().local(hash_and_format).await?;
    if !local.is_complete() {
        app.emit("connect", ())?;
        let connection = endpoint.connect(addr, iroh_blobs::protocol::ALPN).await?;
        let (_hash_seq, sizes) =
            get_hash_seq_and_sizes(&connection, &hash_and_format.hash, 1024 * 1024 * 32, None)
                .await?;
        let total_size = sizes.iter().copied().sum::<u64>();
        let position = local.local_bytes();
        app.emit("total-size", total_size)?;
        app.emit("current-size", position)?;
        let get = db.remote().execute_get(connection, local.missing());
        let mut stream = get.stream();
        while let Some(item) = stream.next().await {
            trace!("got item {item:?}");
            match item {
                GetProgressItem::Progress(offset) => {
                    app.emit("current-size", position + offset)?;
                }
                GetProgressItem::Done(_) => {
                    break;
                }
                GetProgressItem::Error(cause) => {
                    anyhow::bail!(cause);
                }
            }
        }
    }

    let collection = Collection::load(hash_and_format.hash, db.as_ref()).await?;
    let (send, recv) = oneshot::channel();
    app.dialog().file().pick_folder(|root| {
        _ = send.send(root);
    });
    let root = recv
        .await?
        .ok_or(anyhow::anyhow!("Save cancelled!"))?
        .into_path()?;
    app.emit("export-total", collection.len())?;
    for (i, (name, hash)) in collection.iter().enumerate() {
        app.emit("export-current", i)?;
        let target = get_export_path(&root, name)?;
        if target.exists() {
            anyhow::bail!("target {} already exists", target.display());
        }
        db.export_with_opts(ExportOptions {
            hash: *hash,
            target,
            mode: ExportMode::Copy,
        })
        .await?;
    }
    tokio::fs::remove_dir_all(iroh_data_dir).await?;
    Ok(())
}

fn get_export_path(root: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let parts = name.split('/');
    let mut path = root.to_path_buf();
    for part in parts {
        validate_path_component(part)?;
        path.push(part);
    }
    Ok(path)
}

fn validate_path_component(component: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !component.contains('/'),
        "path components must not contain the only correct path separator, /"
    );
    Ok(())
}
