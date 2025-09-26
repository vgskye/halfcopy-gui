mod coupon;
mod recv;
mod send;
mod words;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![recv::recv, send::send_file, send::send_folder])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
