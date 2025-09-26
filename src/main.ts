import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

const code = document.getElementById("code") as HTMLInputElement;
const get = document.getElementById("get") as HTMLButtonElement;
const send_file = document.getElementById("send-file") as HTMLButtonElement;
const send_folder = document.getElementById("send-folder") as HTMLButtonElement;

const main_page = document.getElementById("main-page") as HTMLDivElement;
const error_page = document.getElementById("error-page") as HTMLDivElement;
const progress_page = document.getElementById("progress-page") as HTMLDivElement;

const error_message = document.getElementById("error-message") as HTMLParagraphElement;
const error_go_home = document.getElementById("error-go-home") as HTMLButtonElement;

const progress_bar = document.getElementById("progress-bar") as HTMLDivElement;
const progress_text = document.getElementById("progress-text") as HTMLDivElement;

let currentPage = main_page;

get.addEventListener("click", recv);

send_file.addEventListener("click", () => send("send_file"));
send_folder.addEventListener("click", () => send("send_folder"));

error_go_home.addEventListener("click", go_home);

async function go_home() {
  change_page(main_page);
}

function change_page(new_page: HTMLDivElement) {
  currentPage.classList.add("hidden");
  currentPage.classList.remove("flex");
  currentPage = new_page;
  currentPage.classList.remove("hidden");
  currentPage.classList.add("flex");
}

async function recv() {
  const coupon = code.value;
  change_page(progress_page);
  progress_bar.style.width = "0%";
  progress_text.textContent = "Resolving coupon...";
  invoke("recv", {coupon: coupon}).then(go_home, e => {
    change_page(error_page);
    error_message.textContent = e;
  });
}

listen<void>('connect', () => {
  progress_text.textContent = "Connecting...";
});

let total_size = NaN;

listen<number>('total-size', (event) => {
  total_size = event.payload;
  progress_text.textContent = `Receiving files... (0% complete)`;
});

listen<number>('current-size', (event) => {
  const percent = (event.payload / total_size) * 100;
  progress_bar.style.width = `${percent}%`;
  progress_text.textContent = `Receiving files... (${Math.round(percent)}% complete)`;
});

listen<number>('export-total', (event) => {
  total_size = event.payload;
  progress_text.textContent = `Saving files... (0/${total_size})`;
});

listen<number>('export-current', (event) => {
  const percent = (event.payload / total_size) * 100;
  progress_bar.style.width = `${percent}%`;
  progress_text.textContent = `Saving files... (${event.payload}/${total_size})`;
});

async function send(cmd: string) {
  change_page(progress_page);
  progress_bar.style.width = "0%";
  progress_text.textContent = "Preparing import...";
  invoke(cmd).then(go_home, e => {
    change_page(error_page);
    error_message.textContent = e;
  });
}

listen<number>('import-start', (event) => {
  total_size = event.payload
  progress_text.textContent = `Importing files... (0/${total_size})`;
});

listen<number>('import-progress', (event) => {
  const percent = (event.payload / total_size) * 100;
  progress_bar.style.width = `${percent}%`;
  progress_text.textContent = `Importing files... (${event.payload}/${total_size})`;
});

listen<void>('coupon-start', () => {
  progress_bar.style.width = "100%";
  progress_text.textContent = "Creating coupon...";
});

listen<string>('coupon-available', (event) => {
  progress_text.textContent = `Awaiting receiver. Code is ${event.payload}`;
});

listen<void>('connection-wait', () => {
  progress_text.textContent = "Awaiting receiver connection...";
});

listen<void>('connection-got', () => {
  progress_text.textContent = "Receiver connected. Waiting for request...";
});

listen<[number, number, number]>('request-start', (event) => {
  total_size = event.payload[2];
  progress_text.textContent = `Sending files... (0% complete)`;
});

listen<[number, number, number]>('request-progress', (event) => {
  const percent = (event.payload[2] / total_size) * 100;
  progress_bar.style.width = `${percent}%`;
  progress_text.textContent = `Sending files... (${Math.round(percent)}% complete)`;
});

listen<[number, number]>('request-complete', () => {
  progress_text.textContent = "Request complete!";
});

listen<[number, number]>('request-abort', () => {
  progress_text.textContent = "Request aborted!";
});