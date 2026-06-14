use std::sync::mpsc;
use std::sync::Arc;

use anyhow::Result;
use tao::event::{Event, StartCause};
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopProxy};
use tokio::runtime::Handle;
use tray_icon::menu::{Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem, Submenu};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder, TrayIconEvent};

use crate::config::Config;
use crate::daemon::DaemonHandle;

const QUIT_ID: &str = "quit";

struct DaemonReady {
    daemon: Arc<DaemonHandle>,
    runtime: Handle,
}

enum UserEvent {
    TrayIconEvent,
    MenuEvent(MenuEvent),
    DaemonReady(DaemonReady),
    Refresh,
    Exit,
}

struct TrayState {
    daemon: Option<Arc<DaemonHandle>>,
    runtime: Option<Handle>,
    tray_icon: Option<TrayIcon>,
    remove_ids: Vec<(MenuId, i32)>,
    proxy: EventLoopProxy<UserEvent>,
}

struct TraySession {
    thread_id: i32,
    title: String,
    log_dir: String,
    agent_name: String,
    agent_command: String,
    work_dir: String,
}

pub fn run_daemon_with_tray(config: Config) -> Result<()> {
    let event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    let proxy = event_loop.create_proxy();

    TrayIconEvent::set_event_handler(Some(move |_event| {
        let _ = proxy.send_event(UserEvent::TrayIconEvent);
    }));

    let proxy = event_loop.create_proxy();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = proxy.send_event(UserEvent::MenuEvent(event));
    }));

    let proxy = event_loop.create_proxy();
    ctrlc::set_handler(move || {
        let _ = proxy.send_event(UserEvent::Exit);
    })?;

    let (ready_tx, ready_rx) = mpsc::channel();
    let proxy = event_loop.create_proxy();
    std::thread::spawn(move || {
        if let Ok((daemon, runtime)) = ready_rx.recv() {
            let _ = proxy.send_event(UserEvent::DaemonReady(DaemonReady { daemon, runtime }));
        }
    });

    std::thread::spawn(move || run_daemon_thread(config, ready_tx));

    let mut state = TrayState {
        daemon: None,
        runtime: None,
        tray_icon: None,
        remove_ids: Vec::new(),
        proxy: event_loop.create_proxy(),
    };

    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::Wait;

        match event {
            Event::NewEvents(StartCause::Init) => {
                ensure_tray_icon(&mut state);
            }
            Event::UserEvent(UserEvent::DaemonReady(ready)) => {
                state.daemon = Some(ready.daemon);
                state.runtime = Some(ready.runtime);
                refresh_menu(&mut state);
            }
            Event::UserEvent(UserEvent::TrayIconEvent) => {
                // refresh_menu(&mut state);
            }
            Event::UserEvent(UserEvent::MenuEvent(event)) => {
                if event.id == quit_menu_id() {
                    *control_flow = ControlFlow::Exit;
                } else {
                    handle_menu_event(event, &mut state);
                    // refresh_menu(&mut state);
                }
            }
            Event::UserEvent(UserEvent::Refresh) => {
                refresh_menu(&mut state);
            }
            Event::UserEvent(UserEvent::Exit) => {
                *control_flow = ControlFlow::Exit;
            }
            _ => {}
        }
    });
}

fn run_daemon_thread(config: Config, ready_tx: mpsc::Sender<(Arc<DaemonHandle>, Handle)>) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            tracing::error!("Failed to build daemon runtime: {err}");
            return;
        }
    };
    let local = tokio::task::LocalSet::new();

    let result =
        runtime.block_on(local.run_until(crate::daemon::run_daemon(config, Some(ready_tx))));

    if let Err(err) = result {
        tracing::error!("Daemon exited with error: {err}");
    }
}

fn ensure_tray_icon(state: &mut TrayState) {
    if state.tray_icon.is_some() {
        return;
    }

    let menu = build_menu(state.daemon.as_deref(), &mut state.remove_ids);
    match TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("telegram-acp")
        .with_title("ACP")
        .with_icon(tray_icon())
        .with_icon_as_template(true)
        .build()
    {
        Ok(icon) => state.tray_icon = Some(icon),
        Err(err) => tracing::warn!("Failed to create tray icon: {err}"),
    }

    #[cfg(target_os = "macos")]
    {
        use objc2_core_foundation::CFRunLoop;

        if let Some(rl) = CFRunLoop::main() {
            rl.wake_up();
        }
    }
}

fn refresh_menu(state: &mut TrayState) {
    let menu = build_menu(state.daemon.as_deref(), &mut state.remove_ids);
    if let Some(tray_icon) = &state.tray_icon {
        tray_icon.set_menu(Some(Box::new(menu)));
    }
}

fn build_menu(daemon: Option<&DaemonHandle>, remove_ids: &mut Vec<(MenuId, i32)>) -> Menu {
    remove_ids.clear();
    let menu = Menu::new();

    let Some(daemon) = daemon else {
        let loading = MenuItem::new("Daemon starting...", false, None);
        let _ = menu.append(&loading);
        append_quit(&menu);
        return menu;
    };

    let sessions = snapshot_sessions(daemon);
    if sessions.is_empty() {
        let empty = MenuItem::new("No active sessions", false, None);
        let _ = menu.append(&empty);
        append_quit(&menu);
        return menu;
    }

    for session in sessions {
        let submenu = Submenu::new(session.title, true);

        let log_dir = MenuItem::new(format!("Log dir: {}", session.log_dir), false, None);
        let agent_name = MenuItem::new(format!("Agent name: {}", session.agent_name), false, None);
        let agent_command = MenuItem::new(
            format!("Agent command: {}", session.agent_command),
            false,
            None,
        );
        let work_dir = MenuItem::new(format!("Work dir: {}", session.work_dir), false, None);
        let separator = PredefinedMenuItem::separator();
        let remove = MenuItem::with_id(
            format!("remove:{}", session.thread_id),
            "Remove",
            true,
            None,
        );

        remove_ids.push((remove.id().clone(), session.thread_id));
        let _ = submenu.append_items(&[
            &log_dir,
            &agent_name,
            &agent_command,
            &work_dir,
            &separator,
            &remove,
        ]);
        let _ = menu.append(&submenu);
    }

    append_quit(&menu);
    menu
}

fn append_quit(menu: &Menu) {
    let separator = PredefinedMenuItem::separator();
    let quit = MenuItem::with_id(quit_menu_id(), "Quit", true, None);
    let _ = menu.append_items(&[&separator, &quit]);
}

fn quit_menu_id() -> MenuId {
    MenuId::new(QUIT_ID)
}

fn handle_menu_event(event: MenuEvent, state: &mut TrayState) {
    let Some((_, thread_id)) = state
        .remove_ids
        .iter()
        .find(|(id, _)| *id == event.id)
        .cloned()
    else {
        return;
    };

    let Some(daemon) = state.daemon.clone() else {
        return;
    };
    let Some(runtime) = state.runtime.clone() else {
        return;
    };
    let proxy = state.proxy.clone();

    runtime.spawn(async move {
        if let Err(err) = daemon.delete_topic_and_remove_state(thread_id).await {
            tracing::error!(thread_id, "Failed to remove session from tray menu: {err}");
        }
        let _ = proxy.send_event(UserEvent::Refresh);
    });
}

fn snapshot_sessions(daemon: &DaemonHandle) -> Vec<TraySession> {
    let mut sessions = Vec::new();

    for entry in daemon.topics.iter() {
        let thread_id = *entry.key();
        let Some(active) = entry.value().active.as_ref() else {
            continue;
        };

        sessions.push(TraySession {
            thread_id,
            title: session_title(&active.project_path, thread_id),
            log_dir: active.session_log.log_dir().display().to_string(),
            agent_name: active
                .agent_name
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            agent_command: active.agent_command.clone(),
            work_dir: active.project_path.display().to_string(),
        });
    }

    sessions.sort_by(|a, b| a.title.cmp(&b.title).then(a.thread_id.cmp(&b.thread_id)));
    sessions
}

fn session_title(project_path: &std::path::Path, thread_id: i32) -> String {
    project_path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(|name| name.to_string())
        .unwrap_or_else(|| format!("Thread {thread_id}"))
}

fn tray_icon() -> Icon {
    const SIZE: u32 = 18;
    let mut rgba = vec![0; (SIZE * SIZE * 4) as usize];

    for y in 3..13 {
        for x in 2..16 {
            set_pixel(&mut rgba, SIZE, x, y, [0, 0, 0, 255]);
        }
    }
    for y in 13..16 {
        for x in (8 + (y - 13))..(12 + (y - 13)) {
            set_pixel(&mut rgba, SIZE, x, y, [0, 0, 0, 255]);
        }
    }
    for y in 5..11 {
        for x in 4..14 {
            set_pixel(&mut rgba, SIZE, x, y, [0, 0, 0, 0]);
        }
    }

    Icon::from_rgba(rgba, SIZE, SIZE).expect("valid embedded tray icon")
}

fn set_pixel(rgba: &mut [u8], width: u32, x: u32, y: u32, color: [u8; 4]) {
    let idx = ((y * width + x) * 4) as usize;
    rgba[idx..idx + 4].copy_from_slice(&color);
}
