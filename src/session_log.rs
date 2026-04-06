use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use platform_dirs::AppDirs;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

tokio::task_local! {
    static SESSION_CONTEXT: Arc<SessionContext>;
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionMeta {
    pub created_at: DateTime<Utc>,
    pub runtime_session_id: String,
    pub acp_session_id: Option<String>,
    pub thread_id: i32,
    pub project_path: PathBuf,
    pub agent_command: String,
    pub agent_name: Option<String>,
    pub log_dir: PathBuf,
}

#[derive(Debug)]
pub struct SessionLog {
    meta_path: PathBuf,
    agent_stderr_path: PathBuf,
    meta: Mutex<SessionMeta>,
    events_file: Mutex<File>,
    acp_file: Mutex<File>,
    agent_stderr_file: Mutex<File>,
    seq: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct SessionContext {
    log: Arc<SessionLog>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SessionLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Serialize)]
struct SessionEventRecord {
    ts: DateTime<Utc>,
    seq: u64,
    level: SessionLogLevel,
    session_runtime_id: String,
    acp_session_id: Option<String>,
    thread_id: i32,
    message: String,
    fields: SessionEventFields,
}

#[derive(Serialize)]
struct SessionEventFields {
    project_path: String,
    agent_command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_name: Option<String>,
}

#[derive(Serialize)]
struct AcpTranscriptRecord {
    ts: DateTime<Utc>,
    direction: TranscriptDirection,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptDirection {
    ToAgent,
    FromAgent,
}

impl SessionLog {
    pub fn new(
        runtime_session_id: String,
        thread_id: i32,
        project_path: PathBuf,
        agent_command: String,
        agent_name: Option<String>,
    ) -> Result<Arc<Self>> {
        let sessions_root = sessions_root_dir()?;
        std::fs::create_dir_all(&sessions_root)?;

        let dir = sessions_root.join(format!("runtime-{runtime_session_id}"));
        std::fs::create_dir_all(&dir)?;

        let meta_path = dir.join("meta.json");
        let events_path = dir.join("events.jsonl");
        let acp_path = dir.join("acp.jsonl");
        let agent_stderr_path = dir.join("agent.stderr.log");

        let events_file = open_append_file(&events_path)?;
        let acp_file = open_append_file(&acp_path)?;
        let agent_stderr_file = open_append_file(&agent_stderr_path)?;

        let meta = SessionMeta {
            created_at: Utc::now(),
            runtime_session_id,
            acp_session_id: None,
            thread_id,
            project_path,
            agent_command,
            agent_name,
            log_dir: dir.clone(),
        };

        let log = Arc::new(Self {
            meta_path,
            agent_stderr_path,
            meta: Mutex::new(meta),
            events_file: Mutex::new(events_file),
            acp_file: Mutex::new(acp_file),
            agent_stderr_file: Mutex::new(agent_stderr_file),
            seq: AtomicU64::new(0),
        });
        log.write_meta()?;
        Ok(log)
    }

    pub fn agent_stderr_path(&self) -> PathBuf {
        self.agent_stderr_path.clone()
    }

    pub fn set_acp_session_id(&self, acp_session_id: impl Into<String>) -> Result<()> {
        let mut meta = lock_mutex(&self.meta);
        meta.acp_session_id = Some(acp_session_id.into());
        drop(meta);
        self.write_meta()
    }

    pub fn log_event(&self, level: SessionLogLevel, message: String) -> Result<()> {
        let meta = lock_mutex(&self.meta).clone();
        let record = SessionEventRecord {
            ts: Utc::now(),
            seq: self.seq.fetch_add(1, Ordering::Relaxed) + 1,
            level,
            session_runtime_id: meta.runtime_session_id,
            acp_session_id: meta.acp_session_id,
            thread_id: meta.thread_id,
            message,
            fields: SessionEventFields {
                project_path: meta.project_path.display().to_string(),
                agent_command: meta.agent_command,
                agent_name: meta.agent_name,
            },
        };
        self.append_json_line(&self.events_file, &record)
    }

    pub fn log_acp_payload<T: Serialize>(
        &self,
        direction: TranscriptDirection,
        payload: &T,
    ) -> Result<()> {
        let record = AcpTranscriptRecord {
            ts: Utc::now(),
            direction,
            payload: serde_json::to_value(payload)?,
        };
        self.append_json_line(&self.acp_file, &record)
    }

    pub fn write_agent_stderr_line(&self, line: &str) -> Result<()> {
        let mut file = lock_mutex(&self.agent_stderr_file);
        writeln!(file, "{line}")?;
        file.flush()?;
        Ok(())
    }

    fn write_meta(&self) -> Result<()> {
        let meta = lock_mutex(&self.meta).clone();
        let tmp_path = self.meta_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, serde_json::to_vec_pretty(&meta)?)?;
        std::fs::rename(&tmp_path, &self.meta_path)?;
        Ok(())
    }

    fn append_json_line<T: Serialize>(&self, file: &Mutex<File>, value: &T) -> Result<()> {
        let mut file = lock_mutex(file);
        serde_json::to_writer(&mut *file, value)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

impl SessionContext {
    pub fn new(log: Arc<SessionLog>) -> Arc<Self> {
        Arc::new(Self { log })
    }

    pub fn log(&self) -> &Arc<SessionLog> {
        &self.log
    }
}

pub async fn with_session_context<F>(ctx: Arc<SessionContext>, fut: F) -> F::Output
where
    F: Future,
{
    SESSION_CONTEXT.scope(ctx, fut).await
}

pub fn try_current_session_context() -> Option<Arc<SessionContext>> {
    SESSION_CONTEXT.try_with(Arc::clone).ok()
}

pub fn emit_session_log(level: SessionLogLevel, message: String) {
    if let Some(ctx) = try_current_session_context() {
        let acp_session_id = lock_mutex(&ctx.log.meta).acp_session_id.clone();
        let thread_id = lock_mutex(&ctx.log.meta).thread_id;
        if let Err(err) = ctx.log.log_event(level, message.clone()) {
            tracing::error!(
                thread_id,
                error = %err,
                "Failed to append session log event"
            );
        }
        match level {
            SessionLogLevel::Trace => {
                tracing::trace!(thread_id, acp_session_id = ?acp_session_id, "{message}")
            }
            SessionLogLevel::Debug => {
                tracing::debug!(thread_id, acp_session_id = ?acp_session_id, "{message}")
            }
            SessionLogLevel::Info => {
                tracing::info!(thread_id, acp_session_id = ?acp_session_id, "{message}")
            }
            SessionLogLevel::Warn => {
                tracing::warn!(thread_id, acp_session_id = ?acp_session_id, "{message}")
            }
            SessionLogLevel::Error => {
                tracing::error!(thread_id, acp_session_id = ?acp_session_id, "{message}")
            }
        }
        return;
    }

    match level {
        SessionLogLevel::Trace => tracing::trace!("{message}"),
        SessionLogLevel::Debug => tracing::debug!("{message}"),
        SessionLogLevel::Info => tracing::info!("{message}"),
        SessionLogLevel::Warn => tracing::warn!("{message}"),
        SessionLogLevel::Error => tracing::error!("{message}"),
    }
}

pub fn app_data_dir() -> Result<PathBuf> {
    Ok(AppDirs::new(Some("telegram-acp"), false)
        .context("Failed to resolve platform app directories")?
        .data_dir)
}

pub fn sessions_root_dir() -> Result<PathBuf> {
    Ok(app_data_dir()?.join("sessions"))
}

fn open_append_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open log file {}", path.display()))
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[macro_export]
macro_rules! sess_trace {
    ($($arg:tt)*) => {
        $crate::session_log::emit_session_log(
            $crate::session_log::SessionLogLevel::Trace,
            format!($($arg)*),
        )
    };
}

#[macro_export]
macro_rules! sess_debug {
    ($($arg:tt)*) => {
        $crate::session_log::emit_session_log(
            $crate::session_log::SessionLogLevel::Debug,
            format!($($arg)*),
        )
    };
}

#[macro_export]
macro_rules! sess_info {
    ($($arg:tt)*) => {
        $crate::session_log::emit_session_log(
            $crate::session_log::SessionLogLevel::Info,
            format!($($arg)*),
        )
    };
}

#[macro_export]
macro_rules! sess_warn {
    ($($arg:tt)*) => {
        $crate::session_log::emit_session_log(
            $crate::session_log::SessionLogLevel::Warn,
            format!($($arg)*),
        )
    };
}

#[macro_export]
macro_rules! sess_error {
    ($($arg:tt)*) => {
        $crate::session_log::emit_session_log(
            $crate::session_log::SessionLogLevel::Error,
            format!($($arg)*),
        )
    };
}
