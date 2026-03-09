# telegram-acp

Bridges Telegram and ACP (Agent Client Protocol) coding agents. A long-running daemon manages agent sessions, each mapped to a Telegram forum topic (thread). Users interact with agents by sending messages in topics.

## Architecture

```
CLI ──(Unix socket IPC)──> Daemon ──> ACP Agent subprocess (e.g. claude-agent-acp)
                              │
                              ├──> Telegram Bot API (topics, messages)
                              └──> Telegraph API (code diff posts, not yet wired)
```

Each session:
1. Gets a Telegram forum topic (works in both supergroups and private chats with Threaded Mode)
2. Spawns an ACP agent subprocess communicating over stdin/stdout
3. Routes messages between the topic and the agent via channels

## Code Layout

```
src/
  main.rs          Entrypoint. Clap CLI with subcommands: daemon, new, status.
  config.rs        Config from ~/.config/telegram-acp/config.toml with env var overrides.
  daemon.rs        DaemonHandle (shared state), session spawning, daemon startup.
  session.rs       Prompt loop: receives user text, sends PromptRequest to agent.
  acp.rs           ACP Client trait impl (TelegramClient), subprocess spawning, session init.
  telegram.rs      Teloxide bot dispatcher, /new command, topic message routing, event consumer.
  telegraph.rs     Telegraph account creation and page publishing (for code diffs).
  ipc.rs           Unix socket server (daemon) and client (CLI). Newline-delimited JSON.
  types.rs         Shared types: DaemonCommand, DaemonResponse, SessionInfo, AgentEvent.
  formatting.rs    Telegram HTML formatting, escaping, message splitting/truncation.
  bin/
    mock_agent.rs  Mock ACP agent for testing. Returns canned tool calls and text.
```

## Key Design Details

### ACP and LocalSet

The `agent-client-protocol` crate uses `#[async_trait(?Send)]` — all ACP types are `!Send` and must run on a single thread via `tokio::task::LocalSet`. The daemon runs inside a LocalSet (`main.rs`), but the Telegram bot and IPC server use `tokio::spawn` (multi-threaded). To bridge this, `DaemonHandle` holds an `mpsc` channel that relays agent spawn requests into a `spawn_local` task running inside the LocalSet.

### Channel Wiring Per Session

Each session has two channel pairs:
- `user_tx`/`user_rx` (`mpsc::UnboundedSender<String>`) — Telegram messages from the user to the agent's prompt loop
- `event_tx`/`event_rx` (`mpsc::UnboundedSender<AgentEvent>`) — Agent notifications (text, tool calls, completion) to the Telegram event consumer

### Notification Logic

The event consumer in `telegram.rs` sends Telegram messages with notification control: the first message and the final (completion/error) message send with notification enabled; all intermediate messages are silent.

### Permissions

The ACP Client impl auto-approves all permission requests by selecting the first "allow" option.

## Configuration

TOML file at `~/.config/telegram-acp/config.toml`:

```toml
bot_token = "..."
chat_id = 123456789          # Telegram chat ID (user or supergroup)
default_agent = "claude"

[claude]
cmd = "claude-agent-acp"

[codex]
cmd = "codex --acp"
# socket_path = "/tmp/telegram-acp.sock"
# telegraph_author = "Your Name"
```

Env var overrides: `TELEGRAM_ACP_BOT_TOKEN`, `TELEGRAM_ACP_CHAT_ID`, `TELEGRAM_ACP_SOCKET_PATH`, `TELEGRAM_ACP_DEFAULT_AGENT`, `TELEGRAM_ACP_TELEGRAPH_AUTHOR`.

The bot must have **Threaded Mode** enabled in BotFather settings for private chat topics.

## Building and Testing

```sh
cargo build                          # builds both telegram-acp and mock_agent binaries
cargo run -- daemon                  # start the daemon
cargo run -- new /path/to/project    # create a session via CLI
cargo run -- status                  # list active sessions
```

To test with the mock agent:

```sh
cargo run -- daemon
```
