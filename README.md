# telegram-acp

**Control any coding agent through Telegram.**

## What you get

- Get notifications and view real-time progress on your phone
- Give new tasks to agent even you're away from computer
- Works with any agent that supports [ACP](https://agentclientprotocol.com/). This is almost any agent, including claude code, codex, opencode and cursor.

# Instalation

todo: Setup cargo-binstall
todo: Package with nix

# Hacking

## How it works

```text
CLI ──(Unix socket IPC)──> Daemon ──> ACP Agent subprocesses (stdin/stdout)
                              │
                              └──> Telegram Bot API (topics, messages)
```

Per session:

1. Creates/uses a Telegram forum topic (or threaded private chat topic)
2. Spawns an ACP agent subprocess
3. Routes user messages -> agent and agent events -> Telegram

## Quick start

### 1. Configure

On Telegram, use `@botfather` to create a new bot and get your bot token. You should also enable threaded mode in bot settings.

Create `~/.config/telegram-acp/config.toml`:

```toml
bot_token = "<telegram-bot-token>"
chat_id = 123456789
default_agent_command = "claude-agent-acp"
# socket_path = "/tmp/telegram-acp.sock"
# telegraph_author = "Your Name"
```

Env overrides are also supported:

- `TELEGRAM_ACP_BOT_TOKEN`
- `TELEGRAM_ACP_CHAT_ID`
- `TELEGRAM_ACP_SOCKET_PATH`
- `TELEGRAM_ACP_AGENT_COMMAND`
- `TELEGRAM_ACP_TELEGRAPH_AUTHOR`

### 2. Build

```sh
cargo build
```

### 3. Run daemon

```sh
cargo run -- daemon
```

### 4. Create a session

```sh
cargo run -- new /path/to/project
```

## Mock agent testing

Use the included mock ACP binary to test Telegram/IPC plumbing without a real coding agent:

```sh
TELEGRAM_ACP_AGENT_COMMAND="./target/debug/mock_agent" cargo run -- daemon
```

## Telegram requirements

- Bot must be added to the target chat
- For private chats using topics, enable **Threaded Mode** in BotFather
- For supergroups, forum topics should be enabled

## Notes

- IPC uses newline-delimited JSON over a Unix socket
- Designed for long-running daemon operation
- Current Telegraph support is present but not fully wired into the runtime flow

## Architecture highlights

- `agent-client-protocol` types are `!Send`, so ACP work is pinned to a `tokio::task::LocalSet`
- Telegram dispatcher + IPC server run with `tokio::spawn` and communicate through channels
- Each session has two unbounded channel pairs:
  - `user_tx`/`user_rx`: user text into prompt loop
  - `event_tx`/`event_rx`: agent output back to Telegram consumer
- Notification behavior is intentional:
  - first and final message notify
  - intermediate streaming messages are silent
- Permission prompts are auto-approved by choosing the first allow option

## Project layout

```text
src/
  main.rs          CLI entrypoint (`daemon`, `new`, `status`)
  config.rs        Config loading (TOML + env overrides)
  daemon.rs        Daemon state, session lifecycle, LocalSet bridge
  session.rs       Prompt loop (PromptRequest orchestration)
  acp.rs           ACP client integration + subprocess handling
  telegram.rs      Bot dispatcher, topic routing, event consumer
  telegraph.rs     Telegraph helpers (account/page publishing)
  ipc.rs           Unix socket NDJSON daemon/client protocol
  types.rs         Shared command/response/event types
  formatting.rs    Telegram HTML escaping + message splitting
  bin/mock_agent.rs Mock ACP agent for local testing
```

## License

Add your preferred license here.
