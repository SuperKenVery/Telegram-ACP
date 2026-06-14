# Telegram-ACP

**Control any coding agent through Telegram.**

| Screenshots | Screenshots | Screenshots |
|---|---|---|
| ![photo_1_2026-03-14_01-08-15](https://github.com/user-attachments/assets/e84143a7-58ac-4927-991f-b9cbb772aae2) | ![photo_2_2026-03-14_01-08-15](https://github.com/user-attachments/assets/18346bb9-39a9-4199-b206-e3fe7135e42b) | ![photo_2026-03-14_01-13-09](https://github.com/user-attachments/assets/53ac0332-0ca7-4683-bb36-f0d49e84f0a0) |
| **Talking with agent.** <br/> Handle multiple sessions with tabs and threads. | **Uploading artifacts.** <br/>Use telegraph to view your markdown files, or let it upload images or files. | **Slash commands.** <br/>Manage your sessions with ease, set models and permissions, or use agent's commands. |

## Features

- Get **notifications** and view **real-time progress** on your phone
- **Give new tasks** to agent even you're away from computer
- Works with **any agent** that supports [ACP](https://agentclientprotocol.com/). This is almost any agent, including claude code, codex, opencode and cursor.
- Handle **multiple sessions** simutaniously, in different telegram tabs

# Installation

**Method 1**: Use cargo binstall (not recommended)

`cargo binstall` requires me to manually bump version numbers and do a release, and there's no way to upload a binary on push.
Therefore, the version on `binstall` can be quite old.

```bash
cargo binstall telegram-acp
telegram-acp
```

**Method 2**: Use via nix

```bash
nix run github:SuperKenVery/Telegram-ACP
```

### Home Manager service

The flake exposes a Home Manager module for running the daemon as a user
service:

```nix
{
  imports = [ inputs.telegram-acp.homeManagerModules.default ];

  services.telegram-acp = {
    enable = true;
    botTokenFile = "/run/secrets/telegram-acp-bot-token";
    chatId = 123456789;
    defaultAgent = "codex";
    agents.codex = "codex --acp";
  };
}
```

**Method 3**: Compile from source

```bash
git clone https://github.com/SuperKenVery/Telegram-ACP.git
cd Telegram-ACP
./dev-loop.fish
```

## Configuration

On Telegram, use `@botfather` to create a new bot and get your bot token. You should also **enable threaded mode in bot settings**.

Create `~/.config/telegram-acp/config.toml`:

```toml
bot_token = "<telegram-bot-token>"
chat_id = 123456789
default_agent = "claude"
# tray = false  # disable the menu bar/tray icon; omitted means auto with headless fallback

[claude]
cmd = "claude-agent-acp"

[codex]
cmd = "codex --acp"
# socket_path = "/tmp/telegram-acp.sock"
# telegraph_author = "Your Name"
```

Env overrides are also supported:

- `TELEGRAM_ACP_BOT_TOKEN`
- `TELEGRAM_ACP_CHAT_ID`
- `TELEGRAM_ACP_SOCKET_PATH`
- `TELEGRAM_ACP_DEFAULT_AGENT`
- `TELEGRAM_ACP_TELEGRAPH_AUTHOR`

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


## Mock agent testing

Use the included mock ACP binary to test Telegram/IPC plumbing without a real coding agent:

```sh
cargo run -- daemon
```

Configure it as an agent in your config:

```toml
default_agent = "mock"

[mock]
cmd = "./target/debug/mock_agent"
```

Then you can send some ACP updates as text via telegram, and it would send those updates to our daemon.

## Telegram requirements

- Enable **Threaded Mode** in BotFather
- For supergroups, forum topics should be enabled

## Some design decisions

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

GPL v3
