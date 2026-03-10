# Tool Call Findings

## Confirmed Protocol Fields

From ACP schema and docs:

- `SessionUpdate::ToolCall(ToolCall)` has:
  - `toolCallId`
  - `title`
  - `kind`
  - `status`
  - `content`
  - `locations`
  - `rawInput`
  - `rawOutput`
  - `_meta`

- `SessionUpdate::ToolCallUpdate(ToolCallUpdate)` has:
  - `toolCallId`
  - `fields`
  - `_meta`

- `ToolCallUpdate.fields` has optional:
  - `kind`
  - `status`
  - `title`
  - `content`
  - `locations`
  - `rawInput`
  - `rawOutput`

- `ToolCallStatus` values:
  - `pending`
  - `in_progress`
  - `completed`
  - `failed`

## Key Semantic Constraint

A `ToolCallUpdate` is not equivalent to completion. It can report progress or any partial field update.
Completion should only be inferred from `status = completed`.

## Telegram-ACP Specific Findings (current implementation)

- ACP notifications are converted in `src/acp.rs`.
- `ToolCallUpdate.fields.status` is currently not forwarded into `AgentEvent`.
- Telegram formatter renders all tool updates with a success mark (`✅`) in `format_tool_result`.
- This makes non-complete updates appear finished.

## Corrective Direction

1. Extend local event payload to include tool status.
2. Merge cached + incoming status on each update.
3. Render icon/text by effective status:
   - pending: waiting
   - in_progress: running
   - completed: success
   - failed: failed

## Sources

- ACP docs: https://agentclientprotocol.com/protocol/tool-calls
- ACP docs: https://agentclientprotocol.com/protocol/prompt-turn#3-agent-reports-output
- Local schema:
  - `agent-client-protocol-schema/src/tool_call.rs`
  - `agent-client-protocol-schema/src/client.rs`
