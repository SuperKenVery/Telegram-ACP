---
name: agent-client-protocol
description: Interpret and implement Agent Client Protocol (ACP) behavior for client UIs and integrations, especially SessionUpdate handling for tool calls. Use when mapping ACP session notifications to product UX, verifying protocol fields/status semantics, or debugging ToolCall and ToolCallUpdate rendering.
---

# Agent Client Protocol

## Overview

Use ACP docs as the protocol source of truth and local schema types as implementation source of truth.

Primary docs:
- `https://agentclientprotocol.com/protocol/tool-calls`
- `https://agentclientprotocol.com/protocol/prompt-turn#3-agent-reports-output`

## Workflow

1. Confirm the target ACP version used by the codebase.
2. Extract exact fields from `SessionUpdate`, `ToolCall`, and `ToolCallUpdate`.
3. Map all status-bearing fields into local event types (do not drop status).
4. Render tool-call UI from status, not event name alone.
5. Validate behavior against progress semantics: a `ToolCallUpdate` can be progress, completion, or failure.

## Field Mapping Rules

Use this canonical mapping from protocol payloads:

- `SessionUpdate::ToolCall(ToolCall)` includes:
  - `toolCallId`, `title`, `kind`, `status`, `content`, `locations`, `rawInput`, `rawOutput`, `_meta`
- `SessionUpdate::ToolCallUpdate(ToolCallUpdate)` includes:
  - `toolCallId`, `fields`, `_meta`
- `ToolCallUpdate.fields` can include (all optional):
  - `kind`, `status`, `title`, `content`, `locations`, `rawInput`, `rawOutput`

Interpretation rule:
- A missing field in `ToolCallUpdate.fields` means "unchanged", not "cleared".
- Collections in updates replace prior collection values when present.

## Status-Driven UI Rules

Never assume `ToolCallUpdate` means done.

Use `ToolCallStatus`:
- `pending`: queued/waiting for input or approval
- `in_progress`: currently running
- `completed`: success
- `failed`: tool execution failed

Render recommendation:
- `pending` -> waiting indicator
- `in_progress` -> progress indicator
- `completed` -> success indicator
- `failed` -> error indicator

Do not hardcode success iconography for all updates.

## Telegram/UI Integration Checklist

When adapting ACP to messaging UIs (Telegram, chat, web):

1. Persist `tool_call_id -> UI message id` mapping.
2. Send initial message on `ToolCall`.
3. Edit existing message on `ToolCallUpdate` when possible.
4. Fall back to a new message only when edit fails.
5. Resolve partial updates by merging with cached state.
6. Drive icon/label from merged `status`.

## References

- Protocol findings and practical guidance: `references/tool-call-findings.md`
