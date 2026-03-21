use agent_client_protocol as acp;
use telegram_markdown_v2::UnsupportedTagsStrategy;

/// MarkdownV2 formatting utilities for Telegram messages.

/// Convert regular Markdown into Telegram MarkdownV2 using Escape strategy for unsupported tags.
pub fn markdown_to_telegram_md_v2(markdown: &str) -> String {
    match telegram_markdown_v2::convert_with_strategy(markdown, UnsupportedTagsStrategy::Escape) {
        Ok(converted) => converted.trim_end_matches('\n').to_string(),
        Err(e) => {
            tracing::warn!("telegram_markdown_v2 conversion failed, using escaped fallback: {e}");
            escape_markdown_v2(markdown)
        }
    }
}

/// Escape text for Telegram HTML parse mode.
pub fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Escape text for Telegram MarkdownV2 parse mode.
pub fn escape_markdown_v2(text: &str) -> String {
    let mut out = String::with_capacity(text.len() * 2);
    for ch in text.chars() {
        match ch {
            '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|'
            | '{' | '}' | '.' | '!' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Format an agent text message for Telegram. Truncates to fit within Telegram's 4096 char limit.
pub fn format_text_message(text: &str) -> String {
    // Keep model text as Markdown and convert once at send boundary.
    truncate_message(text, 4096)
}

/// Format a thought/reasoning message for Telegram.
pub fn format_thought_message(text: &str) -> String {
    let prefixed = if text.trim().is_empty() {
        "💭 _Thought_".to_string()
    } else {
        format!("💭 _Thought_\n\n{}", text)
    };
    truncate_message(&prefixed, 4096)
}

/// Format a tool call notification (HTML).
pub fn format_tool_call(
    name: &str,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    details: Option<&str>,
) -> String {
    let header = format_tool_header_html(name, kind, status);
    match details {
        Some(body) => {
            format!("{header}\n{}", format_collapsible_block_html(body, 3800))
        }
        None => header,
    }
}

/// Format a tool call result/update (HTML).
pub fn format_tool_result(
    name: &str,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    output: Option<&str>,
    details: Option<&str>,
) -> String {
    let header = format_tool_header_html(name, kind, status);
    match details.or(output) {
        Some(body) => {
            let section = format_collapsible_block_html(body, 3900);
            format!("{header}\n{section}")
        }
        None => header,
    }
}

/// Format a completion message (HTML).
pub fn format_completion(stop_reason: &str, telegraph_url: Option<&str>) -> String {
    let mut msg = format!("✓ <b>Done</b> ({})", escape_html(stop_reason));
    if let Some(url) = telegraph_url {
        msg.push_str(&format!(
            "\n\n📄 <a href=\"{}\">View changes</a>",
            escape_html(url)
        ));
    }
    msg
}

/// Format an error message (HTML).
pub fn format_error(error: &str) -> String {
    format!("❌ <b>Error:</b> {}", escape_html(error))
}

/// Format a plan message (HTML).
pub fn format_plan(plan: &acp::Plan) -> String {
    let mut entries: Vec<_> = plan.entries.iter().collect();
    entries.sort_by_key(|entry| match entry.status {
        acp::PlanEntryStatus::InProgress => 0,
        acp::PlanEntryStatus::Pending => 1,
        acp::PlanEntryStatus::Completed => 2,
        _ => 3,
    });

    let title = entries
        .iter()
        .find(|entry| matches!(entry.status, acp::PlanEntryStatus::InProgress))
        .map(|entry| entry.content.as_str())
        .unwrap_or("Plan");

    let mut lines = Vec::with_capacity(entries.len() + 2);
    lines.push(format!("<b>{}</b>", escape_html(title)));
    lines.push(String::new());

    for (idx, entry) in entries.iter().enumerate() {
        let content = escape_html(&entry.content);
        let line = match entry.status {
            acp::PlanEntryStatus::Completed => format!("{}. ✅ {}", idx + 1, content),
            _ => format!("{}. {}", idx + 1, content),
        };
        lines.push(line);
    }

    truncate_message(&lines.join("\n"), 4096)
}

/// Format a completed plan message (HTML).
pub fn format_plan_completed(plan: &acp::Plan) -> String {
    let mut lines = Vec::with_capacity(plan.entries.len() + 2);
    lines.push("✅ Plan completed".to_string());
    lines.push(String::new());
    for (idx, entry) in plan.entries.iter().enumerate() {
        lines.push(format!("{}. ✅ {}", idx + 1, escape_html(&entry.content)));
    }
    truncate_message(&lines.join("\n"), 4096)
}

/// Format available agent slash commands (HTML).
pub fn format_available_commands_html(commands: &[acp::AvailableCommand]) -> String {
    if commands.is_empty() {
        return "No agent slash commands are advertised for this session.".to_string();
    }

    let mut lines = Vec::with_capacity(commands.len() * 2 + 2);
    lines.push("Available agent commands:".to_string());
    lines.push(String::new());

    for command in commands {
        let mut line = format!(
            "• /<code>{}</code>: {}",
            escape_html(&command.name),
            escape_html(&command.description)
        );
        if let Some(acp::AvailableCommandInput::Unstructured(input)) = &command.input {
            line.push_str(&format!(" (input: {})", escape_html(&input.hint)));
        }
        lines.push(line);
    }

    truncate_message(&lines.join("\n"), 4096)
}

fn format_collapsible_block_html(text: &str, max_len: usize) -> String {
    let truncated = truncate_message(text, max_len);
    let escaped = escape_html(&truncated);
    format!("<blockquote expandable><pre><code>{escaped}</code></pre></blockquote>")
}

fn format_tool_header_html(name: &str, kind: acp::ToolKind, status: acp::ToolCallStatus) -> String {
    let status_icon = match status {
        acp::ToolCallStatus::Pending => "⏳",
        acp::ToolCallStatus::InProgress => "🚧",
        acp::ToolCallStatus::Completed => "✅",
        acp::ToolCallStatus::Failed => "❌",
        _ => "？",
    };
    let kind_icon = match kind {
        acp::ToolKind::Read => "👀",
        acp::ToolKind::Edit => "✏️",
        acp::ToolKind::Delete => "🗑️",
        acp::ToolKind::Move => "➡️",
        acp::ToolKind::Search => "🔍",
        acp::ToolKind::Execute => "▶️",
        acp::ToolKind::Think => "🧠",
        acp::ToolKind::Fetch => "🌐",
        acp::ToolKind::Other => "🛠️",
        _ => "🛠️",
    };
    format!(
        "{status_icon} {kind_icon} <b>Tool:</b> {}",
        escape_html(name)
    )
}

/// Truncate a message to fit within a maximum length.
/// If truncated, appends "…[truncated]".
pub fn truncate_message(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_string()
    } else {
        let suffix = "…[truncated]";
        let cut = max_len - suffix.len();
        // Find a safe char boundary
        let cut = text.floor_char_boundary(cut);
        format!("{}{}", &text[..cut], suffix)
    }
}

/// Split a long message into multiple chunks that each fit within Telegram's limit.
pub fn split_message(text: &str, max_len: usize) -> Vec<String> {
    if text.len() <= max_len {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        if remaining.len() <= max_len {
            chunks.push(remaining.to_string());
            break;
        }

        // Try to split at a newline near the limit
        let safe_max = remaining.floor_char_boundary(max_len);
        let cut = remaining[..safe_max].rfind('\n').unwrap_or(safe_max);

        let (chunk, rest) = remaining.split_at(cut);
        chunks.push(chunk.to_string());
        remaining = rest.trim_start_matches('\n');
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::{format_available_commands_html, markdown_to_telegram_md_v2};
    use agent_client_protocol as acp;

    #[test]
    fn formats_empty_available_commands() {
        let text = format_available_commands_html(&[]);
        assert!(text.contains("No agent slash commands"));
    }

    #[test]
    fn formats_commands_with_hint() {
        let cmd = acp::AvailableCommand::new("search", "Search the codebase").input(
            acp::AvailableCommandInput::Unstructured(acp::UnstructuredCommandInput::new("query")),
        );
        let text = format_available_commands_html(&[cmd]);
        assert!(text.contains("/search"));
        assert!(text.contains("Search the codebase"));
        assert!(text.contains("input: query"));
    }

    #[test]
    fn split_message_multibyte_boundary() {
        use super::split_message;
        // Place a 3-byte character (em-dash) right at the split boundary
        let mut text = "a".repeat(99);
        text.push('—'); // bytes 99..102
        text.push_str(&"b".repeat(50));
        // Split at 100 — byte 100 is inside the '—' char
        let chunks = split_message(&text, 100);
        // Must not panic; all chunks should be valid UTF-8
        for chunk in &chunks {
            assert!(chunk.len() <= 100 || chunk.chars().count() > 0);
        }
    }

    #[test]
    fn markdown_to_telegram_md_v2_escapes_unsupported_quote_and_pipe() {
        let input = "> a|b";
        let out = markdown_to_telegram_md_v2(input);
        assert!(out.starts_with("\\> "));
        assert!(out.contains("\\|"));
    }
}
