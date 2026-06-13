use agent_client_protocol as acp;
/// Escape text for Telegram HTML parse mode.
pub fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Format an agent text message for Telegram rich Markdown.
pub fn format_text_message(text: &str) -> String {
    // Keep model text as Markdown and convert once at send boundary.
    truncate_message(text, 32_768)
}

/// Format a thought/reasoning message as foldable rich Markdown.
pub fn format_thought_message(text: &str) -> String {
    if text.trim().is_empty() {
        "💭 Thought".to_string()
    } else {
        format_details("💭 Thought", &truncate_message(text, 32_000))
    }
}

/// Format a tool call notification as rich Markdown.
pub fn format_tool_call(
    name: &str,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    details: Option<&str>,
) -> String {
    let (summary, body_from_name) = split_tool_title_and_body(name);
    let body = details.or(body_from_name);
    format_tool_message(
        summary,
        kind,
        status,
        body,
        tool_language(kind, body),
        16_000,
    )
}

/// Format a tool call result/update as rich Markdown.
pub fn format_tool_result(
    name: &str,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    output: Option<&str>,
    details: Option<&str>,
) -> String {
    let (summary, body_from_name) = split_tool_title_and_body(name);
    let body = details.or(output).or(body_from_name);
    format_tool_message(
        summary,
        kind,
        status,
        body,
        tool_language(kind, body),
        16_000,
    )
}

/// Format a completion message (HTML).
pub fn format_completion(stop_reason: &str) -> String {
    format!("✓ <b>Done</b> ({})", escape_html(stop_reason))
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
    lines.push(format!(
        "<b>Progress: {}</b>",
        escape_html(&truncate_message(title, 500))
    ));
    lines.push(String::new());

    for (idx, entry) in entries.iter().enumerate() {
        let content = escape_html(&truncate_message(&entry.content, 500));
        let line = match entry.status {
            acp::PlanEntryStatus::Pending => format!("{}. ⏳ {}", idx + 1, content),
            acp::PlanEntryStatus::InProgress => format!("{}. 🚧 {}", idx + 1, content),
            acp::PlanEntryStatus::Completed => format!("{}. ✅ {}", idx + 1, content),
            _ => format!("{}. {}", idx + 1, content),
        };
        lines.push(line);
    }

    lines.join("\n")
}

/// Format a completed plan message (HTML).
pub fn format_plan_completed(plan: &acp::Plan) -> String {
    let mut lines = Vec::with_capacity(plan.entries.len() + 2);
    lines.push("✅ Plan completed".to_string());
    lines.push(String::new());
    for (idx, entry) in plan.entries.iter().enumerate() {
        lines.push(format!(
            "{}. ✅ {}",
            idx + 1,
            escape_html(&truncate_message(&entry.content, 500))
        ));
    }
    lines.join("\n")
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

fn format_tool_message(
    name: &str,
    kind: acp::ToolKind,
    status: acp::ToolCallStatus,
    body: Option<&str>,
    language: &'static str,
    body_max_len: usize,
) -> String {
    let header = format_tool_summary(name, kind, status);
    match body.map(str::trim).filter(|body| !body.is_empty()) {
        Some(body) => format_code_details(&header, body, language, body_max_len),
        None => header,
    }
}

fn format_tool_summary(name: &str, kind: acp::ToolKind, status: acp::ToolCallStatus) -> String {
    let truncated_name = truncate_message(name.trim(), 500);
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

    if truncated_name.is_empty() {
        format!("{status_icon} {kind_icon} Tool")
    } else {
        format!("{status_icon} {kind_icon} {truncated_name}")
    }
}

fn split_tool_title_and_body(name: &str) -> (&str, Option<&str>) {
    match name.split_once('\n') {
        Some((first_line, remaining)) if !remaining.trim().is_empty() => {
            (first_line.trim(), Some(remaining.trim()))
        }
        _ => (name.trim(), None),
    }
}

fn tool_language(kind: acp::ToolKind, body: Option<&str>) -> &'static str {
    if body.is_some_and(looks_like_unified_diff) {
        "diff"
    } else if matches!(kind, acp::ToolKind::Execute) {
        "shell"
    } else {
        ""
    }
}

fn looks_like_unified_diff(body: &str) -> bool {
    let trimmed = body.trim_start();
    trimmed.starts_with("--- ") || trimmed.starts_with("diff --git")
}

fn format_code_details(summary: &str, body: &str, language: &str, max_len: usize) -> String {
    let truncated = truncate_message(body, max_len);
    let fence = code_fence_for(&truncated);
    format!(
        "<details><summary>{}</summary>\n\n{}{language}\n{}\n{}\n\n</details>",
        escape_details_summary(summary),
        fence,
        truncated,
        fence
    )
}

fn format_details(summary: &str, body: &str) -> String {
    format!(
        "<details><summary>{}</summary>\n\n{}\n\n</details>",
        escape_details_summary(summary),
        body
    )
}

fn code_fence_for(body: &str) -> String {
    let longest_run = body
        .split(|ch| ch != '`')
        .filter(|run| !run.is_empty())
        .map(str::len)
        .max()
        .unwrap_or(0);
    "`".repeat(longest_run.max(2) + 1)
}

fn escape_details_summary(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
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
    use super::{format_available_commands_html, format_tool_call};
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
        assert!(text.contains("/<code>search</code>"));
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
    fn formats_multiline_shell_tool_input_in_foldable_code_block() {
        let text = format_tool_call(
            "Run command
cargo test
-- --nocapture",
            acp::ToolKind::Execute,
            acp::ToolCallStatus::InProgress,
            None,
        );

        assert!(text.starts_with("<details><summary>🚧 ▶️ Run command</summary>"));
        assert!(text.contains(
            "```shell
cargo test
-- --nocapture
```"
        ));
    }

    #[test]
    fn keeps_single_line_tool_input_out_of_details() {
        let text = format_tool_call(
            "Run cargo test",
            acp::ToolKind::Execute,
            acp::ToolCallStatus::InProgress,
            None,
        );

        assert!(!text.contains("<details>"));
        assert_eq!(text, "🚧 ▶️ Run cargo test");
    }

    #[test]
    fn formats_diff_tool_details_in_diff_code_block() {
        let text = format_tool_call(
            "keymap.json",
            acp::ToolKind::Edit,
            acp::ToolCallStatus::Completed,
            Some(
                "--- a/keymap.json
+++ b/keymap.json
@@ -1 +1 @@
-old
+new",
            ),
        );

        assert!(text.starts_with("<details><summary>✅ ✏️ keymap.json</summary>"));
        assert!(text.contains(
            "```diff
--- a/keymap.json"
        ));
    }

    #[test]
    fn truncates_large_tool_result_to_rich_message_limit() {
        use super::format_tool_result;

        let text = format_tool_result(
            "Long tool output",
            acp::ToolKind::Execute,
            acp::ToolCallStatus::Completed,
            Some(&"x".repeat(20_000)),
            None,
        );

        assert!(text.len() <= 16_384);
        assert!(text.contains("…[truncated]"));
    }

    #[test]
    fn truncates_tool_name_before_markdown_formatting() {
        let name = format!(
            "{}
{}",
            "a".repeat(600),
            "b".repeat(20_000)
        );
        let text = format_tool_call(
            &name,
            acp::ToolKind::Execute,
            acp::ToolCallStatus::InProgress,
            None,
        );

        assert!(text.contains("<details><summary>"));
        assert!(text.contains("…[truncated]"));
        assert!(text.contains("```shell"));
    }

    #[test]
    fn formats_thought_as_foldable_markdown() {
        let text = super::format_thought_message("I should inspect the code.");

        assert_eq!(
            text,
            "<details><summary>💭 Thought</summary>

I should inspect the code.

</details>"
        );
    }

    #[test]
    fn truncates_available_commands_without_breaking_html() {
        let cmd = acp::AvailableCommand::new("x".repeat(600), "<tag>".repeat(1200));
        let text = format_available_commands_html(&[cmd]);

        assert!(text.contains("/<code>"));
        assert!(text.contains("</code>"));
        assert!(!text.contains("<tag>"));
    }
}
