/// MarkdownV2 formatting utilities for Telegram messages.

/// Escape text for Telegram MarkdownV2 parse mode.
pub fn escape_markdown_v2(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
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
    // Keep model text untouched so valid MarkdownV2 from the agent can render.
    truncate_message(text, 4096)
}

/// Format a tool call notification.
pub fn format_tool_call(name: &str, details: Option<&str>) -> String {
    let header = format!("🔧 *Tool:* `{}`", escape_markdown_v2_code(name));
    match details {
        Some(body) => {
            format!("{header}\n{}", format_collapsible_block(body, 3800))
        }
        None => header,
    }
}

/// Format a tool call result/update.
pub fn format_tool_result(name: &str, output: Option<&str>, details: Option<&str>) -> String {
    match details.or(output) {
        Some(body) => {
            let section = format_collapsible_block(body, 3900);
            format!(
                "✅ *Tool:* `{}`\n{}",
                escape_markdown_v2_code(name),
                section
            )
        }
        None => format!("✅ *Tool:* `{}`", escape_markdown_v2_code(name)),
    }
}

/// Format a completion message.
pub fn format_completion(stop_reason: &str, telegraph_url: Option<&str>) -> String {
    let mut msg = format!("✓ *Done* \\({}\\)", escape_markdown_v2(stop_reason));
    if let Some(url) = telegraph_url {
        msg.push_str(&format!(
            "\n\n📄 [View changes]({})",
            escape_markdown_v2_url(url)
        ));
    }
    msg
}

/// Format an error message.
pub fn format_error(error: &str) -> String {
    format!("❌ *Error:* {}", escape_markdown_v2(error))
}

/// Truncate a message to fit within a maximum length.
/// If truncated, appends "…[truncated]".
fn truncate_message(text: &str, max_len: usize) -> String {
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

fn format_collapsible_block(text: &str, max_len: usize) -> String {
    let escaped = escape_markdown_v2_code(text);
    let truncated = truncate_message(&escaped, max_len);
    format!("```\n{}\n```", truncated)
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
        let cut = remaining[..max_len]
            .rfind('\n')
            .unwrap_or_else(|| remaining.floor_char_boundary(max_len));

        let (chunk, rest) = remaining.split_at(cut);
        chunks.push(chunk.to_string());
        remaining = rest.trim_start_matches('\n');
    }

    chunks
}

fn escape_markdown_v2_code(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '`' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn escape_markdown_v2_url(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            ')' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}
