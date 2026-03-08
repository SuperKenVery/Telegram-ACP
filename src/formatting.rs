/// HTML formatting utilities for Telegram messages.

/// Escape text for Telegram HTML parse mode.
pub fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Format an agent text message for Telegram. Truncates to fit within Telegram's 4096 char limit.
pub fn format_text_message(text: &str) -> String {
    let escaped = escape_html(text);
    truncate_message(&escaped, 4096)
}

/// Format a tool call notification.
pub fn format_tool_call(name: &str) -> String {
    format!("🔧 <b>Tool:</b> <code>{}</code>", escape_html(name))
}

/// Format a tool call result/update.
pub fn format_tool_result(name: &str, output: Option<&str>) -> String {
    match output {
        Some(out) => {
            let escaped = escape_html(out);
            let truncated = truncate_message(&escaped, 3900);
            format!(
                "✅ <b>Tool:</b> <code>{}</code>\n<pre>{}</pre>",
                escape_html(name),
                truncated
            )
        }
        None => format!("✅ <b>Tool:</b> <code>{}</code>", escape_html(name)),
    }
}

/// Format a completion message.
pub fn format_completion(stop_reason: &str, telegraph_url: Option<&str>) -> String {
    let mut msg = format!("✓ <b>Done</b> ({})", escape_html(stop_reason));
    if let Some(url) = telegraph_url {
        msg.push_str(&format!("\n\n📄 <a href=\"{}\">View changes</a>", url));
    }
    msg
}

/// Format an error message.
pub fn format_error(error: &str) -> String {
    format!("❌ <b>Error:</b> {}", escape_html(error))
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
