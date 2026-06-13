use serde_json::Value;
use teloxide::prelude::*;
use teloxide::types::{MessageId, Seconds, ThreadId};

const RICH_MESSAGE_LIMIT: usize = 32_768;
const TRUNCATED_SUFFIX: &str = "\n\n…[truncated]";

pub fn prepare_rich_markdown(markdown: &str) -> String {
    truncate_rich_markdown(&sanitize_rich_markdown_urls(markdown))
}

pub fn truncate_rich_markdown(markdown: &str) -> String {
    if markdown.len() <= RICH_MESSAGE_LIMIT {
        return markdown.to_string();
    }

    let cut_len = RICH_MESSAGE_LIMIT.saturating_sub(TRUNCATED_SUFFIX.len());
    let cut = markdown.floor_char_boundary(cut_len);
    format!("{}{}", &markdown[..cut], TRUNCATED_SUFFIX)
}

pub fn sanitize_rich_markdown_urls(markdown: &str) -> String {
    let mut out = String::with_capacity(markdown.len());
    let mut in_fence = false;

    for segment in markdown.split_inclusive('\n') {
        let (line, newline) = segment
            .strip_suffix('\n')
            .map(|line| (line, "\n"))
            .unwrap_or((segment, ""));
        let trimmed = line.trim_start();
        let fence_line = trimmed.starts_with("```") || trimmed.starts_with("~~~");

        if in_fence {
            out.push_str(line);
        } else {
            out.push_str(&sanitize_markdown_links_in_line(line));
        }
        out.push_str(newline);

        if fence_line {
            in_fence = !in_fence;
        }
    }

    out
}

fn sanitize_markdown_links_in_line(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut cursor = 0;

    while cursor < line.len() {
        let Some(open_rel) = line[cursor..].find('[') else {
            out.push_str(&line[cursor..]);
            break;
        };
        let open = cursor + open_rel;
        let image_start = open > cursor && line[..open].ends_with('!');
        let copy_until = if image_start { open - 1 } else { open };
        out.push_str(&line[cursor..copy_until]);

        let Some(close_rel) = line[open + 1..].find(']') else {
            out.push_str(&line[copy_until..]);
            break;
        };
        let close = open + 1 + close_rel;
        let after_close = close + 1;
        if !line[after_close..].starts_with('(') {
            out.push_str(&line[copy_until..after_close]);
            cursor = after_close;
            continue;
        }

        let url_start = after_close + 1;
        let Some(url_end_rel) = line[url_start..].find(')') else {
            out.push_str(&line[copy_until..]);
            break;
        };
        let url_end = url_start + url_end_rel;
        let label = &line[open + 1..close];
        let url = line[url_start..url_end].trim();
        if is_supported_rich_markdown_url(url) {
            out.push_str(&line[copy_until..=url_end]);
        } else if !label.is_empty() {
            out.push_str(label);
        }
        cursor = url_end + 1;
    }

    out
}

fn is_supported_rich_markdown_url(url: &str) -> bool {
    let url = url.trim_matches(['<', '>']);
    let lower = url.to_ascii_lowercase();
    lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("mailto:")
        || lower.starts_with("tel:")
        || lower.starts_with("tg://user?id=")
        || lower.starts_with('#')
}

pub async fn send_rich_markdown(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    markdown: &str,
    silent: bool,
) -> anyhow::Result<Message> {
    let mut body = serde_json::json!({
        "chat_id": chat_id.0,
        "rich_message": {
            "markdown": prepare_rich_markdown(markdown),
        },
        "disable_notification": silent,
    });

    if thread_id != 0 {
        body["message_thread_id"] = serde_json::json!(thread_id);
    }

    call_telegram_method(bot, "sendRichMessage", body).await
}

pub async fn edit_rich_markdown(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    markdown: &str,
) -> anyhow::Result<Message> {
    let body = serde_json::json!({
        "chat_id": chat_id.0,
        "message_id": message_id.0,
        "rich_message": {
            "markdown": prepare_rich_markdown(markdown),
        },
    });

    call_telegram_method(bot, "editMessageText", body).await
}

pub async fn send_rich_markdown_draft(
    bot: &Bot,
    chat_id: ChatId,
    thread_id: i32,
    draft_id: i64,
    markdown: &str,
) -> anyhow::Result<()> {
    let mut body = serde_json::json!({
        "chat_id": chat_id.0,
        "draft_id": draft_id,
        "rich_message": {
            "markdown": prepare_rich_markdown(markdown),
        },
    });

    if thread_id != 0 {
        body["message_thread_id"] = serde_json::json!(thread_id);
    }

    call_telegram_method::<Value>(bot, "sendRichMessageDraft", body).await?;
    Ok(())
}

async fn call_telegram_method<T>(bot: &Bot, method: &str, body: Value) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let url = format!("https://api.telegram.org/bot{}/{}", bot.token(), method);
    let resp = bot.client().post(url).json(&body).send().await?;
    let status = resp.status();
    let payload: Value = resp.json().await?;

    if !status.is_success() || !payload.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        if let Some(after) = retry_after_from_payload(&payload) {
            return Err(teloxide::RequestError::RetryAfter(after).into());
        }
        anyhow::bail!("{method} failed ({status}): {payload}");
    }

    let result = payload
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{method} response did not include result"))?;
    Ok(serde_json::from_value(result)?)
}

fn retry_after_from_payload(payload: &Value) -> Option<Seconds> {
    let seconds = payload
        .get("parameters")?
        .get("retry_after")?
        .as_u64()?
        .min(u32::MAX as u64) as u32;
    Some(Seconds::from_seconds(seconds))
}

#[allow(dead_code)]
pub fn thread_id_from_i32(thread_id: i32) -> ThreadId {
    ThreadId(MessageId(thread_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_unsupported_markdown_link_urls() {
        let input = "- [README.md](/Users/ken/project/README.md)
- [site](https://example.com)
- [mail](mailto:a@example.com)";
        let out = sanitize_rich_markdown_urls(input);

        assert!(out.contains("- README.md"));
        assert!(!out.contains("/Users/ken/project/README.md"));
        assert!(out.contains("[site](https://example.com)"));
        assert!(out.contains("[mail](mailto:a@example.com)"));
    }

    #[test]
    fn leaves_markdown_links_inside_fenced_code_unchanged() {
        let input = "```markdown
[README.md](/Users/ken/project/README.md)
```";
        let out = sanitize_rich_markdown_urls(input);

        assert_eq!(out, input);
    }

    #[test]
    fn prepare_sanitizes_before_truncating() {
        let input = format!(
            "[README.md](/Users/ken/project/README.md){}",
            "x".repeat(40_000)
        );
        let out = prepare_rich_markdown(&input);

        assert!(!out.contains("/Users/ken/project/README.md"));
        assert!(out.len() <= RICH_MESSAGE_LIMIT);
        assert!(out.ends_with("…[truncated]"));
    }

    #[test]
    fn extracts_retry_after_from_telegram_error_payload() {
        let payload = serde_json::json!({
            "ok": false,
            "description": "Too Many Requests: retry after 7",
            "parameters": {
                "retry_after": 7
            }
        });

        assert_eq!(
            retry_after_from_payload(&payload),
            Some(Seconds::from_seconds(7))
        );
    }

    #[test]
    fn rich_markdown_limit_is_larger_than_legacy_text_limit() {
        let input = "x".repeat(10_000);
        assert_eq!(truncate_rich_markdown(&input).len(), 10_000);
    }

    #[test]
    fn truncates_at_rich_message_limit() {
        let input = "x".repeat(40_000);
        let out = truncate_rich_markdown(&input);
        assert!(out.len() <= RICH_MESSAGE_LIMIT);
        assert!(out.ends_with("…[truncated]"));
    }
}
