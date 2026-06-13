use serde_json::Value;
use teloxide::prelude::*;
use teloxide::types::{MessageId, ThreadId};

const RICH_MESSAGE_LIMIT: usize = 32_768;
const TRUNCATED_SUFFIX: &str = "\n\n…[truncated]";

pub fn truncate_rich_markdown(markdown: &str) -> String {
    if markdown.len() <= RICH_MESSAGE_LIMIT {
        return markdown.to_string();
    }

    let cut_len = RICH_MESSAGE_LIMIT.saturating_sub(TRUNCATED_SUFFIX.len());
    let cut = markdown.floor_char_boundary(cut_len);
    format!("{}{}", &markdown[..cut], TRUNCATED_SUFFIX)
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
            "markdown": truncate_rich_markdown(markdown),
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
            "markdown": truncate_rich_markdown(markdown),
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
            "markdown": truncate_rich_markdown(markdown),
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
        anyhow::bail!("{method} failed ({status}): {payload}");
    }

    let result = payload
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{method} response did not include result"))?;
    Ok(serde_json::from_value(result)?)
}

#[allow(dead_code)]
pub fn thread_id_from_i32(thread_id: i32) -> ThreadId {
    ThreadId(MessageId(thread_id))
}

#[cfg(test)]
mod tests {
    use super::*;

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
