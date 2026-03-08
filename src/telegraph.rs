use anyhow::Result;
use telegraph_rs::{html_to_node, Telegraph};

/// Create a Telegraph page with file changes from an agent session.
pub async fn create_diff_post(
    telegraph: &Telegraph,
    title: &str,
    file_changes: &[(String, String)], // (filename, content/diff)
) -> Result<String> {
    let mut html = String::new();

    for (filename, content) in file_changes {
        html.push_str(&format!(
            "<h4>{}</h4><pre><code>{}</code></pre>",
            telegraph_escape(filename),
            telegraph_escape(content)
        ));
    }

    let content = html_to_node(&html);

    let page = telegraph
        .create_page(title, &content, false)
        .await
        .map_err(|e| anyhow::anyhow!("Telegraph API error: {e}"))?;

    Ok(page.url)
}

/// Create a Telegraph account for the daemon.
pub async fn create_account(author_name: Option<&str>) -> Result<Telegraph> {
    let name = author_name.unwrap_or("Telegram ACP");
    let telegraph = Telegraph::new(name)
        .create()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Telegraph account: {e}"))?;
    Ok(telegraph)
}

fn telegraph_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
