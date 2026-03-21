use anyhow::Result;
use telegraph_rs::{html_to_node, Telegraph};
use pulldown_cmark::{html, Event, Options, Parser, Tag, TagEnd};

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

/// Create a Telegraph page from Markdown content.
pub async fn create_markdown_post(
    telegraph: &Telegraph,
    title: &str,
    markdown: &str,
) -> Result<String> {
    let html_output = markdown_to_telegraph_html(markdown);
    let content = html_to_node(&html_output);

    let page = telegraph
        .create_page(title, &content, false)
        .await
        .map_err(|e| anyhow::anyhow!("Telegraph API error: {e}"))?;

    Ok(page.url)
}

fn markdown_to_telegraph_html(markdown: &str) -> String {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);

    let parser = Parser::new_ext(markdown, options);

    // State for table collection
    let mut in_table = false;
    let mut in_table_head = false;
    let mut current_cell = String::new();
    let mut current_row: Vec<String> = Vec::new();
    let mut table_head: Vec<String> = Vec::new();
    let mut table_rows: Vec<Vec<String>> = Vec::new();

    // Filtered event stream with tables replaced by <pre> HTML events
    let mut events: Vec<Event<'_>> = Vec::new();

    for event in parser {
        match event {
            Event::Start(Tag::Table(_)) => {
                in_table = true;
                table_head.clear();
                table_rows.clear();
            }
            Event::End(TagEnd::Table) => {
                in_table = false;
                let pre = format!(
                    "<pre>{}</pre>",
                    telegraph_escape(&render_table_as_text(&table_head, &table_rows))
                );
                events.push(Event::Html(pre.into()));
            }
            Event::Start(Tag::TableHead) => {
                in_table_head = true;
                current_row.clear();
            }
            Event::End(TagEnd::TableHead) => {
                in_table_head = false;
                table_head = current_row.clone();
                current_row.clear();
            }
            Event::Start(Tag::TableRow) => {
                current_row.clear();
            }
            Event::End(TagEnd::TableRow) => {
                if !in_table_head {
                    table_rows.push(current_row.clone());
                }
                current_row.clear();
            }
            Event::Start(Tag::TableCell) => {
                current_cell.clear();
            }
            Event::End(TagEnd::TableCell) => {
                current_row.push(current_cell.clone());
                current_cell.clear();
            }
            Event::Text(ref text) if in_table => {
                current_cell.push_str(text);
            }
            Event::Code(ref text) if in_table => {
                current_cell.push_str(text);
            }
            // Task list checkboxes: replace with unicode
            Event::TaskListMarker(checked) => {
                let marker = if checked { "☑ " } else { "☐ " };
                events.push(Event::Text(marker.into()));
            }
            _ if in_table => { /* skip other events inside tables */ }
            other => events.push(other),
        }
    }

    let mut html_output = String::new();
    html::push_html(&mut html_output, events.into_iter());

    // Telegraph only supports h3/h4; map h1→h3, h2→h4, h5/h6→bold paragraph
    // Also map <del> (strikethrough) to <s> which Telegraph supports
    html_output
        .replace("<h1>", "<h3>")
        .replace("</h1>", "</h3>")
        .replace("<h2>", "<h4>")
        .replace("</h2>", "</h4>")
        .replace("<h5>", "<p><b>")
        .replace("</h5>", "</b></p>")
        .replace("<h6>", "<p><b>")
        .replace("</h6>", "</b></p>")
        .replace("<del>", "<s>")
        .replace("</del>", "</s>")
}

fn render_table_as_text(head: &[String], rows: &[Vec<String>]) -> String {
    let num_cols = head
        .len()
        .max(rows.iter().map(|r| r.len()).max().unwrap_or(0));
    if num_cols == 0 {
        return String::new();
    }

    // Column widths
    let mut col_widths: Vec<usize> = (0..num_cols)
        .map(|i| head.get(i).map(|s| s.len()).unwrap_or(0))
        .collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_widths.len() {
                col_widths[i] = col_widths[i].max(cell.len());
            }
        }
    }

    let sep: String = col_widths
        .iter()
        .map(|&w| "-".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("+");
    let sep = format!("+{sep}+");

    let format_row = |cells: &[String]| -> String {
        let parts: Vec<String> = (0..num_cols)
            .map(|i| {
                let cell = cells.get(i).map(|s| s.as_str()).unwrap_or("");
                format!(" {:width$} ", cell, width = col_widths[i])
            })
            .collect();
        format!("|{}|", parts.join("|"))
    };

    let mut out = String::new();
    out.push_str(&sep);
    out.push('\n');
    if !head.is_empty() {
        out.push_str(&format_row(head));
        out.push('\n');
        out.push_str(&sep);
        out.push('\n');
    }
    for row in rows {
        out.push_str(&format_row(row));
        out.push('\n');
    }
    out.push_str(&sep);
    out
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
