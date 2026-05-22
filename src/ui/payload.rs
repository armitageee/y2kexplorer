use ratatui::text::Line;

/// Пытается распарсить строку как JSON и вернуть pretty-print (2 пробела).
pub fn try_pretty_json(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(trimmed).ok()?;
    serde_json::to_string_pretty(&value).ok()
}

pub fn payload_lines(raw: &str, pretty: bool) -> Vec<Line<'static>> {
    let text = if pretty {
        try_pretty_json(raw).unwrap_or_else(|| raw.to_string())
    } else {
        raw.to_string()
    };
    if text.is_empty() {
        return vec![Line::from("<empty>")];
    }
    text.lines()
        .map(|line| Line::from(line.to_string()))
        .collect()
}
