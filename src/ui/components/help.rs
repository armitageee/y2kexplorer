use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::ui::theme;

/// Сколько строк footer выделить под подсказки (status — отдельная строка).
pub fn footer_lines(pairs: &[&str], width: u16, terminal_height: u16, expanded: bool) -> u16 {
    let max = footer_max_lines(terminal_height, expanded);
    let needed = pack_help(pairs, width as usize, max).len();
    needed.max(1).min(max) as u16
}

/// `pairs`: ["key", "desc", "key2", "desc2", ...]
pub fn draw_help(frame: &mut Frame, area: Rect, pairs: &[&str]) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let max_lines = area.height as usize;
    let lines = pack_help(pairs, area.width as usize, max_lines);
    let widget = Paragraph::new(lines).style(theme::footer());
    frame.render_widget(widget, area);
}

fn footer_max_lines(terminal_height: u16, expanded: bool) -> usize {
    if terminal_height < 10 {
        1
    } else if terminal_height < 16 {
        if expanded {
            2
        } else {
            1
        }
    } else if expanded {
        4
    } else {
        2
    }
}

/// Упаковка пар key/desc в строки не шире `width`, не более `max_lines` строк.
fn pack_help(pairs: &[&str], width: usize, max_lines: usize) -> Vec<Line<'static>> {
    let width = width.max(12);
    let max_lines = max_lines.max(1);

    let mut items: Vec<(String, String)> = pairs
        .chunks(2)
        .filter_map(|c| Some(((*c.first()?).to_string(), (*c.get(1)?).to_string())))
        .collect();

    // Переключение темы — всегда в хвосте (`? help` уже в pairs экранов).
    items.push(("T".into(), "theme".into()));

    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut current: Vec<Span> = Vec::new();
    let mut cur_w = 0usize;
    let mut hidden = 0usize;

    let flush =
        |lines: &mut Vec<Line<'static>>, current: &mut Vec<Span<'static>>, cur_w: &mut usize| {
            if !current.is_empty() {
                lines.push(Line::from(std::mem::take(current)));
                *cur_w = 0;
            }
        };

    for (key, desc) in &items {
        let sep = if cur_w == 0 { "" } else { " │ " };
        let segment = format!("{sep} {key} {desc}");
        let seg_w = display_width(&segment);

        if cur_w > 0 && cur_w + seg_w > width {
            if lines.len() + 1 >= max_lines {
                hidden += 1;
                continue;
            }
            flush(&mut lines, &mut current, &mut cur_w);
        }

        if cur_w == 0 && seg_w > width && lines.len() + 1 >= max_lines {
            hidden += 1;
            continue;
        }

        if cur_w == 0 && seg_w > width {
            // Узкий терминал: только ключ.
            let short = format!(" {key}");
            push_key_desc(&mut current, key, "");
            cur_w = display_width(&short);
            if lines.len() + 1 >= max_lines {
                hidden += 1;
            }
            continue;
        }

        if !sep.is_empty() {
            current.push(Span::styled(" │ ", theme::footer_hint()));
        }
        push_key_desc(&mut current, key, desc);
        cur_w += seg_w;
    }

    flush(&mut lines, &mut current, &mut cur_w);

    if hidden > 0 {
        append_overflow(&mut lines, hidden, width, max_lines);
    }

    if lines.is_empty() {
        lines.push(Line::from(vec![
            Span::styled(" ?", theme::footer_key()),
            Span::styled("help", theme::footer_hint()),
        ]));
    }

    lines
}

fn append_overflow(lines: &mut Vec<Line<'static>>, hidden: usize, width: usize, max_lines: usize) {
    let suffix = format!(" … +{hidden}");
    let suffix_w = display_width(&suffix);
    if let Some(last) = lines.last_mut() {
        let line_w: usize = last
            .spans
            .iter()
            .map(|s| display_width(s.content.as_ref()))
            .sum();
        if line_w + suffix_w <= width {
            last.spans.push(Span::styled(suffix, theme::footer_hint()));
            return;
        }
    }
    if lines.len() < max_lines {
        lines.push(Line::from(Span::styled(suffix, theme::footer_hint())));
    } else if let Some(last) = lines.last_mut() {
        // Заменяем хвост последней строки на компактный overflow.
        let compact = format!(" …+{hidden} ?");
        last.spans = vec![Span::styled(compact, theme::footer_hint())];
    }
}

fn push_key_desc(out: &mut Vec<Span<'static>>, key: &str, desc: &str) {
    out.push(Span::styled(format!(" {key}"), theme::footer_key()));
    if !desc.is_empty() {
        out.push(Span::styled(format!(" {desc}"), theme::footer_hint()));
    }
}

fn display_width(s: &str) -> usize {
    s.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_into_multiple_lines_when_narrow() {
        let pairs = &["j/k", "nav", "Enter", "open", "q", "quit"];
        let lines = pack_help(pairs, 28, 4);
        assert!(lines.len() >= 2);
    }

    #[test]
    fn shows_overflow_when_too_many_for_height() {
        let pairs = &[
            "a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "f", "6", "g", "7",
        ];
        let lines = pack_help(pairs, 24, 1);
        assert_eq!(lines.len(), 1);
        let text: String = lines[0].spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(text.contains("…"));
    }

    #[test]
    fn footer_lines_respects_terminal_height() {
        assert_eq!(footer_lines(&["q", "quit"], 80, 8, true), 1);
        assert!(footer_lines(&["q", "quit"], 80, 24, true) >= 1);
    }
}
