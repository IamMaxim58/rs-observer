use chrono::{DateTime, Local, Utc};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::{Frame, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Cell, Paragraph, Row, Table, Wrap};

use crate::app::{TimelineRow, TimelineWindow};
use crate::decoder::DecodedMessage;
use crate::projection::LogicalProjectionSummary;
use crate::ui::model::{status_label, ActivePanel, UiState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DetailLayout {
    pub timeline: Rect,
    pub separator: Rect,
    pub inspector: Rect,
}

pub fn render(
    frame: &mut Frame<'_>,
    state: &UiState,
    summaries: &[LogicalProjectionSummary],
    timeline: &TimelineWindow,
    warnings: &[String],
    marker_count: usize,
    bookmark_count: usize,
) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(1),
        ])
        .split(frame.size());

    render_header(
        frame,
        root[0],
        summaries,
        warnings,
        marker_count,
        bookmark_count,
    );
    match state.active_panel {
        ActivePanel::Dashboard => render_dashboard(frame, root[1], state, summaries),
        ActivePanel::StreamDetail => render_detail(frame, root[1], timeline),
        ActivePanel::Logs => render_logs(frame, root[1], state, warnings),
    }
    render_footer(frame, root[2], state);
}

pub fn detail_timeline_visible_rows(area: Rect) -> usize {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(1),
        ])
        .split(area);
    detail_layout(root[1]).timeline.height.saturating_sub(1) as usize
}

pub fn detail_layout(area: Rect) -> DetailLayout {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(60),
            Constraint::Length(1),
            Constraint::Percentage(40),
        ])
        .split(area);

    DetailLayout {
        timeline: chunks[0],
        separator: chunks[1],
        inspector: chunks[2],
    }
}

pub fn body_visible_rows(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

fn render_header(
    frame: &mut Frame<'_>,
    area: Rect,
    summaries: &[LogicalProjectionSummary],
    warnings: &[String],
    marker_count: usize,
    bookmark_count: usize,
) {
    let total_messages: u64 = summaries.iter().map(|summary| summary.new_count).sum();
    let warning = warnings.last().map_or("", String::as_str);
    let text = format!(
        " rs-observer  streams:{}  messages:{}  markers:{}  bookmarks:{}  {}",
        summaries.len(),
        total_messages,
        marker_count,
        bookmark_count,
        warning
    );
    let style = if warnings.is_empty() {
        Style::default().fg(Color::Black).bg(Color::Green)
    } else {
        Style::default().fg(Color::Black).bg(Color::Yellow)
    };
    frame.render_widget(Paragraph::new(text).style(style), area);
}

fn render_dashboard(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &UiState,
    summaries: &[LogicalProjectionSummary],
) {
    let rows = summaries.iter().enumerate().map(|(index, summary)| {
        let status_style = status_style(summary.status);
        let row_style = if index == state.selected_stream {
            Style::default().fg(Color::Black).bg(Color::Cyan)
        } else {
            Style::default()
        };
        Row::new([
            Cell::from(summary.name.clone()),
            Cell::from(if summary.shards.len() > 1 {
                format!("{}x", summary.shards.len())
            } else {
                "single".to_string()
            }),
            Cell::from(summary.new_count.to_string()),
            Cell::from(format!("{:.1}", summary.rate_per_second)),
            Cell::from(
                summary
                    .last_message_at
                    .map(|time| time.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            Cell::from(summary.decode_errors.to_string()),
            Cell::from(
                summary
                    .group_lag
                    .map(|lag| lag.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            Cell::from(summary.pending_count.to_string()),
            Cell::from(Span::styled(status_label(summary.status), status_style)),
        ])
        .style(row_style)
    });

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(30),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(12),
        ],
    )
    .header(
        Row::new([
            "Stream", "Type", "New", "Rate/s", "Last", "Errors", "Lag", "Pending", "Status",
        ])
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    );

    frame.render_widget(table, area);
}

fn render_detail(frame: &mut Frame<'_>, area: Rect, timeline: &TimelineWindow) {
    let layout = detail_layout(area);

    let rows = timeline_rows(timeline);
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(6),
            Constraint::Length(18),
            Constraint::Length(16),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(["Time", "Shard", "ID", "Type", "Decoded"]).style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    );

    frame.render_widget(table, layout.timeline);
    frame.render_widget(
        Paragraph::new("─".repeat(layout.separator.width as usize))
            .style(Style::default().fg(Color::DarkGray)),
        layout.separator,
    );

    let inspector = timeline
        .selected_message
        .as_ref()
        .map(inspector_text)
        .unwrap_or_else(|| "No message selected".to_string());
    frame.render_widget(
        Paragraph::new(inspector).wrap(Wrap { trim: false }),
        layout.inspector,
    );
}

fn timeline_rows(timeline: &TimelineWindow) -> Vec<Row<'static>> {
    timeline
        .rows
        .iter()
        .map(|entry| match entry {
            TimelineRow::Message {
                message,
                search_match,
            } => {
                let style = if timeline.selected_message.as_ref().is_some_and(|selected| {
                    selected.id == message.id && selected.stream == message.stream
                }) {
                    Style::default().fg(Color::Black).bg(Color::Cyan)
                } else if *search_match {
                    Style::default().fg(Color::Black).bg(Color::Yellow)
                } else if message.decode_error.is_some() {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                };
                Row::new([
                    Cell::from(message.observed_at.format("%H:%M:%S").to_string()),
                    Cell::from(
                        message
                            .shard
                            .map(|shard| format!("{shard:02}"))
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    Cell::from(message.id.to_string()),
                    Cell::from(message.message_type.clone()),
                    Cell::from(compact_json(&message.decoded)),
                ])
                .style(style)
            }
            TimelineRow::SessionBoundary {
                session_number,
                started_at,
            } => Row::new([
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
                Cell::from("session"),
                Cell::from(session_boundary_text(*session_number, *started_at)),
            ])
            .style(Style::default().fg(Color::DarkGray)),
        })
        .collect()
}

pub fn session_boundary_text(session_number: u64, started_at: DateTime<Utc>) -> String {
    format!(
        "--- New session #{} at {} ---",
        session_number,
        started_at.with_timezone(&Local).format("%H:%M")
    )
}

fn render_logs(frame: &mut Frame<'_>, area: Rect, state: &UiState, warnings: &[String]) {
    let text = if warnings.is_empty() {
        "No logs yet".to_string()
    } else {
        warnings
            .iter()
            .enumerate()
            .skip(state.log_scroll_offset)
            .map(|(index, warning)| format!("{:>4}  {}", index + 1, warning))
            .collect::<Vec<_>>()
            .join("\n")
    };
    frame.render_widget(
        Paragraph::new(text)
            .style(Style::default().fg(Color::LightYellow))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn inspector_text(message: &DecodedMessage) -> String {
    format!(
        "stream: {}\nlogical: {}\nshard: {}\nid: {}\ntype: {}\nobserved: {}\n\ndecoded:\n{}",
        message.stream,
        message.logical_stream,
        message
            .shard
            .map(|shard| shard.to_string())
            .unwrap_or_else(|| "-".to_string()),
        message.id,
        message.message_type,
        message.observed_at.to_rfc3339(),
        serde_json::to_string_pretty(&message.decoded)
            .unwrap_or_else(|_| message.decoded.to_string())
    )
}

fn compact_json(value: &serde_json::Value) -> String {
    let text = value.to_string();
    if text.len() > 80 {
        format!("{}...", &text[..77])
    } else {
        text
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let text = if let Some(prompt) = &state.prompt {
        let prefix = match prompt.kind {
            crate::ui::model::PromptKind::Search => "/",
            crate::ui::model::PromptKind::Filter => "filter: ",
        };
        format!("{prefix}{}", prompt.draft)
    } else {
        match state.active_panel {
        ActivePanel::Dashboard => " q quit  j/k select  g/G top/bottom  enter open  ^n new  ^l logs  m marker  e export ",
        ActivePanel::StreamDetail => {
            " esc back  j/k message  g/G top/bottom  / search  f filter  n/N match  ^n new  q quit "
        }
        ActivePanel::Logs => " esc back  j/k scroll  g/G top/bottom  ^d/^u half-page  q quit ",
        }
        .to_string()
    };
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::White).bg(Color::DarkGray)),
        area,
    );
}

fn status_style(status: crate::projection::StreamStatus) -> Style {
    match status {
        crate::projection::StreamStatus::Idle => Style::default().fg(Color::DarkGray),
        crate::projection::StreamStatus::Ok => Style::default().fg(Color::Green),
        crate::projection::StreamStatus::HotShard => Style::default().fg(Color::Yellow),
        crate::projection::StreamStatus::DecodeError => Style::default().fg(Color::Red),
    }
}
