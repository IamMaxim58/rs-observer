use crate::projection::{LogicalProjectionSummary, StreamStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivePanel {
    Dashboard,
    StreamDetail,
    Logs,
}

#[derive(Debug, Clone)]
pub struct UiState {
    pub selected_stream: usize,
    pub selected_message: usize,
    pub message_scroll_offset: usize,
    pub log_scroll_offset: usize,
    pub active_panel: ActivePanel,
    pub show_shards: bool,
}

impl UiState {
    pub fn new() -> Self {
        Self {
            selected_stream: 0,
            selected_message: 0,
            message_scroll_offset: 0,
            log_scroll_offset: 0,
            active_panel: ActivePanel::Dashboard,
            show_shards: false,
        }
    }

    pub fn select_next(&mut self, stream_count: usize) {
        if stream_count == 0 {
            self.selected_stream = 0;
            return;
        }
        self.selected_stream = (self.selected_stream + 1).min(stream_count - 1);
    }

    pub fn select_previous(&mut self) {
        self.selected_stream = self.selected_stream.saturating_sub(1);
    }

    pub fn select_next_message(&mut self, message_count: usize, visible_rows: usize) {
        if message_count == 0 {
            self.selected_message = 0;
            self.message_scroll_offset = 0;
            return;
        }
        self.selected_message = (self.selected_message + 1).min(message_count - 1);
        self.adjust_message_scroll(message_count, visible_rows);
    }

    pub fn select_previous_message(&mut self, message_count: usize, visible_rows: usize) {
        self.selected_message = self.selected_message.saturating_sub(1);
        self.adjust_message_scroll(message_count, visible_rows);
    }

    pub fn reset_message_scroll(&mut self) {
        self.selected_message = 0;
        self.message_scroll_offset = 0;
    }

    pub fn adjust_message_scroll(&mut self, message_count: usize, visible_rows: usize) {
        self.message_scroll_offset = adjust_scroll_offset(
            self.selected_message,
            message_count,
            visible_rows,
            self.message_scroll_offset,
            2,
        );
    }

    pub fn scroll_logs_down(&mut self, log_count: usize, visible_rows: usize, amount: usize) {
        let max_offset = log_count.saturating_sub(visible_rows);
        self.log_scroll_offset = (self.log_scroll_offset + amount.max(1)).min(max_offset);
    }

    pub fn scroll_logs_up(&mut self, amount: usize) {
        self.log_scroll_offset = self.log_scroll_offset.saturating_sub(amount.max(1));
    }
}

impl Default for UiState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn status_label(status: StreamStatus) -> &'static str {
    match status {
        StreamStatus::Idle => "IDLE",
        StreamStatus::Ok => "OK",
        StreamStatus::HotShard => "HOT",
        StreamStatus::DecodeError => "DECODE ERR",
    }
}

pub fn selected_logical_name(
    summaries: &[LogicalProjectionSummary],
    state: &UiState,
) -> Option<String> {
    summaries
        .get(state.selected_stream)
        .map(|summary| summary.name.clone())
}

pub fn adjust_scroll_offset(
    selected_index: usize,
    item_count: usize,
    visible_rows: usize,
    current_offset: usize,
    edge_rows: usize,
) -> usize {
    if item_count == 0 {
        return 0;
    }

    let selected_index = selected_index.min(item_count - 1);
    if visible_rows <= 1 {
        return selected_index;
    }

    let max_offset = item_count.saturating_sub(visible_rows);
    let current_offset = current_offset.min(max_offset);
    let edge_rows = edge_rows.min((visible_rows - 1) / 2);
    let top_edge = current_offset + edge_rows;
    let bottom_edge = current_offset + visible_rows - edge_rows - 1;

    if selected_index < top_edge {
        selected_index.saturating_sub(edge_rows).min(max_offset)
    } else if selected_index > bottom_edge {
        selected_index
            .saturating_sub(visible_rows - edge_rows - 1)
            .min(max_offset)
    } else {
        current_offset
    }
}
