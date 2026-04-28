use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::AppCommand;
use crate::ui::model::ActivePanel;

pub fn command_for_key(key: KeyEvent, active_panel: ActivePanel) -> Option<AppCommand> {
    match (key.code, key.modifiers, active_panel) {
        (KeyCode::Char('c'), KeyModifiers::CONTROL, _) => Some(AppCommand::Quit),
        (KeyCode::Char('d'), KeyModifiers::CONTROL, _) => Some(AppCommand::HalfPageDown),
        (KeyCode::Char('u'), KeyModifiers::CONTROL, _) => Some(AppCommand::HalfPageUp),
        (KeyCode::Char('l'), KeyModifiers::CONTROL, _) => Some(AppCommand::OpenLogs),
        (KeyCode::Char('q'), _, _) => Some(AppCommand::Quit),
        (KeyCode::Esc, _, ActivePanel::StreamDetail | ActivePanel::Logs) => Some(AppCommand::Back),
        (KeyCode::Esc, _, ActivePanel::Dashboard) => Some(AppCommand::Quit),
        (KeyCode::Down | KeyCode::Char('j'), _, _) => Some(AppCommand::SelectNext),
        (KeyCode::Up | KeyCode::Char('k'), _, _) => Some(AppCommand::SelectPrevious),
        (KeyCode::Enter, _, ActivePanel::Dashboard) => Some(AppCommand::OpenSelected),
        (KeyCode::Char('N'), _, _) | (KeyCode::Char('n'), _, _) => Some(AppCommand::NewSession),
        (KeyCode::Char('m'), _, _) => Some(AppCommand::AddMarker),
        (KeyCode::Char('b'), _, _) => Some(AppCommand::BookmarkSelected),
        (KeyCode::Char('e'), _, _) => Some(AppCommand::ExportSession),
        (KeyCode::Char('s'), _, _) => Some(AppCommand::ToggleShardView),
        (KeyCode::Char('r'), _, _) => Some(AppCommand::Refresh),
        _ => None,
    }
}
