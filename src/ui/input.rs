use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::AppCommand;
use crate::ui::model::{ActivePanel, UiState};

pub fn command_for_key(key: KeyEvent, state: &UiState) -> Option<AppCommand> {
    if state.prompt.is_some() {
        return match key.code {
            KeyCode::Enter => Some(AppCommand::SubmitPrompt),
            KeyCode::Esc => Some(AppCommand::CancelPrompt),
            KeyCode::Backspace => Some(AppCommand::PromptBackspace),
            KeyCode::Char(ch)
                if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
            {
                Some(AppCommand::PromptChar(ch))
            }
            _ => None,
        };
    }

    match (key.code, key.modifiers, state.active_panel) {
        (KeyCode::Char('c'), KeyModifiers::CONTROL, _) => Some(AppCommand::Quit),
        (KeyCode::Char('d'), KeyModifiers::CONTROL, _) => Some(AppCommand::HalfPageDown),
        (KeyCode::Char('u'), KeyModifiers::CONTROL, _) => Some(AppCommand::HalfPageUp),
        (KeyCode::Char('l'), KeyModifiers::CONTROL, _) => Some(AppCommand::OpenLogs),
        (KeyCode::Char('n'), KeyModifiers::CONTROL, _) => Some(AppCommand::NewSession),
        (KeyCode::Char('q'), _, _) => Some(AppCommand::Quit),
        (KeyCode::Esc, _, ActivePanel::StreamDetail | ActivePanel::Logs) => Some(AppCommand::Back),
        (KeyCode::Esc, _, ActivePanel::Dashboard) => Some(AppCommand::Quit),
        (KeyCode::Down | KeyCode::Char('j'), _, _) => Some(AppCommand::SelectNext),
        (KeyCode::Up | KeyCode::Char('k'), _, _) => Some(AppCommand::SelectPrevious),
        (KeyCode::Enter, _, ActivePanel::Dashboard) => Some(AppCommand::OpenSelected),
        (KeyCode::Char('n'), _, ActivePanel::StreamDetail) => Some(AppCommand::SearchNext),
        (KeyCode::Char('N'), _, ActivePanel::StreamDetail) => Some(AppCommand::SearchPrevious),
        (KeyCode::Char('/'), _, _) => Some(AppCommand::BeginSearchPrompt),
        (KeyCode::Char('f'), _, _) => Some(AppCommand::BeginFilterPrompt),
        (KeyCode::Char('g'), _, _) => Some(AppCommand::JumpTop),
        (KeyCode::Char('G'), _, _) => Some(AppCommand::JumpBottom),
        (KeyCode::Char('m'), _, _) => Some(AppCommand::AddMarker),
        (KeyCode::Char('b'), _, _) => Some(AppCommand::BookmarkSelected),
        (KeyCode::Char('e'), _, _) => Some(AppCommand::ExportSession),
        (KeyCode::Char('s'), _, _) => Some(AppCommand::ToggleShardView),
        (KeyCode::Char('r'), _, _) => Some(AppCommand::Refresh),
        _ => None,
    }
}
