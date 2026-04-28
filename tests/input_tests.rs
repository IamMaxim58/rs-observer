use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rs_observer::app::AppCommand;
use rs_observer::ui::input::command_for_key;
use rs_observer::ui::model::{ActivePanel, UiState};

#[test]
fn maps_ctrl_d_ctrl_u_and_ctrl_l() {
    let mut state = UiState::new();
    state.active_panel = ActivePanel::StreamDetail;
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL),
            &state
        ),
        Some(AppCommand::HalfPageDown)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL),
            &state
        ),
        Some(AppCommand::HalfPageUp)
    );
    state.active_panel = ActivePanel::Dashboard;
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL),
            &state
        ),
        Some(AppCommand::OpenLogs)
    );
}

#[test]
fn maps_search_filter_jump_and_new_session_keys() {
    let mut state = UiState::new();
    state.active_panel = ActivePanel::StreamDetail;

    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE),
            &state
        ),
        Some(AppCommand::SearchNext)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('N'), KeyModifiers::SHIFT),
            &state
        ),
        Some(AppCommand::SearchPrevious)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL),
            &state
        ),
        Some(AppCommand::NewSession)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE),
            &state
        ),
        Some(AppCommand::BeginSearchPrompt)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE),
            &state
        ),
        Some(AppCommand::BeginFilterPrompt)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE),
            &state
        ),
        Some(AppCommand::JumpTop)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('G'), KeyModifiers::SHIFT),
            &state
        ),
        Some(AppCommand::JumpBottom)
    );
}

#[test]
fn prompt_mode_maps_text_editing_keys() {
    let mut state = UiState::new();
    state.begin_search_prompt();

    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
            &state
        ),
        Some(AppCommand::PromptChar('x'))
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE),
            &state
        ),
        Some(AppCommand::PromptBackspace)
    );
    assert_eq!(
        command_for_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &state),
        Some(AppCommand::SubmitPrompt)
    );
    assert_eq!(
        command_for_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), &state),
        Some(AppCommand::CancelPrompt)
    );
}
