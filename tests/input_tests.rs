use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rs_observer::app::AppCommand;
use rs_observer::ui::input::command_for_key;
use rs_observer::ui::model::ActivePanel;

#[test]
fn maps_ctrl_d_ctrl_u_and_ctrl_l() {
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL),
            ActivePanel::StreamDetail
        ),
        Some(AppCommand::HalfPageDown)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL),
            ActivePanel::StreamDetail
        ),
        Some(AppCommand::HalfPageUp)
    );
    assert_eq!(
        command_for_key(
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL),
            ActivePanel::Dashboard
        ),
        Some(AppCommand::OpenLogs)
    );
}
