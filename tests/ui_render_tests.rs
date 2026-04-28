use ratatui::layout::Rect;
use rs_observer::ui::render::{detail_layout, session_boundary_text};

use chrono::{TimeZone, Utc};

#[test]
fn detail_layout_stacks_timeline_above_inspector() {
    let layout = detail_layout(Rect::new(0, 0, 100, 30));

    assert_eq!(layout.timeline.x, 0);
    assert_eq!(layout.timeline.width, 100);
    assert_eq!(layout.separator.x, 0);
    assert_eq!(layout.separator.width, 100);
    assert_eq!(layout.separator.height, 1);
    assert_eq!(layout.inspector.x, 0);
    assert_eq!(layout.inspector.width, 100);
    assert!(layout.timeline.y < layout.separator.y);
    assert!(layout.separator.y < layout.inspector.y);
}

#[test]
fn session_boundary_text_uses_compact_separator_format() {
    let text = session_boundary_text(42, Utc.timestamp_opt(1_714_294_920, 0).unwrap());

    assert!(text.starts_with("--- New session #42 at "));
    assert!(text.ends_with(" ---"));
}
