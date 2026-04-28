use rs_observer::ui::model::adjust_scroll_offset;

#[test]
fn scroll_offset_does_not_move_while_selection_is_in_middle_region() {
    assert_eq!(adjust_scroll_offset(5, 100, 10, 0, 2), 0);
    assert_eq!(adjust_scroll_offset(6, 100, 10, 0, 2), 0);
    assert_eq!(adjust_scroll_offset(12, 100, 10, 5, 2), 5);
}

#[test]
fn scroll_offset_moves_only_when_selection_enters_edge_zone() {
    assert_eq!(adjust_scroll_offset(8, 100, 10, 0, 2), 1);
    assert_eq!(adjust_scroll_offset(9, 100, 10, 1, 2), 2);
    assert_eq!(adjust_scroll_offset(6, 100, 10, 5, 2), 4);
    assert_eq!(adjust_scroll_offset(5, 100, 10, 4, 2), 3);
}

#[test]
fn scroll_offset_clamps_at_start_and_end() {
    assert_eq!(adjust_scroll_offset(0, 100, 10, 4, 2), 0);
    assert_eq!(adjust_scroll_offset(99, 100, 10, 95, 2), 90);
    assert_eq!(adjust_scroll_offset(200, 12, 5, 99, 2), 7);
}

#[test]
fn scroll_offset_handles_empty_and_tiny_viewports() {
    assert_eq!(adjust_scroll_offset(10, 0, 5, 3, 2), 0);
    assert_eq!(adjust_scroll_offset(10, 100, 0, 0, 2), 10);
    assert_eq!(adjust_scroll_offset(10, 100, 1, 0, 2), 10);
}
