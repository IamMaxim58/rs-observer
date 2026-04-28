use anyhow::{bail, Context, Result};
use crossterm::event::{self, Event};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use rs_observer::app::App;
use rs_observer::config::AppConfig;
use rs_observer::ui::{input, render};
use rs_observer::workers::{bootstrap_baselines, spawn_tail_worker};
use std::io;
use std::time::Duration;
use tokio::sync::mpsc;

fn config_path_from_args() -> Result<String> {
    let mut args = std::env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (Some("--config"), Some(path)) => Ok(path),
        _ => bail!("usage: rs-observer --config <path>"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = config_path_from_args()?;
    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config file `{config_path}`"))?;
    let config = AppConfig::from_yaml(&contents)?;
    let catalog = config.catalog()?;
    let (baselines, warnings) = bootstrap_baselines(&config.redis, &catalog).await;
    let tail_events = spawn_tail_worker(config.redis.clone(), catalog.clone(), baselines.clone());
    let mut app = App::new_with_decoders(catalog, baselines, config.decoders.clone());
    app.warnings.extend(warnings);
    run_tui(app, tail_events)
}

fn run_tui(
    mut app: App,
    mut tail_events: mpsc::UnboundedReceiver<rs_observer::app::WorkerEvent>,
) -> Result<()> {
    enable_raw_mode().context("failed to enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("failed to enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("failed to create terminal")?;

    let result = run_tui_loop(&mut terminal, &mut app, &mut tail_events);

    disable_raw_mode().context("failed to disable raw mode")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)
        .context("failed to leave alternate screen")?;
    terminal.show_cursor().context("failed to show cursor")?;

    result
}

fn run_tui_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    tail_events: &mut mpsc::UnboundedReceiver<rs_observer::app::WorkerEvent>,
) -> Result<()> {
    loop {
        while let Ok(event) = tail_events.try_recv() {
            app.handle_worker_event(event);
        }

        let terminal_area = terminal.size()?;
        app.message_view_rows = render::detail_timeline_visible_rows(terminal_area);
        app.log_view_rows = render::body_visible_rows(terminal_area);

        let summaries = app.summaries();
        let timeline = app.selected_timeline_entries();
        terminal.draw(|frame| {
            render::render(
                frame,
                &app.ui,
                &summaries,
                &timeline,
                &app.warnings,
                app.markers.len(),
                app.bookmarks.len(),
            );
        })?;

        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if let Some(command) = input::command_for_key(key, app.ui.active_panel) {
                    if !app.handle_command(command) {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
