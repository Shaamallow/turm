use crossbeam::{
    channel::{Receiver, TryRecvError, unbounded},
    select,
};
use std::time::Duration;
use std::{cmp::min, collections::HashSet, path::PathBuf, process::Command};

use crate::file_watcher::{FileWatcherError, FileWatcherHandle};
use crate::job_acct::JobAcctWatcherHandle;
use crate::job_watcher::JobWatcherHandle;
use crate::utils::{copy_to_clipboard, fit_text, state_color};

use crossterm::event::{Event, KeyCode, KeyEvent, MouseButton, MouseEventKind};
use ratatui::{
    Frame, Terminal,
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, BorderType, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs, Wrap,
    },
};
use std::io;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter, FromRepr};
use tui_input::{Input, backend::crossterm::EventHandler};

pub enum Focus {
    Jobs,
}

pub enum Dialog {
    ConfirmCancelJob(String),
    SelectCancelSignal { id: String, selected_signal: usize },
    EditTimeLimit { id: String, input: Input },
    CommandError { command: String, output: String },
    HelpPopup,
    YankPopup,
    YankResult(String),
}

struct CommandFailure {
    command: String,
    output: String,
}

#[derive(Clone, Copy)]
pub enum ScrollAnchor {
    Top,
    Bottom,
}

#[derive(Default)]
pub enum OutputFileView {
    #[default]
    Stdout,
    Stderr,
}

#[derive(Default, Clone, Copy, Display, FromRepr, EnumIter)]
pub enum SelectedTab {
    #[default]
    #[strum(to_string = "Jobs")]
    Jobs,
    #[strum(to_string = "Sacct")]
    Sacct,
}

impl SelectedTab {
    /// Get the previous tab, if there is no previous tab return the current tab.
    fn previous(self) -> Self {
        let current_index: usize = self as usize;
        let previous_index = current_index.saturating_sub(1);
        Self::from_repr(previous_index).unwrap_or(self)
    }

    /// Get the next tab, if there is no next tab return the current tab.
    fn next(self) -> Self {
        let current_index = self as usize;
        let next_index = current_index.saturating_add(1);
        Self::from_repr(next_index).unwrap_or(self)
    }
}

#[derive(Default, PartialEq)]
pub enum InputMode {
    #[default]
    Normal,
    Search,
}

pub enum DisplayEntry {
    Job(usize),
    CollapsedGroup { index: usize, total: usize },
}

impl DisplayEntry {
    fn job_index(&self) -> usize {
        match self {
            DisplayEntry::Job(i) => *i,
            DisplayEntry::CollapsedGroup { index, .. } => *index,
        }
    }
}

pub struct App {
    focus: Focus,
    dialog: Option<Dialog>,
    selected_tab: SelectedTab,

    jobs: Vec<Job>,
    job_list_state: ListState,
    jobs_reversed: bool,
    job_output: Result<String, FileWatcherError>,
    job_output_anchor: ScrollAnchor,
    job_output_offset: u16,
    job_output_wrap: bool,
    _job_watcher: JobWatcherHandle,
    job_output_watcher: FileWatcherHandle,

    sacct_jobs: Vec<Job>,
    sacct_job_list_state: ListState,
    sacct_reversed: bool,
    _job_acct_watcher: JobAcctWatcherHandle,

    // sender: Sender<AppMessage>,
    receiver: Receiver<AppMessage>,
    input_receiver: Receiver<std::io::Result<Event>>,
    output_file_view: OutputFileView,
    job_list_height: u16,
    job_list_area: Rect,
    job_output_area: Rect,
    pending_input_event: Option<Event>,
    input_mode: InputMode,
    search_query: String,
    filtered_indices: Vec<DisplayEntry>,
    expanded_arrays: HashSet<String>,
    show_qos: bool,
}

pub struct Job {
    pub job_id: String,
    pub array_id: String,
    pub array_step: Option<String>,
    pub name: String,
    pub state: String,
    pub state_compact: String,
    pub reason: Option<String>,
    pub user: String,
    pub time: String,
    pub time_limit: String,
    pub start_time: String,
    pub tres: String,
    pub partition: String,
    pub qos: String,
    pub nodelist: String,
    pub stdout: Option<PathBuf>,
    pub stderr: Option<PathBuf>,
    pub command: String,
}

impl Job {
    fn id(&self) -> String {
        match self.array_step.as_ref() {
            Some(array_step) => format!("{}_{}", self.array_id, array_step),
            None => self.job_id.clone(),
        }
    }
}

pub enum AppMessage {
    Jobs(Vec<Job>),
    SacctJobs(Vec<Job>),
    JobOutput(Result<String, FileWatcherError>),
    Key(KeyEvent),
    MouseClick(usize),
    MouseWheel {
        target: MouseScrollTarget,
        direction: MouseWheelDirection,
        amount: u16,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MouseWheelDirection {
    Up,
    Down,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MouseScrollTarget {
    Jobs,
    Output,
}

const SCANCEL_SIGNALS: &[&str] = &["TERM", "INT", "HUP", "USR1", "USR2", "STOP", "CONT", "KILL"];
const DIALOG_WIDTH: u16 = 80;

impl App {
    pub fn new(
        input_receiver: Receiver<std::io::Result<Event>>,
        slurm_refresh_rate: u64,
        file_refresh_rate: u64,
        squeue_args: Vec<String>,
    ) -> App {
        let (sender, receiver) = unbounded();
        Self {
            focus: Focus::Jobs,
            dialog: None,
            selected_tab: SelectedTab::Jobs,
            jobs: Vec::new(),
            _job_watcher: JobWatcherHandle::new(
                sender.clone(),
                Duration::from_secs(slurm_refresh_rate),
                squeue_args.clone(),
            ),
            job_list_state: {
                let mut s = ListState::default();
                s.select(Some(0));
                s
            },
            jobs_reversed: false,
            job_output: Ok("".to_string()),
            job_output_anchor: ScrollAnchor::Bottom,
            job_output_offset: 0,
            job_output_wrap: false,
            job_output_watcher: FileWatcherHandle::new(
                sender.clone(),
                Duration::from_secs(file_refresh_rate),
            ),

            sacct_jobs: Vec::new(),
            _job_acct_watcher: JobAcctWatcherHandle::new(
                sender.clone(),
                Duration::from_secs(slurm_refresh_rate),
                squeue_args,
            ),
            sacct_job_list_state: {
                let mut s = ListState::default();
                s.select(Some(0));
                s
            },
            sacct_reversed: false,

            // sender,
            receiver,
            input_receiver,
            output_file_view: OutputFileView::default(),
            job_list_height: 0,
            job_list_area: Rect::default(),
            job_output_area: Rect::default(),
            pending_input_event: None,
            input_mode: InputMode::default(),
            search_query: String::new(),
            filtered_indices: Vec::new(),
            expanded_arrays: HashSet::new(),
            show_qos: false,
        }
    }
}

impl App {
    pub fn run<B: Backend<Error = io::Error>>(
        &mut self,
        terminal: &mut Terminal<B>,
    ) -> io::Result<()> {
        terminal.draw(|f| self.ui(f))?;

        loop {
            let (should_quit, should_draw) = if let Some(event) = self.pending_input_event.take() {
                self.handle_input_event(event)
            } else {
                select! {
                    recv(self.receiver) -> event => {
                        self.handle(event.unwrap());
                        (false, true)
                    }
                    recv(self.input_receiver) -> input_res => {
                        self.handle_input_event(input_res.unwrap().unwrap())
                    }
                }
            };
            if should_quit {
                return Ok(());
            }

            if should_draw {
                terminal.draw(|f| self.ui(f))?;
            }
        }
    }

    fn try_recv_input_event(&mut self) -> Option<Event> {
        if let Some(event) = self.pending_input_event.take() {
            return Some(event);
        }

        loop {
            match self.input_receiver.try_recv() {
                Ok(Ok(event)) => return Some(event),
                Ok(Err(_)) => continue,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return None,
            }
        }
    }

    fn handle_input_event(&mut self, event: Event) -> (bool, bool) {
        match event {
            Event::Key(key) => {
                if self.input_mode == InputMode::Normal && key.code == KeyCode::Char('q') {
                    return (true, false);
                }
                self.handle(AppMessage::Key(key));
                (false, true)
            }
            Event::Paste(_) => (false, false),
            Event::Mouse(mouse) => match mouse.kind {
                MouseEventKind::Down(MouseButton::Left) => {
                    if self.dialog.is_some() {
                        return (false, false);
                    }
                    if let Some(index) = self.job_index_at(mouse.column, mouse.row) {
                        if self.job_list_state.selected() != Some(index) {
                            self.handle(AppMessage::MouseClick(index));
                            return (false, true);
                        }
                    }
                    (false, false)
                }
                MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                    if self.dialog.is_some() {
                        return (false, false);
                    }
                    let Some(target) = self.mouse_scroll_target(mouse.column, mouse.row) else {
                        return (false, false);
                    };
                    let direction = mouse_wheel_direction(mouse.kind).unwrap();
                    let mut amount = 1u16;
                    while let Some(next_event) = self.try_recv_input_event() {
                        let should_merge = if let Event::Mouse(next_mouse) = &next_event {
                            mouse_wheel_direction(next_mouse.kind) == Some(direction)
                                && self.mouse_scroll_target(next_mouse.column, next_mouse.row)
                                    == Some(target)
                        } else {
                            false
                        };
                        if should_merge {
                            amount = amount.saturating_add(1);
                        } else {
                            self.pending_input_event = Some(next_event);
                            break;
                        }
                    }
                    self.handle(AppMessage::MouseWheel {
                        target,
                        direction,
                        amount,
                    });
                    (false, true)
                }
                _ => (false, false),
            },
            Event::Resize(_, _) => (false, true),
            _ => (false, false),
        }
    }

    fn mouse_scroll_target(&self, column: u16, row: u16) -> Option<MouseScrollTarget> {
        if rect_contains(self.job_list_area, column, row) {
            Some(MouseScrollTarget::Jobs)
        } else if rect_contains(self.job_output_area, column, row) {
            Some(MouseScrollTarget::Output)
        } else {
            None
        }
    }

    fn handle(&mut self, msg: AppMessage) {
        match msg {
            AppMessage::Jobs(jobs) => {
                // On refresh: keep the same job selected if it still exists
                let old_index = self.job_list_state.selected();
                let old_id = old_index.and_then(|i| self.jobs.get(i)).map(|j| j.id());

                self.jobs = jobs;

                if self.jobs.is_empty() {
                    self.job_list_state.select(None);
                } else if let Some(id) = old_id {
                    let new_index = self
                        .jobs
                        .iter()
                        .position(|j| j.id() == id)
                        .unwrap_or(old_index.unwrap_or(0).min(self.jobs.len() - 1));
                    self.job_list_state.select(Some(new_index));
                } else {
                    self.job_list_state.select_first();
                }
                if matches!(self.selected_tab, SelectedTab::Jobs) {
                    self.recompute_filtered_indices();
                }
            }
            AppMessage::SacctJobs(jobs) => {
                self.sacct_jobs = jobs;
                if matches!(self.selected_tab, SelectedTab::Sacct) {
                    self.recompute_filtered_indices();
                }
            }
            AppMessage::JobOutput(content) => self.job_output = content,
            AppMessage::Key(key) => {
                if self.dialog.is_some() {
                    let mut close_dialog = false;
                    let mut scancel_request = None;
                    let mut timelimit_request = None;
                    let mut command_failure = None;

                    match self.dialog.as_mut().expect("dialog must exist") {
                        Dialog::ConfirmCancelJob(id) => match key.code {
                            KeyCode::Enter | KeyCode::Char('y') => {
                                scancel_request = Some((id.clone(), None));
                                close_dialog = true;
                            }
                            KeyCode::Esc => {
                                close_dialog = true;
                            }
                            _ => {}
                        },
                        Dialog::SelectCancelSignal {
                            id,
                            selected_signal,
                        } => match key.code {
                            KeyCode::Up | KeyCode::Char('k') => {
                                *selected_signal = selected_signal.saturating_sub(1);
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                *selected_signal = min(
                                    selected_signal.saturating_add(1),
                                    SCANCEL_SIGNALS.len().saturating_sub(1),
                                );
                            }
                            KeyCode::Enter => {
                                scancel_request =
                                    Some((id.clone(), Some(SCANCEL_SIGNALS[*selected_signal])));
                                close_dialog = true;
                            }
                            KeyCode::Esc => {
                                close_dialog = true;
                            }
                            KeyCode::Char(c) if c.is_ascii_digit() => {
                                if let Some(index) = signal_index_for_digit(c) {
                                    if index < SCANCEL_SIGNALS.len() {
                                        *selected_signal = index;
                                    }
                                }
                            }
                            _ => {}
                        },
                        Dialog::EditTimeLimit { id, input } => match key.code {
                            KeyCode::Enter => {
                                if let Some(time_limit) = validated_time_limit(input) {
                                    timelimit_request = Some((id.clone(), time_limit));
                                    close_dialog = true;
                                }
                            }
                            KeyCode::Esc => {
                                close_dialog = true;
                            }
                            _ => {
                                input.handle_event(&Event::Key(key));
                            }
                        },
                        Dialog::CommandError { .. } => match key.code {
                            KeyCode::Enter | KeyCode::Esc => {
                                close_dialog = true;
                            }
                            _ => {}
                        },
                        Dialog::HelpPopup => match key.code {
                            KeyCode::Esc | KeyCode::Char('?') => {
                                self.dialog = None;
                            }
                            _ => {}
                        },
                        Dialog::YankPopup => match key.code {
                            KeyCode::Char('o') => {
                                self.yank_path(false);
                            }
                            KeyCode::Char('s') => {
                                self.yank_path(true);
                            }
                            KeyCode::Char('i') => {
                                self.yank_job_id();
                            }
                            KeyCode::Esc | KeyCode::Char('y') => {
                                self.dialog = None;
                            }
                            _ => {}
                        },
                        Dialog::YankResult(_) => {
                            self.dialog = None;
                        }
                    };

                    if let Some((id, signal)) = scancel_request {
                        command_failure = execute_scancel(&id, signal).err();
                    }
                    if let Some((id, time_limit)) = timelimit_request {
                        command_failure = execute_scontrol_update_timelimit(&id, &time_limit).err();
                    }
                    if let Some(CommandFailure { command, output }) = command_failure {
                        self.dialog = Some(Dialog::CommandError { command, output });
                    } else if close_dialog {
                        self.dialog = None;
                    }
                } else {
                    match self.input_mode {
                        InputMode::Normal => match key.code {
                            KeyCode::Char('h') | KeyCode::Left => self.focus_previous_panel(),
                            KeyCode::Char('l') | KeyCode::Right => self.focus_next_panel(),
                            KeyCode::Char('k') | KeyCode::Up => match self.focus {
                                Focus::Jobs => self.select_previous_job(),
                            },
                            KeyCode::Char('j') | KeyCode::Down => match self.focus {
                                Focus::Jobs => self.select_next_job(),
                            },
                            KeyCode::Char('g') => match self.focus {
                                Focus::Jobs => self.select_first_job(),
                            },
                            KeyCode::Char('G') => match self.focus {
                                Focus::Jobs => self.select_last_job(),
                            },
                            KeyCode::Char('u') => match self.focus {
                                Focus::Jobs => {
                                    if key
                                        .modifiers
                                        .contains(crossterm::event::KeyModifiers::CONTROL)
                                    {
                                        self.scroll_jobs_half_page_up()
                                    }
                                }
                            },
                            KeyCode::Char('d') => match self.focus {
                                Focus::Jobs => {
                                    if key
                                        .modifiers
                                        .contains(crossterm::event::KeyModifiers::CONTROL)
                                    {
                                        self.scroll_jobs_half_page_down()
                                    }
                                }
                            },
                            KeyCode::PageDown => {
                                let delta = if key.modifiers.intersects(
                                    crossterm::event::KeyModifiers::SHIFT
                                        | crossterm::event::KeyModifiers::CONTROL
                                        | crossterm::event::KeyModifiers::ALT,
                                ) {
                                    50
                                } else {
                                    1
                                };
                                self.scroll_job_output_down_by(delta);
                            }
                            KeyCode::PageUp => {
                                let delta = if key.modifiers.intersects(
                                    crossterm::event::KeyModifiers::SHIFT
                                        | crossterm::event::KeyModifiers::CONTROL
                                        | crossterm::event::KeyModifiers::ALT,
                                ) {
                                    50
                                } else {
                                    1
                                };
                                self.scroll_job_output_up_by(delta);
                            }
                            KeyCode::Home => {
                                self.job_output_offset = 0;
                                self.job_output_anchor = ScrollAnchor::Top;
                            }
                            KeyCode::End => {
                                self.job_output_offset = 0;
                                self.job_output_anchor = ScrollAnchor::Bottom;
                            }
                            KeyCode::Char('c') => {
                                if let Some(id) = self.selected_job_id() {
                                    self.dialog = Some(Dialog::ConfirmCancelJob(id));
                                }
                            }
                            KeyCode::Char('C') => {
                                if let Some(id) = self.selected_job_id() {
                                    self.dialog = Some(Dialog::SelectCancelSignal {
                                        id,
                                        selected_signal: 0,
                                    });
                                }
                            }
                            KeyCode::Char('t') => {
                                if let Some(job) = self.selected_job() {
                                    self.dialog = Some(Dialog::EditTimeLimit {
                                        id: job.id(),
                                        input: Input::new(job.time_limit.clone()),
                                    });
                                }
                            }
                            KeyCode::Char('y') => {
                                if self.selected_job().is_some() {
                                    self.dialog = Some(Dialog::YankPopup);
                                }
                            }
                            KeyCode::Char('o') => {
                                self.output_file_view = match self.output_file_view {
                                    OutputFileView::Stdout => OutputFileView::Stderr,
                                    OutputFileView::Stderr => OutputFileView::Stdout,
                                };
                            }
                            KeyCode::Char('w') => {
                                self.job_output_wrap = !self.job_output_wrap;
                            }
                            KeyCode::Char('p') => {
                                self.show_qos = !self.show_qos;
                            }
                            KeyCode::Char('a') => {
                                if matches!(self.selected_tab, SelectedTab::Jobs) {
                                    self.toggle_array_collapse();
                                }
                            }
                            KeyCode::Char('r') => match self.selected_tab {
                                SelectedTab::Jobs => {
                                    self.jobs_reversed = !self.jobs_reversed;
                                    self.recompute_filtered_indices();
                                }
                                SelectedTab::Sacct => {
                                    self.sacct_reversed = !self.sacct_reversed;
                                    self.recompute_filtered_indices();
                                }
                            },
                            KeyCode::Char('?') => {
                                self.dialog = match &self.dialog {
                                    Some(Dialog::HelpPopup) => None,
                                    _ => Some(Dialog::HelpPopup),
                                };
                            }
                            KeyCode::Char(']') => self.next_tab(),
                            KeyCode::Char('[') => self.previous_tab(),
                            KeyCode::Char('/') => {
                                self.input_mode = InputMode::Search;
                                self.search_query.clear();
                                self.recompute_filtered_indices();
                            }
                            _ => {}
                        },
                        InputMode::Search => match key.code {
                            KeyCode::Char(c) => {
                                self.search_query.push(c);
                                self.recompute_filtered_indices();
                                let sel = if self.filtered_indices.is_empty() {
                                    None
                                } else {
                                    Some(0)
                                };
                                self.active_list_state().select(sel);
                            }
                            KeyCode::Backspace => {
                                self.search_query.pop();
                                self.recompute_filtered_indices();
                                let sel = if self.filtered_indices.is_empty() {
                                    None
                                } else {
                                    Some(0)
                                };
                                self.active_list_state().select(sel);
                            }
                            KeyCode::Esc => {
                                self.input_mode = InputMode::Normal;
                                self.search_query.clear();
                                self.recompute_filtered_indices();
                            }
                            KeyCode::Enter => {
                                self.input_mode = InputMode::Normal;
                            }
                            KeyCode::Down => self.select_next_job(),
                            KeyCode::Up => self.select_previous_job(),
                            _ => {}
                        },
                    }
                }
            }
            AppMessage::MouseClick(index) => {
                if self.dialog.is_none() && index < self.jobs.len() {
                    self.job_list_state.select(Some(index));
                }
            }
            AppMessage::MouseWheel {
                target,
                direction,
                amount,
            } => {
                if self.dialog.is_none() {
                    match target {
                        MouseScrollTarget::Jobs => match direction {
                            MouseWheelDirection::Up => self.job_list_state.scroll_up_by(amount),
                            MouseWheelDirection::Down => self.job_list_state.scroll_down_by(amount),
                        },
                        MouseScrollTarget::Output => match direction {
                            MouseWheelDirection::Up => self.scroll_job_output_up_by(amount),
                            MouseWheelDirection::Down => self.scroll_job_output_down_by(amount),
                        },
                    }
                }
            }
        }

        // update
        self.job_output_watcher
            .set_file_path(
                self.selected_job()
                    .and_then(|j| match self.output_file_view {
                        OutputFileView::Stdout => j.stdout.clone(),
                        OutputFileView::Stderr => j.stderr.clone(),
                    }),
            );
    }

    fn ui(&mut self, f: &mut Frame) {
        // Layout

        let content_help = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(1)].as_ref())
            .split(f.area());

        let master_detail = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(50), Constraint::Percentage(70)].as_ref())
            .split(content_help[0]);

        let tabs_content = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(1)].as_ref())
            .split(master_detail[0]);

        let job_detail_log = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(8), Constraint::Min(3)].as_ref())
            .split(master_detail[1]);

        // Help / Search bar
        let blue_style = Style::default().fg(Color::Blue);
        let light_blue_style = Style::default().fg(Color::LightBlue);

        let help = if self.input_mode == InputMode::Search {
            Paragraph::new(Line::from(vec![
                Span::styled("/", blue_style),
                Span::raw(&self.search_query),
                Span::styled("█", Style::default().fg(Color::Gray)),
            ]))
        } else {
            let help_options = [
                ("q", "quit"),
                ("⏶/⏷", "navigate"),
                ("esc", "cancel"),
                ("enter", "confirm"),
                ("/", "search"),
                ("?", "help"),
            ];

            let help = Line::from(help_options.iter().fold(
                Vec::new(),
                |mut acc, (key, description)| {
                    if !acc.is_empty() {
                        acc.push(Span::raw(" | "));
                    }
                    acc.push(Span::styled(*key, blue_style));
                    acc.push(Span::raw(": "));
                    acc.push(Span::styled(*description, light_blue_style));
                    acc
                },
            ));

            Paragraph::new(help)
        };
        f.render_widget(help, content_help[1]);

        // Jobs
        let max_id_len = self.jobs.iter().map(|j| j.id().len()).max().unwrap_or(0);
        let max_user_len = self.jobs.iter().map(|j| j.user.len()).max().unwrap_or(0);
        let max_partition_len = self
            .jobs
            .iter()
            .map(|j| if self.show_qos { j.qos.len() } else { j.partition.len() })
            .max()
            .unwrap_or(0);
        let max_time_len = self.jobs.iter().map(|j| j.time.len()).max().unwrap_or(0);
        let max_state_compact_len = self
            .jobs
            .iter()
            .map(|j| j.state_compact.len())
            .max()
            .unwrap_or(0);
        let jobs: Vec<ListItem> = self
            .filtered_indices
            .iter()
            .filter_map(|entry| match entry {
                DisplayEntry::Job(i) => self.jobs.get(*i).map(|j| {
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!(
                                "{:<max$.max$}",
                                j.state_compact,
                                max = max_state_compact_len
                            ),
                            Style::default().fg(state_color(&j.state_compact)),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:<max$.max$}", j.id(), max = max_id_len),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:<max$.max$}", if self.show_qos { &j.qos } else { &j.partition }, max = max_partition_len),
                            Style::default().fg(Color::Blue),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:<max$.max$}", j.user, max = max_user_len),
                            Style::default().fg(Color::Green),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:>max$.max$}", j.time, max = max_time_len),
                            Style::default().fg(Color::Red),
                        ),
                        Span::raw(" "),
                        Span::raw(&j.name),
                    ]))
                }),
                DisplayEntry::CollapsedGroup { index, total } => self.jobs.get(*index).map(|j| {
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!(
                                "{:<max$.max$}",
                                j.state_compact,
                                max = max_state_compact_len
                            ),
                            Style::default().fg(state_color(&j.state_compact)),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:<max$.max$}", j.array_id, max = max_id_len),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:<max$.max$}", if self.show_qos { &j.qos } else { &j.partition }, max = max_partition_len),
                            Style::default().fg(Color::Blue),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:<max$.max$}", j.user, max = max_user_len),
                            Style::default().fg(Color::Green),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:>max$.max$}", j.time, max = max_time_len),
                            Style::default().fg(Color::Red),
                        ),
                        Span::raw(" "),
                        Span::styled(
                            format!("▶ {} [{}]", j.name, total),
                            Style::default().add_modifier(Modifier::DIM),
                        ),
                    ]))
                }),
            })
            .collect();
        let job_list_title = if self.search_query.is_empty() {
            format!("Jobs ({})", self.jobs.len())
        } else {
            format!("Jobs ({}/{})", self.filtered_indices.len(), self.jobs.len())
        };
        let job_list = List::new(jobs)
            .block(
                Block::default()
                    .title(format!("─{}", job_list_title))
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(if self.dialog.is_some() {
                        Style::default()
                    } else {
                        match self.focus {
                            Focus::Jobs => Style::default().fg(Color::Green),
                        }
                    }),
            )
            .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

        // Sacct Jobs
        let max_id_len = self
            .sacct_jobs
            .iter()
            .map(|j| j.id().len())
            .max()
            .unwrap_or(0);
        let max_user_len = self
            .sacct_jobs
            .iter()
            .map(|j| j.user.len())
            .max()
            .unwrap_or(0);
        let max_partition_len = self
            .sacct_jobs
            .iter()
            .map(|j| if self.show_qos { j.qos.len() } else { j.partition.len() })
            .max()
            .unwrap_or(0);
        let max_time_len = self
            .sacct_jobs
            .iter()
            .map(|j| j.time.len())
            .max()
            .unwrap_or(0);
        let max_state_compact_len = self
            .sacct_jobs
            .iter()
            .map(|j| j.state_compact.len())
            .max()
            .unwrap_or(0);
        let old_jobs: Vec<ListItem> = self
            .filtered_indices
            .iter()
            .filter_map(|entry| self.sacct_jobs.get(entry.job_index()))
            .map(|j| {
                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!(
                            "{:<max$.max$}",
                            j.state_compact,
                            max = max_state_compact_len
                        ),
                        Style::default().fg(state_color(&j.state_compact)),
                    ),
                    Span::raw(" "),
                    Span::styled(
                        format!("{:<max$.max$}", j.id(), max = max_id_len),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::raw(" "),
                    Span::styled(
                        format!("{:<max$.max$}", if self.show_qos { &j.qos } else { &j.partition }, max = max_partition_len),
                        Style::default().fg(Color::Blue),
                    ),
                    Span::raw(" "),
                    Span::styled(
                        format!("{:<max$.max$}", j.user, max = max_user_len),
                        Style::default().fg(Color::Green),
                    ),
                    Span::raw(" "),
                    Span::styled(
                        format!("{:>max$.max$}", j.time, max = max_time_len),
                        Style::default().fg(Color::Red),
                    ),
                    Span::raw(" "),
                    Span::raw(&j.name),
                ]))
            })
            .collect();

        let sacct_list_title = if self.search_query.is_empty() {
            format!("Sacct ({})", self.sacct_jobs.len())
        } else {
            format!(
                "Sacct ({}/{})",
                self.filtered_indices.len(),
                self.sacct_jobs.len()
            )
        };
        let past_jobs_list = List::new(old_jobs)
            .block(
                Block::default()
                    .title(sacct_list_title)
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(if self.dialog.is_some() {
                        Style::default()
                    } else {
                        match self.focus {
                            Focus::Jobs => Style::default().fg(Color::Green),
                        }
                    }),
            )
            .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

        // Enum iterator
        let titles = SelectedTab::iter().map(|x| x.to_string());
        let selected_tab_index = self.selected_tab as usize;

        let tabs = Tabs::new(titles)
            .block(Block::bordered().title("Tabs"))
            .highlight_style(Style::default().fg(Color::Green))
            .select(selected_tab_index)
            .divider(" - ")
            .padding("", "");

        f.render_widget(tabs, tabs_content[0]);
        match self.selected_tab {
            SelectedTab::Jobs => {
                f.render_stateful_widget(job_list, tabs_content[1], &mut self.job_list_state);
                self.job_list_height = tabs_content[1].height.saturating_sub(2);
                self.job_list_area = tabs_content[1];
            }
            SelectedTab::Sacct => {
                f.render_stateful_widget(
                    past_jobs_list,
                    tabs_content[1],
                    &mut self.sacct_job_list_state,
                );
                self.job_list_height = tabs_content[1].height.saturating_sub(2);
                self.job_list_area = tabs_content[1];
            }
        }

        // Job details

        let job_detail = self.selected_job();

        let job_detail = job_detail.map(|j| {
            let mut state_spans = vec![
                Span::styled("State  ", Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(&j.state),
            ];
            if j.state == "PENDING" {
                state_spans.extend([
                    Span::styled(" Start ", Style::default().fg(Color::Yellow)),
                    Span::raw(&j.start_time),
                ]);
            }
            if let Some(s) = j.reason.as_deref() {
                state_spans.extend([
                    Span::styled(" Reason ", Style::default().fg(Color::Yellow)),
                    Span::raw(s),
                ]);
            }
            let state = Line::from(state_spans);
            let name = Line::from(vec![
                Span::styled("Name   ", Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(&j.name),
            ]);
            let command = Line::from(vec![
                Span::styled("Command", Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(&j.command),
            ]);
            let nodes = Line::from(vec![
                Span::styled("Nodes  ", Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(&j.nodelist),
            ]);
            let tres = Line::from(vec![
                Span::styled("TRES   ", Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(&j.tres),
            ]);
            let ui_stdout_text = match self.output_file_view {
                OutputFileView::Stdout => "stdout ",
                OutputFileView::Stderr => "stderr ",
            };
            let stdout = Line::from(vec![
                Span::styled(ui_stdout_text, Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(
                    match self.output_file_view {
                        OutputFileView::Stdout => &j.stdout,
                        OutputFileView::Stderr => &j.stderr,
                    }
                    .as_ref()
                    .map(|p| p.to_str().unwrap_or_default())
                    .unwrap_or_default(),
                ),
            ]);

            Text::from(vec![state, name, command, nodes, tres, stdout])
        });
        let job_detail = Paragraph::new(job_detail.unwrap_or_default()).block(
            Block::default()
                .title("─Details")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded),
        );
        f.render_widget(job_detail, job_detail_log[0]);

        // Log
        let log_area = job_detail_log[1];
        self.job_output_area = log_area;
        let log_title = Line::from(vec![
            Span::raw("─"),
            Span::raw(match self.output_file_view {
                OutputFileView::Stdout => "stdout",
                OutputFileView::Stderr => "stderr",
            }),
            Span::styled(
                match self.job_output_anchor {
                    ScrollAnchor::Top if self.job_output_offset == 0 => "[T]".to_string(),
                    ScrollAnchor::Top => format!("[T+{}]", self.job_output_offset),
                    ScrollAnchor::Bottom if self.job_output_offset == 0 => "".to_string(),
                    ScrollAnchor::Bottom => format!("[B-{}]", self.job_output_offset),
                },
                Style::default().add_modifier(Modifier::DIM),
            ),
        ]);
        let log_block = Block::default().title(log_title).borders(Borders::ALL);
        let log_block = log_block.border_type(BorderType::Rounded);

        // let job_log = self.job_stdout.as_deref().map(|s| {
        //     string_for_paragraph(
        //         s,
        //         log_block.inner(log_area).height as usize,
        //         log_block.inner(log_area).width as usize,
        //         self.job_stdout_offset as usize,
        //     )
        // }).unwrap_or_else(|e| {
        //     self.job_stdout_offset = 0;
        //     "".to_string()
        // });

        let log = match self.job_output.as_deref() {
            Ok(s) => Paragraph::new(fit_text(
                s,
                log_block.inner(log_area).height as usize,
                log_block.inner(log_area).width as usize,
                self.job_output_anchor,
                self.job_output_offset as usize,
                self.job_output_wrap,
            )),
            Err(e) => Paragraph::new(e.to_string())
                .style(Style::default().fg(Color::Red))
                .wrap(Wrap { trim: true }),
        }
        .block(log_block);

        f.render_widget(log, log_area);

        if let Some(dialog) = &self.dialog {
            fn centered_dialog_area(width: u16, lines: u16, viewport: Rect) -> Rect {
                let dialog_width = min(width, viewport.width);
                let dialog_height = min(lines, viewport.height);
                let dialog_x = viewport.x + viewport.width.saturating_sub(dialog_width) / 2;
                let dialog_y = viewport.y + viewport.height.saturating_sub(dialog_height) / 2;

                Rect::new(dialog_x, dialog_y, dialog_width, dialog_height)
            }

            match dialog {
                Dialog::ConfirmCancelJob(id) => {
                    let dialog = Paragraph::new(Line::from(vec![
                        Span::raw("Cancel job "),
                        Span::styled(id, Style::default().add_modifier(Modifier::BOLD)),
                        Span::raw("?"),
                    ]))
                    .style(Style::default().fg(Color::White))
                    .wrap(Wrap { trim: true })
                    .block(
                        Block::default()
                            .title("─Cancel")
                            .borders(Borders::ALL)
                            .border_type(BorderType::Rounded)
                            .style(Style::default().fg(Color::Green)),
                    );

                    let area = centered_dialog_area(DIALOG_WIDTH, 3, f.area());
                    f.render_widget(Clear, area);
                    f.render_widget(dialog, area);
                }
                Dialog::SelectCancelSignal {
                    id,
                    selected_signal,
                } => {
                    let mut rows = vec![
                        Line::from(vec![
                            Span::raw("Send signal to job "),
                            Span::styled(id, Style::default().add_modifier(Modifier::BOLD)),
                            Span::raw(":"),
                        ]),
                        Line::default(),
                    ];
                    rows.extend(SCANCEL_SIGNALS.iter().enumerate().map(|(i, signal)| {
                        let signal_style = if i == *selected_signal {
                            Style::default().fg(Color::Black).bg(Color::Green)
                        } else {
                            Style::default()
                        };
                        let shortcut_style = signal_style.add_modifier(Modifier::DIM);
                        Line::from(vec![
                            Span::styled(format!("{}. ", i + 1), shortcut_style),
                            Span::styled(*signal, signal_style),
                        ])
                    }));

                    let dialog = Paragraph::new(Text::from(rows))
                        .style(Style::default().fg(Color::White))
                        .wrap(Wrap { trim: true })
                        .block(
                            Block::default()
                                .title("─Signal")
                                .borders(Borders::ALL)
                                .border_type(BorderType::Rounded)
                                .style(Style::default().fg(Color::Green)),
                        );

                    let area = centered_dialog_area(
                        DIALOG_WIDTH,
                        SCANCEL_SIGNALS.len() as u16 + 4,
                        f.area(),
                    );
                    f.render_widget(Clear, area);
                    f.render_widget(dialog, area);
                }
                Dialog::EditTimeLimit { id, input } => {
                    let block = Block::default()
                        .title("─Time Limit")
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .style(Style::default().fg(Color::Green));

                    let area = centered_dialog_area(DIALOG_WIDTH, 3, f.area());
                    let inner = block.inner(area);
                    let prompt_prefix = "Set time limit for job ";
                    let prompt_suffix = ": ";
                    let prompt_width = (prompt_prefix.chars().count()
                        + id.chars().count()
                        + prompt_suffix.chars().count())
                        as u16;
                    let available_width = inner.width.saturating_sub(prompt_width).max(1) as usize;
                    let scroll = input.visual_scroll(available_width);
                    let visible_value = input
                        .value()
                        .chars()
                        .skip(scroll)
                        .take(available_width)
                        .collect::<String>();
                    let dialog = Paragraph::new(Line::from(vec![
                        Span::raw(prompt_prefix),
                        Span::styled(id, Style::default().add_modifier(Modifier::BOLD)),
                        Span::raw(prompt_suffix),
                        Span::styled(visible_value, Style::default().fg(Color::Blue)),
                    ]))
                    .style(Style::default().fg(Color::White))
                    .block(block);

                    f.render_widget(Clear, area);
                    f.render_widget(dialog, area);

                    let cursor_offset = input.visual_cursor().saturating_sub(scroll) as u16;
                    let cursor_x = inner
                        .x
                        .saturating_add(prompt_width)
                        .saturating_add(cursor_offset)
                        .min(inner.x.saturating_add(inner.width.saturating_sub(1)));
                    let cursor_y = inner.y;
                    f.set_cursor_position((cursor_x, cursor_y));
                }
                Dialog::CommandError { command, output } => {
                    let dialog_text = format!("Command: {command}\n\n{output}");
                    let lines = dialog_text
                        .lines()
                        .count()
                        .saturating_add(2)
                        .min(u16::MAX as usize) as u16;
                    let dialog = Paragraph::new(dialog_text)
                        .style(Style::default().fg(Color::White))
                        .wrap(Wrap { trim: false })
                        .block(
                            Block::default()
                                .title("─Command Error")
                                .borders(Borders::ALL)
                                .border_type(BorderType::Rounded)
                                .style(Style::default().fg(Color::Red)),
                        );

                    let area = centered_dialog_area(DIALOG_WIDTH, lines, f.area());
                    f.render_widget(Clear, area);
                    f.render_widget(dialog, area);
                }
                Dialog::HelpPopup => {
                    let blue_style = Style::default().fg(Color::Blue);
                    let light_blue_style = Style::default().fg(Color::LightBlue);

                    let shortcuts = [
                        ("q", "quit", "[/]", "prev/next tab"),
                        ("j/k ⏶/⏷", "navigate", "/", "search"),
                        ("pgup/down", "scroll", "c/C", "cancel/signal"),
                        ("home/end", "top/bottom", "t", "set time limit"),
                        ("w", "toggle wrap", "o", "toggle stdout/stderr"),
                        ("p", "partition/qos", "a", "expand/collapse"),
                        ("r", "reverse order", "y", "yank path"),
                        ("?", "show/hide help", "", ""),
                    ];

                    let gap = 2;
                    let w1 = shortcuts.iter().map(|(k, _, _, _)| k.chars().count()).max().unwrap_or(0);
                    let w2 = shortcuts.iter().map(|(_, d, _, _)| d.chars().count()).max().unwrap_or(0);
                    let w3 = shortcuts.iter().map(|(_, _, k, _)| k.chars().count()).max().unwrap_or(0);
                    let w4 = shortcuts.iter().map(|(_, _, _, d)| d.chars().count()).max().unwrap_or(0);

                    let lines: Vec<Line> = shortcuts
                        .iter()
                        .map(|(k1, d1, k2, d2)| {
                            Line::from(vec![
                                Span::styled(format!("{:<w$}", k1, w = w1 + gap), blue_style),
                                Span::styled(format!("{:<w$}", d1, w = w2 + gap), light_blue_style),
                                Span::styled(format!("{:<w$}", k2, w = w3 + gap), blue_style),
                                Span::styled(*d2, light_blue_style),
                            ])
                        })
                        .collect();

                    let dialog = Paragraph::new(lines)
                        .style(Style::default().fg(Color::White))
                        .block(
                            Block::default()
                                .title("Keybindings")
                                .borders(Borders::ALL)
                                .style(Style::default().fg(Color::Blue)),
                        );

                    let content_width = w1 + gap + w2 + gap + w3 + gap + w4;
                    let dialog_width = content_width as u16 + 2; // +2 for borders
                    let area = centered_dialog_area(dialog_width, (shortcuts.len() + 2) as u16, f.area());
                    f.render_widget(Clear, area);
                    f.render_widget(dialog, area);
                }
                Dialog::YankPopup => {
                    let blue_style = Style::default().fg(Color::Blue);
                    let light_blue_style = Style::default().fg(Color::LightBlue);

                    let lines = vec![
                        Line::from(vec![
                            Span::styled("o", blue_style),
                            Span::styled("  stdout path", light_blue_style),
                        ]),
                        Line::from(vec![
                            Span::styled("s", blue_style),
                            Span::styled("  stderr path", light_blue_style),
                        ]),
                        Line::from(vec![
                            Span::styled("i", blue_style),
                            Span::styled("  job id", light_blue_style),
                        ]),
                    ];

                    let dialog = Paragraph::new(lines)
                        .style(Style::default().fg(Color::White))
                        .block(
                            Block::default()
                                .title("Yank")
                                .borders(Borders::ALL)
                                .style(Style::default().fg(Color::Blue)),
                        );

                    let area = centered_dialog_area(20, 5, f.area());
                    f.render_widget(Clear, area);
                    f.render_widget(dialog, area);
                }
                Dialog::YankResult(msg) => {
                    let dialog = Paragraph::new(Line::from(msg.as_str()))
                        .style(Style::default().fg(Color::White))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .style(Style::default().fg(Color::Green)),
                        );

                    let area = centered_dialog_area(80, 3, f.area());
                    f.render_widget(Clear, area);
                    f.render_widget(dialog, area);
                }
            }
        }
    }
}

impl App {
    fn focus_next_panel(&mut self) {
        match self.focus {
            Focus::Jobs => self.focus = Focus::Jobs,
        }
    }

    fn focus_previous_panel(&mut self) {
        match self.focus {
            Focus::Jobs => self.focus = Focus::Jobs,
        }
    }

    fn next_tab(&mut self) {
        self.selected_tab = self.selected_tab.next();
        self.search_query.clear();
        self.recompute_filtered_indices();
    }

    fn previous_tab(&mut self) {
        self.selected_tab = self.selected_tab.previous();
        self.search_query.clear();
        self.recompute_filtered_indices();
    }

    fn active_list_state(&mut self) -> &mut ListState {
        match self.selected_tab {
            SelectedTab::Jobs => &mut self.job_list_state,
            SelectedTab::Sacct => &mut self.sacct_job_list_state,
        }
    }

    fn select_next_job(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }
        let state = self.active_list_state();
        let i = match state.selected() {
            Some(i) => {
                if i >= self.filtered_indices.len() - 1 {
                    self.filtered_indices.len() - 1
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.active_list_state().select(Some(i));
    }

    fn select_previous_job(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }
        let state = self.active_list_state();
        let i = match state.selected() {
            Some(i) => {
                if i == 0 {
                    0
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.active_list_state().select(Some(i));
    }

    fn select_first_job(&mut self) {
        self.active_list_state().select_first();
    }

    fn select_last_job(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }
        let last = self.filtered_indices.len() - 1;
        match self.selected_tab {
            SelectedTab::Jobs => &mut self.job_list_state,
            SelectedTab::Sacct => &mut self.sacct_job_list_state,
        }
        .select(Some(last));
    }

    fn scroll_jobs_half_page_down(&mut self) {
        let amount = self.job_list_height / 2;
        match self.selected_tab {
            SelectedTab::Jobs => &mut self.job_list_state,
            SelectedTab::Sacct => &mut self.sacct_job_list_state,
        }
        .scroll_down_by(amount);
    }

    fn scroll_jobs_half_page_up(&mut self) {
        let amount = self.job_list_height / 2;
        match self.selected_tab {
            SelectedTab::Jobs => &mut self.job_list_state,
            SelectedTab::Sacct => &mut self.sacct_job_list_state,
        }
        .scroll_up_by(amount);
    }

    fn job_index_at(&self, column: u16, row: u16) -> Option<usize> {
        if self.jobs.is_empty() {
            return None;
        }
        let inner = Rect::new(
            self.job_list_area.x.saturating_add(1),
            self.job_list_area.y.saturating_add(1),
            self.job_list_area.width.saturating_sub(2),
            self.job_list_area.height.saturating_sub(2),
        );
        if !rect_contains(inner, column, row) {
            return None;
        }

        let row_in_list = (row - inner.y) as usize;
        let index = self.job_list_state.offset().saturating_add(row_in_list);
        (index < self.jobs.len()).then_some(index)
    }

    fn scroll_job_output_down_by(&mut self, delta: u16) {
        match self.job_output_anchor {
            ScrollAnchor::Top => {
                self.job_output_offset = self.job_output_offset.saturating_add(delta)
            }
            ScrollAnchor::Bottom => {
                self.job_output_offset = self.job_output_offset.saturating_sub(delta)
            }
        }
    }

    fn scroll_job_output_up_by(&mut self, delta: u16) {
        match self.job_output_anchor {
            ScrollAnchor::Top => {
                self.job_output_offset = self.job_output_offset.saturating_sub(delta)
            }
            ScrollAnchor::Bottom => {
                self.job_output_offset = self.job_output_offset.saturating_add(delta)
            }
        }
    }

    fn selected_job(&self) -> Option<&Job> {
        let state = match self.selected_tab {
            SelectedTab::Jobs => &self.job_list_state,
            SelectedTab::Sacct => &self.sacct_job_list_state,
        };
        let jobs = match self.selected_tab {
            SelectedTab::Jobs => &self.jobs,
            SelectedTab::Sacct => &self.sacct_jobs,
        };
        state
            .selected()
            .and_then(|i| self.filtered_indices.get(i))
            .and_then(|entry| jobs.get(entry.job_index()))
    }

    fn selected_job_id(&self) -> Option<String> {
        self.selected_job().map(Job::id)
    }

    fn recompute_filtered_indices(&mut self) {
        let jobs = match self.selected_tab {
            SelectedTab::Jobs => &self.jobs,
            SelectedTab::Sacct => &self.sacct_jobs,
        };
        let query = self.search_query.to_lowercase();
        let mut base_indices: Vec<usize> = if query.is_empty() {
            (0..jobs.len()).collect()
        } else {
            jobs.iter()
                .enumerate()
                .filter(|(_, j)| j.name.to_lowercase().contains(&query))
                .map(|(i, _)| i)
                .collect()
        };

        // On the Jobs tab, collapse PENDING array groups
        if matches!(self.selected_tab, SelectedTab::Jobs) {
            if self.jobs_reversed {
                base_indices.reverse();
            }

            use std::collections::HashMap;

            // Count PENDING entries per array_id (only for array jobs)
            let mut pending_counts: HashMap<&str, usize> = HashMap::new();
            for &idx in &base_indices {
                let job = &jobs[idx];
                if job.array_step.is_some() && job.state == "PENDING" {
                    *pending_counts.entry(&job.array_id).or_default() += 1;
                }
            }

            // Determine which array_ids should have their PENDING subset collapsed
            let mut collapsed_ids: HashSet<&str> = HashSet::new();
            for (array_id, count) in &pending_counts {
                if *count > 1 && !self.expanded_arrays.contains(*array_id) {
                    collapsed_ids.insert(array_id);
                }
            }

            // Build display entries: collapse only the PENDING entries of an array
            // into a single group header; non-PENDING entries (e.g. RUNNING) display
            // individually.
            let mut seen_collapsed: HashSet<&str> = HashSet::new();
            self.filtered_indices = base_indices
                .iter()
                .filter_map(|&idx| {
                    let job = &jobs[idx];
                    let is_collapsible_pending = job.array_step.is_some()
                        && job.state == "PENDING"
                        && collapsed_ids.contains(job.array_id.as_str());
                    if is_collapsible_pending {
                        if seen_collapsed.insert(&job.array_id) {
                            let total = pending_counts[job.array_id.as_str()];
                            Some(DisplayEntry::CollapsedGroup { index: idx, total })
                        } else {
                            None // skip, already represented by group header
                        }
                    } else {
                        Some(DisplayEntry::Job(idx))
                    }
                })
                .collect();
        } else {
            let indices: Vec<usize> = if self.sacct_reversed {
                base_indices.into_iter().rev().collect()
            } else {
                base_indices
            };
            self.filtered_indices = indices.into_iter().map(DisplayEntry::Job).collect();
        }

        // Clamp selection
        let state = match self.selected_tab {
            SelectedTab::Jobs => &mut self.job_list_state,
            SelectedTab::Sacct => &mut self.sacct_job_list_state,
        };
        if self.filtered_indices.is_empty() {
            state.select(None);
        } else if let Some(sel) = state.selected() {
            if sel >= self.filtered_indices.len() {
                state.select(Some(self.filtered_indices.len() - 1));
            }
        }
    }

    fn toggle_array_collapse(&mut self) {
        if let Some(job) = self.selected_job() {
            if job.array_step.is_some() {
                let array_id = job.array_id.clone();
                if self.expanded_arrays.contains(&array_id) {
                    self.expanded_arrays.remove(&array_id);
                } else {
                    self.expanded_arrays.insert(array_id);
                }
                self.recompute_filtered_indices();
            }
        }
    }

    fn yank_path(&mut self, stderr: bool) {
        let path = self.selected_job().and_then(|j| {
            if stderr {
                j.stderr.clone()
            } else {
                j.stdout.clone()
            }
        });
        match path {
            Some(p) => {
                let text = p.to_string_lossy().to_string();
                match copy_to_clipboard(&text) {
                    Ok(()) => self.dialog = Some(Dialog::YankResult(format!("Copied: {}", text))),
                    Err(e) => self.dialog = Some(Dialog::YankResult(format!("Error: {}", e))),
                }
            }
            None => {
                let label = if stderr { "stderr" } else { "stdout" };
                self.dialog = Some(Dialog::YankResult(format!("No {} path available", label)));
            }
        }
    }

    fn yank_job_id(&mut self) {
        match self.selected_job() {
            Some(job) => {
                let id = job.job_id.clone();
                match copy_to_clipboard(&id) {
                    Ok(()) => self.dialog = Some(Dialog::YankResult(format!("Copied: {}", id))),
                    Err(e) => self.dialog = Some(Dialog::YankResult(format!("Error: {}", e))),
                }
            }
            None => {
                self.dialog = Some(Dialog::YankResult("No job selected".to_string()));
            }
        }
    }
}

fn rect_contains(rect: Rect, column: u16, row: u16) -> bool {
    column >= rect.x
        && column < rect.x.saturating_add(rect.width)
        && row >= rect.y
        && row < rect.y.saturating_add(rect.height)
}

fn mouse_wheel_direction(kind: MouseEventKind) -> Option<MouseWheelDirection> {
    match kind {
        MouseEventKind::ScrollUp => Some(MouseWheelDirection::Up),
        MouseEventKind::ScrollDown => Some(MouseWheelDirection::Down),
        _ => None,
    }
}

fn signal_index_for_digit(digit: char) -> Option<usize> {
    let value = digit.to_digit(10)? as usize;
    if value == 0 { None } else { Some(value - 1) }
}

fn validated_time_limit(input: &Input) -> Option<String> {
    let time_limit = input.value().trim();
    if time_limit.is_empty() {
        None
    } else {
        Some(time_limit.to_string())
    }
}

fn execute_scancel(job_id: &str, signal: Option<&str>) -> Result<(), CommandFailure> {
    let mut command = Command::new("scancel");
    let mut command_display = String::from("scancel");

    if let Some(signal) = signal {
        command.arg("--signal").arg(signal);
        command_display.push_str(&format!(" --signal {signal}"));
    }
    command.arg(job_id);
    command_display.push_str(&format!(" {job_id}"));

    execute_command(command, command_display)
}

fn execute_scontrol_update_timelimit(job_id: &str, time_limit: &str) -> Result<(), CommandFailure> {
    let mut command = Command::new("scontrol");
    command
        .arg("update")
        .arg(format!("JobId={job_id}"))
        .arg(format!("TimeLimit={time_limit}"));

    execute_command(
        command,
        format!("scontrol update JobId={job_id} TimeLimit={time_limit}"),
    )
}

fn execute_command(mut command: Command, command_label: String) -> Result<(), CommandFailure> {
    let output = command.output().map_err(|error| CommandFailure {
        command: command_label.clone(),
        output: error.to_string(),
    })?;

    if output.status.success() {
        return Ok(());
    }

    let mut details = vec![match output.status.code() {
        Some(code) => format!("Exit code: {code}"),
        None => "Exit code: N/A".to_string(),
    }];

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stdout = stdout.trim_end();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim_end();
    let has_stdout = !stdout.is_empty();
    let has_stderr = !stderr.is_empty();
    match (has_stdout, has_stderr) {
        (true, true) => {
            details.push(format!("stdout:\n{stdout}"));
            details.push(format!("stderr:\n{stderr}"));
        }
        (true, false) => {
            details.push(stdout.to_string());
        }
        (false, true) => {
            details.push(stderr.to_string());
        }
        (false, false) => {}
    }

    if details.len() == 1 {
        details.push("No output.".to_string());
    }

    Err(CommandFailure {
        command: command_label,
        output: details.join("\n\n"),
    })
}
