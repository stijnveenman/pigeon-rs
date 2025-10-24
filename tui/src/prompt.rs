use std::str::FromStr;

use ratatui::{
    Frame,
    crossterm::event::KeyCode,
    layout::{Constraint, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};
use tokio::sync::oneshot;

use crate::{
    component::Tx,
    style::{BORDER_STYLE, StylizeIf},
    tui_event::TuiEvent,
    widgets::popup::Popup,
};

pub enum InputType {
    String,
    Integer,
}

pub struct Input {
    input_type: InputType,
    name: String,
    value: String,
    required: bool,
}

impl Input {
    pub fn new(name: impl Into<String>, input_type: InputType) -> Self {
        Input {
            input_type,
            name: name.into(),
            required: false,
            value: String::new(),
        }
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, InputType::String)
    }

    pub fn integer(name: impl Into<String>) -> Self {
        Self::new(name, InputType::Integer)
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }
}

pub enum PromptItem {
    Paragraph(String),
    Input(Input),
}

impl PromptItem {
    fn height(&self, width: u16) -> u16 {
        match self {
            PromptItem::Paragraph(text) => Paragraph::new(text.clone())
                .wrap(Wrap { trim: true })
                .line_count(width) as u16,
            PromptItem::Input(_) => 3,
        }
    }

    fn render(&self, f: &mut Frame, area: Rect, active: bool) {
        match self {
            PromptItem::Paragraph(text) => {
                let paragraph = Paragraph::new(text.clone()).wrap(Wrap { trim: true });

                f.render_widget(paragraph, area);
            }
            PromptItem::Input(input) => {
                let mut title = Line::from(Span::from(&input.name));
                if input.required {
                    title.push_span(Span::styled("*", Color::Gray));
                }

                let block = Block::new()
                    .title(title)
                    .border_style(Style::new().gray().fg_if(Color::White, active))
                    .borders(Borders::ALL);

                let input = Paragraph::new(input.value.clone()).block(block);

                f.render_widget(input, area);
            }
        }
    }

    fn push_char(&mut self, c: char) {
        match self {
            PromptItem::Paragraph(_) => {}
            PromptItem::Input(input) => match input.input_type {
                InputType::String => {
                    input.value.push(c);
                }
                InputType::Integer => {
                    if c.is_numeric() {
                        input.value.push(c);
                    }
                }
            },
        }
    }

    fn pop(&mut self) {
        match self {
            PromptItem::Paragraph(_) => {}
            PromptItem::Input(input) => {
                input.value.pop();
            }
        }
    }

    fn selectable(&self) -> bool {
        match self {
            PromptItem::Input(_) => true,
            PromptItem::Paragraph(_) => false,
        }
    }
}

pub enum PromptType {
    Form,
    Success,
    Error,
}

pub struct Prompt {
    items: Vec<PromptItem>,
    width: Constraint,
    title: String,
    active_idx: usize,
    tx: Option<oneshot::Sender<Prompt>>,
    prompt_type: PromptType,
}

impl Prompt {
    pub fn new() -> Self {
        Prompt {
            items: vec![],
            width: Constraint::Percentage(50),
            title: "Create topic".into(),
            active_idx: 0,
            prompt_type: PromptType::Form,
            tx: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Prompt {
        Prompt::new()
            .prompt_type(PromptType::Error)
            .paragraph(message.into())
    }

    pub fn prompt_type(mut self, prompt_type: PromptType) -> Self {
        self.prompt_type = prompt_type;
        self
    }

    pub fn get<T: FromStr>(&self, name: &str) -> Result<T, T::Err> {
        self.items
            .iter()
            .filter_map(|i| match i {
                PromptItem::Input(i) if i.name == name => Some(i),
                _ => None,
            })
            .next()
            .unwrap()
            .value
            .parse()
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    pub fn paragraph(mut self, text: impl Into<String>) -> Self {
        self.items.push(PromptItem::Paragraph(text.into()));
        self
    }

    pub fn input(mut self, input: Input) -> Self {
        self.items.push(PromptItem::Input(input));
        self.select_first();
        self
    }

    fn current_mut(&mut self) -> &mut PromptItem {
        self.items.get_mut(self.active_idx).unwrap()
    }

    fn select_first(&mut self) {
        if let Some(next) = self.items.iter().enumerate().find(|(_, i)| i.selectable()) {
            self.active_idx = next.0;
        }
    }

    fn select_next(&mut self) {
        if let Some(next) = self
            .items
            .iter()
            .enumerate()
            .skip(self.active_idx + 1)
            .find(|(_, i)| i.selectable())
        {
            self.active_idx = next.0;
        }
    }

    fn select_prev(&mut self) {
        if let Some(next) = self
            .items
            .iter()
            .enumerate()
            .rev()
            .skip(self.items.len() - self.active_idx)
            .find(|(_, i)| i.selectable())
        {
            self.active_idx = next.0;
        }
    }

    fn footer(&self) -> Line<'_> {
        match self.prompt_type {
            PromptType::Form => "Esc: Cancel, Enter: Confirm".into(),
            PromptType::Success | PromptType::Error => "Esc/Enter to dismiss".into(),
        }
    }

    pub fn render(&self, f: &mut Frame) {
        let popup = Popup::new()
            .constraint_x(self.width)
            .title(self.title.clone())
            .border_style(
                BORDER_STYLE
                    .fg_if(Color::Red, matches!(self.prompt_type, PromptType::Error))
                    .fg_if(
                        Color::Green,
                        matches!(self.prompt_type, PromptType::Success),
                    ),
            )
            .title_bottom(self.footer().right_aligned());
        let width = popup.inner(f.area()).width;

        let height = 2 + self.items.iter().map(|i| i.height(width)).sum::<u16>();
        let popup = popup.constraint_y(Constraint::Length(height));

        let mut area = popup.inner(f.area());
        f.render_widget(popup, f.area());

        for (idx, item) in self.items.iter().enumerate() {
            let item_area = Rect {
                height: item.height(area.width),
                ..area
            };
            area.y += item_area.height;

            item.render(f, item_area, idx == self.active_idx);
        }
    }

    pub fn event(mut self, event: TuiEvent) -> Option<Prompt> {
        if let TuiEvent::KeyPress(key) = event {
            match key.code {
                KeyCode::Esc => return None,
                KeyCode::Enter => {
                    let tx = self.tx.take().unwrap();
                    let _ = tx.send(self);
                    return None;
                }
                KeyCode::Tab => self.select_next(),
                KeyCode::BackTab => self.select_prev(),
                KeyCode::Char(c) => self.current_mut().push_char(c),
                KeyCode::Backspace => self.current_mut().pop(),
                _ => return Some(self),
            };
        };

        Some(self)
    }

    pub fn show(mut self, tx: Tx) -> oneshot::Receiver<Prompt> {
        let (form_tx, form_rx) = oneshot::channel();
        self.tx = Some(form_tx);

        tx.send(TuiEvent::Prompt(self)).unwrap();

        form_rx
    }
}
