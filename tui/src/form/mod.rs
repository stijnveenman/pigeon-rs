use std::{ops::Add, str::FromStr};

use ratatui::{
    Frame,
    crossterm::event::{KeyCode, KeyModifiers},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Paragraph},
};
use tokio::sync::oneshot;

use crate::{component::Tx, style::StylizeIf, tui_event::TuiEvent, widgets::popup::Popup};

#[derive(Debug)]
pub enum QuestionType {
    String,
    Integer,
}

#[derive(Debug)]
pub struct FormQuestion {
    question_type: QuestionType,
    name: String,
    value: String,
}

impl FormQuestion {
    fn render(&self, f: &mut Frame, rect: Rect, active: bool) -> Rect {
        let block = Block::new()
            .title(self.name.clone())
            .border_style(Style::new().gray().fg_if(Color::White, active))
            .borders(Borders::ALL);

        let p = Paragraph::new(self.value.clone()).block(block);

        let [rect, remaining] = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![Constraint::Length(3), Constraint::Min(0)])
            .areas(rect);

        f.render_widget(p, rect);
        remaining
    }

    fn event(&mut self, key: KeyCode) {
        match key {
            KeyCode::Char(c) => {
                self.value.push(c);
            }
            KeyCode::Backspace => {
                self.value.pop();
            }
            _ => {}
        };
    }

    fn height(&self) -> u16 {
        3
    }
}

#[derive(Debug)]
pub struct Form {
    title: String,
    questions: Vec<FormQuestion>,
}

pub struct FormPopup {
    form: Form,
    active_idx: usize,
    tx: oneshot::Sender<Form>,
}

impl FormPopup {
    pub fn render(&self, f: &mut Frame, rect: Rect) {
        let height = 2 + self.form.questions.iter().map(|q| q.height()).sum::<u16>();

        let popup = Popup::new()
            .constraint_y(Constraint::Length(height))
            .title(self.form.title.clone());

        f.render_widget(popup.clone(), rect);
        let mut rect = popup.inner(rect);

        for (idx, question) in self.form.questions.iter().enumerate() {
            rect = question.render(f, rect, idx == self.active_idx);
        }
    }

    fn finish(self) -> Option<Self> {
        self.tx.send(self.form).unwrap();

        None
    }

    fn close(self) -> Option<Self> {
        None
    }

    pub fn event(mut self, event: TuiEvent) -> Option<Self> {
        match event {
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Esc => self.close(),
                KeyCode::Enter => self.finish(),
                KeyCode::Char(_) => {
                    self.form
                        .questions
                        .get_mut(self.active_idx)
                        .unwrap()
                        .event(key.code);
                    Some(self)
                }
                KeyCode::Backspace => {
                    self.form
                        .questions
                        .get_mut(self.active_idx)
                        .unwrap()
                        .event(key.code);
                    Some(self)
                }
                KeyCode::BackTab => {
                    self.active_idx = self
                        .active_idx
                        .checked_sub(1)
                        .unwrap_or(self.form.questions.len() - 1);
                    Some(self)
                }
                KeyCode::Tab => {
                    self.active_idx = self.active_idx.add(1);
                    if self.active_idx >= self.form.questions.len() {
                        self.active_idx = 0;
                    }
                    Some(self)
                }
                _ => Some(self),
            },
            _ => Some(self),
        }
    }
}

impl Form {
    pub fn new() -> Self {
        Self {
            questions: Vec::new(),
            title: String::new(),
        }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    pub fn push(mut self, name: impl Into<String>, question_type: QuestionType) -> Self {
        self.questions.push(FormQuestion {
            name: name.into(),
            question_type,
            value: String::new(),
        });

        self
    }

    pub fn get<T: FromStr>(&mut self, name: &str) -> Result<T, T::Err> {
        self.questions
            .iter()
            .find(|q| q.name == name)
            .unwrap()
            .value
            .parse()
    }

    pub fn show(self, tx: Tx) -> oneshot::Receiver<Form> {
        let (form_tx, rx) = oneshot::channel();

        tx.send(TuiEvent::Form(FormPopup {
            form: self,
            tx: form_tx,
            active_idx: 0,
        }))
        .unwrap();

        rx
    }
}
