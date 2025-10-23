use std::str::FromStr;

use ratatui::{
    crossterm::event::KeyCode,
    layout::Rect,
    widgets::{Block, Borders, Paragraph},
};
use tokio::sync::oneshot;

use crate::{
    component::Tx,
    style::{ACTIVE_BORDER_COLOR, BORDER_STYLE},
    tui_event::TuiEvent,
    widgets::popup::Popup,
};

#[derive(Debug)]
pub enum QuestionType {
    String,
}

#[derive(Debug)]
pub struct FormQuestion {
    question_type: QuestionType,
    name: String,
    value: String,
}

#[derive(Debug)]
pub struct Form {
    title: String,
    questions: Vec<FormQuestion>,
}

pub struct FormPopup {
    form: Form,
    tx: oneshot::Sender<Form>,
}

impl FormPopup {
    pub fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect, _active: bool) {
        let popup = Popup::new().title(self.form.title.clone());
        f.render_widget(popup.clone(), rect);
        let rect = popup.inner(rect);

        let mut input_rect = rect;
        input_rect.height = 3;

        let block = Block::new()
            .title(self.form.questions.first().unwrap().name.clone())
            .border_style(BORDER_STYLE.fg(ACTIVE_BORDER_COLOR))
            .borders(Borders::ALL);
        let paragraph =
            Paragraph::new(self.form.questions.first().unwrap().value.clone()).block(block);
        f.render_widget(paragraph, input_rect);
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
                KeyCode::Char(c) => {
                    self.form.questions.first_mut().unwrap().value.push(c);
                    Some(self)
                }
                KeyCode::Backspace => {
                    self.form.questions.first_mut().unwrap().value.pop();
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
        }))
        .unwrap();

        rx
    }
}
