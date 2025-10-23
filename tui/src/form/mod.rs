#![allow(unused)]

use std::str::FromStr;

use tokio::sync::oneshot;

use crate::{component::Tx, tui_event::TuiEvent, widgets::popup::Popup};

pub enum QuestionType {
    String,
}

pub struct FormQuestion {
    question_type: QuestionType,
    name: String,
    value: String,
}

pub struct Form {
    questions: Vec<FormQuestion>,
}

pub struct FormPopup {
    form: Form,
    tx: oneshot::Sender<Form>,
}

impl FormPopup {
    pub fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect, _active: bool) {
        let popup = Popup::new().title("Popup");
        f.render_widget(popup.clone(), rect);
        f.render_widget("lorom", popup.inner(rect));
    }

    pub fn event(mut self, event: TuiEvent) -> Option<Self> {
        None
    }
}

impl Form {
    pub fn new() -> Self {
        Self {
            questions: Vec::new(),
        }
    }

    pub fn push(mut self, name: String, question_type: QuestionType) -> Self {
        self.questions.push(FormQuestion {
            name,
            question_type,
            value: String::new(),
        });

        self
    }

    pub fn show(mut self, tx: Tx) -> oneshot::Receiver<Form> {
        let (form_tx, rx) = oneshot::channel();

        tx.send(TuiEvent::Form(FormPopup {
            form: self,
            tx: form_tx,
        }))
        .unwrap();

        rx
    }
}
