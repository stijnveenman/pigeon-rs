use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    widgets::{Paragraph, Wrap},
};

use crate::widgets::popup::Popup;

pub enum PromptItem {
    Paragraph(String),
}

impl PromptItem {
    fn height(&self, width: u16) -> u16 {
        match self {
            PromptItem::Paragraph(text) => Paragraph::new(text.clone())
                .wrap(Wrap { trim: true })
                .line_count(width) as u16,
        }
    }

    fn render(&self, f: &mut Frame, area: Rect) {
        match self {
            PromptItem::Paragraph(text) => {
                let paragraph = Paragraph::new(text.clone()).wrap(Wrap { trim: true });

                f.render_widget(paragraph, area);
            }
        }
    }
}

pub struct Prompt {
    items: Vec<PromptItem>,
    width: Constraint,
    title: String,
}

impl Prompt {
    pub fn new() -> Self {
        Prompt {
            items: vec![
                PromptItem::Paragraph("This is some basic paragraph text".into()),
                PromptItem::Paragraph("This is some basic paragraph text".into()),
                PromptItem::Paragraph("This is some basic paragraph text".into()),
                PromptItem::Paragraph("This is some basic paragraph text".into()),
            ],
            width: Constraint::Percentage(50),
            title: "Create topic".into(),
        }
    }

    pub fn render(&self, f: &mut Frame) {
        let popup = Popup::new()
            .constraint_x(self.width)
            .title(self.title.clone());
        let width = popup.inner(f.area()).width;

        let height = 2 + self.items.iter().map(|i| i.height(width)).sum::<u16>();
        let popup = popup.constraint_y(Constraint::Length(height));

        let mut area = popup.inner(f.area());
        f.render_widget(popup, f.area());

        for item in &self.items {
            let item_area = Rect {
                height: item.height(area.width),
                ..area
            };
            area.y += item_area.height;

            item.render(f, item_area);
        }
    }
}
