use ratatui::{
    layout::{Constraint, Flex, Layout, Rect},
    text::Line,
    widgets::{Block, BorderType, Borders, Clear, Widget},
};

use crate::style::BORDER_STYLE;

#[derive(Clone)]
pub struct Popup<'a> {
    title: Line<'a>,
    constraint_x: Constraint,
    constraint_y: Constraint,
}

#[allow(dead_code)]
impl<'a> Popup<'a> {
    pub fn new() -> Popup<'a> {
        Self {
            title: Line::default(),
            constraint_x: Constraint::Percentage(50),
            constraint_y: Constraint::Percentage(30),
        }
    }

    pub fn title<T: Into<Line<'a>>>(mut self, title: T) -> Self {
        self.title = title.into();
        self
    }

    pub fn constraint_x(mut self, constraint_x: Constraint) -> Self {
        self.constraint_x = constraint_x;
        self
    }

    pub fn constraint_y(mut self, constraint_y: Constraint) -> Self {
        self.constraint_y = constraint_y;
        self
    }

    fn area(&self, area: Rect) -> Rect {
        let vertical = Layout::vertical([self.constraint_y]).flex(Flex::Center);
        let horizontal = Layout::horizontal([self.constraint_x]).flex(Flex::Center);
        let [area] = vertical.areas(area);
        let [area] = horizontal.areas(area);
        area
    }

    pub fn inner(&self, area: Rect) -> Rect {
        let area = self.area(area);
        Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(BORDER_STYLE)
            .inner(area)
    }
}

impl Widget for Popup<'_> {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        let area = self.area(area);

        Clear.render(area, buf);

        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(BORDER_STYLE)
            .title(self.title);

        block.render(area, buf);
    }
}
