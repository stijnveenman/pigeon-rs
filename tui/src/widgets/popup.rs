use ratatui::{
    layout::{Constraint, Flex, Layout, Rect},
    text::Line,
    widgets::{Block, BorderType, Borders, Clear, Widget},
};

use crate::style::BORDER_STYLE;

#[derive(Clone)]
pub struct Popup<'a> {
    title: Line<'a>,
    percent_x: u16,
    percent_y: u16,
}

#[allow(dead_code)]
impl<'a> Popup<'a> {
    pub fn new() -> Popup<'a> {
        Self {
            title: Line::default(),
            percent_x: 50,
            percent_y: 30,
        }
    }

    pub fn title<T: Into<Line<'a>>>(mut self, title: T) -> Self {
        self.title = title.into();
        self
    }

    pub fn percent_x(mut self, percent_x: u16) -> Self {
        self.percent_x = percent_x;
        self
    }

    pub fn percent_y(mut self, percent_y: u16) -> Self {
        self.percent_y = percent_y;
        self
    }

    fn area(&self, area: Rect) -> Rect {
        let vertical =
            Layout::vertical([Constraint::Percentage(self.percent_y)]).flex(Flex::Center);
        let horizontal =
            Layout::horizontal([Constraint::Percentage(self.percent_x)]).flex(Flex::Center);
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
