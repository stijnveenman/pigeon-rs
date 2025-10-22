use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::Style,
    text::Line,
    widgets::{Block, BorderType, Borders, Clear, Widget},
};

use crate::style::BORDER_STYLE;

pub struct Popup<'a> {
    title: Line<'a>,
    border_style: Style,
    title_style: Style,
    style: Style,
    height_pct: u16,
    width_pct: u16,
}

#[allow(dead_code)]
impl<'a> Popup<'a> {
    pub fn new(width_pct: u16, height_pct: u16) -> Popup<'a> {
        Self {
            title: Line::default(),
            border_style: Style::default(),
            title_style: Style::default(),
            style: Style::default(),
            width_pct,
            height_pct,
        }
    }

    pub fn title(mut self, title: Line<'a>) -> Self {
        self.title = title;
        self
    }

    pub fn border_style(mut self, border_style: Style) -> Self {
        self.border_style = border_style;
        self
    }

    pub fn title_style(mut self, title_style: Style) -> Self {
        self.title_style = title_style;
        self
    }

    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }
}

impl Widget for Popup<'_> {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        let [_, width, _] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![
                Constraint::Min(1),
                Constraint::Percentage(self.width_pct),
                Constraint::Min(1),
            ])
            .areas(area);

        let [_, area, _] = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![
                Constraint::Min(1),
                Constraint::Percentage(self.height_pct),
                Constraint::Min(1),
            ])
            .areas(width);

        Clear.render(area, buf);
        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(BORDER_STYLE)
            .title("popup");

        block.render(area, buf);
    }
}
