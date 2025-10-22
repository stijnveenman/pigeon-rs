use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, ToLine},
    widgets::{Block, BorderType, Borders, Clear, Widget},
};

pub struct Popup<'a> {
    title: Line<'a>,
    border_style: Style,
    title_style: Style,
    height_pct: u16,
    width_pct: u16,
    horizontal_alignment: Alignment,
    vertical_alignment: Alignment,
}

#[allow(dead_code)]
impl<'a> Popup<'a> {
    pub fn new(width_pct: u16, height_pct: u16) -> Popup<'a> {
        Self {
            title: Line::default(),
            border_style: Style::default(),
            title_style: Style::default(),
            horizontal_alignment: Alignment::Center,
            vertical_alignment: Alignment::Center,
            width_pct,
            height_pct,
        }
    }

    pub fn title<T: Into<Line<'a>>>(mut self, title: T) -> Self {
        self.title = title.into();
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

    pub fn horizontal_alignment(mut self, alignment: Alignment) -> Self {
        self.horizontal_alignment = alignment;
        self
    }

    pub fn vertical_alignment(mut self, alignment: Alignment) -> Self {
        self.vertical_alignment = alignment;
        self
    }

    fn align_direction(&self, rect: Rect, direction: Direction) -> Rect {
        let alignment = match direction {
            Direction::Horizontal => self.horizontal_alignment,
            Direction::Vertical => self.vertical_alignment,
        };

        let percentage = match direction {
            Direction::Horizontal => self.width_pct,
            Direction::Vertical => self.height_pct,
        };

        match alignment {
            Alignment::Left => {
                let [area, _] = Layout::default()
                    .direction(direction)
                    .constraints(vec![Constraint::Percentage(percentage), Constraint::Min(0)])
                    .areas(rect);
                area
            }
            Alignment::Center => {
                let [_, area, _] = Layout::default()
                    .direction(direction)
                    .constraints(vec![
                        Constraint::Min(0),
                        Constraint::Percentage(percentage),
                        Constraint::Min(0),
                    ])
                    .areas(rect);
                area
            }
            Alignment::Right => {
                let [_, area] = Layout::default()
                    .direction(direction)
                    .constraints(vec![Constraint::Min(0), Constraint::Percentage(percentage)])
                    .areas(rect);
                area
            }
        }
    }

    fn align(&self, rect: Rect) -> Rect {
        let rect = self.align_direction(rect, Direction::Horizontal);
        self.align_direction(rect, Direction::Vertical)
    }
}

impl Widget for Popup<'_> {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        let area = self.align(area);

        Clear.render(area, buf);
        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(self.border_style)
            .title_style(self.title_style)
            .title(self.title);

        block.render(area, buf);
    }
}
