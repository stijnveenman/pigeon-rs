use ratatui::style::{Color, Modifier, Style, Styled};

pub const BORDER_STYLE: Style = Style::new()
    .fg(Color::Blue)
    .add_modifier(Modifier::BOLD)
    .add_modifier(Modifier::ITALIC);

pub const ACTIVE_BORDER_COLOR: Color = Color::Green;

pub trait StylizeIf<'a, T>: Sized {
    fn fg_if<C: Into<Color>>(self, color: C, condition: bool) -> T;
}

impl<T, U> StylizeIf<'_, T> for U
where
    U: Styled<Item = T>,
{
    fn fg_if<C: Into<Color>>(self, color: C, condition: bool) -> T {
        let style = if condition {
            self.style().fg(color.into())
        } else {
            self.style()
        };

        self.set_style(style)
    }
}
