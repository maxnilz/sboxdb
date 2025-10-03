//! Utilities for formatting SQL AST nodes with pretty printing support.
//!
//! The module provides formatters that implement the `Display` trait with support
//! for both regular (`{}`) and pretty (`{:#}`) formatting modes. Pretty printing
//! adds proper indentation and line breaks to make SQL statements more readable.

use core::fmt::Display;
use core::fmt::Formatter;
use core::fmt::Result;
use core::fmt::Write;

/// A wrapper around a value that adds an indent to the value when displayed with {:#}.
pub struct Indent<T>(pub T);

const INDENT: &str = "  ";

impl<T: Display> Display for Indent<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if f.alternate() {
            f.write_str(INDENT)?;
            write!(Indent(f), "{:#}", self.0)
        } else {
            self.0.fmt(f)
        }
    }
}

/// Adds an indent to the inner writer
impl<T: Write> Write for Indent<T> {
    fn write_str(&mut self, s: &str) -> Result {
        self.0.write_str(s)?;
        // Our NewLine and SpaceOrNewline utils always print individual newlines as a single-character string.
        if s == "\n" {
            self.0.write_str(INDENT)?;
        }
        Ok(())
    }
}

/// A value that inserts a newline when displayed with {:#}, but not when displayed with {}.
pub struct NewLine;

impl Display for NewLine {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if f.alternate() {
            f.write_char('\n')
        } else {
            Ok(())
        }
    }
}

/// A value that inserts a space when displayed with {}, but a newline when displayed with {:#}.
pub struct SpaceOrNewline;

impl Display for SpaceOrNewline {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if f.alternate() {
            f.write_char('\n')
        } else {
            f.write_char(' ')
        }
    }
}

/// A value that displays a separated list of values.
/// When pretty-printed (using {:#}), it displays each value on a new line.
pub struct DisplaySeparated<'a, T>
where
    T: Display,
{
    slice: &'a [T],
    sep: Option<&'static str>,
    inline: bool,
}
impl<'a, T: Display> Display for DisplaySeparated<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut first = true;
        for t in self.slice {
            if !first {
                if let Some(sep) = self.sep {
                    f.write_str(sep)?;
                }
                if !self.inline {
                    SpaceOrNewline.fmt(f)?;
                }
            }
            first = false;
            t.fmt(f)?;
        }
        Ok(())
    }
}

pub fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: Display,
{
    DisplaySeparated { slice, sep: Some(","), inline: false }
}

pub fn display_dot_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: Display,
{
    DisplaySeparated { slice, sep: Some("."), inline: false }
}

pub fn display_inline_dot_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: Display,
{
    DisplaySeparated { slice, sep: Some("."), inline: true }
}

pub fn display_space_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: Display,
{
    DisplaySeparated { slice, sep: None, inline: false }
}

/// remove common leading whitespace.
pub fn dedent(s: &str, keep_empty: bool) -> String {
    let lines: Vec<&str> = s.lines().collect();
    // Find the minimum leading whitespace
    let min_indent = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);
    // Remove the common ident from each line
    let mut out = vec![];
    for line in lines.iter() {
        if line.trim().is_empty() {
            if keep_empty {
                out.push("");
            }
            continue;
        }
        let a = &line[min_indent.min(line.len())..];
        out.push(a);
    }
    out.join("\n")
}
