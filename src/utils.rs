use std::iter::once;
use std::process::{Command, Stdio};

use itertools::Either;
use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
};

use crate::app::ScrollAnchor;

pub fn state_color(state_compact: &str) -> Color {
    match state_compact {
        "C" => Color::Green,       // COMPLETED
        "F" => Color::Red,         // FAILED
        "R" => Color::Cyan,        // RUNNING
        "P" => Color::Yellow,      // PENDING
        "T" => Color::Magenta,     // TIMEOUT
        "N" => Color::Red,         // NODE_FAIL
        "B" => Color::Red,         // BOOT_FAIL
        "S" => Color::Yellow,      // SUSPENDED
        "?" => Color::DarkGray,    // Unknown
        _ => Color::White,
    }
}

pub fn copy_to_clipboard(text: &str) -> Result<(), String> {
    use std::io::Write;

    let mut child = Command::new("base64")
        .arg("-w")
        .arg("0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to run base64: {}", e))?;

    child
        .stdin
        .take()
        .unwrap()
        .write_all(text.as_bytes())
        .map_err(|e| e.to_string())?;

    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    let encoded = String::from_utf8(output.stdout).map_err(|e| e.to_string())?;

    let mut tty = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/tty")
        .map_err(|e| format!("failed to open /dev/tty: {}", e))?;

    let in_tmux = std::env::var("TMUX").is_ok();
    if in_tmux {
        // Wrap OSC 52 in DCS passthrough for tmux
        write!(tty, "\x1bPtmux;\x1b\x1b]52;c;{}\x07\x1b\\", encoded)
    } else {
        write!(tty, "\x1b]52;c;{}\x07", encoded)
    }
    .map_err(|e| e.to_string())?;

    Ok(())
}

pub fn chunked_string(s: &str, first_chunk_size: usize, chunk_size: usize) -> Vec<&str> {
    let stepped_indices = s
        .char_indices()
        .map(|(i, _)| i)
        .enumerate()
        .filter(|&(i, _)| {
            if i > (first_chunk_size) {
                chunk_size > 0 && (i - first_chunk_size) % chunk_size == 0
            } else {
                i == 0 || i == first_chunk_size
            }
        })
        .map(|(_, e)| e)
        .collect::<Vec<_>>();
    let windows = stepped_indices.windows(2).collect::<Vec<_>>();

    let iter = windows.iter().map(|w| &s[w[0]..w[1]]);
    let last_index = *stepped_indices.last().unwrap_or(&0);
    iter.chain(once(&s[last_index..])).collect()
}

pub fn fit_text(
    s: &str,
    lines: usize,
    cols: usize,
    anchor: ScrollAnchor,
    offset: usize,
    wrap: bool,
) -> Text<'_> {
    let s = s.rsplit_once(&['\r', '\n']).map_or(s, |(p, _)| p); // skip everything after last line delimiter
    let l = s.lines().flat_map(|l| l.split('\r')); // bandaid for term escape codes
    let iter = match anchor {
        ScrollAnchor::Top => Either::Left(l),
        ScrollAnchor::Bottom => Either::Right(l.rev()),
    };
    let iter = iter
        .skip(offset)
        .flat_map(|l| {
            let iter = if wrap {
                Either::Left(
                    chunked_string(l, cols, cols.saturating_sub(2))
                        .into_iter()
                        .enumerate()
                        .map(|(i, l)| {
                            if i == 0 {
                                Line::raw(l.chars().take(cols).collect::<String>())
                            } else {
                                Line::default().spans(vec![
                                    Span::styled(
                                        "↪ ",
                                        Style::default().add_modifier(Modifier::DIM),
                                    ),
                                    Span::raw(
                                        l.chars().take(cols.saturating_sub(2)).collect::<String>(),
                                    ),
                                ])
                            }
                        }),
                )
            } else {
                match l.chars().nth(cols) {
                    Some(_) => {
                        // has more chars than cols
                        Either::Right(once(Line::default().spans(vec![
                            Span::raw(l.chars().take(cols.saturating_sub(1)).collect::<String>()),
                            Span::styled("…", Style::default().add_modifier(Modifier::DIM)),
                        ])))
                    }
                    None => {
                        Either::Right(once(Line::raw(l.chars().take(cols).collect::<String>())))
                    }
                }
            };
            match anchor {
                ScrollAnchor::Top => Either::Left(iter),
                ScrollAnchor::Bottom => Either::Right(iter.rev()),
            }
        })
        .take(lines);

    match anchor {
        ScrollAnchor::Top => Text::from(iter.collect::<Vec<_>>()),
        ScrollAnchor::Bottom => Text::from(
            iter.collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunked_string() {
        // Divisible
        let input = "abcdefghij";
        let expected = vec!["abcd", "ef", "gh", "ij"];
        assert_eq!(chunked_string(input, 4, 2), expected);

        // Not divisible
        let input = "123456789";
        let expected = vec!["1234", "56", "78", "9"];
        assert_eq!(chunked_string(input, 4, 2), expected);

        // Smaller
        let input = "abc";
        let expected = vec!["abc"];
        assert_eq!(chunked_string(input, 4, 2), expected);

        // Smaller
        let input = "abcde";
        let expected = vec!["abcd", "e"];
        assert_eq!(chunked_string(input, 4, 2), expected);

        // Empty
        let input = "";
        let expected: Vec<&str> = vec![""];
        assert_eq!(chunked_string(input, 4, 2), expected);

        let input = "123456789";
        let expected = vec!["1234", "56789"];
        assert_eq!(chunked_string(input, 4, 0), expected);

        let input = "123456789";
        let expected = vec!["12", "34", "56", "78", "9"];
        assert_eq!(chunked_string(input, 0, 2), expected);

        let input = "123456789";
        let expected = vec!["123456789"];
        assert_eq!(chunked_string(input, 0, 0), expected);
    }
}
