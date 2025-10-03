use std::fmt::Display;
use std::fmt::Formatter;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Lines;
use std::io::Read;
use std::iter::Peekable;
use std::str::Chars;

use crate::error::Result;
use crate::parse_err;

pub enum Record {
    Statement(Statement),
    Query(Query),
    Sleep(i32),
    Halt,
    Include(Include),
}

pub struct Statement {
    pub sql: String,
    pub expect_ok: bool,
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "statement {expect}", expect = if self.expect_ok { "ok" } else { "error" })
    }
}

pub enum SortMode {
    NoSort,
    RowSort,
}

impl Display for SortMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SortMode::NoSort => {
                write!(f, "nosort")
            }
            SortMode::RowSort => {
                write!(f, "rowsort")
            }
        }
    }
}
pub struct Query {
    pub sql: String,
    pub sort_mode: SortMode,
    pub expected_result: Vec<String>,
}

impl Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "query {}", self.sort_mode)
    }
}

pub struct Include {
    pub filename: String,
}

struct LinesIter {
    iter: Lines<BufReader<Box<dyn Read>>>,
    line_no: usize,
}

impl LinesIter {
    fn new(r: BufReader<Box<dyn Read>>) -> Self {
        Self { iter: r.lines(), line_no: 0 }
    }
}

impl Iterator for LinesIter {
    type Item = std::io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        self.line_no += 1;
        self.iter.next()
    }
}

pub struct Parser {}

impl Parser {
    pub fn parse(self, r: BufReader<Box<dyn Read>>) -> Result<Vec<Record>> {
        let mut records = vec![];
        let mut iter = LinesIter::new(r);
        while let Some(line) = iter.next().transpose()? {
            if line.is_empty() || line.starts_with("#") {
                continue;
            }
            let mut p = LineParser::new(&line);
            let next_token = p.next_token();
            let record = match next_token {
                Token::Keyword(w) => match w {
                    Keyword::Statement => self.parse_statement(p, &mut iter),
                    Keyword::Query => self.parse_query(p, &mut iter),
                    Keyword::Halt => self.parse_halt(p),
                    Keyword::Sleep => self.parse_sleep(p),
                    Keyword::Include => self.parse_include(p),
                    _ => Err(parse_err!("Expect command keyword, got {:?}", next_token)),
                },
                _ => Err(parse_err!("Expect command keyword, got {:?}", next_token)),
            }
            .map_err(|err| {
                parse_err!(
                    "Invalid record '{}' at line number {}, err: {}",
                    line,
                    iter.line_no,
                    err
                )
            })?;
            records.push(record)
        }
        Ok(records)
    }

    fn parse_statement(&self, mut p: LineParser, iter: &mut LinesIter) -> Result<Record> {
        let keyword = p.expect_one_of_keywords(&[Keyword::Ok, Keyword::Error])?;
        let ok = match keyword {
            Keyword::Ok => true,
            Keyword::Error => false,
            _ => unreachable!(),
        };
        let mut lines = vec![];
        while let Some(line) = iter.next().transpose()? {
            if line.is_empty() {
                break;
            }
            lines.push(line);
        }
        let sql = lines.join("\n");
        let stmt = Statement { sql, expect_ok: ok };
        Ok(Record::Statement(stmt))
    }

    fn parse_include(&self, mut p: LineParser) -> Result<Record> {
        let tok = p.next_token();
        let filename = match tok {
            Token::String(s) => Ok(s),
            _ => Err(parse_err!("Expect num after sleep, got {:?}", tok)),
        }?;
        Ok(Record::Include(Include { filename }))
    }

    fn parse_sleep(&self, mut p: LineParser) -> Result<Record> {
        let tok = p.next_token();
        let seconds = match tok {
            Token::String(s) => {
                s.parse::<i32>().map_err(|e| parse_err!("Expect num seconds after Sleep, {}", e))
            }
            _ => Err(parse_err!("Expect num after sleep, got {:?}", tok)),
        }?;
        Ok(Record::Sleep(seconds))
    }

    fn parse_halt(&self, mut p: LineParser) -> Result<Record> {
        p.expect_token(&Token::EOF)?;
        Ok(Record::Halt)
    }

    fn parse_query(&self, mut p: LineParser, iter: &mut LinesIter) -> Result<Record> {
        let keyword = p.expect_one_of_keywords(&[Keyword::NoSort, Keyword::RowSort])?;
        let sort_mode = match keyword {
            Keyword::RowSort => SortMode::RowSort,
            Keyword::NoSort => SortMode::NoSort,
            _ => unreachable!(),
        };
        let mut has_result = false;
        let mut lines = vec![];
        while let Some(line) = iter.next().transpose()? {
            if line.is_empty() {
                break;
            }
            if line.starts_with("----") {
                has_result = true;
                break;
            }
            lines.push(line);
        }
        if !has_result {
            return Err(parse_err!("No query result found"));
        }
        let sql = lines.join("\n");

        let mut expected_result = vec![];
        while let Some(line) = iter.next().transpose()? {
            if line.is_empty() {
                break;
            }
            expected_result.push(String::from(line.trim_end()));
        }
        let q = Query { sql, sort_mode, expected_result };
        Ok(Record::Query(q))
    }
}

struct LineParser {
    tokens: Vec<Token>,
    index: usize,
}

impl LineParser {
    fn new(line: &str) -> Self {
        let mut tokens = vec![];
        let mut tokenizer = Tokenizer::new(&line);
        while let Some(token) = tokenizer.next() {
            tokens.push(token)
        }
        Self { tokens, index: 0 }
    }

    fn next_token(&mut self) -> Token {
        self.advance_token();
        self.tokens.get(self.index - 1).unwrap_or(&Token::EOF).clone()
    }

    fn expect_token(&mut self, expected: &Token) -> Result<Token> {
        let tok = self.peek_token_ref();
        if tok == expected {
            return Ok(self.next_token());
        }
        Err(parse_err!("Expect {:?} found {:?}", expected, tok))
    }

    fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> Result<Keyword> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            return Ok(keyword);
        }
        let keywords: Vec<String> = keywords.iter().map(|x| format!("{x:?}")).collect();
        Err(parse_err!("Expect one of {}, got {:?}", keywords.join(" or "), self.peek_token_ref()))
    }

    fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        match &self.peek_token_ref() {
            Token::Keyword(w) => keywords.iter().find(|&keyword| keyword == w).map(|keyword| {
                self.advance_token();
                *keyword
            }),
            _ => None,
        }
    }

    fn peek_token_ref(&self) -> &Token {
        let tok = self.tokens.get(self.index);
        tok.unwrap_or(&Token::EOF)
    }

    fn advance_token(&mut self) {
        self.index += 1
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    EOF,
    String(String),
    Keyword(Keyword),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Keyword {
    Statement,
    Ok,
    Error,
    Query,
    RowSort,
    NoSort,
    Halt,
    Sleep,
    Include,
}

impl Keyword {
    fn from_str(str: &str) -> Option<Keyword> {
        let ans = match str.to_uppercase().as_ref() {
            "STATEMENT" => Self::Statement,
            "OK" => Self::Ok,
            "ERROR" => Self::Error,
            "QUERY" => Self::Query,
            "ROWSORT" => Self::RowSort,
            "NOSORT" => Self::NoSort,
            "HALT" => Self::Halt,
            "SLEEP" => Self::Sleep,
            "INCLUDE" => Self::Include,
            _ => return None,
        };
        Some(ans)
    }
}

pub struct Tokenizer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Tokenizer<'a> {
    pub fn new(line: &'a str) -> Self {
        Self { iter: line.chars().peekable() }
    }

    fn next(&mut self) -> Option<Token> {
        self.skip_whitespace();
        match self.iter.peek() {
            Some(_) => {
                let mut word = String::new();
                while let Some(c) = self.next_if(|c| !c.is_whitespace()) {
                    word.push(c);
                }
                if let Some(keyword) = Keyword::from_str(&word) {
                    return Some(Token::Keyword(keyword));
                }
                Some(Token::String(word))
            }
            None => None,
        }
    }

    /// Skip any consecutive whitespace chars
    fn skip_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    /// Consumes the next consecutive chars as a string until the predicate is not
    /// match anymore.
    fn next_while<F>(&mut self, predicate: F) -> Option<String>
    where
        F: Fn(char) -> bool,
    {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c)
        }
        Some(value).filter(|it| !it.is_empty())
    }

    /// Consumes the next char if it matches the predicate function.
    fn next_if<F>(&mut self, predicate: F) -> Option<char>
    where
        F: Fn(char) -> bool,
    {
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;
    use std::io::Cursor;

    use crate::error::Result;
    use crate::slt::parser::Parser;
    use crate::sql::format;

    #[test]
    fn test_slt_parser() -> Result<()> {
        let script = r#"
            # comment
            statement ok
            CREATE TABLE a (
              b integer primary key,
              c text
            );

            # comment
            statement error
            CREATE TABLE b (
              b integer primary key,
              c text
            );

            # comment
            query nosort
            select * from
            a;
            ----
            1 2 3
            4 5 6

            # comment
            query rowsort
            select * from a;
            ----
            1 2 3
            4 5 6

            # comment
            halt

            # comment
            sleep 100

            include path/to/slt/file

        "#;

        let script = format::dedent(script, true);
        let cursor = Cursor::new(script);
        let records = Parser {}.parse(BufReader::new(Box::new(cursor)))?;
        assert_eq!(7, records.len());
        Ok(())
    }
}
