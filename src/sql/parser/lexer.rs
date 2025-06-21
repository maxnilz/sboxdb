use std::fmt::write;
use std::iter::Peekable;
use std::str::Chars;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    EOF,
    Ident(String, bool),
    CompoundIdent(String),
    Number(String),
    String(String),
    Keyword(Keyword),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Exclamation,
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::EOF => f.write_str("EOF"),
            Token::Ident(s, quoted) => {
                if *quoted {
                    write!(f, "\"{s}\"")
                } else {
                    f.write_str(s)
                }
            }
            Token::CompoundIdent(s) => f.write_str(s),
            Token::Number(n) => f.write_str(n),
            Token::String(s) => f.write_str(s),
            Token::Keyword(k) => f.write_str(k.to_str()),
            Token::LParen => f.write_str("("),
            Token::RParen => f.write_str(")"),
            Token::Comma => f.write_str(","),
            Token::Semicolon => f.write_str(";"),
            Token::Exclamation => f.write_str("!"),
            Token::Plus => f.write_str("+"),
            Token::Minus => f.write_str("-"),
            Token::Mul => f.write_str("*"),
            Token::Div => f.write_str("/"),
            Token::Mod => f.write_str("%"),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("!="),
            Token::Gt => f.write_str(">"),
            Token::GtEq => f.write_str(">="),
            Token::Lt => f.write_str("<"),
            Token::LtEq => f.write_str("<="),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Index,
    Integer,
    BigInt,
    Double,
    Boolean,
    Text,
    Varchar,
    Primary,
    Key,
    If,
    Not,
    Is,
    Null,
    True,
    False,
    Unique,
    Default,
    And,
    Or,
    Like,
    ILike,
    In,
    Exists,
    On,
    Add,
    Drop,
    Alter,
    Column,
}

impl Keyword {
    fn from_str(str: &str) -> Option<Keyword> {
        let ans = match str.to_uppercase().as_ref() {
            "CREATE" => Self::Create,
            "TABLE" => Self::Table,
            "INDEX" => Self::Index,
            "INTEGER" => Self::Integer,
            "BIGINT" => Self::BigInt,
            "DOUBLE" => Self::Double,
            "BOOLEAN" => Self::Boolean,
            "TEXT" => Self::Text,
            "VARCHAR" => Self::Varchar,
            "PRIMARY" => Self::Primary,
            "KEY" => Self::Key,
            "IF" => Self::If,
            "NOT" => Self::Not,
            "IS" => Self::Is,
            "NULL" => Self::Null,
            "TRUE" => Self::True,
            "FALSE" => Self::False,
            "UNIQUE" => Self::Unique,
            "DEFAULT" => Self::Default,
            "AND" => Self::And,
            "OR" => Self::Or,
            "LIKE" => Self::Like,
            "ILIKE" => Self::ILike,
            "IN" => Self::In,
            "EXISTS" => Self::Exists,
            "ON" => Self::On,
            "ADD" => Self::Add,
            "DROP" => Self::Drop,
            "ALTER" => Self::Alter,
            "COLUMN" => Self::Column,
            _ => return None,
        };
        Some(ans)
    }

    fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Index => "INDEX",
            Keyword::Integer => "INTEGER",
            Keyword::BigInt => "BIGINT",
            Keyword::Double => "DOUBLE",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
            Keyword::If => "IF",
            Keyword::Not => "NOT",
            Keyword::Is => "IS",
            Keyword::Null => "NULL",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Unique => "UNIQUE",
            Keyword::Default => "DEFAULT",
            Keyword::And => "AND",
            Keyword::Or => "OR",
            Keyword::Like => "LIKE",
            Keyword::ILike => "ILIKE",
            Keyword::In => "IN",
            Keyword::Exists => "EXISTS",
            Keyword::On => "ON",
            Keyword::Add => "ADD",
            Keyword::Drop => "DROP",
            Keyword::Alter => "ALTER",
            Keyword::Column => "COLUMN",
        }
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

/// Lexer produce token with the given input string, it is
/// also known as tokenizer.
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer { iter: input.chars().peekable() }
    }

    /// Check if the next char match the given char, if yes, consume it
    /// otherwise do not move forward.
    fn consume_if_char(&mut self, c: char) -> bool {
        if let Some(c0) = self.iter.peek() {
            if c0 == &c {
                self.next();
                return true;
            }
        };
        false
    }

    /// Consumes the next char if it matches the predicate function.
    fn next_if<F>(&mut self, predicate: F) -> Option<char>
    where
        F: Fn(char) -> bool,
    {
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    /// Consumes the next single-character token if the tokenizer function returns one
    fn next_if_token<F>(&mut self, tokenizer: F) -> Option<Token>
    where
        F: Fn(char) -> Option<Token>,
    {
        let token = self.iter.peek().and_then(|c| tokenizer(*c))?;
        self.iter.next();
        Some(token)
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

    /// Skip any consecutive whitespace chars
    fn skip_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    /// Scans the input for the next token if any, ignoring leading whitespace
    fn scan(&mut self) -> Result<Option<Token>> {
        self.skip_whitespace();
        match self.iter.peek() {
            Some('\'') => self.scan_string_literal(),
            Some('"') => self.scan_quoted_ident(),
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_alphabetic_chars()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }
    }

    /// Scans the input for the next symbol token, if any, and
    /// handle any multi-symbol tokens
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '(' => Some(Token::LParen),
            ')' => Some(Token::RParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '*' => Some(Token::Mul),
            '/' => Some(Token::Div),
            '%' => Some(Token::Mod),
            '=' => Some(Token::Eq),
            '!' => Some(Token::Exclamation),
            '>' => Some(Token::Gt),
            '<' => Some(Token::Lt),
            _ => None,
        })
        .map(|it| match it {
            Token::Exclamation if self.consume_if_char('=') => Token::Neq,
            Token::Gt if self.consume_if_char('=') => Token::GtEq,
            Token::Lt if self.consume_if_char('=') => Token::LtEq,
            tok => tok,
        })
    }

    /// Scans consecutive chars as keyword or unquoted identifier
    fn scan_alphabetic_chars(&mut self) -> Option<Token> {
        let mut name = self.next_if(|c| c.is_alphabetic())?.to_string();
        let mut compound = false;
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            name.push(c);
            if !compound && c == '.' {
                compound = true
            }
        }
        if let Some(keyword) = Keyword::from_str(&name) {
            return Some(Token::Keyword(keyword));
        }
        if !name.contains('.') {
            return Some(Token::Ident(name, false));
        }
        Some(Token::CompoundIdent(name))
    }

    /// Scans consecutive regular numeric chars
    fn scan_number(&mut self) -> Option<Token> {
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            while let Some(it) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(it)
            }
        }
        if let Some(exp) = self.next_if(|c| c == 'e' || c == 'E') {
            num.push(exp);
            if let Some(it) = self.next_if(|c| c == '-' || c == '+') {
                num.push(it)
            }
            while let Some(it) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(it)
            }
        }
        Some(Token::Number(num))
    }

    /// Scans double quoted identifier
    fn scan_quoted_ident(&mut self) -> Result<Option<Token>> {
        if self.next_if(|c| c == '"').is_none() {
            return Ok(None);
        }
        let mut s = String::new();
        loop {
            match self.iter.next() {
                Some('"') => break,
                Some(c) => s.push(c),
                None => return Err(Error::parse("Unexpected end of quoted identifier")),
            }
        }
        Ok(Some(Token::Ident(s, true)))
    }

    /// Scans string literal quoted with single quote
    fn scan_string_literal(&mut self) -> Result<Option<Token>> {
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }
        let mut s = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => s.push(c),
                None => return Err(Error::parse("Unexpected end of string string literal")),
            }
        }
        Ok(Some(Token::String(s)))
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Result<Token>> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => {
                // if we have any chars left, consider it as syntax error,
                // otherwise return None.
                self.iter.peek().map(|c| Err(Error::parse(format!("Unexpected character {}", c))))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

mod tests {
    use super::*;
    use crate::sql::parser::display_utils::display_comma_separated;

    #[test]
    fn test_ddl_stmts() -> Result<()> {
        let cases = vec![
            (r###"
            CREATE TABLE if not exists foo(
              col1 integer primary key,
              col2 varchar(20) NOT NULL,
              col3 integer default 1,
              col4 double default 3.14,
              col5 double default -2.1E-4 + 2,
              col6 text default 'a' NOT NULL,
              "col 7" text NULL
            );
            "###),
            (r###"
            CREATE UNIQUE INDEX IF NOT EXISTS index1 ON table1 (col1, col2);
            "###),
            (r###"
            DROP TABLE foo;
            "###),
            (r###"
            DROP INDEX index1;
            "###),
            (r###"
            ALTER TABLE table1
                ADD COLUMN IF NOT EXISTS col1 TEXT NOT NULL default 'a',
                DROP COLUMN if EXISTS col2;
            "###),
        ];

        for (i, (sql)) in cases.iter().enumerate() {
            let mut tokens = vec![];
            let mut lexer = Lexer::new(sql);
            while let Some(tok) = lexer.next().transpose()? {
                tokens.push(tok)
            }
            print!("{} ==>", i);
            for token in &tokens {
                print!("{:?},", token)
            }
            println!()
        }
        Ok(())
    }
}
