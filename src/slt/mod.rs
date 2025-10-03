//! SQL Logic Tests (SLT) utils.
//!
//! This module provides tools to parse and execute SQL Logic Test scripts.
//! For more information about the SLT format, see:
//! <https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki>

mod parser;

use std::fmt::Display;
use std::fmt::Formatter;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::thread;
use std::time::Duration;

use crate::access::engine::Engine;
use crate::parse_err;
use crate::session::Session;
use crate::slt::parser::Include;
use crate::slt::parser::Parser;
use crate::slt::parser::Query;
use crate::slt::parser::Record;
use crate::slt::parser::SortMode;
use crate::slt::parser::Statement;

/// SQL Logical Test evaluator
pub struct Slt<E: Engine> {
    engine: E,
}

impl<E: Engine + 'static> Slt<E> {
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    pub fn check_script(&self, script: Box<dyn Read>) -> Result<()> {
        let mut session = Session::new(self.engine.clone());
        let records = Parser {}.parse(BufReader::new(script))?;
        for record in records {
            match record {
                Record::Statement(Statement { sql, .. }) => {
                    let a = format!("CHECK {}", sql);
                    session
                        .execute_query(a)
                        .map_err(|err| parse_err!("Invalid sql {} err: {:?}", sql, err))?;
                }
                Record::Query(Query { sql, .. }) => {
                    let a = format!("CHECK {}", sql);
                    session
                        .execute_query(a)
                        .map_err(|err| parse_err!("Invalid sql {} err: {:?}", sql, err))?;
                }
                Record::Sleep(_) => {}
                Record::Halt => {}
                Record::Include(Include { filename, .. }) => {
                    let file = File::open(filename)?;
                    self.check_script(Box::new(file))?;
                }
            }
        }
        Ok(())
    }
    pub fn evaluate_script(
        &self,
        name: impl Into<String>,
        script: Box<dyn Read>,
    ) -> Result<Report> {
        let mut report = Report::new(name);
        let mut session = Session::new(self.engine.clone());
        self.evaluate(&mut session, script, &mut report)?;
        Ok(report)
    }

    fn evaluate(
        &self,
        session: &mut Session<E>,
        script: Box<dyn Read>,
        report: &mut Report,
    ) -> Result<()> {
        let records = Parser {}.parse(BufReader::new(script))?;

        for record in records.into_iter() {
            match &record {
                Record::Statement(stmt) => {
                    let res = self.evaluate_stmt(session, stmt);
                    report.append(record, res);
                }
                Record::Query(q) => {
                    let res = self.evaluate_query(session, q);
                    report.append(record, res);
                }
                Record::Sleep(seconds) => {
                    thread::sleep(Duration::from_secs(*seconds as u64));
                }
                Record::Halt => return Ok(()),
                Record::Include(Include { filename }) => {
                    let file = File::open(filename)?;
                    self.evaluate(session, Box::new(file), report)?;
                }
            }
        }
        Ok(())
    }

    fn evaluate_stmt(&self, session: &mut Session<E>, stmt: &Statement) -> Result<()> {
        let expect_ok = stmt.expect_ok;
        let res = session.execute_query(&stmt.sql);

        let expected = if stmt.expect_ok { "ok" } else { "error" };
        match res {
            Ok(_) if expect_ok => Ok(()),
            Err(_) if !expect_ok => Ok(()),
            Ok(_) if !expect_ok => Err(Error::new_unexpected(&stmt.sql, expected, "ok")),
            Err(err) if expect_ok => {
                Err(Error::new_unexpected(&stmt.sql, expected, err.to_string()))
            }
            _ => unreachable!(),
        }
    }

    fn evaluate_query(&self, session: &mut Session<E>, query: &Query) -> Result<()> {
        let sql = &query.sql;
        let expected_result = &query.expected_result;
        let rs = session.execute_query(sql)?;
        let mut result = rs
            .iter()
            .map(|row| row.iter().map(|it| it.to_string()).collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        match query.sort_mode {
            SortMode::NoSort => {
                if expected_result == &result {
                    return Ok(());
                }
                Err(Error::new_unexpected(sql, expected_result.join("\n"), result.join("\n")))
            }
            SortMode::RowSort => {
                let mut expected_result = expected_result.clone();
                result.sort();
                expected_result.sort();
                if expected_result == result {
                    return Ok(());
                }
                Err(Error::new_unexpected(sql, expected_result.join("\n"), result.join("\n")))
            }
        }
    }
}

#[derive(Debug)]
pub enum Error {
    ExecError(crate::error::Error),
    Unexpected { sql: String, expected: String, got: String },
}

impl Error {
    fn new_unexpected(
        sql: impl Into<String>,
        expected: impl Into<String>,
        got: impl Into<String>,
    ) -> Self {
        Self::Unexpected { sql: sql.into(), expected: expected.into(), got: got.into() }
    }
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ExecError(err) => write!(f, "{}", err),
            Error::Unexpected { sql, expected, got } => {
                write!(f, "Unexpected result at sql: {}\n", sql)?;
                write!(f, "=== Expected: \n{}\n", expected)?;
                write!(f, "=== Got: \n{}\n", got)?;
                Ok(())
            }
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::ExecError(err.into())
    }
}

impl From<crate::error::Error> for Error {
    fn from(err: crate::error::Error) -> Self {
        Self::ExecError(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Report {
    script_name: String,
    records: Vec<Record>,
    results: Vec<Result<()>>,
}

impl Report {
    fn new(script_name: impl Into<String>) -> Self {
        Self { script_name: script_name.into(), records: vec![], results: vec![] }
    }

    fn append(&mut self, r: Record, res: Result<()>) {
        self.records.push(r);
        self.results.push(res);
    }

    pub fn num_passed(&self) -> usize {
        self.results.iter().filter(|&it| matches!(it, Ok(_))).collect::<Vec<_>>().len()
    }

    pub fn num_failed(&self) -> usize {
        self.results.iter().filter(|&it| matches!(it, Err(_))).collect::<Vec<_>>().len()
    }
}

macro_rules! tap_out {
    ($f:expr, $i:expr,$record:expr) => {
        write!($f, "ok {} - {}\n", $i, $record)
    };
    ($f:expr, $i:expr,$record:expr,$err:expr) => {
        write!($f, "not ok {} - {} {}\n", $i, $record, $err)
    };
}

impl Display for Report {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "1..{num}{name}\n",
            num = self.records.len(),
            name = if self.script_name.is_empty() {
                "".to_string()
            } else {
                format!(" - {}", self.script_name)
            },
        )?;
        for (i, r) in self.records.iter().enumerate() {
            let res = &self.results[i];
            match r {
                Record::Statement(stmt) => match res {
                    Ok(_) => tap_out!(f, i + 1, stmt)?,
                    Err(err) => tap_out!(f, i + 1, stmt, err)?,
                },
                Record::Query(q) => match res {
                    Ok(_) => tap_out!(f, i + 1, q)?,
                    Err(err) => tap_out!(f, i + 1, q, err)?,
                },
                _ => unreachable!(),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;
    use std::path::Path;

    use super::*;
    use crate::access::kv::Kv;
    use crate::sql::format;
    use crate::storage::memory::Memory;

    #[test]
    fn test_evaluate_scripts() -> Result<()> {
        let dir = "src/slt/script";
        let reports = evaluate_dir(dir)?;
        let mut num_failed = 0;
        for report in reports {
            println!("{}\n", report);
            num_failed += report.num_failed();
        }
        assert_eq!(0, num_failed);
        Ok(())
    }

    fn evaluate_dir<P: AsRef<Path>>(dir: P) -> Result<Vec<Report>> {
        let mut scripts = vec![];
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            scripts.push(path)
        }
        let mut reports = vec![];
        for script in scripts {
            let name = script.display().to_string();
            let file = File::open(script)?;
            let slt = Slt::new(Kv::new(Memory::new()));
            let report = slt.evaluate_script(name, Box::new(file))?;
            reports.push(report);
        }
        Ok(reports)
    }

    #[test]
    fn test_check_script_file() -> Result<()> {
        let file = "src/slt/script/001-intro.slt";
        let slt = Slt::new(Kv::new(Memory::new()));
        let file = File::open(file)?;
        slt.check_script(Box::new(file))
    }

    #[test]
    fn test_evaluate_script_file() -> Result<()> {
        let _file = "src/slt/script/003-groupby.slt";
        let file = "src/slt/script/004-join.slt";
        let slt = Slt::new(Kv::new(Memory::new()));
        let file = File::open(file)?;
        let report = slt.evaluate_script("", Box::new(file))?;
        println!("{}", report);
        Ok(())
    }

    #[test]
    fn test_evaluate() -> Result<()> {
        let script = r#"
            statement ok
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP NOT NULL
            );

            statement ok
            INSERT INTO users (id, name, email, created_at) VALUES
                (1, 'Alice', 'alice@example.com', '2025-09-22 07:00:00'),
                (2, 'Bob', 'bob@example.com', '2025-09-22 07:00:01'),
                (3, 'Charlie', 'charlie@example.com', '2025-09-22 07:00:02');

            query rowsort
            SELECT id, name FROM users;
            ----
            1 'Alice'
            2 'Bob'
            3 'Charlie'
        "#;
        let script = format::dedent(script, true);
        let slt = Slt::new(Kv::new(Memory::new()));
        slt.check_script(Box::new(Cursor::new(script.clone())))?;
        let report = slt.evaluate_script("", Box::new(Cursor::new(script)))?;
        println!("{}\n", report);
        assert_eq!(3, report.num_passed());
        assert_eq!(0, report.num_failed());
        Ok(())
    }

    #[test]
    fn test_split() -> Result<()> {
        let a = "\\d abc";
        let splits: Vec<&str> = a.splitn(2, " ").collect();
        println!("{:?}", splits);
        println!("{:}", splits.last().unwrap());
        Ok(())
    }
}
