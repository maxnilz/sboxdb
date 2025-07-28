use std::fmt::Display;
use std::str::FromStr;

use crate::error::Error;
use crate::error::Result;
use crate::sql::parser::ast::AlterTableOperation;
use crate::sql::parser::ast::Assignment;
use crate::sql::parser::ast::BinaryOperator;
use crate::sql::parser::ast::Column;
use crate::sql::parser::ast::CreateIndex;
use crate::sql::parser::ast::DataType;
use crate::sql::parser::ast::Expr;
use crate::sql::parser::ast::Function;
use crate::sql::parser::ast::FunctionArg;
use crate::sql::parser::ast::Ident;
use crate::sql::parser::ast::Insert;
use crate::sql::parser::ast::InsertSource;
use crate::sql::parser::ast::Join;
use crate::sql::parser::ast::JoinConstraint;
use crate::sql::parser::ast::JoinOperator;
use crate::sql::parser::ast::LimitClause;
use crate::sql::parser::ast::ObjectType;
use crate::sql::parser::ast::OrderByExpr;
use crate::sql::parser::ast::Precedence;
use crate::sql::parser::ast::Query;
use crate::sql::parser::ast::SelectItem;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::ast::TableFactor;
use crate::sql::parser::ast::TableWithJoins;
use crate::sql::parser::ast::UnaryOperator;
use crate::sql::parser::ast::Update;
use crate::sql::parser::ast::Value;
use crate::sql::parser::ast::Values;
use crate::sql::parser::ast::WildcardExpr;
use crate::sql::parser::lexer::Keyword;
use crate::sql::parser::lexer::Lexer;
use crate::sql::parser::lexer::Token;

pub mod ast;
mod display_utils;
mod lexer;

pub struct Parser {
    /// The tokens
    tokens: Vec<Token>,
    /// The index of the first unprocessed token in [`Parser::tokens`].
    index: usize,
}

impl Parser {
    /// Creates a new parser for the given string input
    #[allow(dead_code)]
    pub fn new(query: &str) -> Result<Parser> {
        let tokens = Lexer::new(query).into_iter().collect::<Result<Vec<_>>>()?;
        Ok(Parser { tokens, index: 0 })
    }

    /// Parse potentially multiple statements
    /// e.g., "SELECT * FROM foo; SELECT * FROM bar;"
    #[allow(dead_code)]
    pub fn parse_statements(&mut self) -> Result<Vec<Statement>> {
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.consume_token(&Token::Semicolon) {
                expecting_statement_delimiter = false;
            }

            match self.peek_token() {
                Token::EOF => break,
                _ => {}
            }

            if expecting_statement_delimiter {
                return self.expected("end of statement", self.peek_token());
            }

            let statement = self.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Parses the input string into an AST statement
    pub fn parse_statement(&mut self) -> Result<Statement> {
        let next_token = self.next_token();
        match &next_token {
            Token::Keyword(w) => match w {
                Keyword::Begin => self.parse_begin(),
                Keyword::Commit => Ok(Statement::Commit),
                Keyword::Rollback => Ok(Statement::Rollback),
                Keyword::Create => self.parse_ddl_create(),
                Keyword::Drop => self.parse_ddl_drop(),
                Keyword::Alter => self.parse_ddl_alter(),
                Keyword::Select => {
                    self.backup_token();
                    self.parse_dml_select()
                }
                Keyword::Insert => self.parse_dml_insert(),
                Keyword::Update => self.parse_dml_update(),
                Keyword::Delete => self.parse_dml_delete(),
                Keyword::Explain => self.parse_explain(),
                _ => self.expected("an SQL statement", next_token),
            },
            _ => self.expected("an SQL statement", next_token),
        }
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        let analyze = self.parse_keyword(Keyword::Analyze);
        let verbose = self.parse_keyword(Keyword::Verbose);
        let statement = self.parse_statement()?;
        Ok(Statement::Explain { analyze, verbose, statement: Box::new(statement) })
    }

    fn parse_dml_delete(&mut self) -> Result<Statement> {
        self.expect_keyword(&Keyword::From)?;
        let table = self.parse_ident()?;
        let selection =
            if self.parse_keyword(Keyword::Where) { Some(self.parse_expr()?) } else { None };
        Ok(Statement::Delete { table, selection })
    }

    fn parse_dml_update(&mut self) -> Result<Statement> {
        let table = self.parse_ident()?;
        self.expect_keyword(&Keyword::Set)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let selection =
            if self.parse_keyword(Keyword::Where) { Some(self.parse_expr()?) } else { None };
        Ok(Statement::Update(Update { table, assignments, selection }))
    }

    /// Parse a `var = expr` assignment, used in an UPDATE statement
    pub fn parse_assignment(&mut self) -> Result<Assignment> {
        let column = self.parse_ident()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { column, value })
    }
    fn parse_dml_insert(&mut self) -> Result<Statement> {
        self.expect_keyword(&Keyword::Into)?;
        let table = self.parse_ident()?;
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_ident)?;
        self.expect_token(&Token::RParen)?;
        let next_token = self.next_token();
        let source = match next_token {
            Token::Keyword(Keyword::Values) => {
                let values = self.parse_values()?;
                Ok(InsertSource::Values(values))
            }
            Token::Keyword(Keyword::Select) => {
                self.backup_token();
                let query = self.parse_query()?;
                Ok(InsertSource::Select(query))
            }
            _ => self.expected("'(' or 'SELECT' after VALUES", next_token),
        }?;
        Ok(Statement::Insert(Insert { table, columns, source }))
    }

    fn parse_values(&mut self) -> Result<Values> {
        let rows = self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values { rows })
    }

    fn parse_begin(&mut self) -> Result<Statement> {
        let read_only = self.parse_keywords(&[Keyword::Read, Keyword::Only]);
        let as_of = if self.parse_keywords(&[Keyword::As, Keyword::Of]) {
            let next_token = self.next_token();
            match next_token {
                Token::Number(s) => Ok(Some(s)),
                _ => self.expected("as of version", next_token),
            }
        } else {
            Ok(None)
        }?;
        Ok(Statement::Begin { read_only, as_of })
    }

    fn parse_ddl_alter(&mut self) -> Result<Statement> {
        let object_type = self.expect_one_of_keywords(&[Keyword::Table])?;
        match object_type {
            Keyword::Table => {
                let if_exists = self.parse_keywords(&[Keyword::If, Keyword::Exists]);
                let table_name = self.parse_ident()?;
                let operations =
                    self.parse_comma_separated(Parser::parse_ddl_alter_table_operation)?;
                Ok(Statement::AlterTable { table_name, if_exists, operations })
            }
            _ => unreachable!(),
        }
    }

    fn parse_ddl_alter_table_operation(&mut self) -> Result<AlterTableOperation> {
        let keyword = self.expect_one_of_keywords(&[Keyword::Add, Keyword::Drop])?;
        match &keyword {
            Keyword::Add => {
                self.expect_token(&Token::Keyword(Keyword::Column))?;
                let if_not_exists =
                    self.parse_keywords(&[Keyword::If, Keyword::Not, Keyword::Exists]);
                let column = self.parse_ddl_column_spec()?;
                Ok(AlterTableOperation::AddColumn { if_not_exists, column })
            }
            Keyword::Drop => {
                self.expect_token(&Token::Keyword(Keyword::Column))?;
                let if_exists = self.parse_keywords(&[Keyword::If, Keyword::Exists]);
                let column_name = self.parse_ident()?;
                Ok(AlterTableOperation::DropColumn { if_exists, column_name })
            }
            _ => unreachable!(),
        }
    }

    fn parse_ddl_drop(&mut self) -> Result<Statement> {
        let object_type = self.expect_one_of_keywords(&[Keyword::Table, Keyword::Index])?;
        let object_type = match object_type {
            Keyword::Table => ObjectType::Table,
            Keyword::Index => ObjectType::Index,
            _ => unreachable!(),
        };
        let if_exists = self.parse_keywords(&[Keyword::If, Keyword::Exists]);
        let object_name = self.parse_ident()?;
        Ok(Statement::Drop { object_type, if_exists, object_name })
    }

    fn parse_ddl_create(&mut self) -> Result<Statement> {
        if self.parse_keyword(Keyword::Table) {
            return self.parse_ddl_create_table();
        }
        if self.parse_keyword(Keyword::Index) {
            return self.parse_ddl_create_index(false);
        }
        if self.parse_keywords(&[Keyword::Unique, Keyword::Index]) {
            return self.parse_ddl_create_index(true);
        }
        self.expected("an object type after CREATE", self.peek_token())
    }

    fn parse_ddl_create_index(&mut self, unique: bool) -> Result<Statement> {
        let if_not_exists = self.parse_keywords(&[Keyword::If, Keyword::Not, Keyword::Exists]);
        let index_name = self.parse_ident()?;
        self.expect_token(&Token::Keyword(Keyword::On))?;
        let table_name = self.parse_ident()?;
        self.expect_token(&Token::LParen)?;
        let column_names = self.parse_comma_separated(Parser::parse_ident)?;
        self.expect_token(&Token::RParen)?;
        let create_index =
            CreateIndex { name: index_name, table_name, column_names, unique, if_not_exists };
        Ok(Statement::CreateIndex(create_index))
    }

    fn parse_ddl_create_table(&mut self) -> Result<Statement> {
        let if_not_exists = self.parse_keywords(&[Keyword::If, Keyword::Not, Keyword::Exists]);
        let table_name = self.parse_ident()?;
        let columns = self.parse_columns()?;
        let create_table = ast::CreateTable { name: table_name, columns, if_not_exists };
        Ok(Statement::CreateTable(create_table))
    }

    fn parse_columns(&mut self) -> Result<Vec<Column>> {
        let mut columns = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(columns);
        }
        loop {
            columns.push(self.parse_ddl_column_spec()?);
            let comma = self.consume_token(&Token::Comma);
            let rparen = self.peek_token() == Token::RParen;
            if !comma && !rparen {
                return self.expected("',' or ')' after column definition", self.peek_token());
            };
            if rparen {
                let _ = self.consume_token(&Token::RParen);
                break;
            }
        }
        Ok(columns)
    }

    fn parse_ddl_column_spec(&mut self) -> Result<Column> {
        let column_name = self.parse_ident()?;
        let next_token = self.next_token();
        let datatype = match &next_token {
            Token::Keyword(w) => {
                match w {
                    Keyword::Integer => Ok(DataType::Integer),
                    Keyword::BigInt => Ok(DataType::Integer),
                    Keyword::Double => Ok(DataType::Float),
                    Keyword::Boolean => Ok(DataType::Boolean),
                    Keyword::Text => Ok(DataType::String),
                    Keyword::Varchar => {
                        // parse optional character length
                        if self.consume_token(&Token::LParen) {
                            let _ = self.parse_literal_uint()?;
                            self.expect_token(&Token::RParen)?;
                        }
                        Ok(DataType::String)
                    }
                    _ => self.expected("a data type name", next_token),
                }
            }
            _ => self.expected("a data type name", next_token),
        }?;
        let mut column = Column { name: column_name, datatype, ..Default::default() };
        loop {
            let ok = self.parse_optional_column_option(&mut column)?;
            if !ok {
                break;
            }
        }
        Ok(column)
    }

    fn parse_optional_column_option(&mut self, column: &mut Column) -> Result<bool> {
        if self.parse_keywords(&[Keyword::Primary, Keyword::Key]) {
            column.primary_key = true;
            return Ok(true);
        }

        if self.parse_keywords(&[Keyword::Not, Keyword::Null]) {
            column.nullable = false;
            return Ok(true);
        }
        if self.parse_keyword(Keyword::Null) {
            column.nullable = true;
            return Ok(true);
        }

        if self.parse_keyword(Keyword::Unique) {
            column.unique = true;
            return Ok(true);
        }
        if self.parse_keyword(Keyword::Default) {
            let expr = self.parse_expr()?;
            column.default = Some(expr);
            return Ok(true);
        }
        Ok(false)
    }

    /// Parses an expr using Pratt (top-down precedence) parser
    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_subexpr(self.prec_unknown())
    }

    fn parse_subexpr(&mut self, precedence: u8) -> Result<Expr> {
        let mut expr = self.parse_prefix()?;
        loop {
            let next_precedence = self.get_next_precedence()?;
            if next_precedence <= precedence {
                break;
            }
            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Result<Expr> {
        let next_token = self.next_token();
        let regular_binary_op = match &next_token {
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Div => Some(BinaryOperator::Divide),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Keyword(w) => match w {
                Keyword::And => Some(BinaryOperator::And),
                Keyword::Or => Some(BinaryOperator::Or),
                _ => None,
            },
            _ => None,
        };
        if let Some(op) = regular_binary_op {
            let rhs = self.parse_subexpr(precedence)?;
            return Ok(Expr::BinaryOp { left: Box::new(expr), op, right: Box::new(rhs) });
        }

        match &next_token {
            Token::Keyword(w) if w == &Keyword::Is => return self.parse_is(expr),
            Token::Keyword(w)
                if w == &Keyword::Not
                    || w == &Keyword::Like
                    || w == &Keyword::ILike
                    || w == &Keyword::In =>
            {
                self.backup_token();
                let negated = self.parse_keyword(Keyword::Not);
                if self.parse_keyword(Keyword::In) {
                    return self.parse_in(expr, negated);
                }
                if self.parse_keyword(Keyword::Like) {
                    let rhs = self.parse_subexpr(self.prec_value(Precedence::Like))?;
                    return Ok(Expr::Like {
                        negated,
                        expr: Box::new(expr),
                        pattern: Box::new(rhs),
                    });
                }
                if self.parse_keyword(Keyword::ILike) {
                    let rhs = self.parse_subexpr(self.prec_value(Precedence::Like))?;
                    return Ok(Expr::ILike {
                        negated,
                        expr: Box::new(expr),
                        pattern: Box::new(rhs),
                    });
                }
            }
            _ => return self.expected("an infix operator", next_token),
        };
        self.expected("an infix operator", next_token)
    }

    fn parse_in(&mut self, expr: Expr, negated: bool) -> Result<Expr> {
        self.expect_token(&Token::LParen)?;
        let in_op = match self.maybe_parse(|p| p.parse_query())? {
            Some(subquery) => Expr::InSubquery { expr: Box::new(expr), subquery, negated },
            None => {
                let list = self.parse_comma_separated(Parser::parse_expr)?;
                Expr::InList { expr: Box::new(expr), list, negated }
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    fn parse_is(&mut self, expr: Expr) -> Result<Expr> {
        if self.parse_keyword(Keyword::Null) {
            return Ok(Expr::IsNull(Box::new(expr)));
        }
        if self.parse_keywords(&[Keyword::Not, Keyword::Null]) {
            return Ok(Expr::IsNotNull(Box::new(expr)));
        }
        if self.parse_keyword(Keyword::True) {
            return Ok(Expr::IsTrue(Box::new(expr)));
        }
        if self.parse_keywords(&[Keyword::Not, Keyword::True]) {
            return Ok(Expr::IsNotTrue(Box::new(expr)));
        }
        if self.parse_keyword(Keyword::False) {
            return Ok(Expr::IsFalse(Box::new(expr)));
        }
        if self.parse_keywords(&[Keyword::Not, Keyword::False]) {
            return Ok(Expr::IsNotFalse(Box::new(expr)));
        }
        self.expected("[NOT] NULL | TRUE | FALSE", self.peek_token())
    }

    /// Parse expr prefix(position-wise)
    fn parse_prefix(&mut self) -> Result<Expr> {
        let next_token = self.next_token();
        let expr = match &next_token {
            Token::Keyword(w) if w == &Keyword::True => Ok(Expr::Value(Value::Boolean(true))),
            Token::Keyword(w) if w == &Keyword::False => Ok(Expr::Value(Value::Boolean(false))),
            Token::Keyword(w) if w == &Keyword::Not => self.parse_not(),
            Token::Keyword(w) if w == &Keyword::Null => Ok(Expr::Value(Value::Null)),
            Token::Keyword(w) if w == &Keyword::Exists => self.parse_exists_expr(false),
            tok @ Token::Ident(_, _) => match self.peek_token() {
                Token::LParen => self.parse_function(Ident::from_ident_token(tok)),
                _ => {
                    let ident = Ident::from_ident_token(tok);
                    if let Some(idents) = self.try_parse_compound_idents(ident.clone())? {
                        Ok(Expr::CompoundIdentifier(idents))
                    } else {
                        Ok(Expr::Identifier(ident))
                    }
                }
            },
            Token::Number(s) => Ok(Expr::Value(Value::Number(s.clone()))),
            Token::String(s) => Ok(Expr::Value(Value::String(s.clone()))),
            tok @ Token::Plus | tok @ Token::Minus => {
                let op =
                    if *tok == Token::Plus { UnaryOperator::Plus } else { UnaryOperator::Minus };
                // TODO: double check precedence here for grouping the subexpr
                let expr = self.parse_subexpr(self.prec_value(Precedence::UnaryOp))?;
                Ok(Expr::UnaryOp { op, expr: Box::new(expr) })
            }
            Token::LParen => {
                // try to parse the scalar subquery first
                if let Some(expr) = self.try_parse_expr_scalar_subquery()? {
                    self.expect_token(&Token::RParen)?;
                    return Ok(expr);
                }

                // try to parse the tuple/grouped expr
                let exprs = self.parse_comma_separated(Parser::parse_expr)?;
                let expr = match exprs.len() {
                    0 => unreachable!(),
                    1 => Expr::Nested(Box::new(exprs.into_iter().next().unwrap())),
                    _ => Expr::Tuple(exprs),
                };
                self.expect_token(&Token::RParen)?;
                return Ok(expr);
            }
            _ => self.expected_ref("an expression", &next_token),
        }?;
        Ok(expr)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        match self.peek_token() {
            Token::Keyword(w) if w == Keyword::Exists => {
                let negated = true;
                let _ = self.parse_keyword(Keyword::Exists);
                self.parse_exists_expr(negated)
            }
            _ => {
                // TODO: double check precedence here for grouping the subexpr
                let expr = self.parse_subexpr(self.prec_value(Precedence::UnaryOp))?;
                Ok(Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new(expr) })
            }
        }
    }
    fn parse_exists_expr(&mut self, negated: bool) -> Result<Expr> {
        self.expect_token(&Token::LParen)?;
        let expr = Expr::Exists { subquery: self.parse_query()?, negated };
        self.expect_token(&Token::RParen)?;
        Ok(expr)
    }

    fn parse_function(&mut self, func_name: Ident) -> Result<Expr> {
        let func = self.parse_function_call(func_name)?;
        Ok(Expr::Function(func))
    }

    fn parse_function_call(&mut self, func_name: Ident) -> Result<Function> {
        self.expect_token(&Token::LParen)?;
        if self.consume_token(&Token::RParen) {
            return Ok(Function { name: func_name, args: vec![] });
        }
        let args = self.parse_comma_separated(Parser::parse_function_arg)?;
        let function = Function { name: func_name, args };
        self.expect_token(&Token::RParen)?;
        Ok(function)
    }

    fn parse_function_arg(&mut self) -> Result<FunctionArg> {
        let next_token = self.next_token();
        match &next_token {
            Token::Mul => Ok(FunctionArg::Asterisk),
            tok @ Token::Ident(_, _) => match self.peek_token() {
                Token::LParen => Ok(FunctionArg::Function(
                    self.parse_function_call(Ident::from_ident_token(tok))?,
                )),
                _ => Ok(FunctionArg::Identifier(Ident::from_ident_token(tok))),
            },
            Token::Number(s) => Ok(FunctionArg::Value(Value::Number(s.clone()))),
            Token::String(s) => Ok(FunctionArg::Value(Value::String(s.clone()))),
            _ => self.expected("a function argument", next_token),
        }
    }

    fn try_parse_compound_idents(&mut self, ident: Ident) -> Result<Option<Vec<Ident>>> {
        let mut idents = vec![];
        while self.consume_token(&Token::Dot) {
            let next_token = self.peek_token_ref();
            match next_token {
                Token::Mul => {
                    // Put back the consumed `.` tokens before exiting.
                    // If this expression is being parsed in the
                    // context of a projection, then the `.*` could imply
                    // a wildcard expansion. For example:
                    // `SELECT STRUCT('foo').* FROM T`
                    self.backup_token();
                    break;
                }
                tok @ Token::Ident(_, _) => {
                    idents.push(Ident::from_ident_token(tok));
                    self.advance_token();
                }
                _ => self.expected_ref("an identifier after '.'", next_token)?,
            }
        }
        if idents.is_empty() {
            return Ok(None);
        }
        idents.insert(0, ident);
        Ok(Some(idents))
    }

    fn parse_dml_select(&mut self) -> Result<Statement> {
        let query = self.parse_query()?;
        Ok(Statement::Select { query })
    }

    /// Parse a query expression, i.e. a `SELECT` statement
    /// Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    fn parse_query(&mut self) -> Result<Box<Query>> {
        self.expect_token(&Token::Keyword(Keyword::Select))?;
        let projection = self.parse_comma_separated(Parser::parse_select_item)?;
        let from = self.parse_table_with_joins()?;
        let selection =
            if self.parse_keyword(Keyword::Where) { Some(self.parse_expr()?) } else { None };
        let group_by = self.parse_optional_group_by()?.unwrap_or(vec![]);
        let order_by = self.parse_optional_order_by()?.unwrap_or(vec![]);
        let limit_clause = self.parse_optional_limit_clause()?;
        Ok(Box::new(Query { projection, from, selection, group_by, order_by, limit_clause }))
    }

    fn parse_optional_group_by(&mut self) -> Result<Option<Vec<Expr>>> {
        if !self.parse_keywords(&[Keyword::Group, Keyword::By]) {
            return Ok(None);
        }
        let exprs = self.parse_comma_separated(Parser::parse_expr)?;
        Ok(Some(exprs))
    }

    fn parse_optional_order_by(&mut self) -> Result<Option<Vec<OrderByExpr>>> {
        if !self.parse_keywords(&[Keyword::Order, Keyword::By]) {
            return Ok(None);
        }
        let exprs = self.parse_comma_separated(Parser::parse_order_by_item)?;
        Ok(Some(exprs))
    }

    fn parse_optional_limit_clause(&mut self) -> Result<Option<LimitClause>> {
        let offset = if self.parse_keyword(Keyword::Offset) {
            Some(self.parse_literal_uint()?)
        } else {
            None
        };
        let limit = if self.parse_keyword(Keyword::Limit) {
            let number = self.parse_literal_uint()?;
            if self.consume_token(&Token::Comma) {
                // LIMIT offset, limit
                if let Some(_) = offset {
                    return Err(Error::parse(
                        "Unexpected 'OFFSET number' in 'LIMIT offset, limit'",
                    ));
                }
                let limit = self.parse_literal_uint()?;
                return Ok(Some(LimitClause { offset: Some(number), limit: Some(limit) }));
            }
            Some(number)
        } else {
            None
        };
        if offset.is_none() && limit.is_none() {
            return Ok(None);
        }
        Ok(Some(LimitClause { limit, offset }))
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByExpr> {
        let expr = self.parse_expr()?;
        let desc = if self.parse_keyword(Keyword::Desc) {
            Some(true)
        } else if self.parse_keyword(Keyword::Asc) {
            Some(false)
        } else {
            None
        };
        Ok(OrderByExpr { expr, desc })
    }

    fn parse_table_with_joins(&mut self) -> Result<TableWithJoins> {
        self.expect_token(&Token::Keyword(Keyword::From))?;
        let relation = self.parse_table_factor()?;
        let joins = self.parse_joins()?;
        Ok(TableWithJoins { relation, joins })
    }

    fn parse_joins(&mut self) -> Result<Vec<Join>> {
        let mut joins = vec![];
        loop {
            let next_token = self.next_token();
            let join_type = match &next_token {
                Token::Keyword(w) if w == &Keyword::Join => JoinOperator::Join,
                Token::Keyword(w) if w == &Keyword::Inner => {
                    self.expect_one_of_keywords(&[Keyword::Join])?;
                    JoinOperator::Inner
                }
                Token::Keyword(w) if w == &Keyword::Left => {
                    let is_outer = self.parse_keyword(Keyword::Outer);
                    self.expect_one_of_keywords(&[Keyword::Join])?;
                    if is_outer {
                        JoinOperator::LeftOuter
                    } else {
                        JoinOperator::Left
                    }
                }
                Token::Keyword(w) if w == &Keyword::Right => {
                    let is_outer = self.parse_keyword(Keyword::Outer);
                    self.expect_one_of_keywords(&[Keyword::Join])?;
                    if is_outer {
                        JoinOperator::RightOuter
                    } else {
                        JoinOperator::Right
                    }
                }
                Token::Keyword(w) if w == &Keyword::Full => {
                    let is_outer = self.parse_keyword(Keyword::Outer);
                    self.expect_one_of_keywords(&[Keyword::Join])?;
                    if is_outer {
                        JoinOperator::FullOuter
                    } else {
                        JoinOperator::Full
                    }
                }
                _ => {
                    self.backup_token();
                    break;
                }
            };
            let relation = self.parse_table_factor()?;

            self.expect_one_of_keywords(&[Keyword::On])?;
            let join_constraint = JoinConstraint::On(self.parse_expr()?);
            let join_operator = join_type(join_constraint);

            joins.push(Join { join_operator, relation });
        }
        Ok(joins)
    }

    fn parse_table_factor(&mut self) -> Result<TableFactor> {
        let next_token = self.next_token();
        match &next_token {
            Token::Ident(_, _) => {
                let table_name = Ident::from_ident_token(&next_token);
                let alias = if self.parse_keyword(Keyword::As) {
                    let alias = self.parse_ident()?;
                    Some(alias.value)
                } else {
                    None
                };
                Ok(TableFactor::Table { name: table_name, alias })
            }
            Token::LParen => {
                let expr = self.parse_query()?;
                self.expect_token(&Token::RParen)?;
                let alias = if self.parse_keyword(Keyword::As) {
                    let alias = self.parse_ident()?;
                    Some(alias.value)
                } else {
                    None
                };
                Ok(TableFactor::Derived { subquery: expr, alias })
            }
            _ => self.expected("table identifier or '('", next_token),
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    fn parse_select_item(&mut self) -> Result<SelectItem> {
        if let Some(w) = self.try_parse_wildcard_expr()? {
            return Ok(SelectItem::WildcardExpr(w));
        };
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::As) {
            let alias = self.parse_ident()?;
            Ok(SelectItem::ExprWithAlias { expr, alias })
        } else {
            Ok(SelectItem::UnnamedExpr(expr))
        }
    }

    fn try_parse_wildcard_expr(&mut self) -> Result<Option<WildcardExpr>> {
        let index = self.index;
        let next_token = self.next_token();
        match &next_token {
            tok @ (Token::Ident(_, _) | Token::Keyword(_)) if self.peek_token() == Token::Dot => {
                let ident = match tok {
                    Token::Ident(_, _) => Ident::from_ident_token(tok),
                    Token::Keyword(w) => Ident { value: w.to_string(), double_quoted: false },
                    _ => unreachable!(),
                };
                let mut idents = vec![ident];
                while self.consume_token(&Token::Dot) {
                    let next_token = self.next_token();
                    match &next_token {
                        tok @ Token::Ident(_, _) => idents.push(Ident::from_ident_token(tok)),
                        Token::Keyword(w) => {
                            idents.push(Ident { value: w.to_string(), double_quoted: false })
                        }
                        Token::Mul => return Ok(Some(WildcardExpr::QualifiedWildcard(idents))),
                        _ => return self.expected("an identifier or *", next_token),
                    }
                }
            }
            Token::Mul => return Ok(Some(WildcardExpr::Wildcard)),
            _ => self.backup_token(),
        };
        self.index = index;
        Ok(None)
    }

    fn try_parse_expr_scalar_subquery(&mut self) -> Result<Option<Expr>> {
        let query = self.parse_query()?;
        Ok(Some(Expr::ScalarSubquery(query)))
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut Parser) -> Result<T>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    fn get_next_precedence(&self) -> Result<u8> {
        macro_rules! p {
            ($precedence:ident) => {
                self.prec_value(Precedence::$precedence)
            };
        }

        let token = self.peek_token();
        match token {
            Token::Keyword(w) if w == Keyword::Not => match self.peek_nth_token(1) {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, LIKE or ILIKE, it
                // takes on the precedence of those tokens. Otherwise, it
                // is not an infix operator, and therefore has zero precedence.
                Token::Keyword(w) if w == Keyword::In => Ok(p!(Like)),
                Token::Keyword(w) if w == Keyword::Like => Ok(p!(Like)),
                Token::Keyword(w) if w == Keyword::ILike => Ok(p!(Like)),
                _ => Ok(self.prec_unknown()),
            },
            Token::Keyword(w) if w == Keyword::Is => Ok(p!(Is)),
            Token::Keyword(w) if w == Keyword::In => Ok(p!(Like)),
            Token::Keyword(w) if w == Keyword::And => Ok(p!(And)),
            Token::Keyword(w) if w == Keyword::Or => Ok(p!(Or)),
            Token::Keyword(w) if w == Keyword::Like => Ok(p!(Like)),
            Token::Keyword(w) if w == Keyword::ILike => Ok(p!(Like)),
            Token::Mul | Token::Div => Ok(p!(MulDivModOp)),
            Token::Plus | Token::Minus => Ok(p!(PlusMinus)),
            Token::Eq | Token::Neq | Token::Gt | Token::GtEq | Token::Lt | Token::LtEq => {
                Ok(p!(Eq))
            }
            _ => Ok(self.prec_unknown()),
        }
    }

    /// Decide the lexical Precedence of operators.
    ///
    /// Uses (APPROXIMATELY) <https://www.postgresql.org/docs/7.0/operators.htm#AEN2026> as a reference
    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::UnaryOp => 50,
            Precedence::MulDivModOp => 40,
            Precedence::PlusMinus => 30,
            Precedence::Eq => 20,
            Precedence::Like => 19,
            Precedence::Is => 17,
            Precedence::And => 10,
            Precedence::Or => 5,
        }
    }

    fn prec_unknown(&self) -> u8 {
        0
    }

    /// Run a parser method `f`, reverting back to the current position if unsuccessful.
    /// Returns `Ok(None)` if `f` returns any other error.
    pub fn maybe_parse<T, F>(&mut self, f: F) -> Result<Option<T>>
    where
        F: FnMut(&mut Parser) -> Result<T>,
    {
        match self.try_parse(f) {
            Ok(t) => Ok(Some(t)),
            _ => Ok(None),
        }
    }

    /// Run a parser method `f`, reverting back to the current position if unsuccessful.
    fn try_parse<T, F>(&mut self, mut f: F) -> Result<T>
    where
        F: FnMut(&mut Parser) -> Result<T>,
    {
        let index = self.index;
        match f(self) {
            Ok(t) => Ok(t),
            Err(e) => {
                self.index = index;
                Err(e)
            }
        }
    }

    /// Parse an unsigned literal integer/long
    fn parse_literal_uint(&mut self) -> Result<u64> {
        let next_token = self.next_token();
        match next_token {
            Token::Number(s) => Self::parse_str::<u64>(s),
            _ => self.expected("literal int", next_token),
        }
    }

    fn parse_str<T: FromStr>(s: String) -> Result<T>
    where
        <T as FromStr>::Err: Display,
    {
        s.parse::<T>().map_err(|e| {
            Error::parse(format!("Could not parse '{s}' as {}: {e}", std::any::type_name::<T>()))
        })
    }

    fn parse_ident(&mut self) -> Result<Ident> {
        let next_token = self.next_token();
        match &next_token {
            tok @ Token::Ident(_, _) => Ok(Ident::from_ident_token(tok)),
            _ => self.expected("ident", next_token),
        }
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    fn consume_token(&mut self, expected: &Token) -> bool {
        if self.peek_token_ref() == expected {
            self.advance_token();
            true
        } else {
            false
        }
    }

    fn expect_token(&mut self, expected: &Token) -> Result<Token> {
        if self.peek_token_ref() == expected {
            Ok(self.next_token())
        } else {
            self.expected_ref(&expected.to_string(), self.peek_token_ref())
        }
    }

    /// If the current token is one of the expected keywords, consume the token
    /// and return the keyword that matches. Otherwise, return an error.
    pub fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> Result<Keyword> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            return Ok(keyword);
        }
        let keywords: Vec<String> = keywords.iter().map(|x| format!("{x:?}")).collect();
        self.expected_ref(&format!("one of {}", keywords.join(" or ")), self.peek_token_ref())
    }

    fn expect_keyword(&mut self, keyword: &Keyword) -> Result<Keyword> {
        match &self.peek_token_ref() {
            Token::Keyword(w) if w == keyword => {
                self.advance_token();
                Ok(*keyword)
            }
            tok => self.expected_ref(&tok.to_string(), self.peek_token_ref()),
        }
    }

    /// If the current token is one of the given `keywords`, consume the token
    /// and return the keyword that matches. Otherwise, no tokens are consumed
    /// and returns [`None`].
    fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        match &self.peek_token_ref() {
            Token::Keyword(w) => keywords.iter().find(|&keyword| keyword == w).map(|keyword| {
                self.advance_token();
                *keyword
            }),
            _ => None,
        }
    }

    /// If the current and subsequent tokens exactly match the `keywords`
    /// sequence, consume them and returns true. Otherwise, no tokens are
    /// consumed and returns false
    fn parse_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let index = self.index;
        for &keyword in keywords {
            if !self.parse_keyword(keyword) {
                self.index = index;
                return false;
            }
        }
        true
    }

    /// If the current token is the `expected` keyword, consume it and returns
    /// true. Otherwise, no tokens are consumed and returns false.
    fn parse_keyword(&mut self, expected: Keyword) -> bool {
        if self.peek_keyword(expected) {
            self.advance_token();
            true
        } else {
            false
        }
    }

    fn peek_keyword(&self, expected: Keyword) -> bool {
        matches!(self.peek_token(), Token::Keyword(w) if expected == w)
    }

    /// Return the first token that has not yet been processed
    /// or Token::EOF
    fn peek_token(&self) -> Token {
        self.peek_nth_token(0)
    }

    /// Return a reference to the first token that has not yet
    /// been processed or Token::EOF
    fn peek_token_ref(&self) -> &Token {
        self.peek_nth_token_ref(0)
    }

    /// Return nth token that has not yet been processed
    fn peek_nth_token(&self, n: usize) -> Token {
        self.peek_nth_token_ref(n).clone()
    }

    /// Return nth token that has not yet been processed
    fn peek_nth_token_ref(&self, mut n: usize) -> &Token {
        let mut index = self.index;
        loop {
            index += 1;
            let tok = self.tokens.get(index - 1);
            if n == 0 {
                return tok.unwrap_or(&Token::EOF);
            }
            n -= 1
        }
    }

    /// Advances to the next token and returns a copy.
    fn next_token(&mut self) -> Token {
        self.advance_token();
        self.get_current_token().clone()
    }

    /// Seek back the last token.
    fn backup_token(&mut self) {
        assert!(self.index > 0);
        self.index -= 1;
    }

    /// Returns a reference to the current token
    ///
    /// Does not advance the current token.
    pub fn get_current_token(&self) -> &Token {
        self.token_at(self.index.saturating_sub(1))
    }

    /// Return the token at the given location, or EOF if the index is beyond
    /// the length of the current set of tokens.
    pub fn token_at(&self, index: usize) -> &Token {
        self.tokens.get(index).unwrap_or(&Token::EOF)
    }

    /// Advances the current token to the next token
    fn advance_token(&mut self) {
        self.index += 1;
    }

    fn expected<T>(&self, expected: &str, found: Token) -> Result<T> {
        Err(Error::parse(format!("Expected: {expected}, found: {found}")))
    }

    fn expected_ref<T>(&self, expected: &str, found: &Token) -> Result<T> {
        Err(Error::parse(format!("Expected: {expected}, found: {found}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ddl_stmts() -> Result<()> {
        let cases = vec![
            // creat table
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
            // create index
            (r###"
            CREATE UNIQUE INDEX IF NOT EXISTS index1 ON table1 (col1, col2);
            "###),
            // drop table
            (r###"
            DROP TABLE foo;
            "###),
            // drop index
            (r###"
            DROP INDEX bar;
            "###),
            // alter table
            (r###"
            ALTER TABLE foo
                ADD COLUMN IF NOT EXISTS col1 TEXT NOT NULL default 'a',
                DROP COLUMN if EXISTS col2;
            "###),
            // query
            (r###"
            SELECT a, count(*) FROM foo JOIN b ON a.id = b.id LEFT JOIN c ON a.id = c.id where a = 1 and b = 2 group by a order by a LIMIT 0, 10;
            "###),
            // query
            (r###"
            SELECT
                users.name AS user_name,
                orders.id AS order_id,
                products.name AS product_name
            FROM
                users
                JOIN orders ON users.id = orders.user_id
                JOIN products ON orders.product_id = products.id;
            "###),
            // insert with select
            (r###"
            INSERT INTO users (id, name, email)
            SELECT id, name, email
            FROM old_users
            WHERE active = true;            
            "###),
            // insert with values
            (r###"
            INSERT INTO users (id, name, email)
            VALUES
              (1, 'Alice', 'alice@example.com'),
              (2, 'Bob', 'bob@example.com'),
              (3, 'Charlie', 'charlie@example.com');
            "###),
            // update
            (r###"
            UPDATE users
            SET name = 'Alice Smith',
                email = 'alice.smith@example.com'
            WHERE id = 1;
            "###),
            // delete
            (r###"
            DELETE FROM users where id = 1;
            "###),
        ];
        for (i, &sql) in cases.iter().enumerate() {
            let mut parser = Parser::new(sql)?;
            let stmt = parser.parse_statement()?;
            println!("{}==> \n{:#}\n", i, stmt);
        }

        Ok(())
    }

    #[test]
    fn test_parse_multiple_stmts() -> Result<()> {
        let sql = "SELECT * FROM foo; SELECT * FROM bar;";
        let mut parser = Parser::new(sql)?;
        let stmts = parser.parse_statements()?;
        for (i, stmt) in stmts.iter().enumerate() {
            println!("{}==> \n{}\n", i, stmt);
        }
        Ok(())
    }
}
