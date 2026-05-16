//! expr-lite — a lightweight expression engine for the core MVP.
//!
//! Per ADR-003, this crate must **not** depend on DataFusion.
//!
//! # Grammar
//!
//! ```text
//! expr     → or_expr
//! or_expr  → and_expr ("or" and_expr)*
//! and_expr → cmp_expr ("and" cmp_expr)*
//! cmp_expr → add_expr (("==" | "!=" | "<" | "<=" | ">" | ">=") add_expr)?
//! add_expr → mul_expr (("+" | "-") mul_expr)*
//! mul_expr → unary_expr (("*" | "/") unary_expr)*
//! unary    → ("-" | "not")? atom
//! atom     → NUMBER | IDENT | "(" expr ")"
//! ```
//!
//! # Evaluation
//!
//! Expressions are evaluated against a record — any type that can look up
//! named fields by `&str` and return `f64`. The result is always `bool` for
//! filter expressions and `f64` for map expressions.

use std::fmt;

// ── AST ──

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(f64),
    Column(String),
    Binary(Box<Expr>, BinOp, Box<Expr>),
    Unary(UnaryOp, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(v) => {
                if *v == v.trunc() && v.is_finite() {
                    write!(f, "{v:.0}")
                } else {
                    write!(f, "{v}")
                }
            }
            Self::Column(name) => write!(f, "{name}"),
            Self::Binary(lhs, op, rhs) => write!(f, "({lhs} {op} {rhs})"),
            Self::Unary(op, inner) => write!(f, "({op}{inner})"),
        }
    }
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Eq => "==",
            Self::Neq => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::And => "and",
            Self::Or => "or",
        };
        f.write_str(s)
    }
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Neg => f.write_str("-"),
            Self::Not => f.write_str("not "),
        }
    }
}

// ── parser ──

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    UnexpectedEnd,
    UnexpectedToken(String),
    ExpectedExpression,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd => write!(f, "unexpected end of expression"),
            Self::UnexpectedToken(tok) => write!(f, "unexpected token: {tok}"),
            Self::ExpectedExpression => write!(f, "expected an expression"),
        }
    }
}

struct Parser {
    tokens: Vec<String>,
    pos: usize,
}

impl Parser {
    fn new(source: &str) -> Self {
        let tokens = tokenize(source);
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }

    fn advance(&mut self) -> Option<String> {
        let tok = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &str) -> Result<(), ParseError> {
        match self.advance() {
            Some(tok) if tok == expected => Ok(()),
            Some(tok) => Err(ParseError::UnexpectedToken(tok)),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_and()?;
        while self.peek() == Some("or") {
            self.advance();
            let rhs = self.parse_and()?;
            lhs = Expr::Binary(Box::new(lhs), BinOp::Or, Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_cmp()?;
        while self.peek() == Some("and") {
            self.advance();
            let rhs = self.parse_cmp()?;
            lhs = Expr::Binary(Box::new(lhs), BinOp::And, Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_cmp(&mut self) -> Result<Expr, ParseError> {
        let lhs = self.parse_add()?;
        match self.peek() {
            Some("==") => {
                self.advance();
                Ok(Expr::Binary(
                    Box::new(lhs),
                    BinOp::Eq,
                    Box::new(self.parse_add()?),
                ))
            }
            Some("!=") => {
                self.advance();
                Ok(Expr::Binary(
                    Box::new(lhs),
                    BinOp::Neq,
                    Box::new(self.parse_add()?),
                ))
            }
            Some("<") => {
                self.advance();
                Ok(Expr::Binary(
                    Box::new(lhs),
                    BinOp::Lt,
                    Box::new(self.parse_add()?),
                ))
            }
            Some("<=") => {
                self.advance();
                Ok(Expr::Binary(
                    Box::new(lhs),
                    BinOp::Le,
                    Box::new(self.parse_add()?),
                ))
            }
            Some(">") => {
                self.advance();
                Ok(Expr::Binary(
                    Box::new(lhs),
                    BinOp::Gt,
                    Box::new(self.parse_add()?),
                ))
            }
            Some(">=") => {
                self.advance();
                Ok(Expr::Binary(
                    Box::new(lhs),
                    BinOp::Ge,
                    Box::new(self.parse_add()?),
                ))
            }
            _ => Ok(lhs),
        }
    }

    fn parse_add(&mut self) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_mul()?;
        loop {
            match self.peek() {
                Some("+") => {
                    self.advance();
                    lhs = Expr::Binary(Box::new(lhs), BinOp::Add, Box::new(self.parse_mul()?));
                }
                Some("-") => {
                    self.advance();
                    lhs = Expr::Binary(Box::new(lhs), BinOp::Sub, Box::new(self.parse_mul()?));
                }
                _ => break,
            }
        }
        Ok(lhs)
    }

    fn parse_mul(&mut self) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_unary()?;
        loop {
            match self.peek() {
                Some("*") => {
                    self.advance();
                    lhs = Expr::Binary(Box::new(lhs), BinOp::Mul, Box::new(self.parse_unary()?));
                }
                Some("/") => {
                    self.advance();
                    lhs = Expr::Binary(Box::new(lhs), BinOp::Div, Box::new(self.parse_unary()?));
                }
                _ => break,
            }
        }
        Ok(lhs)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        match self.peek() {
            Some("-") => {
                self.advance();
                Ok(Expr::Unary(UnaryOp::Neg, Box::new(self.parse_unary()?)))
            }
            Some("not") => {
                self.advance();
                Ok(Expr::Unary(UnaryOp::Not, Box::new(self.parse_unary()?)))
            }
            _ => self.parse_atom(),
        }
    }

    fn parse_atom(&mut self) -> Result<Expr, ParseError> {
        let tok = self.advance().ok_or(ParseError::UnexpectedEnd)?;

        if tok == "(" {
            let inner = self.parse_expr()?;
            self.expect(")")?;
            return Ok(inner);
        }

        // numeric literal
        if let Ok(num) = tok.parse::<f64>() {
            return Ok(Expr::Literal(num));
        }

        // column reference (identifier)
        if is_ident(&tok) {
            return Ok(Expr::Column(tok));
        }

        Err(ParseError::UnexpectedToken(tok))
    }
}

/// Parse a filter or map expression string into an AST.
///
/// # Example
///
/// ```
/// use dsline_ops::parse_expr;
///
/// let ast = parse_expr("temperature > 20 and humidity < 80").unwrap();
/// ```
pub fn parse_expr(source: &str) -> Result<Expr, ParseError> {
    Parser::new(source).parse_expr()
}

// ── tokenizer ──

fn tokenize(source: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = source.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '(' | ')' => {
                tokens.push(ch.to_string());
                chars.next();
            }
            '<' | '>' | '=' | '!' => {
                chars.next();
                let mut tok = String::from(ch);
                if chars.peek() == Some(&'=') {
                    tok.push('=');
                    chars.next();
                }
                tokens.push(tok);
            }
            '+' | '-' | '*' | '/' => {
                tokens.push(ch.to_string());
                chars.next();
            }
            '0'..='9' | '.' => {
                let mut num = String::new();
                // If it's a dot, ensure the next char is a digit (not just a stray dot).
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() || c == '.' {
                        num.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(num);
            }
            _ if ch.is_alphabetic() || ch == '_' => {
                let mut ident = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        ident.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(ident.to_lowercase());
            }
            other => {
                tokens.push(other.to_string());
                chars.next();
            }
        }
    }

    tokens
}

fn is_ident(s: &str) -> bool {
    s.chars()
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_')
}

// ── evaluator ──

/// Trait for looking up named fields in a record.
pub trait Record {
    fn column(&self, name: &str) -> Option<f64>;
}

impl Record for std::collections::HashMap<String, f64> {
    fn column(&self, name: &str) -> Option<f64> {
        self.get(name).copied()
    }
}

/// Evaluate a parsed expression against a record, producing a `f64`.
///
/// Comparison and logical operators treat `0.0` as `false` and non-zero as
/// `true`. The result of `==`, `!=`, `<`, `<=`, `>`, `>=`, `and`, `or` is
/// `1.0` for true and `0.0` for false.
pub fn eval(expr: &Expr, rec: &dyn Record) -> Option<f64> {
    match expr {
        Expr::Literal(v) => Some(*v),
        Expr::Column(name) => rec.column(name),
        Expr::Binary(lhs, op, rhs) => {
            let l = eval(lhs, rec)?;
            let r = eval(rhs, rec)?;
            match op {
                BinOp::Add => Some(l + r),
                BinOp::Sub => Some(l - r),
                BinOp::Mul => Some(l * r),
                BinOp::Div => {
                    if r == 0.0 {
                        None
                    } else {
                        Some(l / r)
                    }
                }
                BinOp::Eq => Some(bool_f64((l - r).abs() < f64::EPSILON)),
                BinOp::Neq => Some(bool_f64((l - r).abs() >= f64::EPSILON)),
                BinOp::Lt => Some(bool_f64(l < r)),
                BinOp::Le => Some(bool_f64(l <= r)),
                BinOp::Gt => Some(bool_f64(l > r)),
                BinOp::Ge => Some(bool_f64(l >= r)),
                BinOp::And => Some(bool_f64(l != 0.0 && r != 0.0)),
                BinOp::Or => Some(bool_f64(l != 0.0 || r != 0.0)),
            }
        }
        Expr::Unary(op, inner) => {
            let v = eval(inner, rec)?;
            match op {
                UnaryOp::Neg => Some(-v),
                UnaryOp::Not => Some(bool_f64(v == 0.0)),
            }
        }
    }
}

#[inline]
fn bool_f64(b: bool) -> f64 {
    if b {
        1.0
    } else {
        0.0
    }
}

/// Evaluate and cast the result to `bool` (non-zero → true).
pub fn eval_bool(expr: &Expr, rec: &dyn Record) -> Option<bool> {
    eval(expr, rec).map(|v| v != 0.0)
}

// ── tests ──

#[cfg(test)]
mod tests {
    use super::{eval, eval_bool, parse_expr, BinOp, Expr, ParseError, UnaryOp};
    use std::collections::HashMap;

    fn rec(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    // ── tokenizer / parser ──

    #[test]
    fn parse_literal() {
        assert_eq!(parse_expr("42").unwrap(), Expr::Literal(42.0));
        assert_eq!(parse_expr("3.14").unwrap(), Expr::Literal(3.14));
    }

    #[test]
    fn parse_column() {
        assert_eq!(
            parse_expr("temperature").unwrap(),
            Expr::Column("temperature".into())
        );
    }

    #[test]
    fn parse_arithmetic() {
        assert_eq!(
            parse_expr("a + b * 2").unwrap(),
            Expr::Binary(
                Box::new(Expr::Column("a".into())),
                BinOp::Add,
                Box::new(Expr::Binary(
                    Box::new(Expr::Column("b".into())),
                    BinOp::Mul,
                    Box::new(Expr::Literal(2.0)),
                )),
            )
        );
    }

    #[test]
    fn parse_comparison() {
        assert_eq!(
            parse_expr("x > 10").unwrap(),
            Expr::Binary(
                Box::new(Expr::Column("x".into())),
                BinOp::Gt,
                Box::new(Expr::Literal(10.0)),
            )
        );
    }

    #[test]
    fn parse_logic() {
        let ast = parse_expr("a > 0 and b < 10").unwrap();
        assert_eq!(
            ast,
            Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(Expr::Column("a".into())),
                    BinOp::Gt,
                    Box::new(Expr::Literal(0.0)),
                )),
                BinOp::And,
                Box::new(Expr::Binary(
                    Box::new(Expr::Column("b".into())),
                    BinOp::Lt,
                    Box::new(Expr::Literal(10.0)),
                )),
            )
        );
    }

    #[test]
    fn parse_not_unary() {
        assert_eq!(
            parse_expr("not flag").unwrap(),
            Expr::Unary(UnaryOp::Not, Box::new(Expr::Column("flag".into())))
        );
    }

    #[test]
    fn parse_neg_unary() {
        assert_eq!(
            parse_expr("-x").unwrap(),
            Expr::Unary(UnaryOp::Neg, Box::new(Expr::Column("x".into())))
        );
    }

    #[test]
    fn parse_precedence() {
        // "a + b * c" → a + (b * c), not (a + b) * c
        let ast = parse_expr("a + b * c").unwrap();
        assert_eq!(
            ast,
            Expr::Binary(
                Box::new(Expr::Column("a".into())),
                BinOp::Add,
                Box::new(Expr::Binary(
                    Box::new(Expr::Column("b".into())),
                    BinOp::Mul,
                    Box::new(Expr::Column("c".into())),
                )),
            )
        );
    }

    #[test]
    fn parse_parentheses_override_precedence() {
        let ast = parse_expr("(a + b) * c").unwrap();
        assert_eq!(
            ast,
            Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(Expr::Column("a".into())),
                    BinOp::Add,
                    Box::new(Expr::Column("b".into())),
                )),
                BinOp::Mul,
                Box::new(Expr::Column("c".into())),
            )
        );
    }

    #[test]
    fn parse_rejects_empty() {
        assert_eq!(parse_expr("").unwrap_err(), ParseError::UnexpectedEnd);
    }

    #[test]
    fn parse_rejects_unexpected_tokens() {
        assert!(matches!(
            parse_expr("a +").unwrap_err(),
            ParseError::UnexpectedEnd
        ));
    }

    // ── evaluator ──

    #[test]
    fn eval_literal() {
        assert_eq!(eval(&parse_expr("3.14").unwrap(), &rec(&[])), Some(3.14));
    }

    #[test]
    fn eval_column_lookup() {
        let r = rec(&[("x", 5.0)]);
        assert_eq!(eval(&parse_expr("x").unwrap(), &r), Some(5.0));
    }

    #[test]
    fn eval_missing_column() {
        assert_eq!(eval(&parse_expr("y").unwrap(), &rec(&[])), None);
    }

    #[test]
    fn eval_arithmetic() {
        let r = rec(&[("a", 10.0), ("b", 3.0)]);
        assert_eq!(eval(&parse_expr("a + b").unwrap(), &r), Some(13.0));
        assert_eq!(eval(&parse_expr("a - b").unwrap(), &r), Some(7.0));
        assert_eq!(eval(&parse_expr("a * b").unwrap(), &r), Some(30.0));
        assert_eq!(eval(&parse_expr("a / b").unwrap(), &r), Some(10.0 / 3.0));
    }

    #[test]
    fn eval_division_by_zero() {
        let r = rec(&[("x", 1.0)]);
        assert_eq!(eval(&parse_expr("x / 0").unwrap(), &r), None);
    }

    #[test]
    fn eval_comparison() {
        let r = rec(&[("a", 5.0), ("b", 10.0)]);
        assert_eq!(eval_bool(&parse_expr("a < b").unwrap(), &r), Some(true));
        assert_eq!(eval_bool(&parse_expr("a == b").unwrap(), &r), Some(false));
        assert_eq!(eval_bool(&parse_expr("a != b").unwrap(), &r), Some(true));
        assert_eq!(eval_bool(&parse_expr("a >= 5").unwrap(), &r), Some(true));
        assert_eq!(eval_bool(&parse_expr("a <= 3").unwrap(), &r), Some(false));
    }

    #[test]
    fn eval_logic() {
        let r = rec(&[("a", 1.0), ("b", 0.0)]);
        // a and not b → true
        assert_eq!(
            eval_bool(&parse_expr("a and not b").unwrap(), &r),
            Some(true)
        );
        // a and b → false
        assert_eq!(eval_bool(&parse_expr("a and b").unwrap(), &r), Some(false));
        // a or b → true
        assert_eq!(eval_bool(&parse_expr("a or b").unwrap(), &r), Some(true));
    }

    #[test]
    fn eval_complex() {
        let r = rec(&[("temperature", 25.0), ("humidity", 60.0)]);
        let expr = parse_expr("temperature > 20 and humidity < 80").unwrap();
        assert_eq!(eval_bool(&expr, &r), Some(true));

        let r2 = rec(&[("temperature", 30.0), ("humidity", 90.0)]);
        assert_eq!(eval_bool(&expr, &r2), Some(false));
    }

    #[test]
    fn display_round_trips_for_parsing() {
        for input in [
            "42",
            "x",
            "a + b",
            "x > 10",
            "a > 0 and b < 10",
            "not flag",
            "(a + b) * c",
        ] {
            let ast = parse_expr(input).unwrap();
            let rendered = ast.to_string();
            let parsed_again = parse_expr(&rendered).unwrap();
            assert_eq!(ast, parsed_again, "failed round-trip for: {input}");
        }
    }
}
