use chrono::{DateTime, DurationRound, SecondsFormat, Utc};
use std::ops::Add;
use std::{error::Error, fmt::Display, iter::zip};

#[derive(Clone, PartialEq, Debug)]
enum TokenKind {
    Identifier,
    Keyword,
}

#[derive(Clone)]
pub(crate) struct Token {
    #[allow(dead_code)]
    kind: TokenKind,
    value: String,
    #[allow(dead_code)]
    line: usize,
    #[allow(dead_code)]
    pos: usize,
}

pub(crate) struct ParserContext {
    pub tokens: Vec<Token>,
    pub current_pos: usize,
}

const KEYWORDS: [&str; 1] = ["now"];
const STRING_LITERAL_BEGINS: [&str; 1] = ["\""];
const STRING_LITERAL_ENDS: [&str; 1] = ["\""];
const DELIMITERS: [&str; 3] = ["+", "-", "/"];
const DELIMITER_CONTAINING_KEYWORDS: [&str; 0] = [];

#[allow(dead_code)]
const BOOLEAN_OPERATORS: [&str; 0] = [];
const WHITESPACE: [&str; 2] = [" ", "\n"];
#[allow(dead_code)]
const EXPRESSION_ENDERS: [&str; 0] = [];

impl Token {
    fn new(val: String, line: usize, pos: usize) -> Self {
        Token {
            kind: if KEYWORDS.contains(&val.as_str()) {
                TokenKind::Keyword
            } else {
                TokenKind::Identifier
            },
            value: val,
            line: line,
            pos: pos,
        }
    }

    fn keyword(val: String, line: usize, pos: usize) -> Self {
        Token {
            kind: TokenKind::Keyword,
            value: val,
            line: line,
            pos: pos,
        }
    }
}

// Returns the string ends that matches the start
fn is_string_literal_start(command_str: &String, current_index: usize) -> Option<(usize, &str)> {
    for pair in zip(STRING_LITERAL_BEGINS, STRING_LITERAL_ENDS) {
        if &command_str[current_index..current_index + pair.0.len()] == pair.0 {
            return Some((pair.0.len(), pair.1));
        }
    }
    None
}

impl ParserContext {
    fn new(script: &String) -> Self {
        let mut tokens: Vec<Token> = vec![];
        let mut token_start_index: usize = 0;
        let mut token_start_line: usize = 1;
        let mut token_start_pos: usize = 1;
        let mut current_index: usize = 0;
        let mut current_line: usize = 1;
        let mut current_pos: usize = 1;
        let mut end_string_literal: Option<&str> = None;

        while current_index < script.len() {
            let current_val = &script[current_index..current_index + 1];
            let current_val_2 = if current_index + 1 < script.len() {
                &script[current_index..current_index + 2]
            } else {
                ""
            };
            match end_string_literal {
                Some(end) => {
                    if current_val == end {
                        let token = Token::new(
                            script[token_start_index..current_index + end.len()].to_string(),
                            token_start_index,
                            token_start_pos,
                        );
                        tokens.push(token);
                        end_string_literal = None;
                        current_index += end.len();
                        current_pos += end.len();
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos;
                    } else if current_val == "\n" {
                        current_index += 1;
                        current_pos = 1;
                        current_line += 1;
                    } else {
                        current_index += 1;
                        current_pos += 1;
                    }
                }
                None => {
                    let literal_info = is_string_literal_start(script, current_index);
                    if WHITESPACE.contains(&current_val) {
                        let token = Token::new(
                            script[token_start_index..current_index].to_string(),
                            token_start_line,
                            token_start_pos,
                        );
                        tokens.push(token);
                        current_index += 1;
                        if current_val == "\n" {
                            current_pos = 1;
                            current_line += 1;
                        } else {
                            current_pos += 1;
                        }
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos;
                    } else if DELIMITER_CONTAINING_KEYWORDS.contains(&current_val_2) {
                        let token = Token::new(
                            script[token_start_index..current_index].to_string(),
                            token_start_line,
                            token_start_pos,
                        );
                        tokens.push(token);
                        let delimiter_token =
                            Token::keyword(current_val_2.to_string(), current_line, current_pos);
                        tokens.push(delimiter_token);
                        current_index += 2;
                        current_pos += 2;
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos;
                    } else if DELIMITERS.contains(&current_val) {
                        let token = Token::new(
                            script[token_start_index..current_index].to_string(),
                            token_start_line,
                            token_start_pos,
                        );
                        tokens.push(token);
                        let delimiter_token =
                            Token::keyword(current_val.to_string(), current_line, current_pos);
                        tokens.push(delimiter_token);
                        current_index += 1;
                        current_pos += 1;
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos;
                    } else if literal_info.is_some() {
                        let token = Token::new(
                            script[token_start_index..current_index].to_string(),
                            token_start_line,
                            token_start_pos,
                        );
                        tokens.push(token);
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos;
                        end_string_literal = Some(literal_info.unwrap().1);
                        current_index += literal_info.unwrap().0;
                        current_pos += literal_info.unwrap().0;
                    } else {
                        current_pos += 1;
                        current_index += 1;
                    }
                }
            }
        }

        let token = Token::new(
            script[token_start_index..current_index].to_string(),
            token_start_line,
            token_start_pos,
        );
        tokens.push(token);

        ParserContext {
            tokens: tokens
                .iter()
                .filter(|x| x.value.len() > 0)
                .map(|x| x.clone())
                .collect(),
            current_pos: 0,
        }
    }

    fn has_more(&self) -> bool {
        self.current_pos < self.tokens.len()
    }

    fn peek(&self) -> Token {
        self.tokens.get(self.current_pos).unwrap().clone()
    }

    #[allow(dead_code)]
    fn peek_offset(&self, offset: usize) -> Token {
        self.tokens.get(self.current_pos + offset).unwrap().clone()
    }

    fn pop(&mut self) -> Token {
        let val = &self.tokens.get(self.current_pos).unwrap();
        self.current_pos += 1;
        (*val).clone()
    }

    fn pop_validate(&mut self, value: &str) -> () {
        let val = &self.tokens.get(self.current_pos).unwrap();
        self.current_pos += 1;
        assert_eq!(val.value, value);
    }
}

#[derive(Clone)]
struct TranslationContext {
    now: DateTime<Utc>,
}

impl TranslationContext {
    fn now(&self) -> DateTime<Utc> {
        self.now.clone()
    }
}

#[derive(Debug)]
pub(crate) struct TranslationError {
    message: String,
    line: usize,
    pos: usize,
}

impl Display for TranslationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = f.write_str(format!("{}: {}, {}", self.message, self.line, self.pos).as_str());
        Ok(())
    }
}

impl Error for TranslationError {}

trait Expression {
    fn translate(&self, context: &TranslationContext) -> Result<DateTime<Utc>, TranslationError>;
}

struct NowExpression {}

impl Expression for NowExpression {
    fn translate(&self, context: &TranslationContext) -> Result<DateTime<Utc>, TranslationError> {
        Ok(context.now())
    }
}

enum DateUnit {
    Day,
    Hour,
    Minute,
    Week,
}

struct FloorExpression {
    left_expr: Box<dyn Expression>,
    unit: DateUnit,
}

impl Expression for FloorExpression {
    fn translate(&self, context: &TranslationContext) -> Result<DateTime<Utc>, TranslationError> {
        let left_val = self.left_expr.translate(context)?;
        let final_val = match self.unit {
            DateUnit::Day => left_val
                .add(chrono::Duration::hours(-12))
                .duration_round(chrono::Duration::days(1)),
            DateUnit::Hour => left_val
                .add(chrono::Duration::minutes(-30))
                .duration_round(chrono::Duration::hours(1)),
            DateUnit::Minute => left_val
                .add(chrono::Duration::seconds(-30))
                .duration_round(chrono::Duration::minutes(1)),
            DateUnit::Week => left_val
                .add(chrono::Duration::hours(-84))
                .duration_round(chrono::Duration::weeks(1)),
        };

        match final_val {
            Ok(val) => Ok(val),
            Err(err) => Err(TranslationError {
                message: format!("{}", err),
                line: 0,
                pos: 0,
            }),
        }
    }
}

enum ArithmeticOperator {
    Add,
    Sub,
}

struct ArithmeticExpression {
    left_expr: Box<dyn Expression>,
    op: ArithmeticOperator,
    right_expr: IntervalExpression,
}

impl Expression for ArithmeticExpression {
    fn translate(&self, context: &TranslationContext) -> Result<DateTime<Utc>, TranslationError> {
        let left_val = self.left_expr.translate(context)?;
        let right_val = self.right_expr.to_duration();
        match self.op {
            ArithmeticOperator::Add => {
                let final_val = left_val + right_val;
                Ok(final_val)
            }
            ArithmeticOperator::Sub => {
                let final_val = left_val - right_val;
                Ok(final_val)
            }
        }
    }
}

struct IntervalExpression {
    quantity: i64,
    unit: DateUnit,
}

impl IntervalExpression {
    fn to_duration(&self) -> chrono::Duration {
        match self.unit {
            DateUnit::Day => chrono::Duration::days(self.quantity),
            DateUnit::Minute => chrono::Duration::minutes(self.quantity),
            DateUnit::Hour => chrono::Duration::hours(self.quantity),
            DateUnit::Week => chrono::Duration::days(7 * self.quantity),
        }
    }
}

pub(crate) fn evaluate(dt_spec: &String, now: &DateTime<Utc>) -> Result<String, TranslationError> {
    let mut parser_context = ParserContext::new(dt_spec);

    let expression = parse_top_level_expression(&mut parser_context)?;

    let translation_context = TranslationContext { now: now.clone() };
    let final_dt = expression.translate(&translation_context)?;

    Ok(final_dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn parse_top_level_expression(
    parser_context: &mut ParserContext,
) -> Result<Box<dyn Expression>, TranslationError> {
    parser_context.pop_validate("now");

    let expr = Box::new(NowExpression {});

    if !parser_context.has_more() {
        return Ok(expr);
    }

    let expr = match parser_context.peek().value.as_str() {
        "-" => parse_arithmetic_expression(parser_context, expr),
        "+" => parse_arithmetic_expression(parser_context, expr),
        "/" => parse_floor_expression(parser_context, expr),
        _ => {
            panic!("Unexpected token");
        }
    }?;

    Ok(expr)
}

fn parse_date_unit(value: &str) -> DateUnit {
    match value {
        "d" => DateUnit::Day,
        "m" => DateUnit::Minute,
        "h" => DateUnit::Hour,
        "w" => DateUnit::Week,
        _ => {
            todo!();
        }
    }
}

fn parse_interval(value: &str) -> Result<(i64, DateUnit), TranslationError> {
    let mut current_index = 0;
    while current_index < value.len() {
        let current_val = &value[current_index..current_index + 1];
        if current_val >= "0" && current_val <= "9" {
            current_index += 1;
        } else {
            break;
        }
    }

    let quantity = value[0..current_index].to_string().parse::<i64>().unwrap();
    Ok((quantity, parse_date_unit(&value[current_index..])))
}

fn parse_interval_expression(
    parser_context: &mut ParserContext,
) -> Result<IntervalExpression, TranslationError> {
    let val = parser_context.pop();

    let (quantity, unit) = parse_interval(&val.value.as_str())?;

    Ok(IntervalExpression { quantity, unit })
}

fn parse_arithmetic_expression(
    parser_context: &mut ParserContext,
    left_expr: Box<dyn Expression>,
) -> Result<Box<dyn Expression>, TranslationError> {
    let op_token = parser_context.pop();

    let op = match op_token.value.as_str() {
        "+" => ArithmeticOperator::Add,
        "-" => ArithmeticOperator::Sub,
        _ => {
            panic!("Unexpected token");
        }
    };

    let right_expr = parse_interval_expression(parser_context)?;

    Ok(Box::new(ArithmeticExpression {
        left_expr,
        op,
        right_expr,
    }))
}

fn parse_floor_expression(
    parser_context: &mut ParserContext,
    left_expr: Box<dyn Expression>,
) -> Result<Box<dyn Expression>, TranslationError> {
    parser_context.pop_validate("/");

    let unit = parse_date_unit(&parser_context.pop().value.as_str());

    Ok(Box::new(FloorExpression { left_expr, unit }))
}

#[cfg(test)]
mod tests {
    use crate::elastic_search_datetime_parser::evaluate;
    use chrono::DateTime;

    #[test]
    fn test_evaluate() {
        let now = DateTime::parse_from_rfc3339("2025-06-29T13:42:46.228Z")
            .unwrap()
            .to_utc();
        assert_eq!(
            evaluate(&"now".to_string(), &now).unwrap(),
            "2025-06-29T13:42:46.228Z"
        );
        assert_eq!(
            evaluate(&"now/d".to_string(), &now).unwrap(),
            "2025-06-29T00:00:00.000Z"
        );
        assert_eq!(
            evaluate(&"now/h".to_string(), &now).unwrap(),
            "2025-06-29T13:00:00.000Z"
        );
        assert_eq!(
            evaluate(&"now/m".to_string(), &now).unwrap(),
            "2025-06-29T13:42:00.000Z"
        );
        assert_eq!(
            evaluate(&"now/w".to_string(), &now).unwrap(),
            "2025-06-26T00:00:00.000Z"
        );
        assert_eq!(
            evaluate(&"now-1d".to_string(), &now).unwrap(),
            "2025-06-28T13:42:46.228Z"
        );
        assert_eq!(
            evaluate(&"now+1d".to_string(), &now).unwrap(),
            "2025-06-30T13:42:46.228Z"
        );
        assert_eq!(
            evaluate(&"now+2d".to_string(), &now).unwrap(),
            "2025-07-01T13:42:46.228Z"
        );
        assert_eq!(
            evaluate(&"now+2h".to_string(), &now).unwrap(),
            "2025-06-29T15:42:46.228Z"
        );
        assert_eq!(
            evaluate(&"now-1w".to_string(), &now).unwrap(),
            "2025-06-22T13:42:46.228Z"
        );
        assert_eq!(
            evaluate(&"now+5m".to_string(), &now).unwrap(),
            "2025-06-29T13:47:46.228Z"
        );
    }
}
