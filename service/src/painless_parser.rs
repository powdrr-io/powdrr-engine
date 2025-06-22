use std::{any::Any, collections::HashMap, error::Error, fmt::Display, iter::zip};

use datafusion::{common::utils::expr, functions::unicode::left};
use serde_json::Value;
use tracing::field;


#[derive(Clone, PartialEq, Debug)]
enum TokenKind {
    Identifier,
    Keyword,
}


#[derive(Clone)]
struct Token {
    kind: TokenKind,
    value: String,
    line: usize,
    pos: usize,
}

pub(crate) struct ParserContext {
    pub tokens: Vec<Token>,
    pub current_pos: usize,
}


fn is_string_literal_end(command_str: String, current_index: usize, end_string_literal: &str) -> bool {
    if command_str.len() >= current_index + end_string_literal.len() - 1 {
        false
    } else {
        &command_str[current_index..current_index + end_string_literal.len()] == end_string_literal
    }
}

const KEYWORDS: [&str; 2] = ["if", "null"];
const STRING_LITERAL_BEGINS: [&str; 1] = ["\""];
const STRING_LITERAL_ENDS: [&str; 1] = ["\""];
const DELIMITERS: [&str; 17] = [",", "(", ")", "=", ":", ";", "[", "]", "?", ".", "<", ">", "!", "|", "&", "[", "]"];
const DELIMITER_CONTAINING_KEYWORDS: [&str; 7] = ["<=", ">=", "?.", "!=", "==", "||", "&&"];
const BOOLEAN_OPERATORS: [&str; 9] = ["<=", ">=", "?.", "!=", "==", "||", "&&", "<", ">"];
const WHITESPACE: [&str; 2] = [" ", "\n"];
const EXPRESSION_ENDERS: [&str; 4] = [")", "=", "]", ";"];


impl Token {
    fn new(val: String, line: usize, pos: usize) -> Self {
        Token {
            kind: if KEYWORDS.contains(&val.as_str()) { TokenKind::Keyword } else { TokenKind::Identifier },
            value: val,
            line: line,
            pos: pos
        }
    }

    fn keyword(val: String, line: usize, pos: usize) -> Self {
        Token {
            kind: TokenKind::Keyword,
            value: val,
            line: line,
            pos: pos
        }
    }
}


// Returns the string ends that matches the start
fn is_string_literal_start(command_str: &String, current_index: usize) -> Option<(usize, &str)> {
    for pair in zip(STRING_LITERAL_BEGINS, STRING_LITERAL_ENDS) {
        if &command_str[current_index..current_index + pair.0.len()] == pair.0 {
            return Some((pair.0.len(), pair.1))
        }
    }
    None
}


impl ParserContext {
    fn new(script: &String) -> Self {
        let mut tokens: Vec<Token> = vec!();
        let mut token_start_index: usize = 0;
        let mut token_start_line: usize = 1;
        let mut token_start_pos: usize = 1;
        let mut current_index: usize = 0;
        let mut current_line: usize = 1;
        let mut current_pos: usize = 1;
        let mut end_string_literal: Option<&str> = None;

        while current_index < script.len() {
            let current_val = &script[current_index..current_index+1];
            let current_val_2 = if current_index + 1 < script.len() { &script[current_index..current_index+2] } else { "" };
            match end_string_literal {
                Some(end) => {
                    if current_val == end {
                        let token = Token::new(script[token_start_index..current_index + end.len()].to_string(), token_start_index, token_start_pos);
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
                },
                None => {
                    let literal_info = is_string_literal_start(script, current_index);
                    if WHITESPACE.contains(&current_val) {
                        let token = Token::new(script[token_start_index..current_index].to_string(), token_start_line, token_start_pos);
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
                        let token = Token::new(script[token_start_index..current_index].to_string(), token_start_line, token_start_pos);
                        tokens.push(token);
                        let delimiter_token = Token::keyword(current_val_2.to_string(), current_line, current_pos);
                        tokens.push(delimiter_token);
                        current_index += 2;
                        current_pos += 2; 
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos; 
                    } else if DELIMITERS.contains(&current_val) {
                        let token = Token::new(script[token_start_index..current_index].to_string(), token_start_line, token_start_pos);
                        tokens.push(token);
                        let delimiter_token = Token::keyword(current_val.to_string(), current_line, current_pos);
                        tokens.push(delimiter_token);
                        current_index += 1;
                        current_pos += 1; 
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos; 
                    } else if literal_info.is_some() {
                        let token = Token::new(script[token_start_index..current_index].to_string(), token_start_line, token_start_pos);
                        tokens.push(token);
                        token_start_index = current_index;
                        token_start_line = current_line;
                        token_start_pos = current_pos;
                        end_string_literal = Some(literal_info.unwrap().1);
                        current_index +=  literal_info.unwrap().0;
                        current_pos += literal_info.unwrap().0;
                    } else {
                        current_pos += 1;
                        current_index += 1;
                    }
                }
            }
        }

        let token = Token::new(script[token_start_index..current_index].to_string(), token_start_line, token_start_pos);
        tokens.push(token);

        ParserContext {
            tokens: tokens.iter().filter(|x|x.value.len() > 0).map(|x|x.clone()).collect(),
            current_pos: 0,
        }
    }

    fn has_more(&self) -> bool {
        self.current_pos < self.tokens.len()
    }

    fn peek(&self) -> Token {
        self.tokens.get(self.current_pos).unwrap().clone()
    }

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
    nested: u32,
}

impl TranslationContext {
    fn push(&self) -> Self {
        TranslationContext { nested: self.nested + 1 }
    }

    fn nested_prefix(&self) -> String {
        TranslationContext::nested_prefix_worker(self.nested)
    }

    fn nested_prefix_worker(nested: u32) -> String {
            match nested {
            0 => "".to_string(),
            1 => "  ".to_string(),
            2 => "    ".to_string(),
            3 => "      ".to_string(),
            4 => "        ".to_string(),
            5 => "          ".to_string(),
            6 => "            ".to_string(),
            7 => "              ".to_string(),
            8 => "                ".to_string(),
            9 => "                  ".to_string(),
            10 => "                    ".to_string(),
            _ => format!("                    {}", TranslationContext::nested_prefix_worker(nested - 10))
        }
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


trait Expression: Any {
    fn as_any(&self) -> &dyn Any;
    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError>;
}


trait Statement {
    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        Ok(format!("{}{}", context.nested_prefix(), self.translate_worker(context)?))
    }

    fn translate_worker(&self, context: TranslationContext) -> Result<String, TranslationError>;
}

struct ExpressionStatement {
    expression: Box<dyn Expression>,
}

impl Statement for ExpressionStatement {
    fn translate_worker(&self, context: TranslationContext) -> Result<String, TranslationError> {
        Ok(format!("{{{{ {} }}}}", self.expression.translate(context)?))
    }
}

struct IfStatement {
    condition_expr: Box<dyn Expression>,
    then_statements: Vec<Box<dyn Statement>>,
    else_statements: Vec<Box<dyn Statement>>,
}

impl Statement for IfStatement {
    fn translate_worker(&self, context: TranslationContext) -> Result<String, TranslationError> {
        let then_str = self.then_statements.iter().map(|x|x.translate(context.push())).collect::<Result<Vec<String>, TranslationError>>()?;
        let else_str = self.else_statements.iter().map(|x|x.translate(context.push())).collect::<Result<Vec<String>, TranslationError>>()?;
        if self.else_statements.len() == 0 {
            Ok(format!(
                r#"{{% if {} %}}
{}
{{% endif %}}"#,
                self.condition_expr.translate(context)?,
                then_str.join("\n")
            ))
        } else {
            Ok(format!(
                r#"{{% if {} %}}
{}
{{% else %}}
{}
{{% endif %}}"#,
                self.condition_expr.translate(context)?,
                then_str.join("\n"),
                else_str.join("\n"),
            ))
        }
    }
}

struct AssignmentStatement {
    left_side: Box<dyn Expression>,
    right_side: Box<dyn Expression>,
}

impl Statement for AssignmentStatement {
    fn translate_worker(&self, context: TranslationContext) -> Result<String, TranslationError> {
        let mut left_side_raw = vec!();
        let mut current_left = &self.left_side;
        loop {
            match current_left.as_any().downcast_ref::<FieldAccess>() {
                Some(l) => {
                    left_side_raw.push(l.field_name.clone());
                    current_left = &l.expression;
                },
                None => break
            }
        }
        match current_left.as_any().downcast_ref::<VariableOrTypeReference>() {
            Some(l) => {
                left_side_raw.push(l.name.clone());
            },
            None => panic!("Don't know how to translate this type of assignment")
        }
        left_side_raw.reverse();
        Ok(format!(
            "{{{{ __private_impl.assign(\"{}\", {}) }}}}",
            left_side_raw.join("."),
            self.right_side.translate(context)?
        ))
    }
}

struct VariableOrTypeReference {
    name: String,
}

impl Expression for VariableOrTypeReference {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        if self.name == "ctx" || self.name == "params" {
            Ok(self.name.to_string())
        } else if self.name.starts_with("\"") {
            Ok(self.name.to_string())
        } else {
            Ok(format!("__types.{}", self.name))
        }
    }
}

struct NullReference {
}

impl Expression for NullReference {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        Ok("none".to_string())
    }    
}


fn create_expression(token: &Token) -> Result<Box<dyn Expression>, TranslationError> {
    match token.kind {
        TokenKind::Keyword => {
            match token.value.as_str() {
                "null" => Ok(Box::new(NullReference{})),
                _ => panic!("Not implemented"),
            }
        },
        TokenKind::Identifier => {
            Ok(Box::new(VariableOrTypeReference{ name: token.value.clone() }))
        }
    }
}


struct FieldAccess {
    expression: Box<dyn Expression>,
    field_name: String,
}


impl Expression for FieldAccess {
    fn as_any(&self) -> &dyn Any {
        self
    }
        
    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        Ok(format!("{}.{}", self.expression.translate(context)?, self.field_name))
    }
}

struct Index {
    outer_expression: Box<dyn Expression>,
    index_expression: Box<dyn Expression>,
}


impl Expression for Index {
    fn as_any(&self) -> &dyn Any {
        self
    }
        
    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        Ok(format!("{}[{}]", self.outer_expression.translate(context.clone())?, self.index_expression.translate(context.clone())?))
    }
}

fn unwrap_expression(expr: &Box<dyn Expression>) -> (String, &Box<dyn Expression>) {
    match expr.as_any().downcast_ref::<FieldAccess>() {
        Some(e) => (e.field_name.clone(), &e.expression),
        None => panic!("Tried to unwrap expression that does not support it")
    }
}

struct MethodInvocation {
    expression: Box<dyn Expression>,
    params: Vec<Box<dyn Expression>>,
}

impl Expression for MethodInvocation {
    fn as_any(&self) -> &dyn Any {
        self
    }
        
    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        let (method_name, expr) = unwrap_expression(&self.expression);

        match method_name.as_str() {
            "contains" => {
                assert_eq!(self.params.len(), 1);
                Ok(format!(
                    "{} in {}",
                    self.params.get(0).unwrap().translate(context.clone())?,
                    expr.translate(context.clone())?
                ))
            },
            _ => {
                let params_translated: Vec<String> = self.params.iter().map(|x|x.translate(context.clone())).collect::<Result<Vec<String>, TranslationError>>()?;
                Ok(format!("{}({})", 
                    self.expression.translate(context.clone())?,
                    params_translated.join(", ")
                ))
            }
        }
    }
}


struct Comparison {
    left_expression: Box<dyn Expression>,
    operator: String,
    right_expression: Box<dyn Expression>,
}

impl Comparison {
    fn new(left_expression: Box<dyn Expression>, operator: String, right_expression: Box<dyn Expression>) -> Self {
        Comparison { left_expression: left_expression, operator: operator, right_expression: right_expression }
    }

    fn is_null(expr: &Box<dyn Expression>) -> bool {
        expr.as_any().downcast_ref::<NullReference>().is_some()
    }    
}

impl Expression for Comparison {
    fn as_any(&self) -> &dyn Any {
        self
    }
   
    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        // TODO: translate operator
        let right = self.right_expression.translate(context.clone())?;
        let is_right_null = Comparison::is_null(&self.right_expression);
        if self.operator == "!=" && is_right_null {
            Ok(format!("{} is not none", self.left_expression.translate(context.clone())?))
        } else if self.operator == "==" && is_right_null {
            Ok(format!("{} is none", self.left_expression.translate(context.clone())?))
        } else {
            Ok(format!(
                "{} {} {}",
                self.left_expression.translate(context.clone())?,
                self.operator,
                self.right_expression.translate(context.clone())?,
            ))
        }
    }
}


struct NullTestFieldAccess {
    expression: Box<dyn Expression>,
    field_name: String,    
}

fn create_field_access(field_chain: &Vec<String>) -> String {
    field_chain.iter().rev().map(|x|format!("[\"{}\"]", x)).collect::<Vec<String>>().join("")    
}

fn create_field_chains(nested_field_chain: &Vec<String>) -> Vec<Vec<String>> {
    let mut retval: Vec<Vec<String>> = vec!();
    for i in 0..nested_field_chain.len() {
        retval.push(nested_field_chain[i..].to_vec())
    }
    retval
}

fn create_conditions_str(outer_expression_str: &String, nested_field_chain: &Vec<String>) -> String {
    let field_chains = create_field_chains(nested_field_chain);

    let mut condition_str: Vec<String> = field_chains.iter()
        .map(|x|create_field_access(x))
        .map(|x|format!("{}{} is defined and {}{} is mapping", outer_expression_str, x, outer_expression_str, x))
        .collect::<Vec<String>>();
    condition_str.push(format!("{} is defined and {} is mapping", outer_expression_str, outer_expression_str));
    condition_str.reverse();
    condition_str.join(" and ")
}

impl Expression for NullTestFieldAccess {
    fn as_any(&self) -> &dyn Any {
        self
    }
        
    fn translate(&self, context: TranslationContext) -> Result<String, TranslationError> {
        let mut nested_field_chain = vec!();
        let mut test_expression = self;
        let outer_expression: &Box<dyn Expression>;
        loop {
            match test_expression.expression.as_any().downcast_ref::<NullTestFieldAccess>() {
                Some(ntfa) => {
                    test_expression = ntfa;
                    nested_field_chain.push(test_expression.field_name.clone())
                }
                None => {
                    outer_expression = &test_expression.expression;
                    break;
                }
            }
        }

        let last_field_vec = vec!(self.field_name.clone());
        let all_nested_fields = [&last_field_vec[..], &nested_field_chain[..]].concat();
        let expression_str = outer_expression.translate(context)?;
        let field_access_str = create_field_access(&all_nested_fields);
        let conditions_str = create_conditions_str(&expression_str, &nested_field_chain);
        Ok(format!("({}{} if {} else none)", expression_str, field_access_str, conditions_str))
    }
}


pub(crate) fn translate(painless_script: &String) -> Result<String, TranslationError> {
    let mut parser_context = ParserContext::new(painless_script);
    
    let mut statements = vec!();
    while parser_context.has_more() {
        statements.push(parse_statement(&mut parser_context)?);
    }

    let translation_context = TranslationContext{ nested: 0 };
    let translated_statements: Result<Vec<String>, TranslationError> = statements.iter().map(|s|s.translate(translation_context.clone())).collect();
    match translated_statements {
        Ok(t) => Ok(t.join("\n")),
        Err(e) => Err(e)
    }
}


fn parse_statement(parser_context: &mut ParserContext) -> Result<Box<dyn Statement>, TranslationError> {
    let token = parser_context.peek();
    match token.kind {
        TokenKind::Keyword => {
            match token.value.as_str() {
                "if" => parse_if_statement(parser_context),
                _ => panic!("Not implemented")
            }
        },
        TokenKind::Identifier => {
            let next_token = parser_context.peek_offset(1);
            match next_token.kind {
                TokenKind::Keyword => {
                    let expression =  parse_expression(parser_context)?;
                    if parser_context.has_more() {
                        match parser_context.peek().value.as_str() {
                            ";" => {
                                parser_context.pop_validate(";");
                                Ok(Box::new(ExpressionStatement{ expression: expression }))
                            },
                            "=" => {
                                let stmt = parse_assignment_statement(parser_context, expression);
                                if parser_context.has_more() && parser_context.peek().value == ";" {
                                    parser_context.pop_validate(";");
                                }
                                stmt
                            },
                            _ => panic!("What is this?")
                        }
                    } else {
                        // This covers the case of just an expression by itself outside of a full statement
                        Ok(Box::new(ExpressionStatement{ expression: expression }))                        
                    }
                },
                TokenKind::Identifier => {
                    parse_variable_declaration(parser_context, &token)
                },
            }
        }
    }
}

fn parse_if_statement(parser_context: &mut ParserContext) -> Result<Box<dyn Statement>, TranslationError> {
    let mut then_statements = vec!();
    let mut else_statements = vec!();

    parser_context.pop_validate("if");
    parser_context.pop_validate("(");
    let condition_expr = parse_expression(parser_context)?;
    parser_context.pop_validate(")");
    parser_context.pop_validate("{");
    while parser_context.peek().value != "}" {
        then_statements.push(parse_statement(parser_context)?);
    }
    parser_context.pop_validate("}");
    if parser_context.peek().value == "else" {
        parser_context.pop_validate("else");
        match parser_context.peek().value.as_str() {
            "{" => {
                parser_context.pop_validate("{");
                while parser_context.peek().value != "}" {
                    else_statements.push(parse_statement(parser_context)?);
                } 
                parser_context.pop_validate("}");       
            },
            "if" => {
                else_statements.push(parse_if_statement(parser_context)?);
            },
            _ => panic!("What is this?")
        }
    }

    Ok(Box::new(IfStatement{condition_expr, then_statements, else_statements}))
}

fn parse_assignment_statement(parser_context: &mut ParserContext, expression: Box<dyn Expression>) -> Result<Box<dyn Statement>, TranslationError> {
    parser_context.pop_validate("=");
    let right_side = parse_expression(parser_context)?;
    Ok(Box::new(AssignmentStatement{ left_side: expression, right_side: right_side }))
}

fn parse_expression(parser_context: &mut ParserContext) -> Result<Box<dyn Expression>, TranslationError> {
    parse_expression_booleans(parser_context)
}

fn parse_expression_booleans(parser_context: &mut ParserContext) -> Result<Box<dyn Expression>, TranslationError> {
    let mut expression = parse_expression_comparisons(parser_context)?;
    while parser_context.has_more() {
        match parser_context.peek().value.as_str() {
            "&&" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_comparisons(parser_context)?));
            },
            "||" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_comparisons(parser_context)?));
            },
            _ => break,                           
        }
    };
    Ok(expression)
}

fn parse_expression_comparisons(parser_context: &mut ParserContext) -> Result<Box<dyn Expression>, TranslationError> {
    let mut expression = parse_expression_mut_div(parser_context)?;
    while parser_context.has_more() {
        match parser_context.peek().value.as_str() {
            "<" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_mut_div(parser_context)?));
            },
            "<=" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_mut_div(parser_context)?));
            },
            ">" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_mut_div(parser_context)?));
            },
            ">=" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_mut_div(parser_context)?));
            },
            "==" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_mut_div(parser_context)?));
            },
            "!=" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_mut_div(parser_context)?));
            },
            _ => break,                           
        }
    };
    Ok(expression)
}

fn parse_expression_mut_div(parser_context: &mut ParserContext) -> Result<Box<dyn Expression>, TranslationError> {
    let mut expression = parse_expression_add_sub(parser_context)?;
    while parser_context.has_more() {
        match parser_context.peek().value.as_str() {
            "*" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_add_sub(parser_context)?));
            },
            "/" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_add_sub(parser_context)?));
            },
            _ => break,                           
        }
    };
    Ok(expression)
}

fn parse_expression_add_sub(parser_context: &mut ParserContext) -> Result<Box<dyn Expression>, TranslationError> {
    let mut expression = parse_expression_inner_most(parser_context)?;
    while parser_context.has_more() {
        match parser_context.peek().value.as_str() {
            "+" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_inner_most(parser_context)?));
            },
            "-" => {
                expression = Box::new(Comparison::new(expression, parser_context.pop().value,parse_expression_inner_most(parser_context)?));
            },
            _ => break,                          
        }
    };
    Ok(expression)
}

fn parse_expression_inner_most(parser_context: &mut ParserContext) -> Result<Box<dyn Expression>, TranslationError> {
    let mut expression = create_expression(&parser_context.pop())?;
    while parser_context.has_more() {
        match parser_context.peek().value.as_str() {
            "." => {
                expression = parse_field_access(parser_context, expression)?;
            },
            "?." => { 
                expression = parse_null_test_field_access(parser_context, expression)?;
            },
            "(" => {
                expression = parse_method_invocation(parser_context, expression)?;
            },
            "[" => {
                expression = parse_index(parser_context, expression)?;
            }
            _ => break,
        }    
    };
    Ok(expression)
}

fn parse_index(parser_context: &mut ParserContext, expression: Box<dyn Expression>) -> Result<Box<dyn Expression>, TranslationError> {
    parser_context.pop_validate("[");
    let index_expr = parse_expression(parser_context)?;
    parser_context.pop_validate("]");

    Ok(Box::new(Index{ outer_expression: expression, index_expression: index_expr }))
}


fn parse_variable_declaration(parser_context: &mut ParserContext, first_token: &Token) -> Result<Box<dyn Statement>, TranslationError> {
    panic!("Not implemented")
}

fn parse_field_access(parser_context: &mut ParserContext, expression: Box<dyn Expression>) -> Result<Box<dyn Expression>, TranslationError> {
    parser_context.pop_validate(".");
    let field_name = parser_context.pop();

    Ok(Box::new(FieldAccess{ expression: expression, field_name: field_name.value.clone() }))
}

fn parse_null_test_field_access(parser_context: &mut ParserContext, expression: Box<dyn Expression>) -> Result<Box<dyn Expression>, TranslationError> {
    parser_context.pop_validate("?.");
    let field_name = parser_context.pop();

    Ok(Box::new(NullTestFieldAccess{ expression: expression, field_name: field_name.value.clone() }))
}

fn parse_method_invocation(parser_context: &mut ParserContext, left_expression: Box<dyn Expression>) -> Result<Box<dyn Expression>, TranslationError> {
    parser_context.pop_validate("(");

    let mut params = vec!();
    while parser_context.peek().value != ")" {
        params.push(parse_expression(parser_context)?);
    }
    parser_context.pop_validate(")");
    Ok(Box::new(MethodInvocation{expression: left_expression, params: params}))
}


#[cfg(test)]
mod tests {
    use crate::painless_parser::ParserContext;

    use super::translate;

    #[test]
    fn test_tokenizer() {     
        let context = ParserContext::new(&"ctx?.kibana?.log?.meta?.res?.responseTime != null".to_string());

        assert_eq!(context.tokens.len(), 13);
    }

    #[test]
    fn test_translate() {     
        let translated = translate(&"ctx?.kibana?.log?.meta?.res?.responseTime != null".to_string()).unwrap();

        assert_eq!(translated, r#"{{ (ctx["kibana"]["log"]["meta"]["res"]["responseTime"] if ctx is defined and ctx is mapping and ctx["kibana"] is defined and ctx["kibana"] is mapping and ctx["kibana"]["log"] is defined and ctx["kibana"]["log"] is mapping and ctx["kibana"]["log"]["meta"] is defined and ctx["kibana"]["log"]["meta"] is mapping and ctx["kibana"]["log"]["meta"]["res"] is defined and ctx["kibana"]["log"]["meta"]["res"] is mapping else none) is not none }}"#);
    }  

    #[test]
    fn test_translate_datetime_method() {
        let test_val = "ZonedDateTime.parse(\"2025-05-31T12:34:56\").toInstant().toEpochMilli()";
        let translated = translate(&test_val.to_string()).unwrap();

        assert_eq!(translated, r#"{{ __types.ZonedDateTime.parse("2025-05-31T12:34:56").toInstant().toEpochMilli() }}"#);
    }

    #[test]
    fn test_translate_statement() {
        let test_val = r#"
    if (params.claimableTaskTypes.contains(ctx._source.task.taskType)) {
      if (ctx._source.task.schedule != null || ctx._source.task.attempts < params.taskMaxAttempts[ctx._source.task.taskType]) {
        if(ctx._source.task.retryAt != null && ZonedDateTime.parse(ctx._source.task.retryAt).toInstant().toEpochMilli() < params.now) {
          ctx._source.task.scheduledAt=ctx._source.task.retryAt;
        } else {
          ctx._source.task.scheduledAt=ctx._source.task.runAt;
        }
        ctx._source.task.status = "claiming"; ctx._source.task.ownerId=params.fieldUpdates.ownerId; ctx._source.task.retryAt=params.fieldUpdates.retryAt;
      } else {
        ctx._source.task.status = "failed";
      }
    } else if (params.unusedTaskTypes.contains(ctx._source.task.taskType)) {
      ctx._source.task.status = "unrecognized";
    } else {
      ctx.op = "noop";
    }"#;

        let translated = translate(&test_val.to_string()).unwrap();

        println!("{}", translated);
    }  
}