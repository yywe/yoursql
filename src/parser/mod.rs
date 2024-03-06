// here re-export the AST types
pub use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
pub use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Tokenizer;

pub fn parse(sql: &str) -> Result<Statement, ParserError> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let mut parser = Parser::new(&dialect).with_tokens(tokenizer.tokenize()?);
    Ok(parser.parse_statement()?)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_parse() {
        let sql = "SELECT a, b, 123, myfunc(b) \
        FROM table_1 \
        WHERE a > b AND b < 100 \
        ORDER BY a DESC, b";
        let ans = parse(sql);
        //println!("the result of parsing is: {:#?}", ans);
        assert_eq!(ans.is_ok(), true);
    }
}
