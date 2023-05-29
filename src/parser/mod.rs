// here re-export the AST types
pub use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
pub use sqlparser::parser::ParserError;

pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = GenericDialect {}; 
    return Parser::parse_sql(&dialect, sql);
}

#[cfg(test)]
mod test{
    use super::*;

    #[test]
    fn test_parse(){
        let sql = "SELECT a, b, 123, myfunc(b) \
        FROM table_1 \
        WHERE a > b AND b < 100 \
        ORDER BY a DESC, b";
        let ans = parse(sql);
        println!("the result of parsing is: {:#?}", ans);
    }
}