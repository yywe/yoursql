use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use sqlparser::ast::{Expr as SQLExpr, OrderByExpr};

use crate::common::column::Column;
use crate::common::schema::Schema;
use crate::common::table_reference::TableReference;
use crate::common::types::{DataType, DataValue};
use crate::expr::expr::{
    AggregateFunction, AggregateFunctionType, Between, BinaryExpr, Expr, Like, Operator, Sort,
};
use crate::expr::expr_schema::ExprToSchema;
use crate::expr::literal::lit;
use crate::expr::utils::find_columns_referred_by_expr;
use crate::logical_planner::{normalize_ident, LogicalPlanner, PlannerContext};

/// we need to convert the AST expressions to Expressions in logical plan

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn ast_expr_to_expr(&self, ast_expr: SQLExpr, schema: &Schema) -> Result<Expr> {
        let mut expr = self.sql_expr_to_logical_expr(ast_expr, schema)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        Ok(expr)
    }

    /// the datafusion implementation used a stack to avoid stack overflow, here simplified,
    /// directly call internal func
    pub fn sql_expr_to_logical_expr(&self, ast_expr: SQLExpr, schema: &Schema) -> Result<Expr> {
        Ok(self.sql_expr_to_logical_expr_internal(ast_expr, schema)?)
    }
    /// align the qualifier in expr with the qualifier in schema
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &Schema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema.fields().iter().find(|f| match f.qualifier() {
                        Some(fq) => {
                            f.name() == &col.name && fq.to_string().ends_with(&format!(".{q}"))
                        }
                        _ => false,
                    }) {
                        Some(schema_field) => Expr::Column(Column {
                            relation: schema_field.qualifier().cloned(),
                            name: schema_field.name().clone(),
                        }),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    fn sql_expr_to_logical_expr_internal(
        &self,
        ast_expr: SQLExpr,
        schema: &Schema,
    ) -> Result<Expr> {
        use sqlparser::ast::{UnaryOperator, Value};
        match ast_expr {
            SQLExpr::Value(value) => {
                match value {
                    Value::Number(n, _) => {
                        // first try with i64, then u64, lastly try f64
                        // note parse float as decimal is not implemented
                        if let Ok(n) = n.parse::<i64>() {
                            return Ok(lit(n));
                        } else if let Ok(n) = n.parse::<u64>() {
                            return Ok(lit(n));
                        } else {
                            n.parse::<f64>()
                                .map(lit)
                                .context(format!("cannot parse {} as f64", n))
                        }
                    }
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(lit(s)),
                    Value::Null => Ok(Expr::Literal(DataValue::Null)),
                    Value::Boolean(n) => Ok(lit(n)),
                    _ => return Err(anyhow!("unsupported value:{value:?}")),
                }
            }
            SQLExpr::Identifier(id) => {
                let normalized_ident = self.normalizer.normalize(id);
                match schema.field_with_unqualified_name(normalized_ident.as_str()) {
                    Ok(_) => Ok(Expr::Column(Column {
                        relation: None,
                        name: normalized_ident,
                    })),
                    _ => {
                        return Err(anyhow!(
                            "failed to find field:{normalized_ident:?} in the schema"
                        ))
                    }
                }
            }
            SQLExpr::CompoundIdentifier(ids) => {
                let ids = ids
                    .into_iter()
                    .map(|id| self.normalizer.normalize(id))
                    .collect::<Vec<_>>();
                let (table_ref, column_name) = match ids.len() {
                    2 => (
                        TableReference::Bare {
                            table: (&ids[0]).into(),
                        },
                        ids[1].to_owned(),
                    ),
                    3 => (
                        TableReference::Full {
                            database: (&ids[0]).into(),
                            table: (&ids[1]).into(),
                        },
                        ids[2].to_owned(),
                    ),
                    _ => {
                        return Err(anyhow!(format!(
                            "invalid compound identifier length:{}, only 2 or 3 are accepted",
                            ids.len()
                        )))
                    }
                };
                let field = schema.field_with_name(Some(&table_ref), &column_name)?;
                Ok(Expr::Column(field.qualified_column()))
            }
            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),
            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),
            SQLExpr::IsTrue(expr) => Ok(Expr::IsTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),
            SQLExpr::IsNotTrue(expr) => Ok(Expr::IsNotTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),
            SQLExpr::IsFalse(expr) => Ok(Expr::IsFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),
            SQLExpr::IsNotFalse(expr) => Ok(Expr::IsNotFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),
            SQLExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => Ok(Expr::Not(Box::new(
                    self.sql_expr_to_logical_expr(*expr, schema)?,
                ))),
                UnaryOperator::Plus => Ok(self.sql_expr_to_logical_expr(*expr, schema)?),
                UnaryOperator::Minus => match *expr {
                    SQLExpr::Value(Value::Number(n, _)) => match n.parse::<i64>() {
                        Ok(n) => Ok(lit(-n)),
                        Err(_) => Ok(lit(-(n
                            .parse::<f64>()
                            .context(format!("cannot parse {} as f64", n))?))),
                    },
                    _ => {
                        return Err(anyhow!(format!(
                            "unsupported minus operation for expression:{expr:?}"
                        )))
                    }
                },
                _ => return Err(anyhow!(format!("unsupported unary operator {op}"))),
            },
            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between(Between {
                expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema)?),
                negated: negated,
                low: Box::new(self.sql_expr_to_logical_expr(*low, schema)?),
                high: Box::new(self.sql_expr_to_logical_expr(*high, schema)?),
            })),
            SQLExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                let pattern = self.sql_expr_to_logical_expr(*pattern, schema)?;
                let pattern_type = pattern.get_type(schema)?;
                if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
                    return Err(anyhow!("Invalid pattern in like expression"));
                }
                Ok(Expr::Like(Like {
                    negated: negated,
                    expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema)?),
                    pattern: Box::new(pattern),
                    escape_char: escape_char,
                }))
            }
            SQLExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                let pattern = self.sql_expr_to_logical_expr(*pattern, schema)?;
                let pattern_type = pattern.get_type(schema)?;
                if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
                    return Err(anyhow!("Invalid pattern in ilike expression"));
                }
                Ok(Expr::ILike(Like {
                    negated: negated,
                    expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema)?),
                    pattern: Box::new(pattern),
                    escape_char: escape_char,
                }))
            }
            SQLExpr::BinaryOp { left, op, right } => {
                let op = self.parse_sql_binary_op(op)?;
                Ok(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(self.sql_expr_to_logical_expr(*left, schema)?),
                    op: op,
                    right: Box::new(self.sql_expr_to_logical_expr(*right, schema)?),
                }))
            }
            SQLExpr::AggregateExpressionWithFilter { expr, filter } => {
                match self.sql_expr_to_logical_expr(*expr, schema)? {
                    Expr::AggregateFunction(AggregateFunction {
                        fun,
                        args,
                        distinct,
                        order_by,
                        ..
                    }) => Ok(Expr::AggregateFunction(AggregateFunction {
                        fun: fun,
                        args: args,
                        distinct: distinct,
                        filter: Some(Box::new(self.sql_expr_to_logical_expr(*filter, schema)?)),
                        order_by: order_by,
                    })),
                    _ => {
                        return Err(anyhow!(
                            "AggregateExpressionWithFilter should based on AggregateFunction"
                        ))
                    }
                }
            }
            SQLExpr::Function(function) => {
                let name = if function.name.0.len() > 1 {
                    function.name.to_string()
                } else {
                    normalize_ident(function.name.0[0].clone())
                };
                // only support aggregate function, user defined function or built-in function not
                // supported can be added here to enhance the feature
                if let Ok(fun) = AggregateFunctionType::from_str(&name) {
                    let distinct = function.distinct;
                    let order_by = self.order_by_to_sort_expr(&function.order_by, schema)?;
                    let order_by = (!order_by.is_empty()).then_some(order_by);
                    let args = self.function_args_to_expr(function.args, schema)?;
                    return Ok(Expr::AggregateFunction(AggregateFunction {
                        fun,
                        args,
                        distinct,
                        filter: None,
                        order_by,
                    }));
                } else {
                    return Err(anyhow!("unsupported function type: {}", name));
                }
            }

            _ => Err(anyhow!(format!("unsupported ast expression:{ast_expr:?}"))),
        }
    }

    fn order_by_to_sort_expr(&self, exprs: &[OrderByExpr], schema: &Schema) -> Result<Vec<Expr>> {
        use sqlparser::ast::Value;
        let mut expr_vec = vec![];
        for e in exprs {
            let OrderByExpr {
                asc,
                expr,
                nulls_first,
            } = e;
            let expr = match expr {
                // there is a special syntax of order by column index
                SQLExpr::Value(Value::Number(v, _)) => {
                    let field_index = v
                        .parse::<usize>()
                        .context(format!("failed to parse {} to usize", v))?;
                    if field_index == 0 {
                        return Err(anyhow!("order by index start at 1 for index"));
                    } else if schema.fields().len() < field_index {
                        return Err(anyhow!(
                            "oder by index {} larger than number of fields:{}",
                            field_index,
                            schema.fields().len()
                        ));
                    } else {
                        let field = schema.field(field_index - 1);
                        Expr::Column(field.qualified_column())
                    }
                }
                e => self.sql_expr_to_logical_expr(e.clone(), schema)?,
            };
            let asc = asc.unwrap_or(true);
            expr_vec.push(Expr::Sort(Sort {
                expr: Box::new(expr),
                asc: asc,
                nulls_first: nulls_first.unwrap_or(!asc),
            }))
        }
        Ok(expr_vec)
    }

    pub fn function_args_to_expr(
        &self,
        args: Vec<sqlparser::ast::FunctionArg>,
        schema: &Schema,
    ) -> Result<Vec<Expr>> {
        args.into_iter()
            .map(|a| match a {
                sqlparser::ast::FunctionArg::Named {
                    name: _,
                    arg: sqlparser::ast::FunctionArgExpr::Expr(arg),
                } => self.sql_expr_to_logical_expr(arg, schema),
                sqlparser::ast::FunctionArg::Named {
                    name: _,
                    arg: sqlparser::ast::FunctionArgExpr::Wildcard,
                } => Ok(Expr::Wildcard),
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    arg,
                )) => self.sql_expr_to_logical_expr(arg, schema),
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                    Ok(Expr::Wildcard)
                }
                _ => {
                    return Err(anyhow!(format!(
                        "unsupported qualified wildard argument:{a:?}"
                    )))
                }
            })
            .collect::<Result<Vec<Expr>>>()
    }

    fn parse_sql_binary_op(&self, op: sqlparser::ast::BinaryOperator) -> Result<Operator> {
        match op {
            sqlparser::ast::BinaryOperator::Eq => Ok(Operator::Eq),
            sqlparser::ast::BinaryOperator::NotEq => Ok(Operator::NotEq),
            sqlparser::ast::BinaryOperator::Lt => Ok(Operator::Lt),
            sqlparser::ast::BinaryOperator::LtEq => Ok(Operator::LtEq),
            sqlparser::ast::BinaryOperator::Gt => Ok(Operator::Gt),
            sqlparser::ast::BinaryOperator::GtEq => Ok(Operator::GtEq),
            sqlparser::ast::BinaryOperator::Plus => Ok(Operator::Plus),
            sqlparser::ast::BinaryOperator::Minus => Ok(Operator::Minus),
            sqlparser::ast::BinaryOperator::Multiply => Ok(Operator::Multiply),
            sqlparser::ast::BinaryOperator::Divide => Ok(Operator::Divide),
            sqlparser::ast::BinaryOperator::Modulo => Ok(Operator::Modulo),
            sqlparser::ast::BinaryOperator::And => Ok(Operator::And),
            sqlparser::ast::BinaryOperator::Or => Ok(Operator::Or),
            _ => return Err(anyhow!("unsupported binayr operator {}", op)),
        }
    }

    pub fn validate_schema_satisfies_exprs(&self, schema: &Schema, exprs: &[Expr]) -> Result<()> {
        let exprs: Vec<Expr> = exprs
            .iter()
            .flat_map(find_columns_referred_by_expr)
            .map(Expr::Column)
            .collect();
        exprs.iter().try_for_each(|col| match col {
            Expr::Column(col) => match &col.relation {
                Some(r) => {
                    schema.field_with_qualified_name(r, &col.name)?;
                    Ok(())
                }
                None => {
                    if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                        Ok(())
                    } else {
                        return Err(anyhow!("field {} not found in schema", col.name));
                    }
                }
            },
            _ => return Err(anyhow!("Not a column")),
        })
    }
}
