use sqlx::{Database, Encode, QueryBuilder, Type};

pub fn quote_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectColumn<'a> {
    expr: &'a str,
    alias: Option<&'a str>,
}

impl<'a> SelectColumn<'a> {
    pub fn expr(expr: &'a str) -> Self {
        Self { expr, alias: None }
    }

    pub fn alias(mut self, alias: &'a str) -> Self {
        self.alias = Some(alias);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectQuery<'a> {
    columns: Vec<SelectColumn<'a>>,
    from: &'a str,
}

impl<'a> SelectQuery<'a> {
    pub fn new(from: &'a str) -> Self {
        Self {
            columns: Vec::new(),
            from,
        }
    }

    pub fn select_columns<I>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = SelectColumn<'a>>,
    {
        self.columns.extend(columns);
        self
    }

    pub fn render(&self) -> String {
        let mut sql = String::from("SELECT ");

        if self.columns.is_empty() {
            sql.push('*');
        } else {
            for (index, column) in self.columns.iter().enumerate() {
                if index > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(column.expr);
                if let Some(alias) = column.alias {
                    sql.push_str(" AS ");
                    sql.push_str(&quote_ident(alias));
                }
            }
        }

        sql.push_str(" FROM ");
        sql.push_str(self.from);
        sql
    }

    pub fn statement<DB>(&self) -> SelectStatement<DB>
    where
        DB: Database,
    {
        SelectStatement {
            builder: QueryBuilder::<DB>::new(self.render()),
            where_clause: WhereClause::new(),
        }
    }
}

pub struct SelectStatement<DB>
where
    DB: Database,
{
    builder: QueryBuilder<DB>,
    where_clause: WhereClause,
}

impl<DB> SelectStatement<DB>
where
    DB: Database,
{
    pub fn where_eq<'args, T>(&mut self, column_sql: &str, value: T) -> &mut Self
    where
        T: 'args + Encode<'args, DB> + Type<DB>,
    {
        push_eq(&mut self.builder, &mut self.where_clause, column_sql, value);
        self
    }

    pub fn where_in<'args, T>(&mut self, column_sql: &str, values: &[T]) -> &mut Self
    where
        T: Clone + 'args + Encode<'args, DB> + Type<DB>,
    {
        push_in(
            &mut self.builder,
            &mut self.where_clause,
            column_sql,
            values,
        );
        self
    }

    pub fn order_by_sql(&mut self, order_sql: &str) -> &mut Self {
        if !order_sql.trim().is_empty() {
            self.builder.push(" ORDER BY ").push(order_sql);
        }
        self
    }

    pub fn limit<'args>(&mut self, limit: i64) -> &mut Self
    where
        i64: 'args + Encode<'args, DB> + Type<DB>,
    {
        push_limit(&mut self.builder, limit);
        self
    }

    pub fn finish(self) -> QueryBuilder<DB> {
        self.builder
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WhereClause {
    has_clause: bool,
}

impl WhereClause {
    pub fn new() -> Self {
        Self { has_clause: false }
    }

    pub fn with_existing_clause() -> Self {
        Self { has_clause: true }
    }

    pub fn push_next<DB>(&mut self, builder: &mut QueryBuilder<DB>)
    where
        DB: Database,
    {
        if self.has_clause {
            builder.push(" AND ");
        } else {
            builder.push(" WHERE ");
            self.has_clause = true;
        }
    }
}

pub fn push_eq<'args, DB, T>(
    builder: &mut QueryBuilder<DB>,
    where_clause: &mut WhereClause,
    column_sql: &str,
    value: T,
) where
    DB: Database,
    T: 'args + Encode<'args, DB> + Type<DB>,
{
    where_clause.push_next(builder);
    builder.push(column_sql).push(" = ").push_bind(value);
}

pub fn push_optional_eq<'args, DB, T>(
    builder: &mut QueryBuilder<DB>,
    where_clause: &mut WhereClause,
    column_sql: &str,
    value: Option<T>,
) where
    DB: Database,
    T: 'args + Encode<'args, DB> + Type<DB>,
{
    if let Some(value) = value {
        push_eq(builder, where_clause, column_sql, value);
    }
}

pub fn push_in<'args, DB, T>(
    builder: &mut QueryBuilder<DB>,
    where_clause: &mut WhereClause,
    column_sql: &str,
    values: &[T],
) where
    DB: Database,
    T: Clone + 'args + Encode<'args, DB> + Type<DB>,
{
    where_clause.push_next(builder);
    builder.push(column_sql).push(" IN (");
    {
        let mut separated = builder.separated(", ");
        for value in values {
            separated.push_bind(value.clone());
        }
    }
    builder.push(")");
}

pub fn push_ci_contains<'args, DB>(
    builder: &mut QueryBuilder<DB>,
    where_clause: &mut WhereClause,
    column_sql: &str,
    value: &str,
) where
    DB: Database,
    String: 'args + Encode<'args, DB> + Type<DB>,
{
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }

    where_clause.push_next(builder);
    push_ci_contains_predicate(builder, column_sql, trimmed);
}

pub fn push_ci_contains_any<'args, DB>(
    builder: &mut QueryBuilder<DB>,
    where_clause: &mut WhereClause,
    column_sqls: &[&str],
    value: &str,
) where
    DB: Database,
    String: 'args + Encode<'args, DB> + Type<DB>,
{
    let trimmed = value.trim();
    if trimmed.is_empty() || column_sqls.is_empty() {
        return;
    }

    where_clause.push_next(builder);
    builder.push("(");
    for (index, column_sql) in column_sqls.iter().enumerate() {
        if index > 0 {
            builder.push(" OR ");
        }
        push_ci_contains_predicate(builder, column_sql, trimmed);
    }
    builder.push(")");
}

fn push_ci_contains_predicate<'args, DB>(
    builder: &mut QueryBuilder<DB>,
    column_sql: &str,
    trimmed: &str,
) where
    DB: Database,
    String: 'args + Encode<'args, DB> + Type<DB>,
{
    builder
        .push(column_sql)
        .push(" ILIKE ")
        .push_bind(format!("%{trimmed}%"));
}

pub fn push_limit<'args, DB>(builder: &mut QueryBuilder<DB>, limit: i64)
where
    DB: Database,
    i64: 'args + Encode<'args, DB> + Type<DB>,
{
    builder.push(" LIMIT ").push_bind(limit);
}

pub fn push_limit_offset<'args, DB>(builder: &mut QueryBuilder<DB>, limit: i64, offset: i64)
where
    DB: Database,
    i64: 'args + Encode<'args, DB> + Type<DB>,
{
    push_limit(builder, limit);
    builder.push(" OFFSET ").push_bind(offset);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Execute, Postgres, QueryBuilder};

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident("trigger"), "\"trigger\"");
    }

    #[test]
    fn where_clause_pushes_where_then_and() {
        let mut builder = QueryBuilder::<Postgres>::new("SELECT * FROM items");
        let mut where_clause = WhereClause::new();
        push_eq(
            &mut builder,
            &mut where_clause,
            "kind",
            "scheduled".to_string(),
        );
        push_eq(
            &mut builder,
            &mut where_clause,
            "status",
            "running".to_string(),
        );
        let query = builder.build();
        assert!(query
            .sql()
            .as_str()
            .contains(" WHERE kind = $1 AND status = $2"));
    }

    #[test]
    fn ci_contains_uses_ilike_for_postgres() {
        let mut builder = QueryBuilder::<Postgres>::new("SELECT * FROM items");
        let mut where_clause = WhereClause::new();
        push_ci_contains(&mut builder, &mut where_clause, "task_key", " Fetch ");
        let query = builder.build();
        assert!(query.sql().as_str().contains(" WHERE task_key ILIKE $1"));
    }

    #[test]
    fn ci_contains_any_groups_or_predicates() {
        let mut builder = QueryBuilder::<Postgres>::new("SELECT * FROM items");
        let mut where_clause = WhereClause::new();
        push_ci_contains_any(
            &mut builder,
            &mut where_clause,
            &["file_name", "COALESCE(display_name, '')"],
            "Avatar",
        );
        let query = builder.build();
        assert!(query
            .sql()
            .as_str()
            .contains(" WHERE (file_name ILIKE $1 OR COALESCE(display_name, '') ILIKE $2)"));
    }

    #[test]
    fn in_and_limit_offset_are_rendered() {
        let mut builder = QueryBuilder::<Postgres>::new("SELECT * FROM items");
        let mut where_clause = WhereClause::new();
        push_in(
            &mut builder,
            &mut where_clause,
            "id",
            &["a".to_string(), "b".to_string()],
        );
        push_limit_offset(&mut builder, 10, 5);
        let sql = builder.build().sql();
        assert!(sql.as_str().contains(" WHERE id IN ($1, $2)"));
        assert!(sql.as_str().contains(" LIMIT $3 OFFSET $4"));
    }

    #[test]
    fn select_query_renders_projection() {
        let query = SelectQuery::new("providers").select_columns([
            SelectColumn::expr("id").alias("provider_id"),
            SelectColumn::expr("CAST(monthly_quota_usd AS DOUBLE PRECISION)")
                .alias("monthly_quota_usd"),
        ]);

        assert_eq!(
            query.render(),
            "SELECT id AS \"provider_id\", CAST(monthly_quota_usd AS DOUBLE PRECISION) AS \"monthly_quota_usd\" FROM providers"
        );
    }

    #[test]
    fn select_statement_owns_encoded_binds_without_interpolating_input() {
        let query = SelectQuery::new("items").select_columns([SelectColumn::expr("id")]);
        let mut statement = query.statement::<Postgres>();
        {
            let untrusted_name = String::from("name\'; DROP TABLE items; --");
            statement.where_eq("name", untrusted_name.as_str());
        }
        let mut builder = statement.finish();
        assert_eq!(
            builder.build().sql().as_str(),
            "SELECT id FROM items WHERE name = $1"
        );
    }

    #[test]
    fn select_statement_keeps_bind_order_and_limit() {
        let query = SelectQuery::new("items")
            .select_columns([SelectColumn::expr("id"), SelectColumn::expr("name")]);
        let mut statement = query.statement::<Postgres>();
        statement
            .where_eq("kind", "scheduled".to_string())
            .where_in("name", &["Fetch".to_string()])
            .order_by_sql("name ASC")
            .limit(20);

        let mut builder = statement.finish();
        let query = builder.build();
        assert_eq!(
            query.sql(),
            "SELECT id, name FROM items WHERE kind = $1 AND name IN ($2) ORDER BY name ASC LIMIT $3"
        );
    }
}
