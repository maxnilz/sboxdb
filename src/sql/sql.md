# SQL syntax

```ebnf

sql_stmt          := ddl_stmt | dml_stmt ;

-- DDL =================================================================================

ddl_stmt          := create_table_stmt
                   | drop_table_stmt
                   | alter_table_stmt
                   | create_index_stmt
                   | drop_index_stmt ;

create_table_stmt := "CREATE TABLE" identifier "(" column_def ("," column_def)* ")" ;

column_def        := identifier data_type [ column_constraints ] ;

data_type         := "INT"
                   | "BIGINT"
                   | "DOUBLE"
                   | "BOOLEAN"
                   | "TEXT"
                   | "VARCHAR" "(" number ")" ;

column_constraints := "PRIMARY KEY"
                    | "NOT NULL"
                    | "UNIQUE"
                    | "DEFAULT" literal ;

drop_table_stmt   := "DROP TABLE" identifier ;

alter_table_stmt  := "ALTER TABLE" identifier alter_action ;
alter_action      := "ADD COLUMN" column_def
                   | "DROP COLUMN" identifier ;

create_index_stmt := "CREATE" [ "UNIQUE" ] "INDEX" identifier
                     "ON" identifier "(" identifier ("," identifier)* ")" ;

drop_index_stmt   := "DROP INDEX" identifier ;


-- DML =================================================================================

dml_stmt          := with_clause? (
                      select_stmt
                    | insert_stmt
                    | update_stmt
                    | delete_stmt
                    | transaction_stmt
                    | explain_stmt
                   ) ;

with_clause       := "WITH" cte_def ("," cte_def)* ;
cte_def           := identifier "AS" "(" select_stmt ")" ;

select_stmt       := "SELECT" select_list
                      "FROM" from_clause
                      [ where_clause ]
                      [ group_by_clause ]
                      [ order_by_clause ]
                      [ limit_clause ] ;
                      
explain_stmt      := "EXPLAIN" select_stmt ;
               
select_list       := "*" | select_item ("," select_item)* ;
select_item       := expr [ "AS" alias ] ;

from_clause       := table_ref ( join_clause )* ;
table_ref         := identifier [ "AS" alias ]
                    | "(" select_stmt ")" [ "AS" alias ] ;

join_clause       := join_type "JOIN" table_ref "ON" expr ;
join_type         := "INNER" | "LEFT" [ "OUTER" ] | "RIGHT" [ "OUTER" ] | "FULL" [ "OUTER" ] ;

where_clause      := "WHERE" expr ;
group_by_clause   := "GROUP BY" expr ("," expr)* ;
order_by_clause   := "ORDER BY" order_item ("," order_item)* ;
order_item        := expr [ "ASC" | "DESC" ] ;
limit_clause      := "LIMIT" number ;

insert_stmt       := "INSERT INTO" identifier "(" column_list ")"
                      ( "VALUES" "(" value_list ")" | select_stmt ) ;

update_stmt       := "UPDATE" identifier
                      "SET" assignment ("," assignment)* [ where_clause ] ;

delete_stmt       := "DELETE FROM" identifier [ where_clause ] ;

transaction_stmt  := begin_stmt | commit_stmt | rollback_stmt ;

begin_stmt        := "BEGIN" [ transaction_mode ] [ as_of_clause ] ;
transaction_mode  := "READ ONLY" | "READ WRITE" ;
as_of_clause      := "AS OF SYSTEM TIME" number ;

commit_stmt       := "COMMIT" ;
rollback_stmt     := "ROLLBACK" ;

assignment        := identifier "=" expr ;
column_list       := identifier ("," identifier)* ;
value_list        := expr ("," expr)* ;

expr              := literal
                    | qualified_name
                    | expr binary_op expr
                    | "NOT" expr
                    | "(" expr ")"
                    | "(" select_stmt ")"               (* scalar subquery *)
                    | "EXISTS" "(" select_stmt ")"      (* exists subquery *)
                    | expr "IN" "(" select_stmt ")"     (* subquery in predicate *) ;

qualified_name    := identifier ("." identifier)* ;

binary_op         := "+" | "-" | "*" | "/" | "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" | "AND" | "OR" ;

alias             := identifier ;


-- Terminals ==========================================================================

identifier        := /[a-zA-Z_][a-zA-Z0-9_]*/ ;
number            := /[0-9]+/ ;
literal           := number | string_literal ;
string_literal    := "'" /[^']*/ "'" ;

```
