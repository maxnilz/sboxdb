# SQL syntax

```ebnf

sql_stmt          := ddl_stmt | dml_stmt ;

-- DDL =================================================================================

ddl_stmt          := create_table_stmt
                   | create_index_stmt
                   | drop_object_stmt
                   | alter_table_stmt

create_table_stmt := "CREATE" "TABLE" [ "IF" "NOT" "EXISTS" ] identifier "(" column_def ("," column_def)* ")" ;

column_def        := identifier data_type (column_constraint)* ;

data_type         := "INT"
                   | "BIGINT"
                   | "DOUBLE"
                   | "BOOLEAN"
                   | "TEXT"
                   | "VARCHAR" "(" number ")" ;

column_constraint  := "PRIMARY KEY"
                    | [ "NOT" ] "NULL"
                    | "UNIQUE"
                    | "DEFAULT" expr ;

create_index_stmt := "CREATE" [ "UNIQUE" ] "INDEX" [ "IF" "NOT" "EXISTS" ] identifier
                     "ON" identifier "(" identifier ("," identifier)* ")" ;

drop_object_stmt   := "DROP" ( "TABLE" | "INDEX" ) [ "IF" "EXISTS" ] identifier ;

alter_table_stmt  := "ALTER" "TABLE" [ "IF" "EXISTS" ] identifier alter_operation ("," alter_operation)* ;
alter_operation   := "ADD" "COLUMN"  [ "IF" "NOT" "EXISTS" ] column_def
                   | "DROP" "COLUMN" [ "IF" "EXISTS" ] identifier ;

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
group_by_clause   := "GROUP" "BY" expr ("," expr)* ;
order_by_clause   := "ORDER" "BY" order_item ("," order_item)* ;
order_item        := expr [ "ASC" | "DESC" ] ;
limit_clause      := "LIMIT" number ;

insert_stmt       := "INSERT" "INTO" identifier "(" column_list ")"
                      ( "VALUES" "(" value_list ")" | select_stmt ) ;

update_stmt       := "UPDATE" identifier
                      "SET" assignment ("," assignment)* [ where_clause ] ;

delete_stmt       := "DELETE" "FROM" identifier [ where_clause ] ;

transaction_stmt  := begin_stmt | commit_stmt | rollback_stmt ;

begin_stmt        := "BEGIN" [ transaction_mode ] [ as_of_clause ] ;
transaction_mode  := "READ" "ONLY" | "READ" "WRITE" ;
as_of_clause      := "AS" "OF" number ;

commit_stmt       := "COMMIT" ;
rollback_stmt     := "ROLLBACK" ;

assignment        := identifier "=" expr ;
column_list       := identifier ("," identifier)* ;
value_list        := expr ("," expr)* ;

expr              := literal
                    | compound_ident
                    | function_call
                    | unary_op expr
                    | expr binary_op expr
                    | expr [ "NOT" ] "IN" "(" ( select_stmt | expr ("," expr)* ) ")"     (* subquery in predicate *)
                    | expr "IS" [ "NOT" ] "NULL"                                         (* IS NULL predicate *)
                    | [ "NOT" ] "EXISTS" "(" select_stmt ")"                             (* exists subquery *)
                    | "(" select_stmt ")"                                                (* scalar subquery *)
                    | "(" expr ("," expr)* ")" ;                                         (* tuple/grouped expr *)

compound_ident    := identifier ("." identifier)* ;

unary_op          := "+" | "-" | "NOT";
binary_op         := "+" | "-" | "*" | "/" | '%' | "=" | "!=" | "<" | "<=" | ">" | ">=" | "AND" | "OR" | "LIKE" | "ILIKE";

function_call     := identifier "(" function_arg_list ")" ;

function_arg_list := "*"                                 (* e.g., COUNT(*) *)
                    | function_arg ("," function_arg)*   (* e.g., COALESCE(a, b) *)
                    |                                    (* empty argument list *) ;
                    
function_arg      := literal
                    | identifier
                    | function_call                     (* nested function call *) ;

alias             := identifier ;


-- Terminals ==========================================================================


identifier        := unquoted_ident | quoted_ident ;
unquoted_ident    := /[a-zA-Z_][a-zA-Z0-9_]*/ ;
quoted_ident      := '"' /[^"]*/ '"' ;

literal           := number | string_literal ;

number            := /[0-9]+(\.[0-9]*)?([eE][+-]?[0-9]+)?/ ;
                    
string_literal    := "'" /[^']*/ "'" ;

```
