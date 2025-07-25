<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# SQL Parser Derive Proc Macro

**NOTE**: This is copied
from [datafusion-sqlparser-rs derive proc macro](https://github.com/apache/datafusion-sqlparser-rs/tree/main/derive)
with changes to accommodate to the sboxdb specific [ast](../src/sql/parser/ast.rs).

This crate contains a procedural macro that can automatically derive implementations of the `Visit` trait in the parser
at [here](../src/sql/parser/visitor.rs)

```rust
#[derive(Visit, VisitMut)]
struct Foo {
    boolean: bool,
    bar: Bar,
}

#[derive(Visit, VisitMut)]
enum Bar {
    A(),
    B(String, bool),
    C { named: i32 },
}
```

Will generate code akin to

```rust
impl Visit for Foo {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        self.boolean.visit(visitor)?;
        self.bar.visit(visitor)?;
        ControlFlow::Continue(())
    }
}

impl Visit for Bar {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        match self {
            Self::A() => {}
            Self::B(_1, _2) => {
                _1.visit(visitor)?;
                _2.visit(visitor)?;
            }
            Self::C { named } => {
                named.visit(visitor)?;
            }
        }
        ControlFlow::Continue(())
    }
}
```

Some types may wish to call a corresponding method on the visitor:

```rust
#[derive(Visit, VisitMut)]
#[visit(with = "visit_expr")]
enum Expr {
    IsNull(Box<Expr>),
..
}
```

This will result in the following sequence of visitor calls when an `IsNull`
expression is visited

```
visitor.pre_visit_expr(<is null expr>)
visitor.pre_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null expr>)
```

For some types it is only appropriate to call a particular visitor method in
some contexts. For example, not every `Ident` refers to a relation.

In these cases, the `visit` attribute can be used on the field for which we'd
like to call the method:

```rust
#[derive(Visit, VisitMut)]
#[visit(with = "visit_table_factor")]
pub enum TableFactor {
    Table {
        #[visit(with = "visit_relation")]
        name: ObjectName,
        alias: Option<TableAlias>,
    },
..
}
```

This will generate

```rust
impl Visit for TableFactor {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        visitor.pre_visit_table_factor(self)?;
        match self {
            Self::Table { name, alias } => {
                visitor.pre_visit_relation(name)?;
                name.visit(visitor)?;
                visitor.post_visit_relation(name)?;
                alias.visit(visitor)?;
            }
        }
        visitor.post_visit_table_factor(self)?;
        ControlFlow::Continue(())
    }
}
```

Note that annotating both the type and the field is incorrect as it will result
in redundant calls to the method. For example

```rust
#[derive(Visit, VisitMut)]
#[visit(with = "visit_expr")]
enum Expr {
    IsNull(#[visit(with = "visit_expr")] Box<Expr>),
..
}
```

will result in these calls to the visitor

```
visitor.pre_visit_expr(<is null expr>)
visitor.pre_visit_expr(<is null operand>)
visitor.pre_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null expr>)
```

If the field is a `Option` and add `#[with = "visit_xxx"]` to the field, the generated code
will try to access the field only if it is `Some`:

```rust
#[derive(Visit, VisitMut)]
pub struct ShowStatementIn {
    pub clause: ShowStatementInClause,
    pub parent_type: Option<ShowStatementInParentType>,
    #[visit(with = "visit_relation")]
    pub parent_name: Option<ObjectName>,
}
```

This will generate

```rust
impl Visit for ShowStatementIn {
    fn visit<V: Visitor>(
        &self,
        visitor: &mut V,
    ) -> ::std::ops::ControlFlow<V::Break> {
        Visit::visit(&self.clause, visitor)?;
        Visit::visit(&self.parent_type, visitor)?;
        if let Some(value) = &self.parent_name {
            visitor.pre_visit_relation(value)?;
            Visit::visit(value, visitor)?;
            visitor.post_visit_relation(value)?;
        }
        ::std::ops::ControlFlow::Continue(())
    }
}

impl VisitMut for ShowStatementIn {
    fn visit<V: VisitorMut>(
        &mut self,
        visitor: &mut V,
    ) -> ::std::ops::ControlFlow<V::Break> {
        VisitMut::visit(&mut self.clause, visitor)?;
        VisitMut::visit(&mut self.parent_type, visitor)?;
        if let Some(value) = &mut self.parent_name {
            visitor.pre_visit_relation(value)?;
            VisitMut::visit(value, visitor)?;
            visitor.post_visit_relation(value)?;
        }
        ::std::ops::ControlFlow::Continue(())
    }
}
```
