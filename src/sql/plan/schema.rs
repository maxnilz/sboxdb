use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::path::Prefix::Verbatim;
use std::sync::Arc;

use crate::catalog::column::Column;
use crate::catalog::column::ColumnRef;
use crate::catalog::r#type::DataType;
use crate::catalog::schema::Schema;
use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::expr::Expr;

/// A logical named reference to a qualified field in a schema.
#[derive(Clone, Debug)]
pub struct FieldReference {
    /// field/column name.
    pub name: String,
    /// relation/table reference as qualifier to specify the source of the
    /// field/column.
    pub relation: Option<TableReference>,
}

impl FieldReference {
    pub fn new(name: impl Into<String>, relation: Option<TableReference>) -> Self {
        Self { name: name.into(), relation }
    }

    pub fn new_unqualified(name: impl Into<String>) -> Self {
        Self { name: name.into(), relation: None }
    }
}

/// A name or alias used as a reference to a table.
#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TableReference(Arc<str>);

impl TableReference {
    pub fn new(table: &str) -> TableReference {
        TableReference(Arc::from(table))
    }
}

impl From<&str> for TableReference {
    fn from(s: &str) -> Self {
        TableReference::new(s)
    }
}

impl From<String> for TableReference {
    fn from(s: String) -> Self {
        TableReference(Arc::from(s))
    }
}

impl std::fmt::Display for TableReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

/// A reference counted [`Field`]
pub type FieldRef = Arc<Field>;

/// Describes a single column in a [`LogicalSchema`].
#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<Expr>,
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.datatype == other.datatype
            && self.primary_key == other.primary_key
            && self.nullable == other.nullable
            && self.unique == other.unique
    }
}

impl Eq for Field {}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Field {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name
            .cmp(&other.name)
            .then_with(|| self.datatype.cmp(&other.datatype))
            .then_with(|| self.primary_key.cmp(&self.primary_key))
            .then_with(|| self.nullable.cmp(&self.nullable))
            .then_with(|| self.unique.cmp(&self.unique))
    }
}

impl Hash for Field {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.datatype.hash(state);
        self.primary_key.hash(state);
        self.nullable.hash(state);
        self.unique.hash(state);
    }
}

impl From<Column> for Field {
    fn from(column: Column) -> Self {
        Self::from(&ColumnRef::new(column))
    }
}

impl From<&ColumnRef> for Field {
    fn from(column: &ColumnRef) -> Self {
        Self {
            name: column.name.clone(),
            datatype: column.datatype.clone(),
            primary_key: column.primary_key,
            nullable: column.nullable,
            unique: column.unique,
            default: column.default.clone().map(|value| Expr::Value(value)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Fields(Arc<[FieldRef]>);

impl Fields {
    /// Returns a new empty [`Fields`]
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }

    pub fn find(&self, name: &str) -> Option<(usize, &FieldRef)> {
        self.0.iter().enumerate().find(|(_, c)| c.name == name)
    }
}

impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<FieldRef> for Fields {
    fn from_iter<T: IntoIterator<Item = FieldRef>>(iter: T) -> Self {
        iter.into_iter().collect()
    }
}

impl From<Vec<Field>> for Fields {
    fn from(value: Vec<Field>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<FieldRef>> for Fields {
    fn from(value: Vec<FieldRef>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<&FieldRef>> for Fields {
    fn from(value: Vec<&FieldRef>) -> Self {
        value.into_iter().map(|it| Arc::clone(it)).collect()
    }
}

impl Deref for Fields {
    type Target = [FieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
/// Builder for creating [`Field`] instances with a fluent interface
pub struct FieldBuilder {
    name: String,
    datatype: DataType,
    primary_key: bool,
    nullable: bool,
    unique: bool,
    default: Option<Expr>,
}

impl FieldBuilder {
    /// Create a new field builder with the required name and data type
    pub fn new(name: impl Into<String>, datatype: DataType) -> Self {
        Self {
            name: name.into(),
            datatype,
            primary_key: false,
            nullable: true,
            unique: false,
            default: None,
        }
    }

    /// Mark this field as a primary key
    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false; // Primary keys are automatically not nullable
        self.unique = true; // Primary keys are automatically unique
        self
    }

    /// Set whether this field is nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Mark this field as not nullable
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Mark this field as unique
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Set the default value for this field
    pub fn default_value(mut self, value: Expr) -> Self {
        self.default = Some(value);
        self
    }

    /// Build the field
    pub fn build(self) -> Field {
        Field {
            name: self.name,
            datatype: self.datatype,
            primary_key: self.primary_key,
            nullable: self.nullable,
            unique: self.unique,
            default: self.default,
        }
    }
}

/// Logical schema at planner where each field have an optional table reference,
/// typically the table name or table alias, to tracks which table/relation each
/// field belongs to. It is different from the [`Schema`] where it has an
/// aggregated/unified single qualifier, typically the table name, for all the columns.
#[derive(Clone, Debug)]
pub struct LogicalSchema {
    /// A sequence of fields that describe the schema.
    fields: Fields,
    /// Optional qualifiers for each column in this schema to specify the source
    /// of each column. In the same order as the `fields`
    qualifiers: Vec<Option<TableReference>>,
}

impl LogicalSchema {
    pub fn empty() -> Self {
        Self { fields: Fields::empty(), qualifiers: vec![] }
    }

    pub fn new(fields: Fields, qualifiers: Vec<Option<TableReference>>) -> Result<Self> {
        if fields.len() != qualifiers.len() {
            return Err(Error::internal("Invalid fields and qualifiers size"));
        }
        let schema = LogicalSchema { fields, qualifiers };
        schema.check_names()?;
        Ok(schema)
    }

    pub fn from_unqualified_fields(fields: Fields) -> Result<Self> {
        let sz = fields.len();
        let schema = LogicalSchema { fields, qualifiers: vec![None; sz] };
        schema.check_names()?;
        Ok(schema)
    }

    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    pub fn field(&self, index: usize) -> &FieldRef {
        &self.fields[index]
    }

    pub fn field_by_name(&self, name: &str) -> Result<FieldRef> {
        if let Some(idx) = self.field_index_by_name(&None, name) {
            return Ok(self.fields[idx].clone());
        }
        Err(Error::parse(format!("Column {} not found", name)))
    }

    pub fn field_by_ref(&self, field: &FieldReference) -> Result<FieldRef> {
        let relation = &field.relation;
        if let Some(idx) = self.field_index_by_name(relation, &field.name) {
            return Ok(self.fields[idx].clone());
        }
        Err(Error::parse(format!("Column {} not found", field.name)))
    }

    pub fn field_reference(&self, index: usize) -> FieldReference {
        FieldReference::new(self.fields[index].name.clone(), self.qualifiers[index].clone())
    }

    /// Get a logical field reference from the given ident by checking whether the given
    /// ident can be found from the schema.
    pub fn field_reference_by_name(
        &self,
        ident: &str,
    ) -> Option<(FieldReference, Option<FieldRef>)> {
        if let Some((table_reference, f)) = self.find(&ident) {
            return Some((FieldReference::new(&f.name, table_reference), Some(f.clone())));
        }
        // no field with given name found
        None
    }

    /// Get a logical field reference from the given qualifier and ident by checking whether
    /// the given qualifier and ident can be fround from the schema.
    pub fn field_reference_by_qname(
        &self,
        qualifier: &Option<TableReference>,
        ident: &str,
    ) -> Option<(FieldReference, Option<FieldRef>)> {
        if let Some(idx) = self.field_index_by_name(qualifier, ident) {
            let q = &self.qualifiers[idx];
            let f = &self.fields[idx];
            return Some((FieldReference::new(&f.name, q.clone()), Some(f.clone())));
        }
        // no qualified column found
        None
    }

    /// Searches for a column by name, returning it along with its table
    /// reference if found
    pub fn find(&self, name: &str) -> Option<(Option<TableReference>, &FieldRef)> {
        if let Some((i, f)) = self.fields.find(name) {
            let q = self.qualifiers[i].clone();
            return Some((q, f));
        }
        None
    }

    pub fn iter(&self) -> impl Iterator<Item = (Option<&TableReference>, &FieldRef)> {
        self.fields.iter().zip(self.qualifiers.iter()).map(|(f, q)| (q.as_ref(), f))
    }

    pub fn field_index_by_name(
        &self,
        qualifier: &Option<TableReference>,
        name: &str,
    ) -> Option<usize> {
        let mut matches = self
            .iter()
            .enumerate()
            .filter(|(_, (q, f))| {
                match (&qualifier, q) {
                    // The given qualifier and current checking column qualifier are both qualified,
                    // compare both qualifier and the column name.
                    (Some(q), Some(t)) => q.eq(t) && f.name == name,
                    // The given qualifier is qualified but the current checking column qualifier
                    // is not qualified, consider it as a false.
                    (Some(_), None) => false,
                    // The given qualifier is not qualified, compare the column name only.
                    (None, Some(_)) | (None, None) => f.name == name,
                }
            })
            .map(|(idx, _)| idx);
        matches.next()
    }

    /// Modify this schema by appending the fields from the supplied schema, ignoring any
    /// duplicate fields.
    pub fn merge(&mut self, other: &LogicalSchema) {
        if other.fields.is_empty() {
            return;
        }

        let mut new_fields = Vec::new();
        let mut new_qualifiers = Vec::new();

        let self_fields: HashSet<(Option<&TableReference>, &FieldRef)> = self.iter().collect();
        let self_field_names =
            self.fields.iter().map(|it| it.name.as_str()).collect::<HashSet<_>>();
        for (q, f) in other.iter() {
            let dup = match q {
                None => self_field_names.contains(f.name.as_str()),
                Some(q) => self_fields.contains(&(Some(q), f)),
            };
            if dup {
                continue;
            }
            new_fields.push(Arc::clone(f));
            new_qualifiers.push(q.cloned());
        }

        let mut fields = self.fields.to_vec();
        fields.extend(new_fields);
        self.fields = fields.into();
        self.qualifiers.extend(new_qualifiers)
    }

    /// Create a new schema that contains the fields from this schema followed by the fields
    /// from the supplied schema. An error will be returned if there are duplicate field names.
    pub fn join(&self, other: &LogicalSchema) -> Result<Self> {
        let mut fields = Vec::new();
        let mut qualifiers = Vec::new();

        fields.extend_from_slice(self.fields.as_ref());
        qualifiers.extend_from_slice(&self.qualifiers);

        fields.extend_from_slice(other.fields.as_ref());
        qualifiers.extend_from_slice(&other.qualifiers);

        Ok(Self { fields: fields.into(), qualifiers })
    }

    fn check_names(&self) -> Result<()> {
        let mut qualified_names = BTreeSet::new();
        let mut unqualified_names = BTreeSet::new();
        for (c, q) in self.fields.iter().zip(&self.qualifiers) {
            if let Some(q) = q {
                if !qualified_names.insert((q, &c.name)) {
                    return Err(Error::parse(format!(
                        "Invalid schema, duplicate qualified column {}.{}",
                        q, &c.name
                    )));
                }
                continue;
            }
            if !unqualified_names.insert(&c.name) {
                return Err(Error::parse(format!(
                    "Invalid schema, duplicate unqualified column {}",
                    &c.name
                )));
            }
        }
        for (q, name) in qualified_names {
            if unqualified_names.contains(name) {
                return Err(Error::parse(format!(
                    "Invalid schema, ambiguous reference {}.{}",
                    q, name
                )));
            }
        }
        Ok(())
    }
}

impl From<Schema> for LogicalSchema {
    fn from(schema: Schema) -> Self {
        let sz = schema.columns.len();
        let fields = schema.columns.iter().map(|it| Field::from(it)).collect::<Vec<_>>().into();
        let table = TableReference::new(&schema.name);
        Self { fields, qualifiers: vec![Some(table); sz] }
    }
}
