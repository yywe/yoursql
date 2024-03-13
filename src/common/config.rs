use std::fmt::Display;

use anyhow::{anyhow, Result};

/// this is a very tricky implementation. detailed comments are listed.

/// this trait means the action to visit a field of a struct
/// some is the case the field has a value associated with it,
/// while none means the field being visited is None
/// the object impl this trait maybe a container, and will gather the
/// keys (and values if needed). that is why some we have value while none
/// has no value in the signature

trait Visit {
    fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str);
    fn none(&mut self, key: &str, description: &'static str);
}

/// this trait represent a field of config, note the field may be a primitive type like
/// int or string or bool, or the field maybe a struct, in such case, it will call the
/// struct's visit and moving forward. until hit None or a primitive type.
/// for visit, the parameter v may be a container, when hit the key, can collect its
/// value if it is a primitive type
/// set is to set the value of the given key
trait ConfigField {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str);
    fn set(&mut self, key: &str, value: &str) -> Result<()>;
}

/// this is a wrapper for option of F, i.e, ConfigField
impl<F: ConfigField + Default> ConfigField for Option<F> {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        match self {
            Some(s) => s.visit(v, key, description),
            None => v.none(key, description), // directly call the container's none method
        }
    }
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.get_or_insert_with(Default::default).set(key, value)
    }
}

/// this macro generate the ConfigField impl for primitive type like String or int
/// or bool. v maybe seem as the container, key is the key, self is the value of the
/// primitive type. please note for primitive type (can be seem as the last part of
/// the path) is not an option, it must has a value (self). that is why here for visit
/// directly call the some method of the container
///
/// actually for Option<String> (i.e, option of primitive type), it will be converted
/// by the above wrapper
///
/// while for the set method, now it does not care about the key since we now know the
/// reference of self, directly assign *self to the parsed value from the value string
macro_rules! config_field {
    ($t:ty) => {
        impl ConfigField for $t {
            fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
                v.some(key, self, description)
            }
            fn set(&mut self, _: &str, value: &str) -> Result<()> {
                *self = value.parse()?;
                Ok(())
            }
        }
    };
}

config_field!(String);
config_field!(bool);
config_field!(usize);

/// moving forward from primitive type, now will impl ConfigField (and Default) for
/// customized struct, which actually contains a set of primitive type.

macro_rules! config_namespace {
    (
        $(#[doc=$struct_d:tt])*
        $vis:vis struct $struct_name: ident {
            $(
            $(#[doc=$d:tt])*
            $field_vis:vis $field_name:ident : $field_type: ty, default=$default:expr
            )*$(,)*
        }
    ) => {
        $(#[doc = $struct_d])*
        #[derive(Clone, Debug)]
        #[non_exhaustive]
        $vis struct $struct_name {
            $(
                $(#[doc=$d])*
                $field_vis $field_name: $field_type,
            )*
        }

        impl ConfigField for $struct_name {
            /// in the context of struct, the key is just key prefix, full key will combine with primitive type key
            fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                $(
                    let key = format!(concat!("{}.", stringify!($field_name)), key_prefix);
                    let desc = concat!($($d),*).trim();
                    self.$field_name.visit(v, key.as_str(), desc);
                )*
            }
            fn set(&mut self, key: &str, value: &str) -> Result<()> {
                // in the case of primitive type, the remaining does not matter, it just set its value
                let (key, remaining) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                        stringify!($field_name) => self.$field_name.set(remaining, value),
                    )*
                    _=> Err(anyhow!("invalid config key"))
                }
            }

        }
        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }
    };
}

config_namespace! {
    pub struct CatalogOptions {
        pub create_default_catalog_and_schema: bool, default = true

        pub default_database: String, default = "master".to_string()

    }
}

config_namespace! {
    pub struct SqlParserOptions {
        pub parse_float_as_decimal: bool, default = false

        pub enable_ident_normalization: bool, default = true

        pub dialect: String, default = "generic".to_string()
    }
}

config_namespace! {
    pub struct ExecutionOptions {
        pub batch_size: usize, default = 32
    }
}

config_namespace! {
    pub struct OptimizerOptions {
        /// When set to true, the optimizer will insert filters before a join between
        /// a nullable and non-nullable column to filter out nulls on the nullable side. This
        /// filter can add additional overhead when the file format does not fully support
        /// predicate push down.
        pub filter_null_join_keys: bool, default = false


        /// Number of times that the optimizer will attempt to optimize the plan
        pub max_passes: usize, default = 3

        /// When set to true, the physical plan optimizer will run a top down
        /// process to reorder the join keys
        pub top_down_join_key_reordering: bool, default = true

        /// When set to true, the physical plan optimizer will prefer HashJoin over SortMergeJoin.
        /// HashJoin can work more efficiently than SortMergeJoin but consumes more memory
        pub prefer_hash_join: bool, default = true

        /// The maximum estimated size in bytes for one input side of a HashJoin
        /// will be collected into a single partition
        pub hash_join_single_partition_threshold: usize, default = 1024 * 1024
    }
}

#[derive(Debug)]
pub struct ConfigEntry {
    pub key: String,
    pub value: Option<String>,
    pub description: &'static str,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ConfigOptions {
    pub catalog: CatalogOptions,
    pub execution: ExecutionOptions,
    pub optimizer: OptimizerOptions,
    pub sql_parser: SqlParserOptions,
}

/// Now implement ConfigField for ConfigOptions as well

impl ConfigField for ConfigOptions {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "catalog" => self.catalog.set(rem, value),
            "execution" => self.execution.set(rem, value),
            "optimizer" => self.optimizer.set(rem, value),
            "sql_parser" => self.sql_parser.set(rem, value),
            _ => Err(anyhow!("unknown entry type for key {}", key)),
        }
    }
    fn visit<V: Visit>(&self, v: &mut V, _key_prefix: &str, _description: &'static str) {
        self.catalog.visit(v, "yoursql.catalog", "");
        self.execution.visit(v, "yoursql.execution", "");
        self.optimizer.visit(v, "yoursql.optimizer", "");
        self.sql_parser.visit(v, "yoursql.sql_parser", "");
    }
}

impl ConfigOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        ConfigField::set(self, key, value)
    }
    pub fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);
        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }
            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }
        let mut v = Visitor(vec![]);
        self.visit(&mut v, "yoursql", "");
        v.0
    }
}
