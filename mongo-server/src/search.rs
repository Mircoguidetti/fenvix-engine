//! Atlas Search (`$search`, `$searchMeta`) on top of the text index machinery (see results9.md for the operators).

/// Hidden field carrying the search score through a pipeline (`{$meta: "searchScore"}`).
pub const SCORE_KEY: &str = "\u{1}searchScore";
