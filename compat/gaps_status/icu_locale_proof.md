# Collation locales without tailoring in the engine's data

mongod 7.0.45 uses ICU 57 (CLDR 29); the engine uses `icu_collator` 2.4.0 with `icu_collator_data` 2.4.0 (generated with CLDR 49, Unicode 18).
For each locale the same strings are sorted by mongod, by the engine, and by the engine under `en` (which has no tailoring, so it is the root order). `engine == engine(en)` and `engine != mongod` means the engine's data has no tailoring for the locale.

| locale | engine == engine(en) | engine == mongod | first difference (mongod / engine) |
|---|---|---|---|
| `lv` | False | False | position 9: `['å', 'ä', 'ā', 'aa', 'ae']` / `['å', 'ä', 'aa', 'ae', 'æ']` |
| `se` | True | False | position 6: `['a', 'A', 'ā', 'aa', 'ae']` / `['a', 'A', 'á', 'å', 'ä']` |
| `se@collation=search` | True | False | position 6: `['a', 'A', 'ā', 'aa', 'ae']` / `['a', 'A', 'á', 'å', 'ä']` |
| `smn` | True | False | position 6: `['a', 'A', 'ā', 'aa', 'ae']` / `['a', 'A', 'á', 'å', 'ä']` |
| `smn@collation=search` | True | False | position 6: `['a', 'A', 'ā', 'aa', 'ae']` / `['a', 'A', 'á', 'å', 'ä']` |
| `kl` | True | False | position 7: `['A', 'á', 'ā', 'aa', 'ae']` / `['A', 'á', 'å', 'ä', 'ā']` |
| `kl@collation=search` | True | False | position 7: `['A', 'á', 'ā', 'aa', 'ae']` / `['A', 'á', 'å', 'ä', 'ā']` |
| `haw` | True | False | position 12: `['aa', 'ae', 'ai', 'ao', 'au']` / `['aa', 'ae', 'æ', 'ai', 'ao']` |
| `en_US_POSIX` | True | False | position 0: `['æ', ' ', '-']` / `[' ', '_', '-']` |
| `cs@collation=search` | False | False | position 6: `['a', 'A', 'å', 'ä', 'ā']` / `['a', 'A', 'á', 'å', 'ä']` |
| `de@collation=search` | True | False | position 8: `['á', 'å', 'ā', 'aa', 'ae']` / `['á', 'å', 'ä', 'ā', 'aa']` |
| `sk@collation=search` | False | False | position 6: `['a', 'A', 'å', 'ā', 'aa']` / `['a', 'A', 'á', 'å', 'ā']` |

Strings used: ` ` `-` `1` `A` `E` `I` `O` `U` `Y` `Z` `_` `a` `aa` `ae` `ai` `ao` `au` `ay` `az` `b` `c` `ch` `d` `dž` `e` `ea` `ee` `ei` `eo` `eu` `ey` `ez` `f` `g` `h` `i` `ia` `ie` `ii` `ij` …
