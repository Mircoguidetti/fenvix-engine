"""Differential tests for text search (results9): text indexes (weights, compound, wildcard, languages), `$text` with
`$search` / `$language` / `$caseSensitive` / `$diacriticSensitive`, `{$meta: "textScore"}`, stemming and stop words,
and the error cases."""


def register(g):
    add, ins, find, agg, upd, cmd = g["add"], g["ins"], g["find"], g["agg"], g["upd"], g["cmd"]
    G = "text search"

    def t(name, steps, **flags):
        add("text." + name, steps, G, **flags)

    def cix(coll, keys, **opts):
        step = {"op": "create_index", "coll": coll, "keys": keys}
        if opts:
            step["opts"] = opts
        return step

    TX = [["t", "text"]]
    SC = {"s": {"$meta": "textScore"}}

    def q(search, coll="c", proj=True, sort=None, **opts):
        o = {"$search": search}
        o.update(opts)
        kw = {"sort": sort or {"_id": 1}}
        if proj:
            kw["projection"] = SC
        return find(coll, {"$text": o}, **kw)

    def ids(search, **opts):
        return find("c", {"$text": dict({"$search": search}, **opts)}, sort={"_id": 1}, projection={"_id": 1})

    D = [
        {"_id": 1, "t": "The Quick Brown Fox"},
        {"_id": 2, "t": "quick brown dogs"},
        {"_id": 3, "t": "a lazy brown fox jumped over the fence"},
        {"_id": 4, "t": "naïve café résumé"},
        {"_id": 5, "t": "NAIVE CAFE RESUME"},
        {"_id": 6, "t": "brown quick"},
        {"_id": 7, "t": "the brown, quick fox"},
        {"_id": 8, "t": "hello hello hello world"},
        {"_id": 9, "t": ["hello", "world"]},
        {"_id": 10, "t": "Hello"},
        {"_id": 11, "t": ""},
        {"_id": 12},
        {"_id": 13, "t": 42},
        {"_id": 14, "t": None},
        {"_id": 15, "t": "running runs runner ran"},
        {"_id": 16, "t": "Jumping jumps jumped jumper"},
        {"_id": 17, "t": "don't stop believin' O'Brien's"},
        {"_id": 18, "t": "rock&roll e-mail foo_bar user@example.com 3.14 C++"},
    ]

    # ---- scores and matching -----------------------------------------------------------------------------------
    t("single_term_scores", [cix("c", TX), ins("c", D), q("hello")])
    t("two_terms_or", [cix("c", TX), ins("c", D), q("quick fox")])
    t("term_frequency_and_length", [cix("c", TX), ins("c", D), q("hello world")])
    t("stop_words_only", [cix("c", TX), ins("c", D), q("the a of")])
    t("stop_word_ignored_in_query", [cix("c", TX), ins("c", D), q("the quick")])
    t("stemming_plural", [cix("c", TX), ins("c", D), q("dog")])
    t("stemming_ing_ed", [cix("c", TX), ins("c", D), q("jump")])
    t("stemming_run_forms", [cix("c", TX), ins("c", D), q("running")])
    t("stemming_query_inflected", [cix("c", TX), ins("c", D), q("jumper jumping")])
    t("case_insensitive_default", [cix("c", TX), ins("c", D), q("QUICK")])
    t("diacritics_default", [cix("c", TX), ins("c", D), q("naive")])
    t("diacritics_query_accented", [cix("c", TX), ins("c", D), q("café")])
    t("stem_then_strip_diacritics", [cix("c", TX), ins("c", D), q("résumé")])
    t("stem_then_strip_plain", [cix("c", TX), ins("c", D), q("resume")])
    t("apostrophe_english", [cix("c", TX), ins("c", D), q("don't")])
    t("apostrophe_possessive", [cix("c", TX), ins("c", D), q("brien")])
    t("punctuation_delimiters", [cix("c", TX), ins("c", D), q("roll mail foo bar example")])
    t("underscore_is_not_a_delimiter", [cix("c", TX), ins("c", D), q("foo_bar")])
    t("numbers_split_on_dot", [cix("c", TX), ins("c", D), q("14")])
    t("empty_and_missing_and_non_string_fields_never_match", [cix("c", TX), ins("c", D), q("42 null")])
    t("array_elements_scored_separately", [cix("c", TX), ins("c", D), q("world")])
    t("raw_equal_boost", [cix("c", TX), ins("c", [{"_id": 1, "t": "hello"}, {"_id": 2, "t": "Hello"}, {"_id": 3, "t": "hello!"}, {"_id": 4, "t": "HELLO hello"}]), q("hello")])
    t("duplicate_query_terms_count_once", [cix("c", TX), ins("c", D), q("quick quick quick")])
    t("empty_search_matches_nothing", [cix("c", TX), ins("c", D), ids("")])
    t("whitespace_search_matches_nothing", [cix("c", TX), ins("c", D), ids("   ")])
    t("only_negation_matches_nothing", [cix("c", TX), ins("c", D), ids("-fox")])
    t("long_text_repeated_term", [cix("c", TX), ins("c", [{"_id": 1, "t": " ".join(["alpha"] * 30 + ["beta"] * 5 + ["gamma"])}, {"_id": 2, "t": "alpha"}]), q("alpha beta gamma")])

    # ---- phrases and negation ---------------------------------------------------------------------------------------
    t("phrase_basic", [cix("c", TX), ins("c", D), q('"quick brown"')])
    t("phrase_order_matters", [cix("c", TX), ins("c", D), q('"brown quick"')])
    t("phrase_is_raw_substring", [cix("c", TX), ins("c", D), q('"uick brown"')])
    t("phrase_with_punctuation", [cix("c", TX), ins("c", D), q('"brown, quick"')])
    t("phrase_with_stop_word", [cix("c", TX), ins("c", D), q('"the quick"')])
    t("phrase_plus_term", [cix("c", TX), ins("c", D), q('"quick brown" fox')])
    t("two_phrases_and", [cix("c", TX), ins("c", D), q('"quick brown" "brown fox"')])
    t("negated_term", [cix("c", TX), ins("c", D), q("quick -fox")])
    t("negated_stemmed_term", [cix("c", TX), ins("c", D), q("brown -dogs")])
    t("negated_phrase", [cix("c", TX), ins("c", D), q('quick -"brown fox"')])
    t("phrase_and_negated_term", [cix("c", TX), ins("c", D), q('"quick brown" -fox')])
    t("dash_inside_word_is_not_negation", [cix("c", TX), ins("c", D), q("fox-jumped")])
    t("dash_space_is_ignored", [cix("c", TX), ins("c", D), q("quick - fox")])
    t("unmatched_quote_ignored", [cix("c", TX), ins("c", D), q('"quick brown')])
    t("empty_phrase", [cix("c", TX), ins("c", D), ids('""')])

    # ---- case / diacritic sensitivity and language -----------------------------------------------------------------
    t("case_sensitive_term", [cix("c", TX), ins("c", D), q("Quick", **{"$caseSensitive": True})])
    t("case_sensitive_lower", [cix("c", TX), ins("c", D), q("quick", **{"$caseSensitive": True})])
    t("case_sensitive_phrase", [cix("c", TX), ins("c", D), q('"Quick Brown"', **{"$caseSensitive": True})])
    t("case_sensitive_negation", [cix("c", TX), ins("c", D), q("brown -Fox", **{"$caseSensitive": True})])
    t("case_sensitive_stemmed_forms", [cix("c", TX), ins("c", [{"_id": 1, "t": "Jumping dogs"}, {"_id": 2, "t": "jumped Dog"}, {"_id": 3, "t": "JUMPS DOGS"}]), q("Jump dogs", **{"$caseSensitive": True})])
    t("diacritic_sensitive_plain", [cix("c", TX), ins("c", D), q("naive", **{"$diacriticSensitive": True})])
    t("diacritic_sensitive_accented", [cix("c", TX), ins("c", D), q("naïve", **{"$diacriticSensitive": True})])
    t("both_sensitive", [cix("c", TX), ins("c", D), q("NAÏVE", **{"$caseSensitive": True, "$diacriticSensitive": True})])
    t("explicit_false_flags", [cix("c", TX), ins("c", D), q("Quick", **{"$caseSensitive": False, "$diacriticSensitive": False})])
    t("language_param_none", [cix("c", TX), ins("c", D), q("jumped", **{"$language": "none"})])
    t("language_param_english_name", [cix("c", TX), ins("c", D), q("jumped", **{"$language": "english"})])
    t("language_param_code", [cix("c", TX), ins("c", D), q("jumped", **{"$language": "en"})])
    t("language_param_uppercase", [cix("c", TX), ins("c", D), q("jumped", **{"$language": "ENGLISH"})])
    t("language_param_empty_string", [cix("c", TX), ins("c", D), q("jumped", **{"$language": ""})])
    t("language_param_spanish_stop_words", [cix("c", TX), ins("c", D), q("the fox", **{"$language": "spanish"})])
    t("default_language_french", [cix("c", TX, default_language="french"), ins("c", [{"_id": 1, "t": "les chevaux mangent des pommes"}, {"_id": 2, "t": "le cheval mange une pomme"}, {"_id": 3, "t": "la maison est belle"}]), q("cheval pommes"), q("les des")])
    t("default_language_spanish", [cix("c", TX, default_language="es"), ins("c", [{"_id": 1, "t": "los gatos corren rápidamente"}, {"_id": 2, "t": "el gato corre"}, {"_id": 3, "t": "las casas azules"}]), q("gato correr"), q("las los")])
    t("default_language_german", [cix("c", TX, default_language="german"), ins("c", [{"_id": 1, "t": "die Häuser sind groß"}, {"_id": 2, "t": "das Haus ist klein"}, {"_id": 3, "t": "Kinder spielen draußen"}]), q("Haus"), q("Kind")])
    t("default_language_russian", [cix("c", TX, default_language="russian"), ins("c", [{"_id": 1, "t": "красивые дома стоят"}, {"_id": 2, "t": "красивый дом"}, {"_id": 3, "t": "и в на"}]), q("дом"), q("красивая")])
    t("default_language_none_no_stemming", [cix("c", TX, default_language="none"), ins("c", D), q("jumped"), q("jump"), q("the")])
    t("language_override_field", [cix("c", TX), ins("c", [{"_id": 1, "t": "les chevaux", "language": "french"}, {"_id": 2, "t": "los gatos", "language": "spanish"}, {"_id": 3, "t": "the horses"}, {"_id": 4, "t": "cheval", "language": "none"}]), q("cheval"), q("chevaux"), q("gato")])
    t("language_override_custom_name", [cix("c", TX, language_override="idioma"), ins("c", [{"_id": 1, "t": "les chevaux", "idioma": "french"}, {"_id": 2, "t": "les chevaux", "language": "french"}]), q("cheval")])
    t("language_override_unsupported_error", [cix("c", TX), ins("c", [{"_id": 1, "t": "x", "language": "klingon"}]), ids("x")])
    t("language_override_non_string_ignored", [cix("c", TX), ins("c", [{"_id": 1, "t": "hello", "language": 5}]), ids("hello")])
    t("multiple_languages_in_collection", [cix("c", TX), ins("c", [{"_id": 1, "t": "running horses", "language": "english"}, {"_id": 2, "t": "chevaux courants", "language": "fr"}, {"_id": 3, "t": "caballos corriendo", "language": "es"}, {"_id": 4, "t": "Pferde laufen", "language": "de"}]), q("running"), q("cheval"), q("caballo"), q("Pferd")])
    t("cjk_text_is_one_token_run", [cix("c", TX), ins("c", [{"_id": 1, "t": "東京 大阪"}, {"_id": 2, "t": "東京大阪"}, {"_id": 3, "t": "中文 搜索"}]), q("東京"), q("搜索")])
    t("emoji_and_symbols", [cix("c", TX), ins("c", [{"_id": 1, "t": "party 🎉 time ™ © 2024"}]), q("party time"), q("2024")])

    # ---- index definitions -----------------------------------------------------------------------------------------
    full = {"op": "list_indexes", "coll": "c", "full": True}
    t("spec_single_field", [cix("c", TX), full])
    t("spec_two_fields", [cix("c", [["a", "text"], ["b", "text"]]), full])
    t("spec_weights", [cix("c", [["a", "text"], ["b", "text"]], weights={"a": 3, "b": 7}), full])
    t("spec_wildcard", [cix("c", [["$**", "text"]]), full])
    t("spec_wildcard_with_weights", [cix("c", [["$**", "text"]], weights={"b": 7, "a": 3}), full])
    t("spec_prefix", [cix("c", [["p", 1], ["a", "text"]]), full])
    t("spec_suffix", [cix("c", [["a", "text"], ["s", -1]]), full])
    t("spec_prefix_and_suffix", [cix("c", [["p", 1], ["q", -1], ["a", "text"], ["s", 1]]), full])
    t("spec_language_options", [cix("c", TX, default_language="spanish", language_override="idioma"), full])
    t("spec_custom_name", [cix("c", TX, name="mytext"), full])
    t("spec_nested_path", [cix("c", [["a.b", "text"]]), full])
    t("spec_extra_weight_field_sorted", [cix("c", TX, weights={"nope": 5}), full])
    t("spec_weight_fraction_truncated", [cix("c", TX, weights={"t": 2.9}), full])
    t("spec_text_index_version_2", [cix("c", TX, textIndexVersion=2), full])
    t("spec_with_sparse_option", [cix("c", TX, sparse=True), full])
    t("two_text_indexes_error", [cix("c", TX), cix("c", [["b", "text"]]), full])
    t("same_text_index_twice_ok", [cix("c", TX), cix("c", TX), full])
    t("same_text_index_different_options_error", [cix("c", TX), cix("c", TX, default_language="french"), full])
    t("bad_default_language_error", [cix("c", TX, default_language="klingon")])
    t("bad_language_override_error", [cix("c", TX, language_override="$bad")])
    t("bad_weight_zero_error", [cix("c", TX, weights={"t": 0})])
    t("bad_weight_negative_error", [cix("c", TX, weights={"t": -1})])
    t("bad_weight_string_error", [cix("c", TX, weights={"t": "x"})])
    t("bad_text_index_version_error", [cix("c", TX, textIndexVersion=4)])
    t("mixed_index_plugins_error", [cix("c", [["a", "text"], ["b", "hashed"]])])
    t("text_on_two_plugins_2dsphere_error", [cix("c", [["a", "2dsphere"], ["b", "text"]])])
    t("drop_text_index_then_query_error", [cix("c", TX), ins("c", D), {"op": "drop_index", "coll": "c", "index": "t_text"}, ids("hello")])
    t("no_text_index_error", [ins("c", D), ids("hello")])

    # ---- weights, several fields ----------------------------------------------------------------------------------------
    TW = [{"_id": 1, "a": "apple", "b": "banana"}, {"_id": 2, "a": "banana", "b": "apple"}, {"_id": 3, "a": "apple apple", "b": "cherry"}, {"_id": 4, "b": "banana banana banana"}, {"_id": 5, "a": "banana", "x": "apple"}]
    t("two_fields_equal_weights", [cix("c", [["a", "text"], ["b", "text"]]), ins("c", TW), q("apple"), q("banana")])
    t("weights_change_ranking", [cix("c", [["a", "text"], ["b", "text"]], weights={"a": 10, "b": 1}), ins("c", TW), q("apple", sort={"s": {"$meta": "textScore"}, "_id": 1}), q("banana")])
    t("weights_on_wildcard_index", [cix("c", [["$**", "text"]], weights={"a": 5}), ins("c", TW), q("apple"), q("banana")])
    t("wildcard_indexes_every_string_field", [cix("c", [["$**", "text"]]), ins("c", TW + [{"_id": 6, "n": {"deep": {"er": "apple"}}}, {"_id": 7, "arr": [{"k": "apple"}, "banana"]}]), q("apple"), q("cherry")])
    t("nested_path_and_array_of_documents", [cix("c", [["a.b", "text"]]), ins("c", [{"_id": 1, "a": {"b": "red apple"}}, {"_id": 2, "a": [{"b": "green apple"}, {"b": "pear"}]}, {"_id": 3, "a": "apple"}]), q("apple"), q("pear")])
    t("text_in_other_field_not_searched", [cix("c", TX), ins("c", [{"_id": 1, "t": "x", "u": "apple"}]), ids("apple")])
    t("phrase_across_fields_not_joined", [cix("c", [["a", "text"], ["b", "text"]]), ins("c", [{"_id": 1, "a": "red", "b": "apple"}, {"_id": 2, "a": "red apple"}]), q('"red apple"')])

    # ---- compound text indexes -------------------------------------------------------------------------------------------
    CD = [{"_id": 1, "r": "a", "t": "hello world"}, {"_id": 2, "r": "a", "t": "hello there"}, {"_id": 3, "r": "b", "t": "hello world"}, {"_id": 4, "r": "b", "t": "other"}, {"_id": 5, "r": "c", "t": "hello"}]
    CP = [["r", 1], ["t", "text"]]
    t("prefix_equality", [cix("c", CP), ins("c", CD), find("c", {"r": "a", "$text": {"$search": "hello"}}, sort={"_id": 1}, projection=SC)])
    t("prefix_in_list", [cix("c", CP), ins("c", CD), find("c", {"r": {"$in": ["a", "b"]}, "$text": {"$search": "world"}}, sort={"_id": 1}, projection=SC)])
    t("prefix_missing_error", [cix("c", CP), ins("c", CD), q("hello")])
    t("prefix_range_is_not_equality_error", [cix("c", CP), ins("c", CD), find("c", {"r": {"$gt": "a"}, "$text": {"$search": "hello"}}, sort={"_id": 1})])
    t("prefix_with_extra_filter", [cix("c", CP), ins("c", CD), find("c", {"r": "a", "$text": {"$search": "hello"}, "_id": {"$gt": 1}}, sort={"_id": 1}, projection=SC)])
    t("prefix_and_and", [cix("c", CP), ins("c", CD), find("c", {"$and": [{"r": "b"}, {"$text": {"$search": "hello"}}]}, sort={"_id": 1}, projection=SC)])
    t("suffix_index_still_works", [cix("c", [["t", "text"], ["s", 1]]), ins("c", [{"_id": 1, "t": "hello", "s": 1}, {"_id": 2, "t": "hello", "s": 2}]), q("hello")])
    t("prefix_numeric_values", [cix("c", [["n", -1], ["t", "text"]]), ins("c", [{"_id": 1, "n": 5, "t": "hello"}, {"_id": 2, "n": 6, "t": "hello"}]), find("c", {"n": 5, "$text": {"$search": "hello"}}, projection=SC)])
    t("prefix_array_values", [cix("c", CP), ins("c", [{"_id": 1, "r": ["a", "b"], "t": "hello"}, {"_id": 2, "r": "b", "t": "hello"}]), find("c", {"r": "a", "$text": {"$search": "hello"}}, sort={"_id": 1}, projection=SC)])

    # ---- sorting, projection, limits, other commands --------------------------------------------------------------------
    t("sort_by_score", [cix("c", TX), ins("c", D), q("hello world", sort={"s": {"$meta": "textScore"}, "_id": 1})])
    t("sort_by_score_limit", [cix("c", TX), ins("c", D), q("quick brown fox", sort={"s": {"$meta": "textScore"}, "_id": 1}, limit=3)])
    t("sort_by_score_skip", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "quick brown"}}, sort={"x": {"$meta": "textScore"}, "_id": 1}, skip=2, projection=SC)])
    t("meta_projection_with_inclusion", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "quick"}}, sort={"_id": 1}, projection={"t": 1, "score": {"$meta": "textScore"}})])
    t("meta_projection_with_exclusion", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "quick"}}, sort={"_id": 1}, projection={"t": 0, "score": {"$meta": "textScore"}})])
    t("meta_projection_id_off", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "quick"}}, sort={"_id": 1}, projection={"_id": 0, "score": {"$meta": "textScore"}})])
    t("no_meta_no_score_field", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "quick"}}, sort={"_id": 1})])
    t("meta_without_text_error", [cix("c", TX), ins("c", D), find("c", {"_id": 1}, projection=SC)])
    t("meta_sort_without_text_error", [cix("c", TX), ins("c", D), find("c", {"_id": 1}, sort={"s": {"$meta": "textScore"}})])
    t("meta_unknown_projection_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "quick"}}, projection={"s": {"$meta": "bogus"}})])
    t("find_one_with_text", [cix("c", TX), ins("c", D), {"op": "find_one", "coll": "c", "filter": {"$text": {"$search": "fox"}}, "sort": {"_id": 1}, "projection": {"_id": 1}}])
    t("count_with_text", [cix("c", TX), ins("c", D), {"op": "count_documents", "coll": "c", "filter": {"$text": {"$search": "quick"}}}])
    t("count_with_text_and_filter", [cix("c", TX), ins("c", D), {"op": "count_documents", "coll": "c", "filter": {"$text": {"$search": "quick"}, "_id": {"$gt": 2}}}])
    t("distinct_with_text", [cix("c", TX), ins("c", D), {"op": "distinct", "coll": "c", "key": "_id", "filter": {"$text": {"$search": "hello"}}}])
    t("text_and_other_conditions", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "quick"}, "t": {"$regex": "^the", "$options": "i"}}, sort={"_id": 1}, projection={"_id": 1})])
    t("text_with_and_operator", [cix("c", TX), ins("c", D), find("c", {"$and": [{"$text": {"$search": "brown"}}, {"_id": {"$lt": 4}}]}, sort={"_id": 1}, projection=SC)])
    t("text_with_hint", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "brown"}}, sort={"_id": 1}, projection={"_id": 1}, hint="t_text")])
    t("update_many_with_text_filter", [cix("c", TX), ins("c", D), upd("c", {"$text": {"$search": "hello"}}, {"$set": {"seen": True}}, kind="update_many"), find("c", {"seen": True}, sort={"_id": 1}, projection={"_id": 1})])
    t("delete_many_with_text_filter", [cix("c", TX), ins("c", D), {"op": "delete_many", "coll": "c", "filter": {"$text": {"$search": "fox"}}}, find("c", {}, sort={"_id": 1}, projection={"_id": 1})])
    t("find_one_and_update_with_text", [cix("c", TX), ins("c", D), {"op": "find_one_and_update", "coll": "c", "filter": {"$text": {"$search": "fox"}}, "sort": {"_id": 1}, "update": {"$set": {"seen": 1}}, "return_document": "after", "projection": {"seen": 1}}])

    # ---- index maintenance ---------------------------------------------------------------------------------------------------
    t("insert_after_index_creation", [cix("c", TX), ins("c", [{"_id": 1, "t": "alpha"}]), ins("c", [{"_id": 2, "t": "alpha beta"}]), q("alpha"), q("beta")])
    t("index_built_on_existing_documents", [ins("c", D), cix("c", TX), q("quick fox")])
    t("update_changes_terms", [cix("c", TX), ins("c", [{"_id": 1, "t": "alpha"}]), upd("c", {"_id": 1}, {"$set": {"t": "beta"}}), q("alpha"), q("beta")])
    t("update_unset_removes_terms", [cix("c", TX), ins("c", [{"_id": 1, "t": "alpha"}]), upd("c", {"_id": 1}, {"$unset": {"t": ""}}), q("alpha")])
    t("replace_changes_terms", [cix("c", TX), ins("c", [{"_id": 1, "t": "alpha"}]), {"op": "replace_one", "coll": "c", "filter": {"_id": 1}, "update": {"t": "gamma delta"}}, q("alpha"), q("delta")])
    t("delete_removes_terms", [cix("c", TX), ins("c", [{"_id": 1, "t": "alpha"}, {"_id": 2, "t": "alpha"}]), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}}, q("alpha")])
    t("upsert_into_text_index", [cix("c", TX), upd("c", {"_id": 9}, {"$set": {"t": "omega"}}, upsert=True), q("omega")])
    t("language_override_update", [cix("c", TX), ins("c", [{"_id": 1, "t": "chevaux"}]), upd("c", {"_id": 1}, {"$set": {"language": "french"}}), q("cheval"), q("chevaux")])
    t("language_override_bad_on_update_error", [cix("c", TX), ins("c", [{"_id": 1, "t": "x"}]), upd("c", {"_id": 1}, {"$set": {"language": "klingon"}}), q("x")])
    t("validate_text_index", [cix("c", TX), ins("c", D), cmd({"validate": "c"}, ["ok", "valid"])])
    t("coll_stats_counts_text_index", [cix("c", TX), ins("c", D), cmd({"collStats": "c"}, ["ok", "nindexes"])])
    t("many_documents", [cix("c", TX), ins("c", [{"_id": i, "t": f"word{i % 17} common {'rare' if i % 50 == 0 else 'plain'}"} for i in range(1, 401)]), {"op": "count_documents", "coll": "c", "filter": {"$text": {"$search": "rare"}}}, {"op": "count_documents", "coll": "c", "filter": {"$text": {"$search": "word3 word5"}}}, q("rare word1", limit=5, sort={"s": {"$meta": "textScore"}, "_id": 1})])

    # ---- aggregation -------------------------------------------------------------------------------------------------------------
    M = {"$match": {"$text": {"$search": "quick brown"}}}
    t("agg_match_text_first", [cix("c", TX), ins("c", D), agg("c", [M, {"$sort": {"_id": 1}}, {"$project": {"_id": 1}}])])
    t("agg_project_meta", [cix("c", TX), ins("c", D), agg("c", [M, {"$project": {"s": {"$meta": "textScore"}}}, {"$sort": {"_id": 1}}])])
    t("agg_addfields_meta", [cix("c", TX), ins("c", D), agg("c", [M, {"$addFields": {"s": {"$meta": "textScore"}}}, {"$sort": {"_id": 1}}, {"$project": {"s": 1}}])])
    t("agg_sort_meta", [cix("c", TX), ins("c", D), agg("c", [M, {"$sort": {"s": {"$meta": "textScore"}, "_id": 1}}])])
    t("agg_meta_survives_project_and_match", [cix("c", TX), ins("c", D), agg("c", [M, {"$project": {"t": 1}}, {"$match": {"_id": {"$lt": 8}}}, {"$addFields": {"s": {"$meta": "textScore"}}}, {"$sort": {"_id": 1}}])])
    t("agg_group_sum_of_scores", [cix("c", TX), ins("c", D), agg("c", [M, {"$addFields": {"s": {"$meta": "textScore"}}}, {"$group": {"_id": None, "total": {"$sum": "$s"}, "n": {"$sum": 1}}}])])
    t("agg_text_not_first_stage_error", [cix("c", TX), ins("c", D), agg("c", [{"$match": {"_id": {"$lt": 8}}}, M])])
    t("agg_meta_without_text_error", [cix("c", TX), ins("c", D), agg("c", [{"$project": {"s": {"$meta": "textScore"}}}])])
    t("agg_text_no_index_error", [ins("c", D), agg("c", [M])])
    t("agg_text_with_limit", [cix("c", TX), ins("c", D), agg("c", [M, {"$sort": {"s": {"$meta": "textScore"}, "_id": 1}}, {"$limit": 2}, {"$project": {"_id": 1}}])])
    t("agg_text_and_lookup", [cix("c", TX), ins("c", D), ins("o", [{"_id": 1, "k": 1}, {"_id": 2, "k": 2}]), agg("o", [{"$lookup": {"from": "c", "localField": "k", "foreignField": "_id", "as": "j"}}, {"$project": {"j._id": 1}}, {"$sort": {"_id": 1}}])])
    t("agg_count_after_text", [cix("c", TX), ins("c", D), agg("c", [M, {"$count": "n"}])])

    # ---- errors in the query -----------------------------------------------------------------------------------------------------------
    t("search_not_a_string_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": 5}})])
    t("search_missing_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$language": "english"}})])
    t("text_not_an_object_error", [cix("c", TX), ins("c", D), find("c", {"$text": "hello"})])
    t("language_not_a_string_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "a", "$language": 5}})])
    t("unsupported_language_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "a", "$language": "klingon"}})])
    t("case_sensitive_not_bool_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "a", "$caseSensitive": "yes"}})])
    t("diacritic_sensitive_not_bool_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "a", "$diacriticSensitive": 1}})])
    t("extra_field_in_text_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "a", "$bogus": 1}})])
    t("text_inside_or_with_indexed_clause", [cix("c", TX), ins("c", D), find("c", {"$or": [{"$text": {"$search": "hello"}}, {"_id": 1}]}, sort={"_id": 1}, projection={"_id": 1})])
    t("text_inside_nor_error", [cix("c", TX), ins("c", D), find("c", {"$nor": [{"$text": {"$search": "hello"}}]})])
    t("two_text_conditions_error", [cix("c", TX), ins("c", D), find("c", {"$and": [{"$text": {"$search": "a"}}, {"$text": {"$search": "b"}}]})])
    t("text_with_near_error", [cix("c", TX), ins("c", D), find("c", {"$text": {"$search": "a"}, "loc": {"$near": [0, 0]}})])
    t("text_in_field_condition_error", [cix("c", TX), ins("c", D), find("c", {"t": {"$text": {"$search": "a"}}})])
