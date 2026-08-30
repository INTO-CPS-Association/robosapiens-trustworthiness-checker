(function registerDsrvHighlighting() {
    "use strict";

    if (typeof hljs === "undefined") {
        return;
    }

    hljs.registerLanguage("dsrv", function dsrvLanguage(hljsApi) {
        var identifier = "[A-Za-z_][A-Za-z0-9_]*";
        var builtInFunctions = [
            "defer",
            "update",
            "is_defined",
            "when",
            "latch",
            "dynamic",
            "eval",
            "default",
            "init",
            "fix",
            "partial",
            "monitored_at",
            "dist",
            "sin",
            "cos",
            "tan",
            "abs",
        ].join("|");

        return {
            name: "DSRV",
            aliases: ["dsrv"],
            keywords: {
                keyword: "in out var aux if then else",
                literal: "true false",
            },
            contains: [
                hljsApi.COMMENT("//", "$"),
                hljsApi.COMMENT("\\(\\*", "\\*\\)"),
                {
                    className: "string",
                    begin: /"/,
                    end: /"/,
                    contains: [hljsApi.BACKSLASH_ESCAPE],
                },
                {
                    className: "built_in",
                    begin: new RegExp("\\b(?:" + builtInFunctions + ")\\b(?=\\s*\\()"),
                },
                {
                    className: "built_in",
                    begin: /\b(?:List\.(?:get|append|concat|head|tail|len|map|filter|fold)|Map\.(?:get|insert|remove|has_key))\b(?=\s*\()/,
                },
                {
                    className: "built_in",
                    begin: /\b(?:List|Tuple|Map|Struct)\b(?=\s*\()/,
                },
                {
                    className: "type",
                    begin: /\b(?:Int|Float|Bool|Str|Unit|Any|List|Map|Struct)\b/,
                },
                {
                    className: "attr",
                    begin: new RegExp("\\b" + identifier + "\\b(?=\\s*:)")
                },
                {
                    className: "property",
                    begin: new RegExp("\\." + identifier + "\\b"),
                },
                {
                    className: "number",
                    variants: [
                        { begin: /\b(?:0|[1-9][0-9]*)\.[0-9]*(?:[eE]-?[0-9]+)?\b/ },
                        { begin: /\b[0-9]+\b/ },
                    ],
                    relevance: 0,
                },
                {
                    className: "keyword",
                    begin: /(?:\.\.\.|\+\+|&&|\|\||=>|->|\\|==|<=|>=|[!<>+\-*\/%=])/,
                    relevance: 0,
                },
            ],
        };
    });

    // mdBook's book.js runs before additional-js entries and has already
    // skipped language-dsrv as unknown. Revisit those blocks now that the
    // grammar is registered.
    Array.prototype.forEach.call(
        document.querySelectorAll("pre code.language-dsrv"),
        function highlightDsrvBlock(block) {
            hljs.highlightBlock(block);
            block.classList.add("hljs");
        }
    );
}());
