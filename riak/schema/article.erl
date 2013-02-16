{
    schema,
    [
        {version, "1"},
        {default_field, "subject"},
        {default_op, "or"},
        {n_val, 3},
        {analyzer_factory, {erlang, text_analyzers, noop_analyzer_factory}}
    ],
    [
        {field, [
            {name, "from_"},
            {analyzer_factory, {erlang, text_analyzers, noop_analyzer_factory}}
        ]},
        {field, [
            {name, "subject"},
            {analyzer_factory, {erlang, text_analyzers, noop_analyzer_factory}}
        ]},
        %% catch all the group<->articlenum key combos
        {dynamic_field, [
            {name, "xref_*_int"},
            {type, integer},
            {padding_size, 10 },
            {analyzer_factory, {erlang, text_analyzers, integer_analyzer_factory}}
        ]},
        %% don't actually index anything else...
        {dynamic_field, [
            {name, "*"},
            {skip, "true"}
        ]}
    ]
}.
