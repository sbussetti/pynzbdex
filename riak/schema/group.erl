{
    schema,
    [
        {version, "1"},
        {default_field, "key"},
        {default_op, "or"},
        {n_val, 3},
        {analyzer_factory, {erlang, text_analyzers, noop_analyzer_factory}}
    ],
    [
        %% just use default noop on key, rest skip
        {field, [
            {name, "name"}
        ]},
        {dynamic_field, [
            {name, "*"},
            {skip, "true"}
        ]}
    ]
}.
