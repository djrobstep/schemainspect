select
        sequence_schema as schema,
        sequence_name as name,
        sequence_schema || '.' || sequence_name as fullname
    from information_schema.sequences;
