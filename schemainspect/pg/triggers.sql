SELECT
    t.trigger_name as name,
    t.trigger_schema as schema,
    t.event_manipulation,
    t.event_object_schema,
    t.event_object_table,
    t.action_condition,
    t.action_statement,
    t.action_orientation,
    t.action_timing
FROM information_schema.triggers t
order by
    t.trigger_name
