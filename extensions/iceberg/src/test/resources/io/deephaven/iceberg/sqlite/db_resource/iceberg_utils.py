from pyiceberg.schema import Schema
from pyiceberg.table.update.schema import UpdateSchema

def _verify(update_schema: UpdateSchema, expected: Schema):
    # This does not modify any internal state, right?
    actual = update_schema._apply()
    if actual != expected:
        raise Exception("UpdateSchema not properly constructed, or schema_to is invalid")

def do_union_update(update_schema: UpdateSchema, schema_to: Schema):
    update_schema.union_by_name(schema_to)
    _verify(update_schema, schema_to)

def do_update(update_schema: UpdateSchema, schema_to: Schema):
    # union_by_name does not handle field_id mapping
    # update_schema.union_by_name(schema_to)
    # # This does not modify any internal state, right?
    # proposed = update_schema._apply()
    # if proposed != schema_to:
    #     print(proposed)
    #     print(schema_to)
    #     raise Exception("PyIceberg can't handle this sort of schema update")

    schema_from = update_schema._schema
    # This would be a good candidate to add to pyiceberg?
    # TODO: this could likely be better written with the visitor pattern
    for from_field in schema_from.fields:
        to_field = schema_to.find_field(from_field.field_id)
        if to_field:
            if to_field == from_field:
                continue

            if to_field.name != from_field.name:
                update_schema.rename_column(from_field.name, to_field.name)

            if (
                to_field.field_type != from_field.field_type
                or to_field.required != from_field.required
                or to_field.doc != from_field.doc
            ):
                update_schema.update_column(
                    path=to_field.name,
                    field_type=to_field.field_type,
                    required=to_field.required,
                    doc=to_field.doc,
                )
        else:
            update_schema.delete_column(from_field.name)
    for to_field in schema_to.fields:
        from_field = schema_to.find_field(to_field.field_id)
        if not from_field:
            update_schema.add_column(
                name=to_field.name,
                field_type=to_field.field_type,
                doc=to_field.doc,
                required=to_field.required,
            )
    # TODO: handle column ordering
    _verify(update_schema, schema_to)
