import json

import pandas as pd
import pyarrow as pa
from typing import List, Any, Dict
from wt_sdk.models import (
    LandingRecord,
    ServingRecord,
    LandingRecordBatch,
    ServingRecordBatch,
)


JSON_COLUMN_NAMES = (
    "messages",
    "response",
    "chosen_trace",
    "rejected_trace",
    "meta_json",
)


def _deserialize_json_value(value: Any) -> Any:
    """Deserialize one JSON string without making reads fail on malformed data."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def deserialize_json_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a frame whose present JSON columns contain Python values."""
    result = dataframe.copy()
    for column in JSON_COLUMN_NAMES:
        if column in result.columns:
            values = [_deserialize_json_value(value) for value in result[column].tolist()]
            result[column] = pd.Series(values, index=result.index, dtype=object)
    return result


def _query_value_to_python(value: Any, *, exclude_none: bool) -> Any:
    """Convert Arrow/Pandas nested values to plain Python query output."""
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="python", exclude_none=exclude_none)
    elif hasattr(value, "as_py"):
        value = value.as_py()
    elif not isinstance(value, (str, bytes, bytearray, memoryview)) and hasattr(
        value, "tolist"
    ):
        value = value.tolist()

    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            converted = _query_value_to_python(item, exclude_none=exclude_none)
            if exclude_none and converted is None:
                continue
            result[str(key)] = converted
        return result

    if isinstance(value, (list, tuple)):
        # Preserve null list elements because their positions may carry meaning.
        return [
            _query_value_to_python(item, exclude_none=exclude_none)
            for item in value
        ]

    if value is None:
        return None

    try:
        is_null = pd.isna(value)
        if not hasattr(is_null, "__len__") and bool(is_null):
            return None
    except (TypeError, ValueError):
        pass

    return value


def dataframe_to_dict_records(
    dataframe: pd.DataFrame,
    *,
    exclude_none: bool = True,
    deserialize_json: bool = False,
) -> List[Dict[str, Any]]:
    """Convert query results to dictionaries with optional JSON deserialization."""
    records = []
    for raw_record in dataframe.to_dict(orient="records"):
        record = _query_value_to_python(raw_record, exclude_none=exclude_none)
        if deserialize_json:
            for column in JSON_COLUMN_NAMES:
                if column in record:
                    record[column] = _deserialize_json_value(record[column])
        records.append(record)
    return records


def pydantic_to_dict(model: Any) -> Dict:
    """Convert a Pydantic model to a storage dictionary."""
    if hasattr(model, 'model_dump'):
        # Preserve None so every Arrow schema column receives one value per row.
        return _serialize_with_schema(model)
    elif hasattr(model, 'dict'):
        return _serialize_with_schema(model)
    else:
        return model.__dict__


def _serialize_with_schema(model: Any) -> Dict:
    """Serialize a model recursively while preserving nullable columns."""
    if not hasattr(model, 'model_dump') and not hasattr(model, 'dict'):
        return model

    # Get dict with all fields (including None)
    if hasattr(model, 'model_dump'):
        data = model.model_dump(exclude_none=False, mode='python')
    else:
        data = model.dict(exclude_none=False)

    # Process nested structures recursively
    result = {}
    for key, value in data.items():
        if value is None:
            # Keep None values - they're required for Arrow struct fields
            result[key] = None
        elif isinstance(value, list):
            # Process list items recursively
            processed_list = []
            for item in value:
                if item is None:
                    processed_list.append(None)
                elif hasattr(item, 'model_dump') or hasattr(item, 'dict'):
                    # Nested Pydantic model in list
                    processed_list.append(_serialize_with_schema(item))
                else:
                    processed_list.append(item)
            result[key] = processed_list
        elif isinstance(value, dict):
            # Process dict values recursively
            result[key] = {
                k: _serialize_with_schema(v) if hasattr(v, 'model_dump') or hasattr(v, 'dict') else v
                for k, v in value.items()
            }
        elif hasattr(value, 'model_dump') or hasattr(value, 'dict'):
            # Nested Pydantic model
            result[key] = _serialize_with_schema(value)
        else:
            # Primitive value
            result[key] = value

    return result


def landing_record_to_dataframe(record: LandingRecord) -> pd.DataFrame:
    """Convert a single LandingRecord to a one-row DataFrame."""
    data = pydantic_to_dict(record)
    df = pd.DataFrame([data])

    # JSON extension columns use Python strings at the DataFrame boundary.
    json_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace', 'meta_json']
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def landing_batch_to_dataframe(batch: LandingRecordBatch) -> pd.DataFrame:
    """Convert a batch of LandingRecords to a DataFrame."""
    records_data = [pydantic_to_dict(record) for record in batch.records]
    df = pd.DataFrame(records_data)

    json_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace', 'meta_json']
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def serving_record_to_dataframe(record: ServingRecord) -> pd.DataFrame:
    """Convert a single ServingRecord to a one-row DataFrame."""
    data = pydantic_to_dict(record)
    df = pd.DataFrame([data])

    json_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace', 'meta_json']
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def serving_batch_to_dataframe(batch: ServingRecordBatch) -> pd.DataFrame:
    """Convert a batch of ServingRecords to a DataFrame."""
    records_data = [pydantic_to_dict(record) for record in batch.records]
    df = pd.DataFrame(records_data)

    json_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace', 'meta_json']
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def dataframe_to_landing_records(df: pd.DataFrame) -> List[LandingRecord]:
    """
    Convert a DataFrame to a list of LandingRecords.

    Uses model_construct to bypass validation for Arrow-returned data.
    JSON extension columns remain JSON strings and are not parsed or validated.
    """
    records = []
    for _, row in df.iterrows():
        # Convert row to dict, handling NaN/null values
        row_dict = {}
        for col, val in row.items():
            # Check if value is null/NaN
            if val is None or (isinstance(val, float) and pd.isna(val)):
                # None or NaN scalar - skip
                continue
            elif hasattr(val, 'as_py'):
                # PyArrow scalar - convert
                try:
                    converted = val.as_py()
                    if converted is not None:
                        row_dict[col] = converted
                except (ValueError, AttributeError):
                    pass
            elif isinstance(val, (list, tuple)):
                # List/array value - include as-is
                row_dict[col] = val
            elif hasattr(val, 'tolist'):
                # PyArrow/NumPy array - convert to Python list
                converted = val.tolist()
                if converted is not None:
                    row_dict[col] = converted
            else:
                # Regular scalar value
                row_dict[col] = val

        # Use model_construct to bypass validation
        # Arrow data is already validated by schema, so we don't need Pydantic validation
        records.append(LandingRecord.model_construct(**row_dict))
    return records


def dataframe_to_serving_records(df: pd.DataFrame) -> List[ServingRecord]:
    """
    Convert a DataFrame to a list of ServingRecords.

    Uses model_construct to bypass validation for Arrow-returned data.
    JSON extension columns remain JSON strings and are not parsed or validated.
    """
    records = []
    for _, row in df.iterrows():
        # Convert row to dict, handling NaN/null values
        row_dict = {}
        for col, val in row.items():
            # Check if value is null/NaN
            if val is None or (isinstance(val, float) and pd.isna(val)):
                # None or NaN scalar - skip
                continue
            elif hasattr(val, 'as_py'):
                # PyArrow scalar - convert
                try:
                    converted = val.as_py()
                    if converted is not None:
                        row_dict[col] = converted
                except (ValueError, AttributeError):
                    pass
            elif isinstance(val, (list, tuple)):
                # List/array value - include as-is
                row_dict[col] = val
            elif hasattr(val, 'tolist'):
                # PyArrow/NumPy array - convert to Python list
                converted = val.tolist()
                if converted is not None:
                    row_dict[col] = converted
            else:
                # Regular scalar value
                row_dict[col] = val

        # Use model_construct to bypass validation
        # Arrow data is already validated by schema, so we don't need Pydantic validation
        records.append(ServingRecord.model_construct(**row_dict))
    return records


def dict_to_pyarrow_schema(
    data: Dict,
    schema: pa.Schema,
    *,
    columnar: bool = False,
) -> pa.Table:
    """
    Convert dictionary data to PyArrow Table with explicit schema.

    This ensures that nested structs match the Arrow schema exactly.
    """
    num_rows = 1
    if columnar and data:
        num_rows = len(next(iter(data.values())))

    # Build arrays for each field in the schema
    arrays = []
    for field in schema:
        field_name = field.name
        if field_name in data:
            value = data[field_name]
            # Special handling for list-type fields
            if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
                row_values = value if columnar else [value]
                arrays.append(_convert_list_column(row_values, field.type))
            else:
                arrays.append(_convert_value_to_array(value, field.type))
        else:
            # Missing optional list fields remain null rather than becoming [].
            if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
                arrays.append(_convert_list_column([None for _ in range(num_rows)], field.type))
            else:
                arrays.append(pa.nulls(num_rows, field.type))

    return pa.Table.from_arrays(arrays, schema=schema)


def _convert_list_column(values: List[Any], list_type: pa.ListType) -> pa.Array:
    """
    Convert a list of lists to a PyArrow ListArray for a column.

    Each element in 'values' is a list for one row.
    """
    element_type = list_type.value_type
    offsets = [0]
    all_elements = []

    for value_list in values:
        if value_list is None:
            offsets.append(offsets[-1])
        elif isinstance(value_list, list):
            # Extend all_elements with this row's elements
            all_elements.extend(value_list)
            offsets.append(len(all_elements))
        else:
            # Single value, not a list - wrap it
            all_elements.append(value_list)
            offsets.append(len(all_elements))

    # Convert the flattened elements to an array
    if pa.types.is_struct(element_type):
        # Convert elements to struct array
        if all_elements:
            element_array = _convert_struct_to_array(all_elements, element_type)
        else:
            element_array = pa.nulls(0, element_type)
    else:
        # Convert to primitive array
        element_array = pa.array(all_elements, type=element_type)

    null_mask = pa.array([value_list is None for value_list in values], type=pa.bool_())
    return pa.ListArray.from_arrays(
        offsets=pa.array(offsets),
        values=element_array,
        type=list_type,
        mask=null_mask,
    )


def _convert_value_to_array(value: Any, arrow_type: pa.DataType) -> pa.Array:
    """
    Convert a Python value to a PyArrow Array with the specified type.
    """
    import numpy as np

    # Handle list (multiple rows)
    if isinstance(value, list):
        return _convert_list_to_array(value, arrow_type)

    # Handle single value
    if value is None:
        return pa.nulls(1, arrow_type)

    # Convert based on Arrow type
    if pa.types.is_struct(arrow_type):
        return _convert_struct_to_array([value], arrow_type)
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return _convert_list_to_array([value], arrow_type)
    else:
        # Primitive type
        return pa.array([value], type=arrow_type)


def _convert_list_to_array(values: List[Any], arrow_type: pa.DataType) -> pa.Array:
    """
    Convert a list of values to a PyArrow Array.
    """
    if pa.types.is_struct(arrow_type):
        return _convert_struct_to_array(values, arrow_type)
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        # List of lists
        return _convert_nested_list_to_array(values, arrow_type)
    else:
        # Primitive array
        return pa.array(values, type=arrow_type)


def _convert_struct_to_array(values: List[Dict], struct_type: pa.StructType) -> pa.Array:
    """
    Convert a list of dicts to a PyArrow StructArray.
    """
    # Build arrays for each field in the struct
    field_arrays = []
    for field in struct_type:
        field_values = []
        for value in values:
            if value is None:
                field_values.append(None)
            elif not isinstance(value, dict):
                # Value is not a dict (shouldn't happen, but handle gracefully)
                field_values.append(None)
            elif field.name in value:
                field_values.append(value[field.name])
            else:
                field_values.append(None)

        # Recursively convert nested types
        if pa.types.is_struct(field.type):
            field_arrays.append(_convert_struct_to_array(field_values, field.type))
        elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            field_arrays.append(_convert_nested_list_to_array(field_values, field.type))
        else:
            field_arrays.append(pa.array(field_values, type=field.type))

    # Preserve null struct parents instead of materializing valid structs whose
    # children are all null. Lance 3 requires this distinction for nested fields.
    null_mask = pa.array([value is None for value in values], type=pa.bool_())
    return pa.StructArray.from_arrays(
        field_arrays,
        fields=struct_type.fields,
        mask=null_mask,
    )


def _convert_nested_list_to_array(values: List[Any], list_type: pa.ListType) -> pa.Array:
    """
    Convert a list of lists to a PyArrow ListArray.
    """
    # Flatten and convert
    element_type = list_type.value_type

    if pa.types.is_struct(element_type):
        # List of structs
        all_elements = []
        offsets = [0]
        for value_list in values:
            if value_list is None:
                offsets.append(offsets[-1])
            else:
                for item in value_list:
                    all_elements.append(item)
                offsets.append(len(all_elements))

        # Convert elements to struct array
        if all_elements:
            struct_array = _convert_struct_to_array(all_elements, element_type)
        else:
            struct_array = pa.nulls(0, element_type)

        null_mask = pa.array(
            [value_list is None for value_list in values],
            type=pa.bool_(),
        )
        return pa.ListArray.from_arrays(
            offsets=pa.array(offsets),
            values=struct_array,
            type=list_type,
            mask=null_mask,
        )
    else:
        # List of primitives
        return pa.array(values, type=list_type)


def landing_record_to_arrow(record: LandingRecord, schema: pa.Schema) -> pa.Table:
    """
    Convert a single LandingRecord to a PyArrow Table with explicit schema.
    """
    data = pydantic_to_dict(record)
    return dict_to_pyarrow_schema(data, schema)


def landing_batch_to_arrow(batch: LandingRecordBatch, schema: pa.Schema) -> pa.Table:
    """
    Convert a batch of LandingRecords to a PyArrow Table with explicit schema.
    """
    # Stack all records into columnar format
    columns = {}
    for record in batch.records:
        data = pydantic_to_dict(record)
        for key, value in data.items():
            if key not in columns:
                columns[key] = []
            columns[key].append(value)

    return dict_to_pyarrow_schema(columns, schema, columnar=True)

def serving_record_to_arrow(record: ServingRecord, schema: pa.Schema) -> pa.Table:
    """
    Convert a single ServingRecord to a PyArrow Table with explicit schema.
    """
    data = pydantic_to_dict(record)
    return dict_to_pyarrow_schema(data, schema)


def serving_batch_to_arrow(batch: ServingRecordBatch, schema: pa.Schema) -> pa.Table:
    """
    Convert a batch of ServingRecords to a PyArrow Table with explicit schema.
    """
    # Stack all records into columnar format
    columns = {}
    for record in batch.records:
        data = pydantic_to_dict(record)
        for key, value in data.items():
            if key not in columns:
                columns[key] = []
            columns[key].append(value)

    return dict_to_pyarrow_schema(columns, schema, columnar=True)
