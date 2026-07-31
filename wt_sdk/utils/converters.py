import pandas as pd
import pyarrow as pa
from typing import List, Any, Dict
from wt_sdk.models import (
    LandingRecord,
    ServingRecord,
    LandingRecordBatch,
    ServingRecordBatch,
)


def pydantic_to_dict(model: Any) -> Dict:
    """
    Convert Pydantic model to dictionary, handling special types.

    NOTE: For nested structures (ChatMessage, ContentItem, etc.), we keep all fields
    even if None to match LanceDB Arrow schema requirements. Arrow structs expect
    all fields to be present, even when null.
    """
    if hasattr(model, 'model_dump'):
        # Use exclude_none=False to preserve all struct fields for Arrow compatibility
        return _serialize_with_schema(model)
    elif hasattr(model, 'dict'):
        return _serialize_with_schema(model)
    else:
        return model.__dict__


def _serialize_with_schema(model: Any) -> Dict:
    """
    Serialize Pydantic model ensuring all struct fields are present for Arrow schema.

    This recursively processes nested models to ensure:
    1. All struct fields are present (even if None) for Arrow compatibility
    2. Proper handling of lists and nested objects
    """
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
    """
    Convert a single LandingRecord to a one-row DataFrame.

    NOTE: Uses object dtype for struct fields to ensure LanceDB can properly
    infer the nested Arrow schema from the dictionaries.
    """
    data = pydantic_to_dict(record)
    df = pd.DataFrame([data])

    # Ensure struct columns are object type for Arrow compatibility
    struct_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace']
    for col in struct_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def landing_batch_to_dataframe(batch: LandingRecordBatch) -> pd.DataFrame:
    """
    Convert a batch of LandingRecords to a DataFrame.

    NOTE: Uses object dtype for struct fields to ensure LanceDB can properly
    infer the nested Arrow schema from the dictionaries.
    """
    records_data = [pydantic_to_dict(record) for record in batch.records]
    df = pd.DataFrame(records_data)

    # Ensure struct columns are object type for Arrow compatibility
    struct_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace']
    for col in struct_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def serving_record_to_dataframe(record: ServingRecord) -> pd.DataFrame:
    """
    Convert a single ServingRecord to a one-row DataFrame.

    NOTE: Uses object dtype for struct fields to ensure LanceDB can properly
    infer the nested Arrow schema from the dictionaries.
    """
    data = pydantic_to_dict(record)
    df = pd.DataFrame([data])

    # Ensure struct columns are object type for Arrow compatibility
    struct_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace']
    for col in struct_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def serving_batch_to_dataframe(batch: ServingRecordBatch) -> pd.DataFrame:
    """
    Convert a batch of ServingRecords to a DataFrame.

    NOTE: Uses object dtype for struct fields to ensure LanceDB can properly
    infer the nested Arrow schema from the dictionaries.
    """
    records_data = [pydantic_to_dict(record) for record in batch.records]
    df = pd.DataFrame(records_data)

    # Ensure struct columns are object type for Arrow compatibility
    struct_columns = ['messages', 'response', 'chosen_trace', 'rejected_trace']
    for col in struct_columns:
        if col in df.columns:
            df[col] = df[col].astype(object)

    return df


def dataframe_to_landing_records(df: pd.DataFrame) -> List[LandingRecord]:
    """
    Convert a DataFrame to a list of LandingRecords.

    Uses model_construct to bypass validation for Arrow-returned data.
    Converts PyArrow arrays to Python lists and recursively converts nested dicts to Pydantic models.
    """
    from wt_sdk.models.common import ChatMessage, ContentItem, ImageUrl, InputAudio, ToolCall, Function
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

        # Recursively convert nested dicts to Pydantic models
        row_dict = _convert_dict_to_pydantic(row_dict)

        # Use model_construct to bypass validation
        # Arrow data is already validated by schema, so we don't need Pydantic validation
        records.append(LandingRecord.model_construct(**row_dict))
    return records


def dataframe_to_serving_records(df: pd.DataFrame) -> List[ServingRecord]:
    """
    Convert a DataFrame to a list of ServingRecords.

    Uses model_construct to bypass validation for Arrow-returned data.
    Converts PyArrow arrays to Python lists and recursively converts nested dicts to Pydantic models.
    """
    from wt_sdk.models.common import ChatMessage, ContentItem, ImageUrl, InputAudio, ToolCall, Function
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

        # Recursively convert nested dicts to Pydantic models
        row_dict = _convert_dict_to_pydantic(row_dict)

        # Use model_construct to bypass validation
        # Arrow data is already validated by schema, so we don't need Pydantic validation
        records.append(ServingRecord.model_construct(**row_dict))
    return records


def _convert_dict_to_pydantic(data: Any) -> Any:
    """
    Recursively convert dictionaries to Pydantic models based on field types.

    NOTE: Also handles numpy arrays which may be returned from Arrow/Pandas conversions.
    """
    import numpy as np

    if isinstance(data, dict):
        # Try to identify what Pydantic model this should be
        # For LandingRecord/ServingRecord messages field
        if 'role' in data and 'content' in data:
            # This is a ChatMessage
            from wt_sdk.models.common import ChatMessage, ContentItem, ImageUrl, InputAudio, ToolCall, Function

            # Recursively convert content list (may be numpy array or list)
            if 'content' in data and data['content'] is not None:
                if isinstance(data['content'], np.ndarray):
                    # Convert numpy array to list and process each item
                    data['content'] = [_convert_dict_to_pydantic(item) for item in data['content']]
                elif isinstance(data['content'], list):
                    data['content'] = [_convert_dict_to_pydantic(item) for item in data['content']]

            # Recursively convert tool_calls (may be numpy array or list)
            if 'tool_calls' in data and data['tool_calls'] is not None:
                if isinstance(data['tool_calls'], np.ndarray):
                    # Handle numpy array - filter out empty items
                    items = [_convert_dict_to_pydantic(item) for item in data['tool_calls']]
                    data['tool_calls'] = [i for i in items if i is not None]
                elif isinstance(data['tool_calls'], list):
                    data['tool_calls'] = [_convert_dict_to_pydantic(item) for item in data['tool_calls']]

            return ChatMessage.model_construct(**data)

        elif 'type' in data and 'text' in data:
            # This is a ContentItem
            from wt_sdk.models.common import ContentItem, ImageUrl, InputAudio

            # Convert nested ImageUrl if present
            image_url = data.get('image_url')
            if isinstance(image_url, dict) and image_url.get('url'):
                data['image_url'] = ImageUrl.model_construct(**image_url)
            elif image_url is None or isinstance(image_url, dict):
                # Null or empty ImageUrl - set to None
                data['image_url'] = None

            # Convert nested InputAudio if present
            input_audio = data.get('input_audio')
            if isinstance(input_audio, dict) and input_audio.get('url'):
                data['input_audio'] = InputAudio.model_construct(**input_audio)
            elif input_audio is None or isinstance(input_audio, dict):
                # Null or empty InputAudio - set to None
                data['input_audio'] = None

            return ContentItem.model_construct(**data)

        elif 'url' in data and 'detail' in data:
            # This is an ImageUrl
            from wt_sdk.models.common import ImageUrl
            return ImageUrl.model_construct(**data)

        elif 'url' in data and 'format' in data:
            # This is an InputAudio
            from wt_sdk.models.common import InputAudio
            return InputAudio.model_construct(**data)

        elif 'id' in data and 'type' in data and 'function' in data:
            # This is a ToolCall
            from wt_sdk.models.common import ToolCall, Function
            if 'function' in data and isinstance(data['function'], dict):
                data['function'] = Function.model_construct(**data['function'])
            return ToolCall.model_construct(**data)

        elif 'name' in data and 'arguments' in data:
            # This is a Function
            from wt_sdk.models.common import Function
            return Function.model_construct(**data)

        else:
            # Unknown dict type - recursively convert values
            return {k: _convert_dict_to_pydantic(v) for k, v in data.items()}

    elif isinstance(data, list):
        # Recursively convert list items
        return [_convert_dict_to_pydantic(item) for item in data]

    elif isinstance(data, np.ndarray):
        # Handle numpy arrays (from Arrow/Pandas conversion)
        # Convert to list and process each item
        return [_convert_dict_to_pydantic(item) for item in data]

    else:
        # Primitive value - return as-is
        return data


def _clean_array_value(value: Any) -> Any:
    """
    Clean array/list values by removing empty structs.

    For Pydantic models with optional nested structs, Arrow stores the struct
    with all None values, but Pydantic expects the field to be None entirely.
    """
    if not isinstance(value, (list, tuple)):
        return value

    cleaned_list = []
    for item in value:
        if isinstance(item, dict):
            # Check if this is an empty struct (all values are None)
            if all(v is None for v in item.values()):
                # Empty struct - replace with None
                continue
            else:
                # Recursively clean nested dicts
                cleaned_list.append(_clean_dict_value(item))
        elif isinstance(item, (list, tuple)):
            # Nested list - recursively clean
            cleaned_list.append(_clean_array_value(item))
        else:
            # Primitive value
            cleaned_list.append(item)

    return cleaned_list


def _clean_dict_value(d: Dict) -> Any:
    """
    Clean dict values by removing None-only nested dicts.
    """
    if not isinstance(d, dict):
        return d

    cleaned = {}
    for key, value in d.items():
        if isinstance(value, dict):
            # Check if this is an empty struct (all values are None)
            if all(v is None for v in value.values()):
                # Empty struct - skip it (Pydantic will treat as None)
                continue
            else:
                # Recursively clean
                cleaned[key] = _clean_dict_value(value)
        elif isinstance(value, (list, tuple)):
            # Recursively clean list
            cleaned[key] = _clean_array_value(value)
        elif value is not None:
            # Keep non-None values
            cleaned[key] = value

    return cleaned


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


def normalize_content_field(record: Dict) -> Dict:
    """
    Normalize content field to ensure it's always a list of ContentItem.
    This is important for LanceDB schema compatibility.
    """
    from wt_sdk.models.common import ContentItem

    # Handle messages field
    if "messages" in record and record["messages"]:
        normalized_messages = []
        for msg in record["messages"]:
            if isinstance(msg, dict):
                # Ensure content is normalized
                if "content" in msg and isinstance(msg["content"], str):
                    msg["content"] = [ContentItem(type="text", text=msg["content"])]
            normalized_messages.append(msg)
        record["messages"] = normalized_messages

    # Handle response
    for field in ["response"]:
        if field in record and record[field]:
            if isinstance(record[field], dict):
                if "content" in record[field] and isinstance(record[field]["content"], str):
                    record[field]["content"] = [
                        ContentItem(type="text", text=record[field]["content"])
                    ]

    # Handle chosen/rejected trace message lists
    for field in ["chosen_trace", "rejected_trace"]:
        for message in record.get(field) or []:
            if isinstance(message, dict) and isinstance(message.get("content"), str):
                message["content"] = [ContentItem(type="text", text=message["content"])]

    return record
