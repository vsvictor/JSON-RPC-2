use super::*;
use serde_json::json;

fn invalid<T>(result: Result<T, RpcError>) {
    let error = result.err().expect("expected invalid params");
    assert_eq!(error, RpcError::invalid_params());
}

#[test]
fn only_objects_are_accepted() {
    for value in [
        Value::Null,
        json!([]),
        json!("secret"),
        json!(1),
        json!(false),
    ] {
        invalid(object(value));
    }
    assert!(object(json!({})).unwrap().is_empty());
}

#[test]
fn required_strings_are_borrowed_and_not_normalized() {
    let map = object(json!({"text":" value ", "blank":" \t", "null":null, "number":3})).unwrap();
    let text = required_str(&map, "text").unwrap();
    assert_eq!(text, " value ");
    assert!(std::ptr::eq(
        text.as_ptr(),
        map["text"].as_str().unwrap().as_ptr()
    ));
    for key in ["missing", "blank", "null", "number"] {
        invalid(required_str(&map, key));
    }
}

#[test]
fn required_values_reject_only_missing_and_null() {
    let map =
        object(json!({"null":null, "false":false, "zero":0, "empty":"", "object":{}})).unwrap();
    for key in ["missing", "null"] {
        invalid(required_value(&map, key));
    }
    for key in ["false", "zero", "empty", "object"] {
        assert_eq!(required_value(&map, key).unwrap(), map[key]);
    }
}

#[test]
fn storage_error_preserves_code_message_and_wire_shape() {
    let error = crate::storage_error("Internal storage error");
    assert_eq!(error.code, crate::STORAGE_ERROR_CODE);
    assert_eq!(
        serde_json::to_value(error).unwrap(),
        json!({
            "code": -32000, "message": "Internal storage error"
        })
    );
}
